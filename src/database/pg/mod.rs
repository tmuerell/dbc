use super::Connection;
use super::ConnectionParams;
use super::Error;
use super::{Column, QueryResult};
use anyhow::anyhow;
use anyhow::Result;
use byteorder::{BigEndian, ReadBytesExt};
use chrono;
use chrono::offset::FixedOffset;
use colored::Colorize;
use postgres::types::FromSql;
use postgres::types::{accepts, Type};
use postgres::{Client, NoTls, Row};
use prettytable::format;
use prettytable::{color, Attr, Cell, Row as OtherRow, Table};
use regex::Regex;
use std::fmt::Display;

pub struct PgConnection {
    identifier: String,
    client: Client,
    _params: ConnectionParams,
}

impl PgConnection {
    pub fn create(identifier: &str, params: ConnectionParams) -> Result<Self> {
        let re = Regex::new(r"//([^/:]+):(\d+)/(\w+)$").unwrap();
        let p = params.clone();
        let u = p.url.expect("PG needs a URL");
        let c = re
            .captures(&u)
            .expect("Format of URL needs to be //host:port/db");
        let s = format!(
            "host={} port={} user={} password={} dbname={}",
            &c[1],
            &c[2],
            p.username.unwrap(),
            p.password.unwrap(),
            &c[3]
        );
        let mut client = Client::connect(&s, NoTls)?;

        let rows = client.query("show server_version;", &[])?;

        if let Some(r) = rows.iter().nth(0) {
            let s: String = r.get(0);
            println!("Postgres: Connected to {}", s.green());
        }

        Ok(Self {
            identifier: identifier.to_string(),
            client,
            _params: params,
        })
    }

    fn describe_table(&mut self, obj: &str) -> Result<()> {
        {
            let rows = self
                .client
                .query(include_str!("table_columns.sql"), &[&obj])?;

            println!("{}", "Columns:".magenta());
            let mut table = Table::new();
            table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
            for row in rows {
                let max_length: Option<i32> = row.get("max_length");
                let default_value: Option<String> = row.get("default_value");
                let max_length_str = match max_length {
                    Some(i) => format!("{}", i),
                    None => "".into(),
                };
                table.add_row(OtherRow::new(vec![
                    Cell::new(row.get("column_name"))
                        .with_style(Attr::Bold)
                        .with_style(Attr::ForegroundColor(color::GREEN)),
                    Cell::new(row.get("data_type")),
                    Cell::new(&max_length_str),
                    Cell::new(row.get("is_nullable")),
                    Cell::new(&default_value.unwrap_or("".into())),
                ]));
            }
            table.printstd();
        }
        {
            println!("{}", "Foreign Keys:".magenta());
            let rows = self
                .client
                .query(include_str!("foreign_keys.sql"), &[&obj])?;
            for row in rows {
                let other_table_schema: &str = row.get("foreign_table_schema");
                let other_table: &str = row.get("foreign_table_name");
                let columns: &str = row.get("foreign_column_name");
                let my_column: String = row.get("column_name");
                println!(
                    "  {} -> {} ({})",
                    my_column.green(),
                    format!("{}.{}", other_table_schema, other_table).blue(),
                    columns.yellow()
                );
            }
        }

        //

        Ok(())
    }

    fn describe_sequence(&mut self, obj: &str) -> Result<()> {
        let rows = self
            .client
            .query(include_str!("sequence_data.sql"), &[&obj])?;
        if let Some(row) = rows.iter().nth(0) {
            let mut table = Table::new();
            table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
            table.add_row(OtherRow::new(vec![
                Cell::new("name")
                    .with_style(Attr::Bold)
                    .with_style(Attr::ForegroundColor(color::GREEN)),
                Cell::new(row.get("sequencename")),
            ]));
            table.add_row(OtherRow::new(vec![
                Cell::new("start_value")
                    .with_style(Attr::Bold)
                    .with_style(Attr::ForegroundColor(color::GREEN)),
                Cell::new(&row.get::<_, i64>("start_value").to_string()),
            ]));
            table.add_row(OtherRow::new(vec![
                Cell::new("min_value")
                    .with_style(Attr::Bold)
                    .with_style(Attr::ForegroundColor(color::GREEN)),
                Cell::new(&row.get::<_, i64>("min_value").to_string()),
            ]));
            table.add_row(OtherRow::new(vec![
                Cell::new("max_value")
                    .with_style(Attr::Bold)
                    .with_style(Attr::ForegroundColor(color::GREEN)),
                Cell::new(&row.get::<_, i64>("max_value").to_string()),
            ]));
            table.add_row(OtherRow::new(vec![
                Cell::new("increment_by")
                    .with_style(Attr::Bold)
                    .with_style(Attr::ForegroundColor(color::GREEN)),
                Cell::new(&row.get::<_, i64>("increment_by").to_string()),
            ]));
            table.printstd();
        }

        Ok(())
    }
}

impl Connection for PgConnection {
    fn execute(&mut self, statement: &str) -> Result<u64> {
        let rows_affected = self.client.execute(statement, &[])?;
        Ok(rows_affected)
    }
    fn query(&mut self, statement: &str) -> Result<QueryResult> {
        let res = self.client.query(statement, &[])?;
        if res.len() == 0 {
            return Err(anyhow!(Error::NoResultError));
        } else {
            let columns: Vec<Column> = res
                .iter()
                .nth(0)
                .unwrap()
                .columns()
                .iter()
                .map(|c| Column {
                    name: c.name().to_string(),
                })
                .collect();
            Ok(QueryResult {
                columns,
                rows: res.iter().map(|r| row_values(r)).collect(),
            })
        }
    }
    fn prompt(&self) -> String {
        format!("{} {}{} ", self.identifier.cyan(), "(pg)".magenta(), ">")
    }
    fn list_tables(&mut self) -> Result<Vec<super::TableRef>> {
        let mut v: Vec<super::TableRef> = vec![];

        let rows = self.client.query(
            "select table_schema, table_name from information_schema.tables",
            &[],
        )?;

        for row in rows {
            let tr = super::TableRef {
                schema: row.get(0),
                name: row.get(1),
            };
            v.push(tr);
        }

        return Ok(v);
    }
    fn standard_queries(&self) -> Vec<super::StandardQuery> {
        let s = super::StandardQuery {
            name: "locks",
            query: include_str!("query_locks.sql"),
        };
        let s2 = super::StandardQuery {
            name: "queries",
            query: include_str!("query_queries.sql"),
        };
        let s3 = super::StandardQuery {
            name: "sizes",
            query: include_str!("query_sizes.sql"),
        };
        let s4 = super::StandardQuery {
            name: "sessions",
            query: include_str!("query_sessions.sql"),
        };
        vec![s, s2, s3, s4]
    }
    fn describe(&mut self, obj: &str) -> Result<()> {
        let obj = obj.to_ascii_lowercase();
        let relkind: String = {
            let row = self.client.query_one(
                "select relkind::text from pg_class where relname = $1",
                &[&obj],
            )?;
            row.get(0)
        };

        println!(
            "{} is a {}",
            obj.yellow(),
            readable_type(&relkind).magenta()
        );

        match relkind.as_ref() {
            "r" => self.describe_table(&obj)?,
            "S" => self.describe_sequence(&obj)?,
            _ => {}
        }

        Ok(())
    }
    fn search(&mut self, obj: &str) -> Result<()> {
        let rows = self.client.query(
            "select relname::text, relkind::text from pg_class where relname LIKE $1",
            &[&obj.to_ascii_lowercase()],
        )?;

        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        table.set_titles(OtherRow::new(vec![
            Cell::new("name")
                .with_style(Attr::Bold)
                .with_style(Attr::ForegroundColor(color::GREEN)),
            Cell::new("type")
                .with_style(Attr::Bold)
                .with_style(Attr::ForegroundColor(color::GREEN)),
        ]));
        for row in rows {
            table.add_row(OtherRow::new(vec![
                Cell::new(row.get(0)),
                Cell::new(readable_type(row.get(1))),
            ]));
        }
        table.printstd();

        Ok(())
    }
}

fn readable_type(t: &str) -> &str {
    match t.as_ref() {
        "v" => "view",
        "r" => "table",
        "i" => "index",
        "S" => "sequence",
        "m" => "materialized view",
        _ => "unkown object",
    }
}

/// This type represents a Postgres Interval type
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Interval {
    pub seconds: i64,
    pub microseconds: i64,
    pub days: i32,
    pub months: i32,
}

impl Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}", "P")?;
        if self.months != 0 {
            write!(f, "{}M", self.months)?;
        }
        if self.days != 0 {
            write!(f, "{}D", self.days)?;
        }
        write!(f, "{}", "T")?;
        let hours = self.seconds / 3600;
        let minutes = (self.seconds % 3600) / 60;
        let seconds = self.seconds % 60;
        if self.seconds != 0 || self.microseconds != 0 {
            write!(
                f,
                "{}H{}M{}.{:06}S",
                hours, minutes, seconds, self.microseconds
            )?;
        }
        Ok(())
    }
}

impl<'a> FromSql<'a> for Interval {
    fn from_sql(
        _: &Type,
        raw: &[u8],
    ) -> Result<Interval, Box<dyn std::error::Error + Sync + Send>> {
        let t = interval_from_sql(raw)?;
        Ok(t)
    }

    accepts!(INTERVAL);
}

#[inline]
fn interval_from_sql(mut buf: &[u8]) -> Result<Interval, Box<dyn std::error::Error + Sync + Send>> {
    let time = buf.read_i64::<BigEndian>()?;
    let seconds = time / 1000000;
    let microseconds = time % 1000000;
    let days = buf.read_i32::<BigEndian>()?;
    let months = buf.read_i32::<BigEndian>()?;

    Ok(Interval {
        microseconds,
        seconds,
        days,
        months,
    })
}

fn row_values(row: &Row) -> super::Row {
    super::Row {
        data: (0..row.len())
            .map(|i| {
                let c = row.columns().iter().nth(i);
                if let Some(c) = c {
                    match c.type_() {
                        &Type::TEXT | &Type::VARCHAR | &Type::NAME => {
                            let s: Option<String> = row.get(i);
                            s
                        }
                        &Type::INT8 => {
                            let x: Option<i64> = row.get(i);
                            x.map(|y| format!("{}", y))
                        }
                        &Type::INT4 => {
                            let x: Option<i32> = row.get(i);
                            x.map(|y| format!("{}", y))
                        }
                        &Type::INT2 => {
                            let x: Option<i16> = row.get(i);
                            x.map(|y| format!("{}", y))
                        }
                        &Type::TIMESTAMP => {
                            let x: Option<chrono::NaiveDateTime> = row.get(i);
                            x.map(|y| format!("{}", y))
                        }
                        &Type::TIMESTAMPTZ => {
                            let x: Option<chrono::DateTime<FixedOffset>> = row.get(i);
                            x.map(|y| format!("{}", y))
                        }
                        &Type::BOOL => {
                            let x: Option<bool> = row.get(i);
                            x.map(|y| format!("{}", if y { "true" } else { "false" }))
                        }
                        &Type::INTERVAL => {
                            let x: Option<Interval> = row.get(i);
                            println!("{:?}", x);
                            x.map(|y| format!("{}", y))
                        }

                        x => Some(format!("?{:?}", x)),
                    }
                } else {
                    Some("???".into())
                }
            })
            .collect(),
    }
}
