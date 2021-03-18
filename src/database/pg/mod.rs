use super::Connection;
use super::ConnectionParams;
use super::Error;
use super::{Column, QueryResult};
use anyhow::anyhow;
use anyhow::Result;
use chrono;
use chrono::offset::FixedOffset;
use colored::Colorize;
use postgres::types::Type;
use postgres::{Client, NoTls, Row};
use prettytable::format;
use prettytable::{color, Attr, Cell, Row as OtherRow, Table};
use regex::Regex;

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
        let rows = self
            .client
            .query(include_str!("table_columns.sql"), &[&obj])?;

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
        vec![s, s2, s3]
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

        match relkind.as_ref() {
            "v" => println!("View"),
            "r" => self.describe_table(&obj)?,
            "i" => println!("Index"),
            "S" => println!("Sequece"),
            "m" => println!("materialized view"),
            _ => println!("Unsupported"),
        }

        Ok(())
    }
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

                        x => Some(format!("?{:?}", x)),
                    }
                } else {
                    Some("???".into())
                }
            })
            .collect(),
    }
}
