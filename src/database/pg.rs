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
        let client = Client::connect(&s, NoTls)?;

        Ok(Self {
            identifier: identifier.to_string(),
            client,
            _params: params,
        })
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
        format!(
            "{} {}{} ",
            self.identifier.bright_blue(),
            "(pg)".magenta(),
            ">"
        )
    }
}

fn row_values(row: &Row) -> super::Row {
    super::Row {
        data: (0..row.len())
            .map(|i| {
                let c = row.columns().iter().nth(i);
                if let Some(c) = c {
                    match c.type_() {
                        &Type::TEXT | &Type::VARCHAR => {
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
