use super::Connection;
use super::ConnectionParams;
use super::{Column, QueryResult};
use anyhow::Result;
use colored::*;
use postgres::fallible_iterator::FallibleIterator;
use rusqlite::params;
use rusqlite::Row;
use std::convert::TryInto;
use std::path::Path;
use rusqlite::types::ValueRef;

pub struct SqliteConnection {
    identifier: String,
    client: rusqlite::Connection,
    _params: ConnectionParams,
}

impl SqliteConnection {
    pub fn create(identifier: &str, params: ConnectionParams) -> Result<Self> {
        let u = params.clone().url.unwrap();
        match u.as_ref() {
            "memory" => {
                let conn = rusqlite::Connection::open_in_memory()?;
                println!(
                    "{}",
                    "Warning: This is an in-memory database. All changes will be lost.".yellow()
                );
                Ok(Self {
                    identifier: identifier.to_string(),
                    client: conn,
                    _params: params,
                })
            }
            x => {
                let conn = rusqlite::Connection::open(Path::new(x))?;
                Ok(Self {
                    identifier: identifier.to_string(),
                    client: conn,
                    _params: params,
                })
            }
        }
    }
}

impl Connection for SqliteConnection {
    fn execute(&mut self, statement: &str) -> Result<u64> {
        let rows_affected = self.client.execute(statement, params![])?;
        Ok(rows_affected.try_into().unwrap_or(0))
    }
    fn query(&mut self, statement: &str) -> Result<QueryResult> {
        let mut stmt = self.client.prepare(statement)?;
        let columns: Vec<Column> = stmt
            .column_names()
            .iter()
            .map(|c| Column {
                name: c.to_string(),
            })
            .collect();
        let res = stmt.query(params![])?;
        Ok(QueryResult {
            columns,
            rows: res.map(|r| Ok(row_values(r))).collect().unwrap(),
        })
    }
    fn prompt(&self) -> String {
        format!(
            "{} {}{} ",
            self.identifier.bright_blue(),
            "(sqlite)".magenta(),
            ">"
        )
    }
}

fn row_values(row: &Row) -> super::Row {
    super::Row {
        data: (0..row.column_count())
            .map(|i| {
                let v = row.get_raw(i);
                match v {
                    ValueRef::Null => None,
                    ValueRef::Integer(i) => {
                        Some(format!("{}", i))
                    },
                    ValueRef::Real(f) => {
                        Some(format!("{}", f))
                    },
                    ValueRef::Text(t) => {
                        Some(format!("{}", String::from_utf8_lossy(t)))
                    }
                    ValueRef::Blob(_t) => {
                        Some("???".into())
                    }
                }
            })
            .collect(),
    }
}
