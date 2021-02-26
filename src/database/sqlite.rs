use super::Connection;
use super::ConnectionParams;
use super::{Column, QueryResult};
use anyhow::Result;
use postgres::fallible_iterator::FallibleIterator;
use rusqlite::params;
use rusqlite::Row;
use std::path::Path;
use colored::*;
use std::convert::TryInto;

pub struct SqliteConnection {
    identifier: String,
    client: rusqlite::Connection,
    params: ConnectionParams,
}

impl SqliteConnection {
    pub fn create(identifier: &str, params: ConnectionParams) -> Result<Self> {
        let u = params.clone().url.unwrap();
        match u.as_ref() {
            "memory" => {
                let conn = rusqlite::Connection::open_in_memory()?;
                println!("{}", "Warning: This is an in-memory database. All changes will be lost.".yellow());
            Ok(Self {
                identifier: identifier.to_string(),
                client: conn,
                params: params,
            })
            },
            x if x.ends_with(".sqlite3") => {
                let conn = rusqlite::Connection::open(Path::new(x))?;
                Ok(Self {
                    identifier: identifier.to_string(),
                    client: conn,
                    params: params,
                })
            },
            _ => panic!("URL not implemented")
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
        format!("{} {}{} ", self.identifier.blue(), "(sqlite)".magenta(), ">")
    }
}

fn row_values(row: &Row) -> super::Row {
    super::Row {
        data: (0..row.column_count())
            .map(|i| {
                let s: Option<String> = row.get(i).unwrap();
                s
            })
            .collect(),
    }
}
