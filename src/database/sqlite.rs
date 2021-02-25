use super::Connection;
use super::ConnectionParams;
use super::Error;
use super::{Column, QueryResult};
use anyhow::anyhow;
use anyhow::Result;
use chrono;
use chrono::offset::FixedOffset;
use postgres::fallible_iterator::FallibleIterator;
use rusqlite::params;
use rusqlite::Row;

pub struct SqliteConnection {
    client: rusqlite::Connection,
    params: ConnectionParams,
}

impl SqliteConnection {
    pub fn create(params: ConnectionParams) -> Result<Self> {
        if params.host == "memory" {
            let conn = rusqlite::Connection::open_in_memory()?;
            Ok(Self {
                client: conn,
                params: params,
            })
        } else {
            panic!("not implemented");
        }
    }
}

impl Connection for SqliteConnection {
    fn execute(&mut self, statement: &str) -> Result<()> {
        self.client.execute(statement, params![])?;
        Ok(())
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
        format!("{} (sqlite)> ", self.params.host)
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
