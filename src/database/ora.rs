use super::Connection;
use super::ConnectionParams;
use super::Error;
use super::{Column, QueryResult};
use anyhow::anyhow;
use anyhow::Result;
use chrono;
use chrono::offset::FixedOffset;
use colored::Colorize;

pub struct OracleConnection {
    identifier: String,
    conn: oracle::Connection,
    params: ConnectionParams,
}

impl OracleConnection {
    pub fn create(identifier: &str, params: ConnectionParams) -> Result<Self> {
        let p = params.clone();
        let conn = oracle::Connection::connect(
            &p.username.unwrap(),
            &p.password.unwrap(),
            format!("//{}/{}", p.url.unwrap(), p.dbname.unwrap()),
        )?;

        Ok(Self {
            identifier: identifier.to_string(),
            conn: conn,
            params: params,
        })
    }
}

impl Connection for OracleConnection {
    fn execute(&mut self, statement: &str) -> Result<u64> {
        self.conn.execute(statement, &[])?;
        Ok(0)
    }
    fn query(&mut self, statement: &str) -> Result<QueryResult> {
        let rows = self.conn.query(statement, &[])?;
        let columns: Vec<Column> = rows
            .column_info()
            .iter()
            .map(|c| Column {
                name: c.name().to_string(),
            })
            .collect();
        Ok(QueryResult {
            columns,
            rows: rows.map(|r| row_values(&r.unwrap())).collect(),
        })
    }
    fn prompt(&self) -> String {
        format!("{} {}{} ", self.identifier.blue(), "(ora)".magenta(), ">")
    }
}

fn row_values(row: &oracle::Row) -> super::Row {
    super::Row { data: vec![] }
}
