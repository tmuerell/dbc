use anyhow::anyhow;
use anyhow::Result;
use thiserror::Error;

pub mod pg;
#[cfg(feature = "sqlite")]
pub mod sqlite;

#[derive(Error, Debug)]
pub enum Error {
    #[error("No result found")]
    NoResultError,
}

pub struct ConnectionParams {
    pub connector: String,
    pub host: String,
    pub username: String,
    pub password: String,
    pub dbname: String,
}

pub struct Column {
    pub name: String,
}

pub struct Row {
    pub data: Vec<Option<String>>,
}

pub struct QueryResult {
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

pub trait Connection {
    fn execute(&mut self, statement: &str) -> Result<()>;

    fn query(&mut self, statement: &str) -> Result<QueryResult>;

    fn prompt(&self) -> String;
}

pub fn create_connection(params: ConnectionParams) -> Result<Box<dyn Connection>> {
    match params.connector.as_ref() {
        "pg" => Ok(Box::new(pg::PgConnection::create(params)?)),
        #[cfg(feature = "sqlite")]
        "sqlite" => Ok(Box::new(sqlite::SqliteConnection::create(params)?)),
        _ => Err(anyhow!("Unknown database type {}", params.connector)),
    }
}
