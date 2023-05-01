use anyhow::anyhow;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(feature = "mysql-support")]
pub mod mysql;
#[cfg(feature = "oracle-support")]
pub mod ora;
pub mod pg;
#[cfg(feature = "sqlite-support")]
pub mod sqlite;

#[derive(Error, Debug)]
pub enum Error {
    #[error("No result found")]
    NoResultError,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionParams {
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub url: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub dbname: Option<String>,
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

pub struct TableRef {
    pub schema: String,
    pub name: String,
}

#[derive(Clone)]
pub struct StandardQuery<'a> {
    pub name: &'a str,
    pub query: &'a str,
}

pub trait Connection {
    fn print_connection_info(&mut self) -> Result<()>;

    fn execute(&mut self, statement: &str) -> Result<u64>;

    fn query(&mut self, statement: &str) -> Result<QueryResult>;

    fn list_tables(&mut self) -> Result<Vec<TableRef>>;

    fn prompt(&self) -> String;

    fn standard_queries(&self) -> Vec<StandardQuery>;

    fn describe(&mut self, obj: &str) -> Result<()>;

    fn search(&mut self, obj: &str) -> Result<()>;
}

pub fn create_connection(
    identifier: &str,
    params: ConnectionParams,
) -> Result<Box<dyn Connection>> {
    match params.clone().type_.unwrap_or("ora".into()).as_ref() {
        "pg" | "postgresql" => Ok(Box::new(pg::PgConnection::create(identifier, params)?)),
        #[cfg(feature = "sqlite-support")]
        "sqlite" => Ok(Box::new(sqlite::SqliteConnection::create(
            identifier, params,
        )?)),
        #[cfg(feature = "oracle-support")]
        "ora" | "oracle" => Ok(Box::new(ora::OracleConnection::create(identifier, params)?)),
        #[cfg(feature = "mysql-support")]
        "mysql" => Ok(Box::new(mysql::MysqlConnection::create(
            identifier, params,
        )?)),
        _ => Err(anyhow!("Unknown database type {:?}", &params.type_)),
    }
}
