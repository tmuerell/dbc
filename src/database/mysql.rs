use super::Connection;
use super::ConnectionParams;
use super::{Column, QueryResult, Row};
use anyhow::Result;
use chrono;
use chrono::{Local, TimeZone};
use colored::Colorize;
use mysql::prelude::*;
use mysql::OptsBuilder;
use regex::Regex;

pub struct MysqlConnection {
    identifier: String,
    conn: mysql::Conn,
    _params: ConnectionParams,
}

impl MysqlConnection {
    pub fn create(identifier: &str, params: ConnectionParams) -> Result<Self> {
        let re = Regex::new(r"//([^/:]+)/(\w+)$").unwrap();
        let p = params.clone();
        let u = p.url.expect("PG needs a URL");
        let c = re
            .captures(&u)
            .expect("Format of URL needs to be //host/db");
        let p = params.clone();
        let mut b = OptsBuilder::new();
        b = b
            .ip_or_hostname(Some(&c[1]))
            .db_name(Some(&c[2]))
            .user(p.username)
            .pass(p.password);
        let conn = mysql::Conn::new(b)?;

        // println!("MySQL: {}", conn.info_str().yellow());

        Ok(Self {
            identifier: identifier.to_string(),
            conn: conn,
            _params: params,
        })
    }
}

impl Connection for MysqlConnection {
    fn execute(&mut self, statement: &str) -> Result<u64> {
        self.conn.exec_drop(statement, ())?;
        Ok(self.conn.affected_rows())
    }
    fn query(&mut self, statement: &str) -> Result<QueryResult> {
        let stmt = self.conn.prep(statement)?;
        let rows: Vec<mysql::Row> = self.conn.exec(&stmt, ())?;
        let columns: Vec<Column> = stmt
            .columns()
            .iter()
            .map(|c| Column {
                name: c.name_str().to_string(),
            })
            .collect();
        let data = rows
            .iter()
            .map(|r| {
                let n = r.len();

                Row {
                    data: (0..n).map(|i| conv(r.get(i))).collect(),
                }
            })
            .collect();
        Ok(QueryResult {
            columns,
            rows: data,
        })
    }
    fn prompt(&self) -> String {
        format!("{} {}{} ", self.identifier.cyan(), "(my)".magenta(), ">")
    }
    fn list_tables(&mut self) -> std::result::Result<Vec<super::TableRef>, anyhow::Error> {
        Ok(self
            .conn
            .query_map("show tables", |name: String| super::TableRef {
                schema: "".into(),
                name: name.into(),
            })?)
    }
    fn standard_queries(&self) -> Vec<super::StandardQuery> {
        vec![]
    }
    fn describe(&mut self, _: &str) -> Result<()> {
        todo!()
    }
    fn search(&mut self, obj: &str) -> Result<()> {
        todo!()
    }
}

fn conv(v: Option<mysql::Value>) -> Option<String> {
    match v {
        Some(mysql::Value::NULL) => None,
        Some(mysql::Value::Bytes(x)) => Some(String::from_utf8_lossy(&x).to_string()),
        Some(mysql::Value::Int(x)) => Some(format!("{}", x)),
        Some(mysql::Value::Float(x)) => Some(format!("{}", x)),
        Some(mysql::Value::Date(y, m, d, ho, mi, se, mic)) => {
            let t = Local.ymd(y.into(), m.into(), d.into()).and_hms_micro(
                ho.into(),
                mi.into(),
                se.into(),
                mic.into(),
            );
            Some(format!("{}", t))
        }
        _ => Some("x".into()),
    }
}
