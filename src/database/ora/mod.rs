use super::Connection;
use super::ConnectionParams;
use super::{Column, QueryResult, Row};
use anyhow::Result;
use chrono;
use colored::Colorize;
use oracle::sql_type::OracleType;
use prettytable::format;
use prettytable::{color, Attr, Cell, Row as OtherRow, Table};

pub struct OracleConnection {
    identifier: String,
    conn: oracle::Connection,
    _params: ConnectionParams,
}

impl OracleConnection {
    pub fn create(identifier: &str, params: ConnectionParams) -> Result<Self> {
        let p = params.clone();
        let u = p.url.clone().unwrap();
        let s = if let Some(dbname) = p.dbname {
            format!("//{}/{}", p.url.unwrap(), dbname)
        } else {
            u
        };
        let conn = oracle::Connection::connect(&p.username.unwrap(), &p.password.unwrap(), s)?;

        let client_ver = oracle::Version::client().unwrap();

        let (server_ver, banner) = conn.server_version().unwrap();
        println!(
            "Oracle: Client {} connected to database {}",
            client_ver.to_string().yellow(),
            server_ver.to_string().green()
        );
        println!("{}", banner.magenta());

        Ok(Self {
            identifier: identifier.to_string(),
            conn: conn,
            _params: params,
        })
    }

    fn describe_table(&mut self, obj: &str) -> Result<()> {
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        let rows = self
            .conn
            .query(include_str!("table_columns.sql"), &[&obj])?;
        for row in rows {
            let row = row.unwrap();
            let n: String = row.get("COLUMN_NAME").unwrap();
            let d: String = row.get("DATA_TYPE").unwrap();
            let dl: i32 = row.get("DATA_LENGTH").unwrap();
            let dp: Option<i32> = row.get("DATA_PRECISION").unwrap();
            let nl: Option<String> = row.get("NULLABLE").unwrap();
            let dd: Option<String> = row.get("DATA_DEFAULT").unwrap();
            table.add_row(OtherRow::new(vec![
                Cell::new(&n)
                    .with_style(Attr::Bold)
                    .with_style(Attr::ForegroundColor(color::GREEN)),
                Cell::new(&d),
                Cell::new(&format!("{}", dl)),
                Cell::new(&dp.map(|v| format!("{}", v)).unwrap_or("".into())),
                Cell::new(&nl.map(|v| format!("{}", v)).unwrap_or("".into())),
                Cell::new(&dd.map(|v| format!("{}", v)).unwrap_or("".into())),
            ]));
        }
        table.printstd();

        Ok(())
    }
}

impl Connection for OracleConnection {
    fn execute(&mut self, statement: &str) -> Result<u64> {
        let r = self.conn.execute(statement, &[])?;
        Ok(r.row_count().unwrap())
    }
    fn query(&mut self, statement: &str) -> Result<QueryResult> {
        let rows = self.conn.query(statement, &[])?;
        let ci = rows.column_info();
        let columns: Vec<Column> = ci
            .iter()
            .map(|c| Column {
                name: c.name().to_string(),
            })
            .collect();
        Ok(QueryResult {
            columns,
            rows: rows
                .map(|r| Row {
                    data: r
                        .unwrap()
                        .sql_values()
                        .iter()
                        .map(|x| {
                            if x.is_null().unwrap() {
                                None
                            } else {
                                match x.oracle_type().unwrap() {
                                    OracleType::Varchar2(_) => {
                                        let s: String = x.get().unwrap();
                                        Some(s)
                                    }
                                    OracleType::Int64 | OracleType::UInt64 => {
                                        let y: i64 = x.get().unwrap();
                                        Some(format!("{}", y))
                                    }
                                    OracleType::Number(_s, p) if *p == 0 => {
                                        let y: i64 = x.get().unwrap();
                                        Some(format!("{}", y))
                                    }
                                    OracleType::Number(_s, p) if *p > 0 => {
                                        let y: f64 = x.get().unwrap();
                                        Some(format!("{}", y))
                                    }
                                    OracleType::Timestamp(_) | OracleType::Date => {
                                        let y: chrono::NaiveDateTime = x.get().unwrap();
                                        Some(format!("{}", y))
                                    }
                                    _ => Some("???".into()),
                                }
                            }
                        })
                        .collect(),
                })
                .collect(),
        })
    }
    fn prompt(&self) -> String {
        format!("{} {}{} ", self.identifier.cyan(), "(ora)".magenta(), ">")
    }
    fn list_tables(&mut self) -> std::result::Result<Vec<super::TableRef>, anyhow::Error> {
        let mut v: Vec<super::TableRef> = vec![];
        let rows = self
            .conn
            .query("select null, table_name from user_tables", &[])?;
        for row in rows {
            let row = row.unwrap();
            let t: String = row.get(1).unwrap();
            let tr = super::TableRef {
                schema: "".into(),
                name: t.to_lowercase(),
            };
            v.push(tr);
        }
        return Ok(v);
    }
    fn standard_queries(&self) -> Vec<super::StandardQuery> {
        vec![]
    }
    fn describe(&mut self, obj: &str) -> Result<()> {
        let obj = obj.to_uppercase();
        let (typ, owner): (String, String) = {
            let rows = self.conn.query(
                "SELECT OBJECT_TYPE, OWNER FROM ALL_OBJECTS WHERE OBJECT_NAME = :1",
                &[&obj],
            )?;
            let row = rows.into_iter().nth(0).unwrap().unwrap();
            (row.get(0).unwrap(), row.get(1).unwrap())
        };

        match typ.as_ref() {
            "TABLE" => self.describe_table(&obj)?,
            _ => println!("{}", typ),
        };

        Ok(())
    }
    fn search(&mut self, obj: &str) -> Result<()> {
        todo!()
    }
}
