use super::Connection;
use super::ConnectionParams;
use super::Error;
use super::{Column, QueryResult};
use anyhow::anyhow;
use anyhow::Result;
use chrono;
use chrono::offset::FixedOffset;
use postgres::types::Type;
use postgres::{Client, NoTls, Row};

pub struct PgConnection {
    client: Client,
    params: ConnectionParams,
}

impl PgConnection {
    pub fn create(params: ConnectionParams) -> Result<Self> {
        let s = format!(
            "host={} user={} password={} dbname={}",
            params.host, params.username, params.password, params.dbname
        );
        let client = Client::connect(&s, NoTls)?;

        Ok(Self {
            client,
            params: params,
        })
    }
}

impl Connection for PgConnection {
    fn execute(&mut self, statement: &str) -> Result<()> {
        self.client.execute(statement, &[])?;
        Ok(())
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
        format!("{} (pg)> ", self.params.host)
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
                        &Type::INT8 | &Type::INT4 | &Type::INT2 => {
                            let x: Option<i64> = row.get(i);
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
