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
use prettytable::format;
use prettytable::{color, Attr, Cell, Row as OtherRow, Table};

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
        let mut client = Client::connect(&s, NoTls)?;

        let rows = client.query("show server_version;", &[])?;

        if let Some(r) = rows.iter().nth(0) {
            let s: String = r.get(0);
            println!("Postgres: Connected to {}", s.green());
        }

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
        format!("{} {}{} ", self.identifier.cyan(), "(pg)".magenta(), ">")
    }
    fn list_tables(&mut self) -> Result<Vec<super::TableRef>> {
        let mut v: Vec<super::TableRef> = vec![];

        let rows = self.client.query(
            "select table_schema, table_name from information_schema.tables",
            &[],
        )?;

        for row in rows {
            let tr = super::TableRef {
                schema: row.get(0),
                name: row.get(1),
            };
            v.push(tr);
        }

        return Ok(v);
    }
    fn standard_queries(&self) -> Vec<super::StandardQuery> {
        let s = super::StandardQuery {
            name: "locks",
            query: "select blocked_locks.pid     AS blocked_pid,
            blocked_activity.usename  AS blocked_user,
            blocking_locks.pid     AS blocking_pid,
            blocking_activity.usename AS blocking_user,
            blocked_activity.query    AS blocked_statement,
            blocking_activity.query   AS current_statement_in_blocking_process,
            blocked_activity.application_name AS blocked_application,
            blocking_activity.application_name AS blocking_application
      FROM  pg_catalog.pg_locks         blocked_locks
       JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
       JOIN pg_catalog.pg_locks         blocking_locks
           ON blocking_locks.locktype = blocked_locks.locktype
           AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
           AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
           AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
           AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
           AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
           AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
           AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
           AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
           AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
           AND blocking_locks.pid != blocked_locks.pid
      JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
     WHERE NOT blocked_locks.GRANTED"
        };
        let s2 = super::StandardQuery {
            name: "queries",
            query: "SELECT pid, age(clock_timestamp(), query_start), usename, query 
            FROM pg_stat_activity 
            WHERE query != '<IDLE>' AND query NOT ILIKE '%pg_stat_activity%' 
            ORDER BY query_start desc",
        };
        let s3 = super::StandardQuery {
            name: "sizes",
            query: "select datname, pg_size_pretty(pg_database_size(datname))
            from pg_database
            order by pg_database_size(datname) desc",
        };
        vec![s, s2, s3]
    }
    fn describe(&mut self, obj: &str) -> Result<()> { 
        let obj = obj.to_ascii_lowercase();
        let row = self.client.query_one("select relkind::text from pg_class where relname = $1", &[&obj])?;
        let relkind : String = row.get(0);

        match relkind.as_ref() {
            "v" => println!("View"),
            "r" => {
                let rows = self.client.query("select ordinal_position as position,
                column_name,
                data_type,
                case when character_maximum_length is not null
                     then character_maximum_length
                     else numeric_precision end as max_length,
                is_nullable,
                column_default as default_value
         from information_schema.columns
         where table_name = $1
         order by ordinal_position", &[&obj])?;

                let mut table = Table::new();
                table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
                for row in rows {
                        table.add_row(OtherRow::new(vec![
                            Cell::new(row.get("column_name"))
                                .with_style(Attr::Bold)
                                .with_style(Attr::ForegroundColor(color::GREEN)),
                                Cell::new(row.get("data_type"))
                        ]));
                }
                table.printstd();
     
            },
            "i" => println!("Index"),
            "S" => println!("Sequece"),
            "m" => println!("materialized view"),
            _ => println!("Unsupported")
        }

        Ok(())

     }
}

fn row_values(row: &Row) -> super::Row {
    super::Row {
        data: (0..row.len())
            .map(|i| {
                let c = row.columns().iter().nth(i);
                if let Some(c) = c {
                    match c.type_() {
                        &Type::TEXT | &Type::VARCHAR | &Type::NAME => {
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
