use anyhow::Result;
use crate::database::Connection;
use colored::Colorize;
use prettytable::format;
use prettytable::{color, Attr, Cell, Row, Table};
use crate::ui::DbcClient;

pub fn execute_query_and_print_results(client : &DbcClient, conn : &mut Box<dyn Connection>, query : &str) -> Result<()> {
    if query.trim().starts_with("select") {
        let row_limit = client.options.row_limit;
        let col_limit = client.options.column_limit;

        match conn.query(&query) {
            Ok(res) => {
                if res.rows.len() == 1 {
                    let mut table = Table::new();
                    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
                    let row = res.rows.iter().nth(0).unwrap();
                    for (pos, e) in row.data.iter().enumerate() {
                        table.add_row(Row::new(vec![
                            Cell::new(&res.columns.iter().nth(pos).unwrap().name)
                                .with_style(Attr::Bold)
                                .with_style(Attr::ForegroundColor(color::GREEN)),
                            match e {
                                Some(v) => Cell::new(v),
                                None => Cell::new("NULL")
                                    .with_style(Attr::ForegroundColor(color::MAGENTA)),
                            },
                        ]));
                    }
                    table.printstd();
                } else {
                    let mut table = Table::new();
                    table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);

                    table.set_titles(Row::new(
                        res.columns
                            .iter()
                            .take(col_limit)
                            .map(|s| {
                                Cell::new(&s.name)
                                    .with_style(Attr::Bold)
                                    .with_style(Attr::ForegroundColor(color::GREEN))
                            })
                            .collect(),
                    ));

                    let mut c = 0;
                    for r in res.rows {
                        table.add_row(Row::new(
                            r.data
                                .iter()
                                .take(col_limit)
                                .map(|s| match s {
                                    Some(v) => Cell::new(v),
                                    None => Cell::new("NULL").with_style(
                                        Attr::ForegroundColor(color::MAGENTA),
                                    ),
                                })
                                .collect(),
                        ));
                        c = c + 1;
                        if c >= row_limit {
                            break;
                        }
                    }

                    table.printstd();
                }
            }
            Err(e) => println!("{}: {:?}", "Error".red(), e),
        }
    } else {
        let rows_updated = conn.execute(&query)?;
        println!("{}", format!("{} rows updated.", rows_updated).magenta());
    };
    Ok(())
}