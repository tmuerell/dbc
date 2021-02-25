use anyhow::Result;
use colored::Colorize;
use dbc::database::{create_connection, ConnectionParams};
use dirs::home_dir;
use prettytable::format;
use prettytable::{color, Attr, Cell, Row, Table};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use structopt::StructOpt;

/// Command line database client
#[derive(StructOpt, Debug)]
#[structopt(name = "dbc")]
struct Opt {
    /// Database identifier
    #[structopt()]
    identifier: String,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();

    let params = if opt.identifier == "vmdevpre" {
        ConnectionParams {
            connector: "pg".into(),
            host: "vmdevpre.int.compax.at".into(),
            username: "aax2".into(),
            password: "aax2".into(),
            dbname: "aax2db".into(),
        }
    } else {
        ConnectionParams {
            connector: "sqlite".into(),
            host: "memory".into(),
            username: "xx".into(),
            password: "xx".into(),
            dbname: "xxx".into(),
        }
    };

    let mut conn = create_connection(params)?;

    let mut rl = Editor::<()>::new();
    let history_file = home_dir().unwrap().join(".dbc_history");
    if rl.load_history(&history_file).is_err() {
        println!("No previous history.");
    }
    loop {
        let readline = rl.readline(&conn.prompt());
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());

                if line.trim().starts_with("select") {

                let row_limit = 20;
                let col_limit = 10;

                match conn.query(&line) {
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
                                c = c + 1;
                                table.add_row(Row::new(
                                    r.data
                                        .iter()
                                        .take(col_limit)
                                        .map(|s| match s {
                                            Some(v) => Cell::new(v),
                                            None => Cell::new("NULL")
                                                .with_style(Attr::ForegroundColor(color::MAGENTA)),
                                        })
                                        .collect(),
                                ));
                                if c > row_limit {
                                    break;
                                }
                            }

                            table.printstd();
                        }
                    }
                    Err(e) => println!("{}: {:?}", "Error".red(), e),
                }
            } else {
                conn.execute(&line)?;
            }
            }
            Err(ReadlineError::Interrupted) => {
                break;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history(&history_file).unwrap();
    Ok(())
}
