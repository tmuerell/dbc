use anyhow::Result;
use colored::Colorize;
use dbc::database::create_connection;
use dbc::ui::{DbcClient, Helper, Opt};
use dirs::home_dir;
use regex::Regex;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use structopt::StructOpt;

fn main() -> Result<()> {
    let opt = Opt::from_args();

    let config = dbc::config::read_config()?;
    let mut client = DbcClient::default();

    let params = config.get(&opt.identifier).expect("No such identifier");

    if !opt.quiet {
        println!("{}", "*".repeat(60));
        println!("{} {}", "*", "Welcome to dbc");
        println!("{}", "*".repeat(60));
        println!("{}", "");
    }

    let mut conn = create_connection(&opt.identifier, params.clone())?;

    let tables = if opt.cache {
        println!("{}", "Reading DB schema...".yellow());
        conn.list_tables()?
    } else {
        vec![]
    };

    let completions: Vec<String> = tables.iter().map(|x| x.name.clone()).collect();
    let query_completions: Vec<String> = conn
        .standard_queries()
        .iter()
        .map(|q| q.name.to_string())
        .collect();

    let helper = Helper {
        completions: completions,
        query_completions: query_completions,
    };
    let mut rl = Editor::<Helper>::new();
    rl.set_helper(Some(helper));
    let history_file = home_dir().unwrap().join(".dbc_history");
    if rl.load_history(&history_file).is_err() {
        println!("No previous history.");
    }
    loop {
        let readline = rl.readline(&conn.prompt());
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());

                if line.starts_with(":") {
                    if line.starts_with(":set") {
                        let re = Regex::new(r":set (\S+) (\S+)$").unwrap();
                        if let Some(c) = re.captures(&line) {
                            if &c[1] == "column_limit" {
                                client.options.set_column_limit(c[2].parse()?);
                            }
                            if &c[1] == "row_limit" {
                                client.options.set_row_limit(c[2].parse()?);
                            }
                        }
                    } else if line.starts_with(":list") {
                        let last_line = client.last_select.clone();
                        match last_line {
                            Some(l) => dbc::commands::query::execute_query_and_print_results(
                                &mut client,
                                &mut conn,
                                &l,
                                1,
                            )?,
                            None => println!("No last query."),
                        }
                    } else if line.starts_with(":all") {
                        let last_line = client.last_select.clone();
                        match last_line {
                            Some(l) => dbc::commands::query::execute_query_and_print_results(
                                &mut client,
                                &mut conn,
                                &l,
                                1000,
                            )?,
                            None => println!("No last query."),
                        }
                    } else if line.starts_with(":export") {
                        let re = Regex::new(r":export (\S+) (\S+)$").unwrap();
                        if let Some(c) = re.captures(&line) {
                            let last_line = client.last_select.clone();
                            match last_line {
                                Some(l) => {
                                    let f = if &c[2] == "-" {
                                        None
                                    } else {
                                        Some(String::from(&c[2]))
                                    };
                                    if &c[1] == "csv" {
                                        dbc::commands::export::execute_query_to_csv(
                                            &mut client,
                                            &mut conn,
                                            &l,
                                            f,
                                        )?;
                                    } else if &c[1] == "insert" {
                                        dbc::commands::export::execute_query_to_insert(
                                            &mut client,
                                            &mut conn,
                                            &l,
                                            f,
                                        )?;
                                    } else if &c[1] == "excel" {
                                        dbc::commands::export::execute_query_to_excel(
                                            &mut client,
                                            &mut conn,
                                            &l,
                                            f.expect("Export of Excel to stdout not supported"),
                                        )?;
                                    } else {
                                        println!("{}", "Format not supported".red());
                                    }
                                }
                                None => println!("No last query."),
                            }
                        }
                    } else {
                        println!("{}", "ERROR: Unsupported command".red());
                    }
                } else if line.starts_with("@") {
                    let q = {
                        let queries = conn.standard_queries();
                        let v = queries.into_iter().filter(|x| x.name == &line[1..]).nth(0);
                        v.map(|x| x.query.to_string())
                    };
                    match q {
                        Some(x) => dbc::commands::query::execute_query_and_print_results(
                            &mut client,
                            &mut conn,
                            &x,
                            1000,
                        )?,
                        None => println!("Query not found {}", &line[1..]),
                    };
                } else {
                    let limit = client.options.row_limit;
                    let res = 
                    dbc::commands::query::execute_query_and_print_results(
                        &mut client,
                        &mut conn,
                        &line,
                        limit,
                    );
                    match res {
                        Err(e) => println!("{}: {}", "Cannot execute statement:".red(), e),
                        Ok(_) => {}
                    }
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

    if !opt.quiet {
        println!();
        println!("{}", "Thank you for using dbc.");
    }
    Ok(())
}
