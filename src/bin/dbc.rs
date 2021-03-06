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

    let mut completions: Vec<String> = tables.iter().map(|x| x.name.clone()).collect();

    let helper = Helper {
        completions: completions,
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
                        dbc::commands::query::execute_query_and_print_results(
                            &mut client,
                            &mut conn,
                            &last_line,
                            1,
                        )?;
                    } else if line.starts_with(":all") {
                        let last_line = client.last_select.clone();
                        dbc::commands::query::execute_query_and_print_results(
                            &mut client,
                            &mut conn,
                            &last_line,
                            1000,
                        )?;
                    } else {
                        println!("{}", "ERROR: Unsupported command".red());
                    }
                } else {
                    let limit = client.options.row_limit;
                    dbc::commands::query::execute_query_and_print_results(
                        &mut client,
                        &mut conn,
                        &line,
                        limit,
                    )?;
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
