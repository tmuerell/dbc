use anyhow::Result;
use dbc::database::create_connection;
use dbc::ui::{DbcClient, DbcClientOptions, Helper, Opt};
use dirs::home_dir;
use regex::Regex;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use structopt::StructOpt;

fn main() -> Result<()> {
    let opt = Opt::from_args();

    let config = dbc::config::read_config()?;
    let mut client = DbcClient {
        options: DbcClientOptions::default(),
    };

    let params = config.get(&opt.identifier).expect("No such identifier");

    if !opt.quiet {
        println!("{}", "*".repeat(60));
        println!("{} {}", "*", "Welcome to dbc");
        println!("{}", "*".repeat(60));
        println!("{}", "");
    }

    let mut conn = create_connection(&opt.identifier, params.clone())?;

    let helper = Helper {};
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
                    let re = Regex::new(r":set (\S+) (\S+)$").unwrap();
                    if let Some(c) = re.captures(&line) {
                        if &c[1] == "column_limit" {
                            client.options.set_column_limit(c[2].parse()?);
                        }
                        if &c[1] == "row_limit" {
                            client.options.set_row_limit(c[2].parse()?);
                        }
                    }
                } else {
                    dbc::commands::query::execute_query_and_print_results(
                        &client, &mut conn, &line,
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
