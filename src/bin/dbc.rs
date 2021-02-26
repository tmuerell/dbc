use anyhow::Result;
use dbc::database::{create_connection};
use dirs::home_dir;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use structopt::StructOpt;
use dbc::ui::{Opt, Helper, DbcClient, DbcClientOptions};



fn main() -> Result<()> {
    let opt = Opt::from_args();

    let config = dbc::config::read_config()?;
    let client = DbcClient {
        options: DbcClientOptions::default()
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

                dbc::commands::query::execute_query_and_print_results(&client, &mut conn, &line)?;

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
