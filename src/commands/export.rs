use crate::database::Connection;
use crate::ui::DbcClient;
use anyhow::Result;
use csv::WriterBuilder;
use std::io::stdout;
use std::io::Write;
use std::path::Path;

pub fn execute_query_to_csv(
    client: &mut DbcClient,
    conn: &mut Box<dyn Connection>,
    query: &str,
    export_filename: Option<String>,
) -> Result<()> {
    let res = conn.query(&query)?;
    let mut builder = WriterBuilder::new();
    let builder = builder.delimiter(b';');
    match export_filename {
        Some(f) => {
            let mut wtr = builder.from_path(f)?;
            wtr.write_record(res.columns.into_iter().map(|c| c.name))?;
            for d in res.rows {
                wtr.write_record(d.data.into_iter().map(|x| x.unwrap_or("".into())))?;
            }
        }
        None => {
            let mut wtr = builder.from_writer(stdout());
            wtr.write_record(res.columns.into_iter().map(|c| c.name))?;
            for d in res.rows {
                wtr.write_record(d.data.into_iter().map(|x| x.unwrap_or("".into())))?;
            }
        }
    };

    Ok(())
}
