use crate::database::Connection;
use crate::ui::DbcClient;
use anyhow::Result;
use csv::WriterBuilder;
use regex::Regex;
use simple_excel_writer::*;
use std::fs::File;
use std::io::stdout;
use std::io::Write;

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

fn value(v: &Option<String>) -> String {
    match v {
        Some(x) => {
            format!("'{}'", x.replace("'", "''"))
        }
        None => "".into(),
    }
}

pub fn execute_query_to_insert(
    client: &mut DbcClient,
    conn: &mut Box<dyn Connection>,
    query: &str,
    export_filename: Option<String>,
) -> Result<()> {
    let res = conn.query(&query)?;
    let re = Regex::new(r"\s+from\s+([a-z0-9_]+)\b").unwrap();
    let table_name = match re.captures(query) {
        Some(caps) => String::from(&caps[1]),
        None => "xxxxx".into(),
    };

    let mut writer: Box<dyn Write> = match export_filename {
        Some(f) => Box::new(File::create(f)?),
        None => Box::new(stdout()),
    };

    let columns = res
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect::<Vec<String>>()
        .join(", ");
    for row in res.rows {
        let values = row
            .data
            .iter()
            .map(|v| value(v))
            .collect::<Vec<String>>()
            .join(", ");
        writeln!(
            writer,
            "INSERT INTO {} ({}) VALUES ({});",
            table_name, columns, values
        );
    }

    writer.flush()?;

    Ok(())
}

pub fn execute_query_to_excel(
    client: &mut DbcClient,
    conn: &mut Box<dyn Connection>,
    query: &str,
    export_filename: String,
) -> Result<()> {
    let mut workbook = Workbook::create(&export_filename);
    let mut sheet = workbook.create_sheet("Data");

    let res = conn.query(&query)?;

    workbook.write_sheet(&mut sheet, |sheet_writer| {
        let sw = sheet_writer;
        let mut r = Row::new();
        for c in res.columns {
            r.add_cell(c.name);
        }
        sw.append_row(r)?;

        for d in res.rows {
            let mut r = Row::new();
            for v in d.data {
                r.add_cell(v.unwrap_or("".into()));
            }
            sw.append_row(r)?;
        }

        Ok(())
    })?;

    let mut sheet = workbook.create_sheet("Query");
    workbook.write_sheet(&mut sheet, |sheet_writer| {
        sheet_writer.append_row(row![query])
    })?;

    workbook.close()?;

    Ok(())
}
