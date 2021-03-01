use colored::Colorize;
use rustyline::highlight::Highlighter;
use rustyline_derive::{Completer, Helper, Hinter, Validator};
use std::borrow::Cow;
use structopt::StructOpt;

#[derive(Debug, Clone)]
pub struct DbcClientOptions {
    pub row_limit: usize,
    pub column_limit: usize,
}

impl DbcClientOptions {
    pub fn default() -> DbcClientOptions {
        DbcClientOptions {
            row_limit: 20,
            column_limit: 10,
        }
    }

    pub fn set_row_limit(&mut self, n: usize) {
        self.row_limit = n;
    }
    pub fn set_column_limit(&mut self, n: usize) {
        self.column_limit = n;
    }
}

#[derive(Debug, Clone)]
pub struct DbcClient {
    pub last_select: String,
    pub options: DbcClientOptions,
}

impl DbcClient {
    pub fn default() -> Self {
        DbcClient {
            last_select: "".into(),
            options: DbcClientOptions::default(),
        }
    }

    pub fn set_last_select(&mut self, query: &str) {
        self.last_select = String::from(query)
    }
}

/// Command line database client
#[derive(StructOpt, Debug)]
#[structopt(name = "dbc")]
pub struct Opt {
    /// Database identifier
    #[structopt()]
    pub identifier: String,

    /// Quiet (do not print banners)
    #[structopt(short = "q")]
    pub quiet: bool,
}

#[derive(Helper, Hinter, Completer, Validator)]
pub struct Helper {}

impl Highlighter for Helper {
    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        hint.into()
    }

    fn highlight_candidate<'c>(
        &self,
        candidate: &'c str,
        _completion: rustyline::CompletionType,
    ) -> Cow<'c, str> {
        self.highlight(candidate, 0)
    }

    fn highlight_char(&self, line: &str, _: usize) -> bool {
        !line.is_empty()
    }

    fn highlight<'l>(&self, line: &'l str, _: usize) -> Cow<'l, str> {
        let s: Vec<String> = line
            .split_whitespace()
            .map(|x| {
                if x == "select"
                    || x == "from"
                    || x == "where"
                    || x == "order"
                    || x == "group"
                    || x == "by"
                {
                    format!("{}", x.green())
                } else {
                    x.into()
                }
            })
            .collect();

        s.join(" ").into()
    }
}
