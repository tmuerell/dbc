use colored::Colorize;
use rustyline::completion::extract_word;
use rustyline::completion::Completer;
use rustyline::highlight::Highlighter;
use rustyline::{Context, Result};
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

    /// Cache the DB schema for completion
    #[structopt(short = "c")]
    pub cache: bool,
}

#[derive(Helper, Hinter, Validator)]
pub struct Helper {
    pub completions: Vec<String>,
}

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
                    || x == "set"
                    || x == "update"
                    || x == "insert"
                    || x == "values"
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

impl Completer for Helper {
    type Candidate = String;

    fn complete(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Result<(usize, Vec<String>)> {
        let break_chars: [u8; 1] = [b' '];
        let (start, word) = extract_word(line, pos, None, &break_chars);

        let words: Vec<String> = self
            .completions
            .clone()
            .into_iter()
            .filter(|x| x.starts_with(word))
            .collect();

        return Ok((start, words));
    }
}
