use colored::Colorize;
use rustyline::completion::extract_word;
use rustyline::completion::Completer;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::{Context, Result};
use rustyline_derive::{Helper, Validator};
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
    pub last_select: Option<String>,
    pub options: DbcClientOptions,
}

impl DbcClient {
    pub fn default() -> Self {
        DbcClient {
            last_select: None,
            options: DbcClientOptions::default(),
        }
    }

    pub fn set_last_select(&mut self, query: &str) {
        self.last_select = Some(String::from(query))
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
    #[structopt(long = "no-cache", parse(from_flag = std::ops::Not::not))]
    pub cache: bool,
}

#[derive(Helper, Validator)]
pub struct Helper {
    pub completions: Vec<String>,
    pub query_completions: Vec<String>,
    pub command_completions: Vec<String>,
}

const KEYWORDS: &[&str] = &[
    "select", "from", "where", "order", "group", "by", "set", "update", "insert", "values",
    "delete", "and", "or",
];
const BREAK_CHARS: [u8; 1] = [b' '];

fn is_keyword(v: &str) -> bool {
    for k in KEYWORDS {
        if *k == v {
            return true;
        }
    }
    return false;
}

impl Highlighter for Helper {
    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        format!("{}", hint.blue()).into()
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
                if is_keyword(x) {
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
        return Ok(complete(&self, line, pos, ctx));
    }
}

fn complete(helper: &Helper, line: &str, pos: usize, _ctx: &Context<'_>) -> (usize, Vec<String>) {
    if line.starts_with("@") {
        let words: Vec<String> = helper
            .query_completions
            .clone()
            .into_iter()
            .filter(|x| x.starts_with(&line[1..]))
            .collect();
        (1, words)
    } else if line.starts_with(":") {
        let words: Vec<String> = helper
            .command_completions
            .clone()
            .into_iter()
            .filter(|x| x.starts_with(&line[1..]))
            .collect();
        (1, words)
    } else {
        let (start, word) = extract_word(line, pos, None, &BREAK_CHARS);

        let words: Vec<String> = helper
            .completions
            .clone()
            .into_iter()
            .filter(|x| x.starts_with(word))
            .collect();
        (start, words)
    }
}

impl Hinter for Helper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<Self::Hint> {
        if line.starts_with("@") {
            let idx = pos - 1;
            let (_start, words) = complete(&self, line, pos, ctx);
            words.iter().nth(0).map(|x| String::from(&x[idx..]))
        } else if pos > 5 {
            let (start, words) = complete(&self, line, pos, ctx);
            let idx = pos - start;

            if idx > 3 {
                words.iter().nth(0).map(|x| String::from(&x[idx..]))
            } else {
                None
            }
        } else {
            None
        }
    }
}
