# dbc

[![Build](https://github.com/tmuerell/dbc/actions/workflows/build.yml/badge.svg)](https://github.com/tmuerell/dbc/actions/workflows/build.yml)


A command line database client

This project is in Alpha Status

## Features

dbc allows you to

* ... connect to Postgres, Sqlite3 and Oracle databases
* ... fire SQL statements agains them
* ... see results from queries
* ... export results to CSV, Insert, Excel

## Demo

[![asciicast](https://asciinema.org/a/HdyAGv32aLRa7Sk2OPlAgctra.svg)](https://asciinema.org/a/HdyAGv32aLRa7Sk2OPlAgctra)

## Installation 

### Installation of releases

To be done..

### Installation of bleeding edge builds

```
curl -sSf https://raw.githubusercontent.com/tmuerell/dbc/main/install.sh | bash
```

### Installation via rust and cargo

You need to have rust (https://rustup.sh) installed.

* Clone the repository
* Run `cargo install`

## Features

* `ora`
* `postgres`
* `sqlite`

## Usage

There is help in the program available. Just run `dbc --help`.
