use dbc::nu_plugin_sql::Sql;
use nu_plugin::{serve_plugin, MsgPackSerializer};

fn main() {
    serve_plugin(&mut Sql {}, MsgPackSerializer {})
}
