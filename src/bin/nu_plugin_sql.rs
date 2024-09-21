use dbc::nu_plugin_sql::SqlPlugin;
use nu_plugin::{serve_plugin, MsgPackSerializer};

fn main() {
    serve_plugin(&mut SqlPlugin {}, MsgPackSerializer {})
}
