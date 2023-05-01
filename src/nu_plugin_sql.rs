use nu_plugin::{EvaluatedCall, LabeledError, Plugin};
use nu_protocol::{Category, PluginExample, PluginSignature, SyntaxShape, Value};

use crate::{database::create_connection, ui::DbcClient};
pub struct Sql;

impl Plugin for Sql {
    fn signature(&self) -> Vec<PluginSignature> {
        // It is possible to declare multiple signature in a plugin
        // Each signature will be converted to a command declaration once the
        // plugin is registered to nushell
        vec![PluginSignature::build("sql")
            .usage("Queries the database")
            .required(
                "conn",
                SyntaxShape::String,
                "The connection identifier to use",
            )
            .rest("query", SyntaxShape::String, "The query to execute")
            .plugin_examples(vec![PluginExample {
                example: "sql identifier \"select 1 from dual\"".into(),
                description: "Runs a SQL query against the database".into(),
                result: None,
            }])
            .category(Category::Experimental)]
    }

    fn run(
        &mut self,
        name: &str,
        call: &EvaluatedCall,
        input: &Value,
    ) -> Result<Value, LabeledError> {
        // You can use the name to identify what plugin signature was called
        match name {
            "sql" => self.query(call, input),
            _ => Err(LabeledError {
                label: "Plugin call with wrong name signature".into(),
                msg: "the signature used to call the plugin does not match any name in the plugin signature vector".into(),
                span: Some(call.head),
            }),
        }
    }
}

impl Sql {
    fn query(&self, call: &EvaluatedCall, _input: &Value) -> Result<Value, LabeledError> {
        let identifier: String = call.req(0)?;
        let query: Vec<String> = call.rest(1)?;
        let query = query.join(" ");

        let config = crate::config::read_config().map_err(|e| LabeledError {
            label: "Config read error".to_string(),
            msg: format!("{:?}", e),
            span: Some(call.head),
        })?;

        let params = config.get(&identifier).ok_or(LabeledError {
            label: "Database identifier not found".to_string(),
            msg: "No such identifier".to_string(),
            span: call.nth(0).map(|x| x.span().unwrap()),
        })?;

        let mut conn =
            create_connection(&identifier, params.clone()).map_err(|e| LabeledError {
                label: "Create connection error".to_string(),
                msg: format!("{:?}", e),
                span: Some(call.head),
            })?;

        let res = conn.query(&query).map_err(|e| LabeledError {
            label: "Query error".to_string(),
            msg: format!("{:?}", e),
            span: call.nth(1).map(|x| x.span().unwrap()),
        })?;

        let cols: Vec<String> = res.columns.iter().map(|c| c.name.clone()).collect();

        let vals = res
            .rows
            .iter()
            .map(|r| {
                let vals = r
                    .data
                    .iter()
                    .map(|c| match c {
                        Some(x) => Value::String {
                            val: x.to_string(),
                            span: call.head,
                        },
                        None => Value::Nothing { span: call.head },
                    })
                    .collect();
                Value::Record {
                    cols: cols.clone(),
                    vals,
                    span: call.head,
                }
            })
            .collect();

        Ok(Value::List {
            vals,
            span: call.head,
        })
    }
}
