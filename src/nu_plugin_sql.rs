use nu_plugin::{EvaluatedCall, Plugin, PluginCommand};
use nu_protocol::{Category, IntoInterruptiblePipelineData, LabeledError, ListStream, PipelineData, PipelineMetadata, Signature, SyntaxShape, Value};

use crate::database::create_connection;
pub struct SqlPlugin;
pub struct Query;

impl Plugin for SqlPlugin {
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").into()
    }
    
    fn commands(&self) -> Vec<Box<dyn nu_plugin::PluginCommand<Plugin = Self>>> {
        // It is possible to declare multiple signature in a plugin
        // Each signature will be converted to a command declaration once the
        // plugin is registered to nushell
        vec![Box::new(Query{})]
    }
}


impl PluginCommand for Query {
    type Plugin = SqlPlugin;

    fn name(&self) -> &str {
        "query"
    }

    fn signature(&self) -> nu_protocol::Signature {
        Signature::new("query").description("Queries the database")
        .required(
            "conn",
            SyntaxShape::String,
            "The connection identifier to use",
        ).rest("query", SyntaxShape::String, "The query to execute").category(Category::Experimental)
    }

    fn description(&self) -> &str {
        "Queries the database"
    }

    fn run(
        &self,
        plugin: &Self::Plugin,
        engine: &nu_plugin::EngineInterface,
        call: &EvaluatedCall,
        input: nu_protocol::PipelineData,
    ) -> Result<nu_protocol::PipelineData, nu_protocol::LabeledError> {
        self.query(call, &input)
    }
}

impl Query {
    fn query(&self, call: &EvaluatedCall, _input: &PipelineData) -> Result<PipelineData, LabeledError> {
        let identifier: String = call.req(0)?;
        let query: Vec<String> = call.rest(1)?;
        let query = query.join(" ");

        let config = crate::config::read_config().map_err(|e| LabeledError::new(format!("{:?}", e)).with_label("happened here", call.head))?;

        let params = config.get(&identifier).ok_or(LabeledError::new("No such identifier".to_string()).with_label("Database identifier not found", call.head))?;

        let mut conn =
            create_connection(&identifier, params.clone()).map_err(|e| LabeledError::new(format!("{:?}", e)).with_label("happened here", call.head))?;

        let res = conn.query(&query).map_err(|e| LabeledError::new(format!("{:?}", e)).with_label("Query error", call.nth(1).map(|x| x.span()).unwrap()))?;

        let cols: Vec<String> = res.columns.iter().map(|c| c.name.clone()).collect();

        let vals = res
            .rows
            .iter()
            .map(|r| {
                let vals : Vec<Value> = r
                    .data
                    .iter()
                    .map(|c| match c {
                        Some(x) => Value::String {
                            val: x.to_string(),
                            internal_span: call.head,
                        },
                        None => Value::Nothing { internal_span: call.head },
                    })
                    .collect();
                Value::Record {
                    val: Value::List { vals: vals, internal_span: call.head }.into_record().unwrap().into(),
                    internal_span: call.head,
                }
            })
            .collect();

        Ok(PipelineData::Value(Value::list(vals, call.head), None))
    }
}
