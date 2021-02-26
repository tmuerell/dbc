use super::database::ConnectionParams;
use anyhow::Result;
use dirs::home_dir;
use std::collections::HashMap;
use std::fs::File;

pub fn read_config() -> Result<HashMap<String, ConnectionParams>> {
    let f = home_dir().unwrap().join(".dbc.yml");
    if f.exists() {
        let file = File::open(f)?;
        let res: HashMap<String, ConnectionParams> = serde_yaml::from_reader(file)?;
        Ok(res)
    } else {
        Ok(HashMap::new())
    }
}
