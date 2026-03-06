use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize)]
pub struct Config {
    pub token: Option<String>,
}

pub fn load_config() -> Option<Config> {
    let home = std::env::var("HOME").ok()?;
    let path = PathBuf::from(home).join(".dagster-cli").join("config.toml");
    let content = std::fs::read_to_string(path).ok()?;
    toml::from_str(&content).ok()
}
