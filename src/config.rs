use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize)]
pub struct Config {
    pub token: Option<String>,
}

pub fn load_config() -> Option<Config> {
    let home = std::env::var("HOME").ok()?;
    let path = PathBuf::from(home).join(".dagster-cli").join("config.toml");
    load_config_from_path(&path)
}

#[cfg(not(test))]
fn load_config_from_path(path: &PathBuf) -> Option<Config> {
    let content = std::fs::read_to_string(path).ok()?;
    toml::from_str(&content).ok()
}

#[cfg(test)]
pub(crate) fn load_config_from_path(path: &PathBuf) -> Option<Config> {
    let content = std::fs::read_to_string(path).ok()?;
    toml::from_str(&content).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_parse_valid_config() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        fs::write(&config_path, "token = \"test-token\"").unwrap();

        let config = load_config_from_path(&config_path);
        assert!(config.is_some());
        assert_eq!(config.unwrap().token, Some("test-token".to_string()));
    }

    #[test]
    fn test_parse_config_without_token() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        fs::write(&config_path, "").unwrap();

        let config = load_config_from_path(&config_path);
        assert!(config.is_some());
        assert_eq!(config.unwrap().token, None);
    }

    #[test]
    fn test_missing_config_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("nonexistent.toml");

        let config = load_config_from_path(&config_path);
        assert!(config.is_none());
    }

    #[test]
    fn test_invalid_toml() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        fs::write(&config_path, "invalid toml content [[[").unwrap();

        let config = load_config_from_path(&config_path);
        assert!(config.is_none());
    }
}
