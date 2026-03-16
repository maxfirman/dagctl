use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize)]
pub struct Config {
    pub token: Option<String>,
    pub organization: Option<String>,
    pub deployment: Option<String>,
}

pub fn load_config() -> Option<Config> {
    let home = std::env::var("HOME").ok()?;
    let path = PathBuf::from(home).join(".dagctl").join("config.toml");
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
    use proptest::prelude::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_parse_valid_config() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        fs::write(&config_path, "token = \"test-token\"").unwrap();

        let config = load_config_from_path(&config_path);
        assert!(config.is_some());
        let c = config.unwrap();
        assert_eq!(c.token, Some("test-token".to_string()));
        assert_eq!(c.organization, None);
        assert_eq!(c.deployment, None);
    }

    #[test]
    fn test_parse_full_config() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        fs::write(
            &config_path,
            "token = \"test-token\"\norganization = \"myorg\"\ndeployment = \"prod\"",
        )
        .unwrap();

        let config = load_config_from_path(&config_path);
        assert!(config.is_some());
        let c = config.unwrap();
        assert_eq!(c.token, Some("test-token".to_string()));
        assert_eq!(c.organization, Some("myorg".to_string()));
        assert_eq!(c.deployment, Some("prod".to_string()));
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

    // Property-based tests
    mod proptests {
        use super::*;
        use proptest::proptest;

        proptest! {
            #[test]
            fn test_any_valid_token_string(token in "[a-zA-Z0-9_-]+") {
                let temp_dir = TempDir::new().unwrap();
                let config_path = temp_dir.path().join("config.toml");
                let content = format!("token = \"{}\"", token);
                fs::write(&config_path, content).unwrap();

                let config = load_config_from_path(&config_path);
                prop_assert!(config.is_some());
                prop_assert_eq!(config.unwrap().token, Some(token));
            }

            #[test]
            fn test_config_with_extra_fields(
                token in "[a-zA-Z0-9_-]+",
                extra_key in "[a-z]+",
                extra_value in "[a-zA-Z0-9_-]+"
            ) {
                let temp_dir = TempDir::new().unwrap();
                let config_path = temp_dir.path().join("config.toml");
                let content = format!(
                    "token = \"{}\"\n{} = \"{}\"",
                    token,
                    extra_key,
                    extra_value
                );
                fs::write(&config_path, content).unwrap();

                let config = load_config_from_path(&config_path);
                // Should still parse even with extra fields
                prop_assert!(config.is_some());
                prop_assert_eq!(config.unwrap().token, Some(token));
            }
        }
    }
}
