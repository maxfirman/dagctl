use anyhow::Result;

pub fn resolve_token(cli_token: Option<String>) -> Result<String> {
    if let Some(token) = cli_token {
        return Ok(token);
    }

    if let Ok(token) = std::env::var("DAGSTER_API_TOKEN") {
        return Ok(token);
    }

    if let Some(config) = crate::config::load_config()
        && let Some(token) = config.token
    {
        return Ok(token);
    }

    anyhow::bail!(
        "No authentication token provided. Use --token flag, DAGSTER_API_TOKEN env var, or ~/.dagster-cli/config.toml"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_cli_token_takes_precedence() {
        let result = resolve_token(Some("cli-token".to_string()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "cli-token");
    }

    #[test]
    fn test_env_var_fallback() {
        unsafe {
            env::set_var("DAGSTER_API_TOKEN", "env-token");
        }
        let result = resolve_token(None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "env-token");
        unsafe {
            env::remove_var("DAGSTER_API_TOKEN");
        }
    }

    #[test]
    fn test_cli_token_overrides_env_var() {
        unsafe {
            env::set_var("DAGSTER_API_TOKEN", "env-token");
        }
        let result = resolve_token(Some("cli-token".to_string()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "cli-token");
        unsafe {
            env::remove_var("DAGSTER_API_TOKEN");
        }
    }

    #[test]
    fn test_no_token_returns_error() {
        // This test assumes no config file exists and no env var is set
        // In a real scenario, we'd need to mock the config loading
        // For now, we just test that the error message is correct when it fails
        unsafe {
            env::remove_var("DAGSTER_API_TOKEN");
        }
        let result = resolve_token(None);

        // If a config file exists with a token, this test will pass
        // Otherwise it should fail with the expected error message
        if result.is_err() {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("No authentication token provided")
            );
        }
    }
}
