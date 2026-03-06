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
    
    anyhow::bail!("No authentication token provided. Use --token flag, DAGSTER_API_TOKEN env var, or ~/.dagster-cli/config.toml")
}
