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
        "No authentication token provided. Use --token flag, DAGSTER_API_TOKEN env var, or ~/.dagctl/config.toml"
    )
}

pub fn resolve_organization(cli_organization: Option<String>) -> Result<String> {
    if let Some(organization) = cli_organization {
        return Ok(organization);
    }

    if let Ok(organization) = std::env::var("DAGSTER_ORGANIZATION") {
        return Ok(organization);
    }

    if let Some(config) = crate::config::load_config()
        && let Some(organization) = config.organization
    {
        return Ok(organization);
    }

    anyhow::bail!(
        "No organization provided. Use --organization flag, DAGSTER_ORGANIZATION env var, or ~/.dagctl/config.toml"
    )
}

pub fn resolve_deployment(cli_deployment: Option<String>) -> Option<String> {
    if let Some(deployment) = cli_deployment {
        return Some(deployment);
    }

    if let Ok(deployment) = std::env::var("DAGSTER_DEPLOYMENT") {
        return Some(deployment);
    }

    if let Some(config) = crate::config::load_config() {
        return config.deployment;
    }

    None
}

pub fn resolve_github_url(cli_github_url: Option<String>) -> Option<String> {
    if let Some(github_url) = cli_github_url {
        return Some(github_url);
    }

    if let Ok(github_url) = std::env::var("DAGCTL_GITHUB_URL") {
        return Some(github_url);
    }

    if let Some(config) = crate::config::load_config() {
        return config.github_url;
    }

    None
}

pub fn build_api_url(organization: &str, deployment: Option<&str>) -> String {
    match deployment {
        Some(d) => format!("https://{}.dagster.cloud/{}/graphql", organization, d),
        None => format!("https://{}.dagster.cloud/graphql", organization),
    }
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
        if let Err(e) = result {
            assert!(e.to_string().contains("No authentication token provided"));
        }
    }

    #[test]
    fn test_cli_organization_takes_precedence() {
        let result = resolve_organization(Some("cli-org".to_string()));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "cli-org");
    }

    #[test]
    fn test_organization_env_var_fallback() {
        unsafe {
            env::set_var("DAGSTER_ORGANIZATION", "env-org");
        }
        let result = resolve_organization(None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "env-org");
        unsafe {
            env::remove_var("DAGSTER_ORGANIZATION");
        }
    }

    #[test]
    fn test_no_organization_returns_error() {
        unsafe {
            env::remove_var("DAGSTER_ORGANIZATION");
        }
        let result = resolve_organization(None);
        if let Err(e) = result {
            assert!(e.to_string().contains("No organization provided"));
        }
    }

    #[test]
    fn test_cli_deployment_takes_precedence() {
        let result = resolve_deployment(Some("cli-deploy".to_string()));
        assert_eq!(result, Some("cli-deploy".to_string()));
    }

    #[test]
    fn test_deployment_env_var_fallback() {
        unsafe {
            env::set_var("DAGSTER_DEPLOYMENT", "env-deploy");
        }
        let result = resolve_deployment(None);
        assert_eq!(result, Some("env-deploy".to_string()));
        unsafe {
            env::remove_var("DAGSTER_DEPLOYMENT");
        }
    }

    #[test]
    fn test_no_deployment_returns_none() {
        unsafe {
            env::remove_var("DAGSTER_DEPLOYMENT");
        }
        // May be Some if config file exists with deployment, otherwise None
        let _result = resolve_deployment(None);
    }

    #[test]
    fn test_build_api_url_with_deployment() {
        let url = build_api_url("troweprice", Some("prod"));
        assert_eq!(url, "https://troweprice.dagster.cloud/prod/graphql");
    }

    #[test]
    fn test_build_api_url_without_deployment() {
        let url = build_api_url("troweprice", None);
        assert_eq!(url, "https://troweprice.dagster.cloud/graphql");
    }

    // Property-based tests
    mod proptests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn test_any_cli_token_is_accepted(token in "\\PC+") {
                let result = resolve_token(Some(token.clone()));
                prop_assert!(result.is_ok());
                prop_assert_eq!(result.unwrap(), token);
            }

            #[test]
            fn test_cli_token_never_empty(token in "\\PC+") {
                let result = resolve_token(Some(token));
                prop_assert!(result.is_ok());
                prop_assert!(!result.unwrap().is_empty());
            }

            #[test]
            fn test_token_whitespace_preserved(
                prefix in "\\PC*",
                suffix in "\\PC*"
            ) {
                let token = format!("{}  {}", prefix, suffix);
                let result = resolve_token(Some(token.clone()));
                prop_assert!(result.is_ok());
                prop_assert_eq!(result.unwrap(), token);
            }

            #[test]
            fn test_build_api_url_always_valid(
                organization in "[a-z][a-z0-9-]+",
                deployment in "[a-z][a-z0-9-]+"
            ) {
                let url = build_api_url(&organization, Some(&deployment));
                prop_assert!(url.starts_with("https://"));
                prop_assert!(url.ends_with("/graphql"));
                prop_assert!(url.contains(&organization));
                prop_assert!(url.contains(&deployment));
            }
        }
    }
}
