use anyhow::Result;
use colored::Colorize;

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "CloudQuery")]
#[cynic(schema_module = "crate::schema::schema")]
struct VersionQuery {
    version: String,
}

pub async fn run_debug(
    token: &str,
    organization: &str,
    deployment: Option<&str>,
    api_url: &str,
) -> Result<()> {
    let w = 15;
    let label = |s: &str| format!("{:<w$}", s).bold();
    println!(
        "{} {}",
        label("CLI Version:"),
        env!("CARGO_PKG_VERSION").cyan()
    );
    println!("{} {}", label("Organization:"), organization.cyan());
    println!(
        "{} {}",
        label("Deployment:"),
        deployment.unwrap_or("None").cyan()
    );
    println!("{} {}", label("API URL:"), api_url.cyan());

    match fetch_version(token, api_url).await {
        Ok(version) => {
            println!("{} {}", label("Connection:"), "✓ Connected".green());
            println!("{} {}", label("Cloud Version:"), version.cyan());
        }
        Err(e) => {
            println!(
                "{} {} {}",
                label("Connection:"),
                "✗ Failed:".red(),
                e.to_string().red()
            );
        }
    }

    Ok(())
}

async fn fetch_version(token: &str, api_url: &str) -> Result<String> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let operation = VersionQuery::build(());

    let client = reqwest::Client::new();
    let response = client
        .post(api_url)
        .header("Authorization", format!("Bearer {}", token))
        .run_graphql(operation)
        .await?;

    if let Some(errors) = response.errors {
        anyhow::bail!("GraphQL errors: {:?}", errors);
    }

    let data = response
        .data
        .ok_or_else(|| anyhow::anyhow!("No data in response"))?;

    Ok(data.version)
}
