use anyhow::Result;
use std::fs;
use std::process::Command;

const DAGSTER_API_URL: &str = "https://troweprice.dagster.cloud/prod/graphql";

fn get_api_url() -> String {
    std::env::var("DAGSTER_API_URL").unwrap_or_else(|_| DAGSTER_API_URL.to_string())
}

pub fn download_schema(token: &str) -> Result<()> {
    fs::create_dir_all("schemas")?;

    let output = Command::new("cynic")
        .arg("introspect")
        .arg(get_api_url())
        .arg("-H")
        .arg(format!("Authorization: Bearer {}", token))
        .arg("-o")
        .arg("schemas/dagster.graphql")
        .output()?;

    if !output.status.success() {
        anyhow::bail!(
            "Failed to download schema: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    println!("Schema downloaded successfully to schemas/dagster.graphql");
    Ok(())
}
