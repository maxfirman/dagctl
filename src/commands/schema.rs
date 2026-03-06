use anyhow::Result;
use std::fs;
use std::process::Command;

const DAGSTER_API_URL: &str = "https://troweprice.dagster.cloud/prod/graphql";

pub fn download_schema(token: &str) -> Result<()> {
    fs::create_dir_all("schemas")?;

    let output = Command::new("cynic")
        .arg("introspect")
        .arg(DAGSTER_API_URL)
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
