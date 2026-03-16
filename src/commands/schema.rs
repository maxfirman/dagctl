use anyhow::Result;
use std::fs;
use std::process::Command;

pub fn download_schema(token: &str, api_url: &str) -> Result<()> {
    fs::create_dir_all("schemas")?;

    let output = Command::new("cynic")
        .arg("introspect")
        .arg(api_url)
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
