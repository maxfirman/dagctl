use anyhow::Result;
use self_update::cargo_crate_version;

pub fn run_update(github_url: Option<String>) -> Result<()> {
    let mut builder = self_update::backends::github::Update::configure();
    builder
        .repo_owner("maxfirman")
        .repo_name("dagctl")
        .bin_name("dagctl")
        .show_download_progress(true)
        .current_version(cargo_crate_version!());
    if let Some(ref url) = github_url {
        builder.with_url(url.trim_end_matches('/'));
    }
    let status = builder.build()?.update()?;
    println!("Updated to version: {}", status.version());
    Ok(())
}
