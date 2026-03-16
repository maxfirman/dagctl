use anyhow::Result;
use self_update::cargo_crate_version;

pub fn run_update() -> Result<()> {
    let status = self_update::backends::github::Update::configure()
        .repo_owner("maxfirman")
        .repo_name("dagctl")
        .bin_name("dagctl")
        .show_download_progress(true)
        .current_version(cargo_crate_version!())
        .build()?
        .update()?;
    println!("Updated to version: {}", status.version());
    Ok(())
}
