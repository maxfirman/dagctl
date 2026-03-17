use anyhow::Result;
use chrono::{DateTime, Utc};
use comfy_table::{Cell, CellAlignment, Color, Table, presets::UTF8_FULL};
use serde::Serialize;

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum OutputFormat {
    Json,
    Yaml,
}

pub fn render<T: Serialize>(value: &T, format: &OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(value)?),
        OutputFormat::Yaml => print!("{}", serde_yaml_ng::to_string(value)?),
    }
    Ok(())
}

pub fn format_timestamp(epoch: Option<f64>) -> String {
    match epoch {
        Some(ts) => DateTime::<Utc>::from_timestamp(ts as i64, 0)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| "-".into()),
        None => "-".into(),
    }
}

fn status_color(status: &str) -> Color {
    let s = status.split_once(' ').map_or(status, |(prefix, _)| prefix);
    match s {
        "Success" | "SUCCEEDED" | "Pass" | "Healthy" => Color::Green,
        "Failure" | "FAILED" | "EXECUTION_FAILED" | "Fail" | "Degraded" => Color::Red,
        "Canceled" | "Canceling" | "SKIPPED" | "Warning" => Color::Yellow,
        "Started" | "Starting" | "IN_PROGRESS" => Color::Cyan,
        "Queued" | "NotStarted" | "Managed" => Color::White,
        _ => Color::White,
    }
}

fn new_table() -> Table {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table
}

// --- get runs ---

pub type RunRow = (String, String, String, Option<f64>, Option<f64>);

pub fn format_runs_table(runs: &[RunRow]) {
    let mut table = new_table();
    table.set_header(vec!["RUN ID", "JOB", "STATUS", "START", "END"]);
    for (run_id, job, status, start, end) in runs {
        table.add_row(vec![
            Cell::new(run_id),
            Cell::new(job),
            Cell::new(status).fg(status_color(status)),
            Cell::new(format_timestamp(*start)),
            Cell::new(format_timestamp(*end)),
        ]);
    }
    println!("{table}");
}

// --- get run (detail) ---

pub fn format_run_detail(
    run_id: &str,
    job: &str,
    status: &str,
    start: Option<f64>,
    end: Option<f64>,
    config_yaml: &str,
) {
    let mut table = new_table();
    table.set_header(vec![
        Cell::new("Field").set_alignment(CellAlignment::Right),
        Cell::new("Value"),
    ]);
    table.add_row(vec![Cell::new("Run ID"), Cell::new(run_id)]);
    table.add_row(vec![Cell::new("Job"), Cell::new(job)]);
    table.add_row(vec![
        Cell::new("Status"),
        Cell::new(status).fg(status_color(status)),
    ]);
    table.add_row(vec![Cell::new("Start"), Cell::new(format_timestamp(start))]);
    table.add_row(vec![Cell::new("End"), Cell::new(format_timestamp(end))]);
    table.add_row(vec![Cell::new("Config"), Cell::new(config_yaml)]);
    println!("{table}");
}

// --- get code-locations ---

pub fn format_code_locations_table(locations: &[(String, String, f64)]) {
    let mut table = new_table();
    table.set_header(vec!["NAME", "STATUS", "UPDATED"]);
    for (name, status, ts) in locations {
        table.add_row(vec![
            Cell::new(name),
            Cell::new(status),
            Cell::new(format_timestamp(Some(*ts))),
        ]);
    }
    println!("{table}");
}

// --- get code-location (detail) ---

pub fn format_code_location_detail(
    name: &str,
    status: &str,
    updated: f64,
    repos: &[(String, usize, usize, usize)],
    libraries: &[(String, String)],
) {
    let mut table = new_table();
    table.set_header(vec![
        Cell::new("Field").set_alignment(CellAlignment::Right),
        Cell::new("Value"),
    ]);
    table.add_row(vec![Cell::new("Name"), Cell::new(name)]);
    table.add_row(vec![Cell::new("Status"), Cell::new(status)]);
    table.add_row(vec![
        Cell::new("Updated"),
        Cell::new(format_timestamp(Some(updated))),
    ]);

    if !repos.is_empty() {
        let repo_lines: Vec<String> = repos
            .iter()
            .map(|(n, jobs, scheds, sensors)| {
                format!("{n} ({jobs} jobs, {scheds} schedules, {sensors} sensors)")
            })
            .collect();
        table.add_row(vec![
            Cell::new("Repositories"),
            Cell::new(repo_lines.join("\n")),
        ]);
    }

    if !libraries.is_empty() {
        let lib_lines: Vec<String> = libraries.iter().map(|(n, v)| format!("{n} {v}")).collect();
        table.add_row(vec![
            Cell::new("Libraries"),
            Cell::new(lib_lines.join("\n")),
        ]);
    }

    println!("{table}");
}

// --- events ---

pub fn format_events_table(events: &[(String, String, String, String, String)]) {
    let mut table = new_table();
    table.set_header(vec!["TIMESTAMP", "TYPE", "LEVEL", "STEP", "MESSAGE"]);
    for (ts, event_type, level, step, message) in events {
        let msg = if message.len() > 80 {
            format!("{}…", &message[..79])
        } else {
            message.clone()
        };
        table.add_row(vec![
            Cell::new(ts),
            Cell::new(event_type),
            Cell::new(level),
            Cell::new(step),
            Cell::new(msg),
        ]);
    }
    println!("{table}");
}

// --- logs ---

pub fn format_logs_raw(stdout: Option<&str>, stderr: Option<&str>) {
    println!("=== STDOUT ===");
    println!("{}", stdout.unwrap_or(""));
    println!("=== STDERR ===");
    println!("{}", stderr.unwrap_or(""));
}

// --- get jobs ---

pub fn format_jobs_table(jobs: &[(String, String, usize, usize)]) {
    let mut table = new_table();
    table.set_header(vec!["JOB", "CODE LOCATION", "SCHEDULES", "SENSORS"]);
    for (name, location, schedules, sensors) in jobs {
        table.add_row(vec![
            Cell::new(name),
            Cell::new(location),
            Cell::new(schedules),
            Cell::new(sensors),
        ]);
    }
    println!("{table}");
}

// --- get job (detail) ---

pub fn format_job_detail(
    name: &str,
    code_location: &str,
    description: &str,
    schedules: &[String],
    sensors: &[String],
    tags: &[String],
) {
    let mut table = new_table();
    table.set_header(vec![
        Cell::new("Field").set_alignment(CellAlignment::Right),
        Cell::new("Value"),
    ]);
    table.add_row(vec![Cell::new("Name"), Cell::new(name)]);
    table.add_row(vec![Cell::new("Code Location"), Cell::new(code_location)]);
    if !description.is_empty() {
        table.add_row(vec![Cell::new("Description"), Cell::new(description)]);
    }
    if !schedules.is_empty() {
        table.add_row(vec![
            Cell::new("Schedules"),
            Cell::new(schedules.join("\n")),
        ]);
    }
    if !sensors.is_empty() {
        table.add_row(vec![Cell::new("Sensors"), Cell::new(sensors.join("\n"))]);
    }
    if !tags.is_empty() {
        table.add_row(vec![Cell::new("Tags"), Cell::new(tags.join("\n"))]);
    }
    println!("{table}");
}

// --- get assets ---

pub fn format_assets_table(assets: &[(String, String, String, String, String, String)]) {
    let mut table = new_table();
    table.set_header(vec![
        "ASSET KEY",
        "GROUP",
        "CODE LOCATION",
        "KIND",
        "HEALTH",
        "INFO",
    ]);
    for (key, group, location, kinds, health, info) in assets {
        table.add_row(vec![
            Cell::new(key),
            Cell::new(group),
            Cell::new(location),
            Cell::new(kinds),
            Cell::new(health).fg(status_color(health)),
            Cell::new(info),
        ]);
    }
    println!("{table}");
}

// --- get asset (detail) ---

pub struct AssetDetail<'a> {
    pub key: &'a str,
    pub group: &'a str,
    pub code_location: &'a str,
    pub description: &'a str,
    pub kinds: &'a [String],
    pub partitioned: bool,
    pub computed_by: &'a str,
    pub code_version: &'a str,
    pub dependencies: &'a [String],
    pub dependents: &'a [String],
    pub jobs: &'a [String],
    pub owners: &'a [String],
    pub automation_condition: &'a str,
    pub sensors: &'a [String],
    pub schedules: &'a [String],
    pub tags: &'a [String],
    pub metadata: &'a [(String, String)],
    pub health: &'a str,
    pub health_materialization: &'a str,
    pub health_checks: &'a str,
    pub health_freshness: &'a str,
}

pub fn format_asset_detail(detail: &AssetDetail) {
    let mut table = new_table();
    table.set_header(vec![
        Cell::new("Field").set_alignment(CellAlignment::Right),
        Cell::new("Value"),
    ]);
    table.add_row(vec![Cell::new("Key"), Cell::new(detail.key)]);
    table.add_row(vec![Cell::new("Group"), Cell::new(detail.group)]);
    table.add_row(vec![
        Cell::new("Code Location"),
        Cell::new(detail.code_location),
    ]);
    if !detail.health.is_empty() {
        table.add_row(vec![
            Cell::new("Health"),
            Cell::new(detail.health).fg(status_color(detail.health)),
        ]);
        table.add_row(vec![
            Cell::new("  Materialization"),
            Cell::new(detail.health_materialization)
                .fg(status_color(detail.health_materialization)),
        ]);
        table.add_row(vec![
            Cell::new("  Checks"),
            Cell::new(detail.health_checks).fg(status_color(detail.health_checks)),
        ]);
        table.add_row(vec![
            Cell::new("  Freshness"),
            Cell::new(detail.health_freshness).fg(status_color(detail.health_freshness)),
        ]);
    }
    if !detail.description.is_empty() {
        table.add_row(vec![
            Cell::new("Description"),
            Cell::new(detail.description),
        ]);
    }
    if !detail.kinds.is_empty() {
        table.add_row(vec![Cell::new("Kinds"), Cell::new(detail.kinds.join(", "))]);
    }
    table.add_row(vec![
        Cell::new("Partitioned"),
        Cell::new(if detail.partitioned { "Yes" } else { "No" }),
    ]);
    if !detail.computed_by.is_empty() {
        table.add_row(vec![
            Cell::new("Computed By"),
            Cell::new(detail.computed_by),
        ]);
    }
    if !detail.code_version.is_empty() {
        table.add_row(vec![
            Cell::new("Code Version"),
            Cell::new(detail.code_version),
        ]);
    }
    if !detail.owners.is_empty() {
        table.add_row(vec![
            Cell::new("Owners"),
            Cell::new(detail.owners.join("\n")),
        ]);
    }
    if !detail.tags.is_empty() {
        table.add_row(vec![Cell::new("Tags"), Cell::new(detail.tags.join("\n"))]);
    }
    if !detail.metadata.is_empty() {
        let lines: Vec<String> = detail
            .metadata
            .iter()
            .map(|(k, v)| {
                if v.is_empty() {
                    k.clone()
                } else {
                    format!("{k}: {v}")
                }
            })
            .collect();
        table.add_row(vec![Cell::new("Metadata"), Cell::new(lines.join("\n"))]);
    }
    if !detail.jobs.is_empty() {
        table.add_row(vec![Cell::new("Jobs"), Cell::new(detail.jobs.join("\n"))]);
    }
    if !detail.dependencies.is_empty() {
        table.add_row(vec![
            Cell::new("Dependencies"),
            Cell::new(detail.dependencies.join("\n")),
        ]);
    }
    if !detail.dependents.is_empty() {
        table.add_row(vec![
            Cell::new("Dependents"),
            Cell::new(detail.dependents.join("\n")),
        ]);
    }
    if !detail.automation_condition.is_empty() {
        table.add_row(vec![
            Cell::new("Automation"),
            Cell::new(detail.automation_condition),
        ]);
    }
    if !detail.sensors.is_empty() {
        table.add_row(vec![
            Cell::new("Sensors"),
            Cell::new(detail.sensors.join("\n")),
        ]);
    }
    if !detail.schedules.is_empty() {
        table.add_row(vec![
            Cell::new("Schedules"),
            Cell::new(detail.schedules.join("\n")),
        ]);
    }
    println!("{table}");
}

// --- get asset-events ---

pub fn format_asset_events_table(events: &[(String, String, String, String, String, String)]) {
    let mut table = new_table();
    table.set_header(vec![
        "TIMESTAMP",
        "TYPE",
        "STATUS",
        "RUN ID",
        "PARTITION",
        "MESSAGE",
    ]);
    for (ts, event_type, status, run_id, partition, message) in events {
        let msg = if message.len() > 80 {
            format!("{}…", &message[..79])
        } else {
            message.clone()
        };
        table.add_row(vec![
            Cell::new(ts),
            Cell::new(event_type),
            Cell::new(status).fg(status_color(status)),
            Cell::new(run_id),
            Cell::new(partition),
            Cell::new(msg),
        ]);
    }
    println!("{table}");
}

// --- get asset-partitions ---

pub fn format_asset_partitions_table(
    total: i32,
    materialized: i32,
    failed: i32,
    materializing: i32,
) {
    let mut table = new_table();
    table.set_header(vec![
        Cell::new("Field").set_alignment(CellAlignment::Right),
        Cell::new("Value"),
    ]);
    table.add_row(vec![Cell::new("Total"), Cell::new(total)]);
    table.add_row(vec![Cell::new("Materialized"), Cell::new(materialized)]);
    table.add_row(vec![Cell::new("Failed"), Cell::new(failed)]);
    table.add_row(vec![Cell::new("Materializing"), Cell::new(materializing)]);
    println!("{table}");
}

// --- get asset-checks ---

pub fn format_asset_checks_table(checks: &[(String, String, String, String, String, String)]) {
    let mut table = new_table();
    table.set_header(vec![
        "NAME",
        "BLOCKING",
        "LATEST STATUS",
        "LATEST RUN ID",
        "LATEST TIMESTAMP",
        "DESCRIPTION",
    ]);
    for (name, blocking, status, run_id, ts, description) in checks {
        table.add_row(vec![
            Cell::new(name),
            Cell::new(blocking),
            Cell::new(status).fg(status_color(status)),
            Cell::new(run_id),
            Cell::new(ts),
            Cell::new(description),
        ]);
    }
    println!("{table}");
}

// --- get asset-check (detail) ---

pub struct AssetCheckDetail<'a> {
    pub name: &'a str,
    pub description: &'a str,
    pub blocking: bool,
    pub jobs: &'a [String],
    pub can_execute_individually: &'a str,
    pub automation_condition: &'a str,
    pub severity: &'a str,
    pub latest_status: &'a str,
    pub latest_run_id: &'a str,
    pub latest_timestamp: &'a str,
    pub latest_result: &'a str,
}

pub fn format_asset_check_detail(detail: &AssetCheckDetail) {
    let mut table = new_table();
    table.set_header(vec![
        Cell::new("Field").set_alignment(CellAlignment::Right),
        Cell::new("Value"),
    ]);
    table.add_row(vec![Cell::new("Name"), Cell::new(detail.name)]);
    if !detail.description.is_empty() {
        table.add_row(vec![
            Cell::new("Description"),
            Cell::new(detail.description),
        ]);
    }
    table.add_row(vec![
        Cell::new("Blocking"),
        Cell::new(if detail.blocking { "Yes" } else { "No" }),
    ]);
    if !detail.jobs.is_empty() {
        table.add_row(vec![Cell::new("Jobs"), Cell::new(detail.jobs.join(", "))]);
    }
    table.add_row(vec![
        Cell::new("Can Execute Individually"),
        Cell::new(detail.can_execute_individually),
    ]);
    if !detail.automation_condition.is_empty() {
        table.add_row(vec![
            Cell::new("Automation"),
            Cell::new(detail.automation_condition),
        ]);
    }
    if !detail.severity.is_empty() {
        table.add_row(vec![Cell::new("Severity"), Cell::new(detail.severity)]);
    }
    if !detail.latest_status.is_empty() {
        table.add_row(vec![
            Cell::new("Latest Status"),
            Cell::new(detail.latest_status).fg(status_color(detail.latest_status)),
        ]);
        if !detail.latest_result.is_empty() {
            table.add_row(vec![
                Cell::new("Latest Result"),
                Cell::new(detail.latest_result).fg(status_color(detail.latest_result)),
            ]);
        }
        table.add_row(vec![
            Cell::new("Latest Run ID"),
            Cell::new(detail.latest_run_id),
        ]);
        table.add_row(vec![
            Cell::new("Latest Timestamp"),
            Cell::new(detail.latest_timestamp),
        ]);
    }
    println!("{table}");
}

// --- get asset-check-executions ---

pub fn format_asset_check_executions_table(
    executions: &[(String, String, String, String, String)],
) {
    let mut table = new_table();
    table.set_header(vec![
        "TIMESTAMP",
        "STATUS",
        "RUN ID",
        "PARTITION",
        "SEVERITY",
    ]);
    for (ts, status, run_id, partition, severity) in executions {
        table.add_row(vec![
            Cell::new(ts),
            Cell::new(status).fg(status_color(status)),
            Cell::new(run_id),
            Cell::new(partition),
            Cell::new(severity),
        ]);
    }
    println!("{table}");
}
