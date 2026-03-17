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
    match status {
        "Success" => Color::Green,
        "Failure" => Color::Red,
        "Canceled" | "Canceling" => Color::Yellow,
        "Started" | "Starting" => Color::Cyan,
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

pub fn format_runs_table(runs: &[(String, String, String, Option<f64>, Option<f64>)]) {
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
        let lib_lines: Vec<String> = libraries
            .iter()
            .map(|(n, v)| format!("{n} {v}"))
            .collect();
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
