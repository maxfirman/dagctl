use anyhow::Result;
use std::fs;
use std::io::Write;

const SKILL_CONTENT: &str = r#"---
name: dagctl
description: Interact with Dagster Cloud using the dagctl CLI. Use when querying runs, assets, jobs, code locations, or managing Dagster infrastructure from the terminal.
---

## Overview

dagctl is a CLI for the Dagster Cloud GraphQL API. It queries runs, events, logs, assets, jobs, and code locations.

## Authentication

dagctl needs an API token, organization name, and optionally a deployment name. Resolution priority: CLI flag → environment variable → config file.

### Config file (`~/.dagctl/config.toml`)

```toml
token = "your-api-token"
organization = "your-org"
deployment = "prod"
```

### Environment variables

| Variable | Purpose |
|----------|---------|
| `DAGSTER_API_TOKEN` | API token |
| `DAGSTER_ORGANIZATION` | Organization name |
| `DAGSTER_DEPLOYMENT` | Deployment name |

### CLI flags

```
dagctl --token <TOKEN> --organization <ORG> --deployment <DEPLOYMENT> <command>
```

## Output Formats

All `get` commands support `-o` / `--output` to change format:

- Default: formatted table
- `-o json`: JSON (pipe to `jq` for filtering)
- `-o yaml`: YAML

```bash
dagctl get runs -o json | jq '[.[] | select(.status == "FAILURE")]'
```

## Commands

### Runs

```bash
# List runs (default limit applies)
dagctl get runs

# With limit
dagctl get runs --limit 10

# Filter by status (comma-separated: queued, not-started, managed, starting, started, success, failure, canceling, canceled)
dagctl get runs --status failure
dagctl get runs --status failure,canceled

# Filter by job name
dagctl get runs --job my_job

# Filter by who launched the run
dagctl get runs --launched-by user@example.com

# Filter by partition
dagctl get runs --partition 2026-03-17

# Filter by arbitrary tags (key=value, comma-separated)
dagctl get runs --tags env=prod,team=data

# Combine filters
dagctl get runs --status failure --job my_job --limit 10
```

### Run Details

```bash
# Get details for a specific run (includes tags)
dagctl get run <RUN_ID>

# Get all events for a run
dagctl get run-events <RUN_ID>

# Get captured logs for a run (stdout/stderr)
dagctl get run-logs <RUN_ID>
```

### Code Locations

```bash
# List all code locations
dagctl get code-locations

# Get details for a specific code location
dagctl get code-location <NAME>
```

### Jobs

```bash
# List all jobs across all code locations
dagctl get jobs

# List jobs in a specific code location
dagctl get jobs --code-location <NAME>

# Get details for a specific job (--code-location is required)
dagctl get job <NAME> --code-location <LOC>
```

### Assets

```bash
# List all assets (includes health status)
dagctl get assets

# Filter by group
dagctl get assets --group <GROUP>

# Filter by code location
dagctl get assets --code-location <NAME>

# Filter by health status (comma-separated: healthy, warning, degraded, unknown, not-applicable)
dagctl get assets --health degraded,warning
```

### Asset Details

```bash
# Get details for a specific asset (slash-separated key)
dagctl get asset <KEY>
dagctl get asset my_prefix/my_asset
```

### Asset Events

```bash
# Get event history (materializations, observations, failures)
dagctl get asset-events <KEY>
dagctl get asset-events <KEY> --limit 20

# Filter by event type (comma-separated: materialization, observation, failed-to-materialize)
dagctl get asset-events <KEY> --type materialization

# Filter by status (comma-separated: success, failure)
dagctl get asset-events <KEY> --status failure

# Filter by partition
dagctl get asset-events <KEY> --partition 2026-03-17

# Get detail for a specific event (use timestamp from asset-events output)
dagctl get asset-event <KEY> <TIMESTAMP>
```

### Asset Partitions

```bash
# Get partition status summary
dagctl get asset-partitions <KEY>
```

### Asset Checks

```bash
# List asset checks with latest execution status
dagctl get asset-checks <KEY>

# Get details for a specific asset check
dagctl get asset-check <KEY> <CHECK_NAME>

# List historic executions for an asset check
dagctl get asset-check-executions <KEY> <CHECK_NAME>
dagctl get asset-check-executions <KEY> <CHECK_NAME> --limit 20

# Filter by status (comma-separated: in-progress,succeeded,failed,execution-failed,skipped)
dagctl get asset-check-executions <KEY> <CHECK_NAME> --status failed
dagctl get asset-check-executions <KEY> <CHECK_NAME> --status failed,execution-failed
```

### Schema Management

```bash
# Download the Dagster GraphQL schema (for building from source)
dagctl schema download
```

### Diagnostics

```bash
# Print version, config, and API connectivity info
dagctl debug
```

### Self Management

```bash
# Update dagctl to the latest release
dagctl self update

# Generate a Kiro SKILL.md file (prints to stdout)
dagctl self skill

# Install the SKILL.md to ~/.kiro/skills/dagctl/
dagctl self skill --install
```

### Shell Completion

```bash
dagctl completion bash
dagctl completion zsh
dagctl completion fish
```

## Common Patterns

```bash
# Check recent failures
dagctl get runs --status failure --limit 5

# Get logs for a failed run
dagctl get runs --status failure --limit 1 -o json | jq -r '.[0].runId' | xargs dagctl get run-logs

# List degraded assets
dagctl get assets --health degraded

# Check asset materialization history
dagctl get asset-events my_prefix/my_asset --type materialization --limit 10

# Extract stdout from run logs
dagctl get run-logs <RUN_ID> -o json | jq -r '.stdout'
```
"#;

pub fn run_skill(install: bool) -> Result<()> {
    if install {
        let home = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE"))?;
        let dir = std::path::PathBuf::from(home).join(".kiro/skills/dagctl");
        fs::create_dir_all(&dir)?;
        let path = dir.join("SKILL.md");
        fs::File::create(&path)?.write_all(SKILL_CONTENT.as_bytes())?;
        eprintln!("Installed SKILL.md to {}", path.display());
    } else {
        print!("{SKILL_CONTENT}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_content_has_valid_frontmatter() {
        let content = SKILL_CONTENT.trim();
        assert!(content.starts_with("---\n"), "must start with YAML frontmatter delimiter");
        let end = content[4..].find("\n---\n").expect("must have closing frontmatter delimiter");
        let frontmatter = &content[4..4 + end];
        assert!(frontmatter.contains("name:"), "frontmatter must contain name field");
        assert!(frontmatter.contains("description:"), "frontmatter must contain description field");
    }
}
