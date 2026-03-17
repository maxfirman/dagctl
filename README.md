# dagctl

A CLI for interacting with the Dagster Cloud GraphQL API. Query runs, events, logs, and code locations from your terminal.

## Installation

### Download a release binary

Download the latest binary for your platform from [GitHub Releases](https://github.com/maxfirman/dagctl/releases) and place it on your `PATH`.

**macOS / Linux:**
```bash
# Example for macOS ARM (Apple Silicon)
curl -L https://github.com/maxfirman/dagctl/releases/latest/download/dagctl-v<VERSION>-aarch64-apple-darwin.tar.gz | tar xz
sudo mv dagctl /usr/local/bin/
```

Available targets:
| Platform | Target |
|----------|--------|
| macOS (Apple Silicon) | `aarch64-apple-darwin` |
| macOS (Intel) | `x86_64-apple-darwin` |
| Linux (x86_64) | `x86_64-unknown-linux-gnu` |
| Linux (ARM64) | `aarch64-unknown-linux-gnu` |
| Windows (x86_64) | `x86_64-pc-windows-msvc` |

### Build from source

Requires [Rust](https://rustup.rs/) and `cynic-cli`:

```bash
cargo install --locked cynic-cli
```

Clone and build:
```bash
git clone https://github.com/maxfirman/dagctl.git
cd dagctl
cargo build --release
# Binary at ./target/release/dagctl
```

> **Note:** The project requires the Dagster GraphQL schema at `schemas/dagster.graphql` to compile. If building from source for the first time, you may need to download it — see [Schema Management](#schema-management).

## Getting Started

### 1. Configure authentication

dagctl needs a Dagster Cloud API token and your organization name. The quickest way is a config file:

```bash
mkdir -p ~/.dagctl
cat > ~/.dagctl/config.toml << 'EOF'
token = "your-api-token-here"
organization = "your-org"
deployment = "prod"
EOF
```

Alternatively, use environment variables:
```bash
export DAGSTER_API_TOKEN="your-api-token-here"
export DAGSTER_ORGANIZATION="your-org"
export DAGSTER_DEPLOYMENT="prod"
```

Or pass flags directly:
```bash
dagctl --token <TOKEN> --organization <ORG> --deployment prod get runs
```

Authentication is resolved in priority order: CLI flag → environment variable → config file.

### 2. Verify it works

```bash
dagctl get runs --limit 5
```

## Updating

dagctl can update itself to the latest release:

```bash
dagctl self update
```

## Shell Completion

Generate completions for your shell:

**Bash:**
```bash
dagctl completion bash > ~/.local/share/bash-completion/completions/dagctl
```

**Zsh:**
```bash
dagctl completion zsh > ~/.zfunc/_dagctl
# Ensure ~/.zfunc is in fpath (add to .zshrc):
#   fpath=(~/.zfunc $fpath)
#   autoload -Uz compinit && compinit
```

**Zsh (Oh My Zsh):**
```bash
mkdir -p ~/.oh-my-zsh/completions
dagctl completion zsh > ~/.oh-my-zsh/completions/_dagctl
exec zsh
```

**Fish:**
```bash
dagctl completion fish > ~/.config/fish/completions/dagctl.fish
```

## Usage

### Runs

```bash
# List runs
dagctl get runs

# List runs with a limit
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

# Get details for a specific run (includes tags)
dagctl get run <RUN_ID>

# Get all events for a run
dagctl get run-events <RUN_ID>

# Get captured logs for a run
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

# Get details for a specific job
dagctl get job <NAME> --code-location <LOC>
```

### Assets

```bash
# List all assets (includes health status)
dagctl get assets

# Filter assets by group
dagctl get assets --group <GROUP>

# Filter assets by code location
dagctl get assets --code-location <NAME>

# Filter by health status (comma-separated: healthy, warning, degraded, unknown, not-applicable)
dagctl get assets --health degraded,warning

# Get details for a specific asset (slash-separated key, includes health info)
dagctl get asset <KEY>
dagctl get asset my_prefix/my_asset

# Get event history for an asset (materializations, observations, failures)
dagctl get asset-events <KEY>
dagctl get asset-events <KEY> --limit 20

# Filter events by type, status, or partition
dagctl get asset-events <KEY> --type materialization
dagctl get asset-events <KEY> --status failure
dagctl get asset-events <KEY> --partition 2026-03-17

# Get detail for a specific event (use timestamp from asset-events output)
dagctl get asset-event <KEY> <TIMESTAMP>

# Get partition status summary for an asset
dagctl get asset-partitions <KEY>

# List asset checks with latest execution status
dagctl get asset-checks <KEY>

# Get details for a specific asset check
dagctl get asset-check <KEY> <CHECK_NAME>

# List historic executions for an asset check
dagctl get asset-check-executions <KEY> <CHECK_NAME>
dagctl get asset-check-executions <KEY> <CHECK_NAME> --limit 20
```

### Schema Management

Download or update the GraphQL schema (needed when building from source):

```bash
dagctl schema download --token <TOKEN>
```

Or use `cynic-cli` directly:
```bash
cynic introspect https://<ORG>.dagster.cloud/<DEPLOYMENT>/graphql \
  -H "Authorization: Bearer <TOKEN>" \
  -o schemas/dagster.graphql
```

Rebuild after updating the schema:
```bash
cargo build --release
```

### Debug

Print diagnostic info (API connectivity, version, config):

```bash
dagctl debug
```

## Output

By default, commands display results as formatted tables. Use `-o` to switch format:

```bash
# Default table output
dagctl get runs --limit 5

# JSON output (for scripting/piping)
dagctl get runs --limit 5 -o json

# YAML output
dagctl get runs --limit 5 -o yaml
```

JSON output can be piped to `jq` for filtering:

```bash
# Pretty-print runs
dagctl get runs -o json | jq .

# Get only failed runs
dagctl get runs -o json | jq '[.[] | select(.status == "FAILURE")]'

# Extract stdout from logs
dagctl get run-logs <RUN_ID> -o json | jq -r '.stdout'
```

## Configuration Reference

### Config file (`~/.dagctl/config.toml`)

```toml
token = "your-api-token"
organization = "your-org"
deployment = "prod"          # optional
```

### Environment variables

| Variable | Purpose |
|----------|---------|
| `DAGSTER_API_TOKEN` | API token |
| `DAGSTER_ORGANIZATION` | Dagster Cloud organization name |
| `DAGSTER_DEPLOYMENT` | Deployment name (e.g., `prod`) |

## Development

See [TESTING.md](TESTING.md) for test strategy and coverage details.

```bash
# Run tests
cargo test

# Run with coverage
./coverage.sh
```

## License

[Apache License 2.0](LICENSE)
