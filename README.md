# Dagster CLI

A Rust CLI tool for interacting with the Dagster GraphQL API.

## Prerequisites

Install `cynic-cli` for schema management:

```bash
cargo install --locked cynic-cli
```

## Setup

### 1. Download the Dagster GraphQL Schema

**Important**: This project requires the actual Dagster GraphQL schema to compile. You must download it first using your Dagster API token.

Option A - Use cynic-cli directly:
```bash
cynic introspect https://troweprice.dagster.cloud/prod/graphql \
  -H "Authorization: Bearer <YOUR_TOKEN>" \
  -o schemas/dagster.graphql
```

Option B - Build and use the CLI's schema command:
```bash
# First build (will have warnings about placeholder schema)
cargo build

# Download schema
./target/debug/dagster-cli schema download --token <YOUR_TOKEN>
```

### 2. Build the Project

After downloading the real schema:

```bash
cargo build --release
```

The binary will be at `./target/release/dagster-cli`

## Authentication

The CLI supports multiple authentication methods (in priority order):

1. **CLI flag**: `--token <TOKEN>`
2. **Environment variable**: `DAGSTER_API_TOKEN`
3. **Config file**: `~/.dagster-cli/config.toml`

Example config file:
```toml
token = "your-api-token-here"
```

## Configuration

### API URL

By default, the CLI connects to `https://troweprice.dagster.cloud/prod/graphql`. You can override this with the `DAGSTER_API_URL` environment variable:

```bash
export DAGSTER_API_URL=https://your-instance.dagster.cloud/graphql
dagctl get runs
```

This is useful for:
- Testing against different Dagster instances
- Local development with Dagster
- Integration testing with mock servers

## Usage

### Schema Management

Download or update the GraphQL schema:
```bash
dagster-cli schema download --token <YOUR_TOKEN>
```

After downloading a new schema, rebuild the project:
```bash
cargo build --release
```

### Resource Commands

List all runs:
```bash
dagctl get runs
```

List runs with a limit:
```bash
dagctl get runs --limit 10
```

Get details for a specific run:
```bash
dagctl get run <RUN_ID>
```

Get all events for a specific run:
```bash
dagctl events <RUN_ID>
```

Get captured logs for a specific run:
```bash
dagctl logs <RUN_ID>
```

### Using Environment Variable

```bash
export DAGSTER_API_TOKEN=<YOUR_TOKEN>
dagctl get runs
```

### Using Config File

Create `~/.dagster-cli/config.toml`:
```toml
token = "your-api-token-here"
```

Then run commands without the `--token` flag:
```bash
dagctl get runs
```

## Output Format

All commands output JSON to stdout. Errors are printed to stderr and the CLI exits with a non-zero status code.

Example output from `get runs`:
```json
[
  {
    "run_id": "abc123",
    "job_name": "my_job",
    "status": "Success",
    "start_time": 1234567890.0,
    "end_time": 1234567900.0
  }
]
```

## Development

See [IMPLEMENTATION_STATUS.md](IMPLEMENTATION_STATUS.md) for implementation details and current status.
