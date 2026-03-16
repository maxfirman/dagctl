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

# Get details for a specific run
dagctl get run <RUN_ID>

# Get all events for a run
dagctl events <RUN_ID>

# Get captured logs for a run
dagctl logs <RUN_ID>
```

### Code Locations

```bash
# List all code locations
dagctl get code-locations

# Get details for a specific code location
dagctl get code-location <NAME>
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

All commands output JSON to stdout. Errors go to stderr with a non-zero exit code. Pipe to `jq` for filtering:

```bash
# Pretty-print runs
dagctl get runs --limit 5 | jq .

# Get only failed runs
dagctl get runs | jq '[.[] | select(.status == "FAILURE")]'

# Extract stdout from logs
dagctl logs <RUN_ID> | jq -r '.stdout'
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
