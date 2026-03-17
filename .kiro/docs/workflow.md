# Development Workflow

## Prerequisites

```bash
cargo install --locked cynic-cli
cargo install cargo-llvm-cov  # optional, for coverage
```

## Initial Setup

1. Clone the repo
2. Download the schema (requires a valid Dagster API token):
   ```bash
   dagctl schema download --token <TOKEN>
   ```
   Or with cynic directly:
   ```bash
   cynic introspect https://<ORG>.dagster.cloud/<DEPLOYMENT>/graphql \
     -H "Authorization: Bearer <TOKEN>" \
     -o schemas/dagster.graphql
   ```
3. Build:
   ```bash
   cargo build --release
   ```

The schema file must exist before `cargo build` — cynic validates types at compile time.

## Running the CLI

```bash
# With token flag
dagctl --token <TOKEN> --organization <ORG> get runs

# With env vars
export DAGSTER_API_TOKEN=<TOKEN>
export DAGSTER_ORGANIZATION=<ORG>
export DAGSTER_DEPLOYMENT=prod
dagctl get runs --limit 10

# With config file (~/.dagctl/config.toml)
dagctl get run <RUN_ID>
```

## Code Quality

Always run before committing:

```bash
cargo fmt
cargo clippy -- -D warnings
cargo test
```

All three must pass cleanly — no warnings, no failures.

## Testing

```bash
# All tests
cargo test

# Unit tests only
cargo test --lib

# Integration tests only
cargo test --test integration_tests

# With output
cargo test -- --nocapture

# Property tests with more cases
PROPTEST_CASES=1000 cargo test
```

## Coverage

```bash
./coverage.sh
# or
cargo llvm-cov --all-features --workspace --html
# Report at target/llvm-cov/html/index.html
```

## Adding a New Command

1. Add the subcommand variant to the appropriate enum in `src/main.rs`
2. Add the match arm in `run()` to dispatch to a handler
3. Implement the handler in the appropriate `src/commands/*.rs` module
4. Follow the GraphQL/cynic patterns in `conventions.md`
5. Add integration tests in `tests/integration_tests.rs`

## Updating the Schema

If the Dagster API schema changes:

```bash
dagctl schema download --token <TOKEN>
cargo build --release
```

Compile errors after schema update indicate breaking changes — update the cynic types accordingly.

## Releasing

Releases are automated via GitHub Actions. Push a version tag to trigger a cross-platform build and GitHub release:

```bash
git tag v0.2.0
git push origin v0.2.0
```

The workflow builds for all supported targets (macOS, Linux, Windows) and creates a GitHub release with the binaries.

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `DAGSTER_API_TOKEN` | Auth token (fallback after `--token` flag) |
| `DAGSTER_ORGANIZATION` | Organization name (fallback after `--organization` flag) |
| `DAGSTER_DEPLOYMENT` | Deployment name (fallback after `--deployment` flag) |
| `DAGSTER_API_URL` | Override the default API endpoint |
| `PROPTEST_CASES` | Number of proptest iterations (default 256) |
