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
   cynic introspect https://troweprice.dagster.cloud/prod/graphql \
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
./target/release/dagster-cli runs list --token <TOKEN>

# With env var
export DAGSTER_API_TOKEN=<TOKEN>
./target/release/dagster-cli runs list --limit 10

# With config file (~/.dagster-cli/config.toml)
# token = "your-token"
./target/release/dagster-cli runs get <RUN_ID>
```

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
./target/release/dagster-cli schema download --token <TOKEN>
# or
cynic introspect https://troweprice.dagster.cloud/prod/graphql \
  -H "Authorization: Bearer <TOKEN>" \
  -o schemas/dagster.graphql

cargo build --release
```

Compile errors after schema update indicate breaking changes — update the cynic types accordingly.

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `DAGSTER_API_TOKEN` | Auth token (fallback after `--token` flag) |
| `DAGSTER_API_URL` | Override the default API endpoint |
| `PROPTEST_CASES` | Number of proptest iterations (default 256) |
