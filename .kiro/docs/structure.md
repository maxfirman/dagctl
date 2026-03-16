# Project Structure

```
dagster-cli/
├── build.rs                        # Registers schemas/dagster.graphql with cynic at compile time
├── Cargo.toml
├── schemas/
│   └── dagster.graphql             # Dagster Cloud GraphQL schema (~9,500 lines, downloaded via CLI)
├── src/
│   ├── main.rs                     # CLI entry point: Cli/Commands/RunsCommands/SchemaCommands enums, tokio runtime setup
│   ├── auth.rs                     # resolve_token(): CLI flag → DAGSTER_API_TOKEN env → ~/.dagster-cli/config.toml
│   ├── config.rs                   # Config struct, load_config() reads TOML from ~/.dagster-cli/config.toml
│   ├── schema.rs                   # #[cynic::schema("dagster")] pub mod schema {} — re-exports the registered schema
│   └── commands/
│       ├── mod.rs                  # Re-exports: pub mod runs; pub mod schema;
│       ├── runs.rs                 # All run-related GraphQL queries and command handlers
│       └── schema.rs               # download_schema() — shells out to `cynic introspect`
├── tests/
│   └── integration_tests.rs        # Mockito-based integration tests for API interactions
├── proptest-regressions/
│   └── config.txt                  # Proptest regression data
├── .env                            # Local token storage (gitignored)
├── .gitignore
├── coverage.sh                     # Runs cargo-llvm-cov
├── README.md                       # User-facing documentation
├── TESTING.md                      # Test strategy and coverage details
├── IMPLEMENTATION_STATUS.md        # Implementation progress tracking
└── EVENTS_LOGS_IMPLEMENTATION.md   # Design notes for events/logs features
```

## Module Responsibilities

### `src/main.rs`
Defines the CLI structure with clap derive macros. Parses args, resolves auth token, dispatches to command handlers. Creates a tokio runtime for async commands.

### `src/auth.rs`
Single function `resolve_token()` implementing the auth priority chain. Returns `anyhow::Result<String>`.

### `src/config.rs`
Reads `~/.dagster-cli/config.toml`. The `Config` struct has an optional `token` field. Uses a separate `load_config_from_path()` for testability.

### `src/commands/runs.rs`
The largest module. Contains all cynic-derived GraphQL types and four public async functions:
- `list_runs()` — queries `runsOrError` with optional cursor/limit
- `get_run()` — queries `runOrError` by ID, returns detail including `runConfigYaml`
- `get_events()` — queries `runOrError` → `eventConnection` for all run events
- `get_logs()` — two-step: first fetches events to find `LogsCapturedEvent.fileKey`, then queries `capturedLogs`

Each function builds a cynic operation, posts to the API, and prints JSON to stdout.

### `src/commands/schema.rs`
Shells out to `cynic introspect` to download the schema. Uses `DAGSTER_API_URL` env var with fallback to the default URL.

## Data Flow

1. User invokes CLI → clap parses args
2. `auth::resolve_token()` resolves the bearer token
3. Command handler builds a cynic `QueryBuilder` operation
4. `reqwest` POSTs the GraphQL query with auth header
5. Response is deserialized into cynic types
6. Result is serialized to JSON via serde and printed to stdout
