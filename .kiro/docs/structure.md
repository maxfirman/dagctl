# Project Structure

```
dagctl/
├── build.rs                        # Registers schemas/dagster.graphql with cynic at compile time
├── Cargo.toml
├── LICENSE                         # Apache License 2.0
├── README.md                       # User-facing documentation
├── TESTING.md                      # Test strategy and coverage details
├── coverage.sh                     # Runs cargo-llvm-cov
├── schemas/
│   └── dagster.graphql             # Dagster Cloud GraphQL schema (~9,500 lines, downloaded via CLI)
├── src/
│   ├── main.rs                     # CLI entry point: Cli/Commands enums, clap setup, dispatch
│   ├── auth.rs                     # resolve_token/organization/deployment + build_api_url
│   ├── config.rs                   # Config struct, load_config() reads TOML from ~/.dagctl/config.toml
│   ├── lib.rs                      # Crate root, re-exports modules
│   ├── output.rs                   # OutputFormat enum, render helper, table formatters (comfy-table)
│   ├── schema.rs                   # #[cynic::schema("dagster")] pub mod schema {} — re-exports the registered schema
│   └── commands/
│       ├── mod.rs                  # Re-exports command modules
│       ├── code_locations.rs       # Code location list/detail GraphQL queries and handlers
│       ├── debug.rs                # Debug/diagnostic command
│       ├── runs.rs                 # Run list/detail/events/logs GraphQL queries and handlers
│       ├── schema.rs               # Schema download command (shells out to cynic introspect)
│       └── update.rs               # Self-update via GitHub releases
├── tests/
│   └── integration_tests.rs        # Mockito-based integration tests for API interactions
├── proptest-regressions/
│   └── config.txt                  # Proptest regression data
├── .github/
│   └── workflows/
│       ├── ci.yml                  # CI: test + clippy + fmt
│       └── release.yml             # Cross-platform release builds on tag push
└── .kiro/
    └── docs/                       # Steering documentation
```

## Module Responsibilities

### `src/main.rs`
Defines the CLI structure with clap derive macros. Top-level commands: `get`, `events`, `logs`, `schema`, `debug`, `completion`, `self`. Global `-o`/`--output` flag for format selection (json, yaml; table is the default when omitted). Parses args, resolves auth, dispatches to command handlers with the output format.

### `src/output.rs`
`OutputFormat` enum (Json, Yaml) and helpers: `render()` for JSON/YAML serialization of any `Serialize` type, plus command-specific table formatters using `comfy-table` (`format_runs_table`, `format_run_detail`, `format_code_locations_table`, `format_code_location_detail`, `format_events_table`, `format_logs_raw`).

### `src/auth.rs`
Three resolution functions (`resolve_token`, `resolve_organization`, `resolve_deployment`) implementing the auth priority chain: CLI flag → env var → config file. Also provides `build_api_url()`.

### `src/config.rs`
Reads `~/.dagctl/config.toml`. The `Config` struct has optional `token`, `organization`, and `deployment` fields.

### `src/commands/runs.rs`
Contains all cynic-derived GraphQL types and four public async functions: `list_runs`, `get_run`, `get_events`, `get_logs`.

### `src/commands/code_locations.rs`
GraphQL queries for listing workspace locations and getting detailed code location info including repositories, schedules, sensors, and jobs.

### `src/commands/update.rs`
Self-update using `self_update` crate, pulling releases from `maxfirman/dagctl` on GitHub.

### `src/commands/debug.rs`
Diagnostic command that prints version, config, and API connectivity info.

### `src/commands/schema.rs`
Shells out to `cynic introspect` to download the GraphQL schema.

## Data Flow

1. User invokes CLI → clap parses args
2. `auth::resolve_token/organization/deployment()` resolves credentials
3. `auth::build_api_url()` constructs the endpoint
4. Command handler builds a cynic `QueryBuilder` operation
5. `reqwest` POSTs the GraphQL query with auth header
6. Response is deserialized into cynic types
7. Result is serialized to JSON via serde and printed to stdout
