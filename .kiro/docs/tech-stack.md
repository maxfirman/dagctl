# Tech Stack

## Language & Edition

- Rust, edition 2024

## Key Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `clap` | 4 (derive) | CLI argument parsing |
| `cynic` | 3 (http-reqwest) | Type-safe GraphQL client with compile-time schema validation |
| `reqwest` | 0.13 (json) | HTTP client — must stay on 0.13 to match cynic's `http-reqwest` feature |
| `serde` / `serde_json` | 1 | JSON serialization for CLI output |
| `tokio` | 1 (full) | Async runtime |
| `anyhow` | 1 | Error handling |
| `toml` | 0.8 | Config file parsing |

## Dev Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `mockito` | 1.5 | HTTP mocking for integration tests |
| `tempfile` | 3.13 | Temp files for config tests |
| `proptest` | 1.5 | Property-based testing |

## Build Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `cynic-codegen` | 3 | Schema registration at build time via `build.rs` |

## External Tools

- `cynic-cli` — required for schema introspection (`cynic introspect`)

## Build System

- `build.rs` registers the GraphQL schema from `schemas/dagster.graphql` with cynic at compile time
- The schema file must exist before building; without it, cynic types won't compile
- Coverage via `cargo-llvm-cov`

## Important Constraints

- `reqwest` version must be 0.13 to match cynic's http-reqwest feature — do not upgrade independently
- Dagster Cloud uses `CloudQuery` as the root query type, not `Query`
- The `RunStatus` enum includes a `Managed` variant specific to Dagster Cloud
- All cynic derive macros require `#[cynic(schema_module = "crate::schema::schema")]`
- GraphQL field renames use `#[cynic(rename = "fieldName")]` to map camelCase → snake_case
