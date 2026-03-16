# Coding Conventions

## Rust Style

- Use `anyhow::Result` for all fallible functions — no custom error types currently
- Use `anyhow::bail!()` for error returns with descriptive messages
- Errors go to stderr (via `eprintln!` in main), data goes to stdout as JSON
- Exit code 1 on any error

## GraphQL / Cynic Patterns

When adding new GraphQL queries, follow the established pattern in `src/commands/runs.rs`:

1. Define a `QueryVariables` struct with `#[derive(cynic::QueryVariables)]`
2. Define response fragment structs with `#[derive(cynic::QueryFragment, Debug, Serialize)]`
3. Use `#[cynic(schema = "dagster", graphql_type = "...")]` on every type
4. Always include `#[cynic(schema_module = "crate::schema::schema")]`
5. Use `#[cynic(rename = "camelCaseName")]` to map to Rust snake_case fields
6. Use `cynic::InlineFragments` for union types (e.g., `RunsOrError`, `RunOrError`)
7. Always include a `#[cynic(fallback)] Other` variant on inline fragment enums
8. Root query type is `CloudQuery`, not `Query`

### Command handler pattern

```rust
pub async fn my_command(token: &str, api_url: &str, args...) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let operation = MyQuery::build(MyQueryVariables { ... });
    let client = reqwest::Client::new();
    let response = client
        .post(api_url)
        .header("Authorization", format!("Bearer {}", token))
        .run_graphql(operation)
        .await?;

    if let Some(errors) = response.errors {
        anyhow::bail!("GraphQL errors: {:?}", errors);
    }

    let data = response.data
        .ok_or_else(|| anyhow::anyhow!("No data in response"))?;

    match data.my_field {
        MyUnion::Success(val) => {
            println!("{}", serde_json::to_string_pretty(&val)?);
            Ok(())
        }
        MyUnion::Error(err) => anyhow::bail!("..."),
        MyUnion::Other => anyhow::bail!("Unexpected response type from API"),
    }
}
```

## CLI Patterns

- Use clap derive macros for all CLI definitions
- Top-level commands: `Get`, `Events`, `Logs`, `Schema`, `Debug`, `Completion`, `SelfCmd`
- `Get` has nested subcommands via `GetResource` enum (`Runs`, `Run`, `CodeLocations`, `CodeLocation`)
- Global args (`--token`, `--organization`, `--deployment`) go on the top-level `Cli` struct with `#[arg(global = true)]`
- Auth resolution and API URL construction happen in `run()` before dispatching to handlers
- Async commands run inside `tokio::runtime::Runtime::new()?.block_on()`
- `Completion` and `SelfCmd::Update` are handled before auth resolution (they don't need credentials)

## Testing Patterns

- Unit tests live in `#[cfg(test)] mod tests` inside each module
- Integration tests in `tests/integration_tests.rs` use `mockito` for HTTP mocking
- Property-based tests use `proptest` for input validation edge cases
- Config module uses `#[cfg(test)]` / `#[cfg(not(test))]` to swap `load_config_from_path` visibility
- Use `tempfile::TempDir` for file system tests
- Env var manipulation in tests uses `unsafe { env::set_var(...) }` / `env::remove_var(...)`

## Serialization

- All output types derive `Serialize`
- Enum variants that appear in JSON use `#[serde(tag = "eventType")]` for tagged representation (see `DagsterRunEvent`)
- `RunStatus` is a cynic `Enum` (not `Serialize`) — it gets serialized through the parent struct
