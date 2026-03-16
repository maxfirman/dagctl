# Testing

## Running Tests

```bash
# All tests
cargo test

# Unit tests only
cargo test --lib

# Integration tests only
cargo test --test integration_tests

# With output
cargo test -- --nocapture

# Specific test
cargo test test_cli_token_takes_precedence

# Property-based tests with more cases
PROPTEST_CASES=1000 cargo test
```

## Coverage

```bash
./coverage.sh
# or
cargo llvm-cov --all-features --workspace --html
# Report at target/llvm-cov/html/index.html
```

## Test Structure

### Unit Tests

Located in `#[cfg(test)] mod tests` blocks within each source module.

**`src/auth.rs`** — Token, organization, and deployment resolution logic:
- Priority chain tests (CLI flag → env var → config file)
- Error cases when no credentials available
- Property-based tests for arbitrary token strings

**`src/config.rs`** — TOML config file parsing:
- Valid config parsing (partial and full)
- Missing file and invalid TOML handling
- Property-based tests for arbitrary token values and extra fields

### Integration Tests

Located in `tests/integration_tests.rs`. Use `mockito` to mock the Dagster GraphQL API.

- Runs list/get with mocked responses
- Events and logs retrieval
- Code location list/detail (success, loading, error states)
- GraphQL error handling
- Network error handling

## Test Dependencies

| Crate | Purpose |
|-------|---------|
| `mockito` | HTTP mocking for integration tests |
| `tempfile` | Temporary files for config tests |
| `proptest` | Property-based testing |

## Coverage by Module

| Module | Coverage | Notes |
|--------|----------|-------|
| `auth.rs` | ~87% lines | Excellent — property tests cover edge cases |
| `config.rs` | ~90% lines | Excellent — property tests cover edge cases |
| `commands/*` | Low | Covered by integration tests at the HTTP level |
| `main.rs` | Low | CLI dispatch logic |
