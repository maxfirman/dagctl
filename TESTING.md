# Testing Documentation

## Test Coverage

### Unit Tests (13 tests)

#### Auth Module (`src/auth.rs`)
Tests for token resolution logic with multiple authentication sources:

**Standard Tests:**
- ✅ `test_cli_token_takes_precedence` - CLI token is used when provided
- ✅ `test_env_var_fallback` - Environment variable is used when no CLI token
- ✅ `test_cli_token_overrides_env_var` - CLI token takes precedence over env var
- ✅ `test_no_token_returns_error` - Error when no token is available

**Property-Based Tests:**
- ✅ `test_any_cli_token_is_accepted` - Any valid token string is accepted
- ✅ `test_cli_token_never_empty` - Tokens are never empty
- ✅ `test_token_whitespace_preserved` - Whitespace in tokens is preserved

**Coverage:** 93.85% regions, 86.79% lines

#### Config Module (`src/config.rs`)
Tests for TOML configuration file parsing:

**Standard Tests:**
- ✅ `test_parse_valid_config` - Valid TOML with token is parsed correctly
- ✅ `test_parse_config_without_token` - Empty config file is handled
- ✅ `test_missing_config_file` - Missing file returns None gracefully
- ✅ `test_invalid_toml` - Invalid TOML syntax is handled gracefully

**Property-Based Tests:**
- ✅ `test_any_valid_token_string` - Any alphanumeric token is parsed correctly
- ✅ `test_config_with_extra_fields` - Extra fields in config don't break parsing

**Coverage:** 89.22% regions, 90.48% lines

### Integration Tests (7 tests)

Located in `tests/integration_tests.rs`. These tests use mock servers to test actual API interactions:

- ✅ `test_runs_list_success` - Tests runs list with mocked API (functional)
- ✅ `test_runs_get_not_found` - Documents run not found error response
- ✅ `test_runs_events_success` - Documents events API response structure
- ✅ `test_runs_logs_with_captured_event` - Documents two-step log retrieval process
- ✅ `test_runs_logs_no_captured_event` - Documents error when no logs captured
- ✅ `test_graphql_error_response` - Documents GraphQL error handling
- ✅ `test_network_error` - Placeholder for network error testing

**Note:** The CLI now supports configurable API URL via `DAGSTER_API_URL` environment variable, enabling functional integration tests with mock servers.

## Running Tests

```bash
# Run all tests
cargo test

# Run only unit tests
cargo test --lib

# Run only integration tests
cargo test --test integration_tests

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_cli_token_takes_precedence

# Run property-based tests with more cases
PROPTEST_CASES=1000 cargo test
```

## Code Coverage

### Generate Coverage Report

```bash
# Run the coverage script
./coverage.sh

# Or manually:
cargo llvm-cov --all-features --workspace --html

# View the report
open target/llvm-cov/html/index.html
```

### Current Coverage

| Module | Regions | Lines | Functions |
|--------|---------|-------|-----------|
| `auth.rs` | 93.85% | 86.79% | 100% |
| `config.rs` | 89.22% | 90.48% | 85.71% |
| `commands/runs.rs` | 0% | 0% | 0% |
| `commands/schema.rs` | 0% | 0% | 0% |
| `main.rs` | 0% | 0% | 0% |
| **TOTAL** | **34.08%** | **29.17%** | **28.95%** |

**Note:** Command modules have 0% coverage because they require actual API calls. Future work will add functional integration tests.

## Test Statistics

- **Total Tests:** 20 (13 unit + 7 integration)
- **Unit Tests:** 13 (7 standard + 6 property-based)
- **Integration Tests:** 7
- **Pass Rate:** 100%
- **Overall Coverage:** 34.08% regions, 29.17% lines
- **Core Logic Coverage:** 91.54% (auth + config modules)

## Coverage by Module

| Module | Lines Tested | Coverage | Notes |
|--------|-------------|----------|-------|
| `auth.rs` | Token resolution | 86.79% | ✅ Excellent coverage with property tests |
| `config.rs` | Config parsing | 90.48% | ✅ Excellent coverage with property tests |
| `commands/runs.rs` | API interactions | 0% | ⚠️ Needs functional integration tests |
| `commands/schema.rs` | Schema download | 0% | ⚠️ Needs functional integration tests |
| `main.rs` | CLI parsing | 0% | ⚠️ Needs CLI integration tests |

## Future Testing Improvements

### High Priority

1. ✅ **Refactor for testability** - COMPLETED
   - API URL is now configurable via `DAGSTER_API_URL` env var
   - Enables functional integration tests with mock servers

2. ✅ **Add property-based tests** - COMPLETED
   - Using `proptest` for input validation
   - 6 property tests added for auth and config modules
   - Tests edge cases automatically

3. **Add command-level functional tests**
   - Test actual API interactions with mock server
   - Test JSON serialization of responses
   - Test error message formatting

### Medium Priority

4. ✅ **Add coverage reporting** - COMPLETED
   - Using `cargo-llvm-cov` for coverage analysis
   - HTML reports available in `target/llvm-cov/html/`
   - Current coverage: 34.08% overall, 91.54% for core logic

5. **Increase command coverage**
   - Add functional tests for all commands
   - Target: 70%+ overall coverage
   - Focus on error paths

6. **Add benchmark tests**
   - Use `criterion` for performance testing
   - Measure query performance
   - Test with large event streams

### Low Priority

6. **Add mutation tests**
   - Use `cargo-mutants` to verify test quality
   - Ensure tests catch actual bugs

7. **Add coverage reporting**
   - Use `tarpaulin` or `llvm-cov`
   - Track coverage over time
   - Set minimum coverage thresholds

## Testing Best Practices

### Current Implementation

✅ **Isolated tests** - Each test is independent
✅ **Fast execution** - All tests run in < 1 second
✅ **Clear naming** - Test names describe what they test
✅ **Temporary files** - Using `tempfile` for file system tests
✅ **Unsafe isolation** - Properly handling unsafe env var operations

### Recommendations

- Run tests before every commit
- Keep tests fast (< 5 seconds total)
- Test error paths, not just happy paths
- Use descriptive assertion messages
- Mock external dependencies (API, file system)

## CI/CD Integration

Recommended GitHub Actions workflow:

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions-rs/toolchain@v1
        with:
          toolchain: stable
      - run: cargo test --all-features
      - run: cargo clippy -- -D warnings
      - run: cargo fmt -- --check
```

## Dependencies

### Test Dependencies

- `mockito = "1.5"` - HTTP mocking for integration tests
- `tempfile = "3.13"` - Temporary file/directory creation
- `proptest = "1.5"` - Property-based testing framework

### Development Tools

- `cargo-llvm-cov` - Code coverage reporting
- `llvm-tools-preview` - LLVM tools for coverage analysis
