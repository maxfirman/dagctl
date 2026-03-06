# Events and Logs Commands Implementation

## Summary

Successfully added two new commands to the Dagster CLI:
1. `dagster-cli runs events <run-id>` - Fetch all events for a specific run
2. `dagster-cli runs logs <run-id>` - Auto-discover and fetch captured logs for a run

## Implementation Details

### Events Command

**Usage:**
```bash
dagster-cli runs events <RUN_ID> [--token <TOKEN>]
```

**Functionality:**
- Queries the Dagster GraphQL API for all events associated with a run
- Uses the `Run.eventConnection` field to fetch events
- Handles the `DagsterRunEvent` union type with inline fragments
- Supports the following event types:
  - ExecutionStepStartEvent
  - ExecutionStepSuccessEvent
  - ExecutionStepFailureEvent
  - LogMessageEvent
  - LogsCapturedEvent
  - MaterializationEvent
  - EngineEvent
  - RunStartEvent
  - RunSuccessEvent
  - RunFailureEvent
  - Other (fallback for unhandled event types)

**Output:**
- Returns JSON array of events with detailed information
- Each event includes: runId, message, timestamp, level, and type-specific fields
- Step events include stepKey
- Run events include pipelineName
- Failure events include error details
- LogsCapturedEvent includes fileKey

**Example Output:**
```json
[
  {
    "ExecutionStepStartEvent": {
      "run_id": "abc123",
      "message": "Started execution of step...",
      "timestamp": "2026-03-06T16:00:00Z",
      "level": "Info",
      "step_key": "my_step"
    }
  },
  {
    "LogsCapturedEvent": {
      "run_id": "abc123",
      "message": "Logs captured",
      "timestamp": "2026-03-06T16:01:00Z",
      "level": "Info",
      "step_key": null,
      "file_key": "compute_logs"
    }
  }
]
```

### Logs Command

**Usage:**
```bash
dagster-cli runs logs <RUN_ID> [--token <TOKEN>]
```

**Functionality:**
- Auto-discovers the log file key by first fetching events
- Searches for `LogsCapturedEvent` in the event stream
- Extracts the `fileKey` from the first LogsCapturedEvent found
- Uses the fileKey to query `Run.capturedLogs(fileKey: String!)`
- Returns stdout and stderr logs

**Output:**
- Returns JSON object with stdout and stderr fields
- Fields may be null if no logs were captured

**Example Output:**
```json
{
  "stdout": "Starting job execution...\nProcessing data...\nCompleted successfully\n",
  "stderr": null
}
```

**Error Handling:**
- Returns error if run is not found
- Returns error if no LogsCapturedEvent exists for the run
- Handles GraphQL errors gracefully

## Technical Implementation

### GraphQL Query Structures

**Events Query:**
```graphql
query RunEventsQuery($run_id: ID!) {
  runOrError(runId: $run_id) {
    ... on Run {
      eventConnection {
        events {
          ... on ExecutionStepStartEvent { runId, message, timestamp, level, stepKey }
          ... on ExecutionStepSuccessEvent { runId, message, timestamp, level, stepKey }
          ... on ExecutionStepFailureEvent { runId, message, timestamp, level, stepKey, error { message } }
          ... on LogMessageEvent { runId, message, timestamp, level, stepKey }
          ... on LogsCapturedEvent { runId, message, timestamp, level, stepKey, fileKey }
          ... on MaterializationEvent { runId, message, timestamp, level, stepKey }
          ... on EngineEvent { runId, message, timestamp, level, stepKey }
          ... on RunStartEvent { runId, message, timestamp, level, pipelineName }
          ... on RunSuccessEvent { runId, message, timestamp, level, pipelineName }
          ... on RunFailureEvent { runId, message, timestamp, level, pipelineName, error { message } }
        }
      }
    }
    ... on RunNotFoundError { message }
  }
}
```

**Logs Query:**
```graphql
query RunLogsQuery($run_id: ID!, $file_key: String!) {
  runOrError(runId: $run_id) {
    ... on Run {
      capturedLogs(fileKey: $file_key) {
        stdout
        stderr
      }
    }
    ... on RunNotFoundError { message }
  }
}
```

### Code Structure

**Files Modified:**
1. `src/main.rs` - Added `Events` and `Logs` variants to `RunsCommands` enum
2. `src/commands/runs.rs` - Added GraphQL query structures and implementation functions

**New Types Added:**
- `EventConnection` - Wrapper for events array
- `DagsterRunEvent` - Union type with inline fragments for different event types
- `ExecutionStepStartEvent`, `ExecutionStepSuccessEvent`, `ExecutionStepFailureEvent` - Step execution events
- `LogMessageEvent` - General log messages
- `LogsCapturedEvent` - Log capture events with fileKey
- `MaterializationEvent` - Asset materialization events
- `EngineEvent` - Engine-level events
- `RunStartEvent`, `RunSuccessEvent`, `RunFailureEvent` - Run lifecycle events
- `PythonError` - Error details
- `LogLevel` - Log level enum (Critical, Error, Info, Warning, Debug)
- `CapturedLogs` - Stdout/stderr container
- `RunWithEvents`, `RunWithLogs` - Run fragments for different queries
- `RunOrErrorEvents`, `RunOrErrorLogs` - Union types for query responses

**New Functions Added:**
- `get_events(token: &str, run_id: String) -> Result<()>` - Fetch and display events
- `get_logs(token: &str, run_id: String) -> Result<()>` - Auto-discover fileKey and fetch logs

## Testing

### Build Status
✅ Debug build: Successful
✅ Release build: Successful
✅ No compilation errors or warnings

### Command Availability
✅ `dagster-cli runs events --help` - Shows usage
✅ `dagster-cli runs logs --help` - Shows usage
✅ Both commands available in release binary

### Authentication
Both commands support the same authentication methods as existing commands:
1. CLI flag: `--token <TOKEN>`
2. Environment variable: `DAGSTER_API_TOKEN`
3. Config file: `~/.dagster-cli/config.toml`

## Usage Examples

### Fetch events for a run
```bash
# Using token flag
dagster-cli runs events abc123 --token <YOUR_TOKEN>

# Using environment variable
export DAGSTER_API_TOKEN=<YOUR_TOKEN>
dagster-cli runs events abc123

# Using config file
dagster-cli runs events abc123
```

### Fetch logs for a run
```bash
# Using token flag
dagster-cli runs logs abc123 --token <YOUR_TOKEN>

# Using environment variable
export DAGSTER_API_TOKEN=<YOUR_TOKEN>
dagster-cli runs logs abc123

# Using config file
dagster-cli runs logs abc123
```

### Pipe output to jq for filtering
```bash
# Get only error events
dagster-cli runs events abc123 | jq '.[] | select(.ExecutionStepFailureEvent or .RunFailureEvent)'

# Get only stdout from logs
dagster-cli runs logs abc123 | jq -r '.stdout'

# Count events by type
dagster-cli runs events abc123 | jq 'group_by(keys[0]) | map({type: .[0] | keys[0], count: length})'
```

## Future Enhancements

Potential improvements for future iterations:
- Add filtering options for events (by type, level, step)
- Add pagination support for large event streams
- Add option to specify fileKey directly for logs command
- Add option to fetch logs for specific steps
- Add formatted output options (table view, colored output)
- Add streaming support for real-time event monitoring
- Add support for fetching logs from multiple fileKeys
