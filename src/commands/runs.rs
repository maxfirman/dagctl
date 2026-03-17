use anyhow::Result;
use serde::Serialize;

use crate::output::{self, OutputFormat};

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "Run")]
#[cynic(schema_module = "crate::schema::schema")]
struct Run {
    #[cynic(rename = "runId")]
    run_id: String,
    #[cynic(rename = "pipelineName")]
    job_name: String,
    status: RunStatus,
    #[cynic(rename = "startTime")]
    start_time: Option<f64>,
    #[cynic(rename = "endTime")]
    end_time: Option<f64>,
}

#[derive(cynic::Enum, Clone, Copy, Debug)]
#[cynic(schema = "dagster", graphql_type = "RunStatus")]
#[cynic(schema_module = "crate::schema::schema")]
enum RunStatus {
    Queued,
    NotStarted,
    Managed,
    Starting,
    Started,
    Success,
    Failure,
    Canceling,
    Canceled,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "Runs")]
#[cynic(schema_module = "crate::schema::schema")]
struct Runs {
    results: Vec<Run>,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "RunsOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum RunsOrError {
    Runs(Runs),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct RunsQueryVariables {
    cursor: Option<String>,
    limit: Option<i32>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "RunsQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct RunsQuery {
    #[arguments(cursor: $cursor, limit: $limit)]
    #[cynic(rename = "runsOrError")]
    runs_or_error: RunsOrError,
}

pub async fn list_runs(
    token: &str,
    api_url: &str,
    limit: Option<i32>,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let operation = RunsQuery::build(RunsQueryVariables {
        cursor: None,
        limit,
    });

    let client = reqwest::Client::new();
    let response = client
        .post(api_url)
        .header("Authorization", format!("Bearer {}", token))
        .run_graphql(operation)
        .await?;

    if let Some(errors) = response.errors {
        anyhow::bail!("GraphQL errors: {:?}", errors);
    }

    let data = response
        .data
        .ok_or_else(|| anyhow::anyhow!("No data in response"))?;

    match data.runs_or_error {
        RunsOrError::Runs(runs) => match fmt {
            Some(f) => output::render(&runs.results, f),
            None => {
                let rows: Vec<_> = runs
                    .results
                    .iter()
                    .map(|r| {
                        (
                            r.run_id.clone(),
                            r.job_name.clone(),
                            format!("{:?}", r.status),
                            r.start_time,
                            r.end_time,
                        )
                    })
                    .collect();
                output::format_runs_table(&rows);
                Ok(())
            }
        },
        RunsOrError::Other => anyhow::bail!("Unexpected response type from API"),
    }
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "Run")]
#[cynic(schema_module = "crate::schema::schema")]
struct RunDetail {
    #[cynic(rename = "runId")]
    run_id: String,
    #[cynic(rename = "pipelineName")]
    job_name: String,
    status: RunStatus,
    #[cynic(rename = "startTime")]
    start_time: Option<f64>,
    #[cynic(rename = "endTime")]
    end_time: Option<f64>,
    #[cynic(rename = "runConfigYaml")]
    run_config_yaml: String,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "RunOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum RunOrError {
    Run(RunDetail),
    RunNotFoundError(RunNotFoundError),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct RunNotFoundError {
    message: String,
}

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct RunQueryVariables {
    run_id: cynic::Id,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "RunQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct RunQuery {
    #[arguments(runId: $run_id)]
    #[cynic(rename = "runOrError")]
    run_or_error: RunOrError,
}

pub async fn get_run(
    token: &str,
    api_url: &str,
    run_id: String,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let operation = RunQuery::build(RunQueryVariables {
        run_id: cynic::Id::new(run_id),
    });

    let client = reqwest::Client::new();
    let response = client
        .post(api_url)
        .header("Authorization", format!("Bearer {}", token))
        .run_graphql(operation)
        .await?;

    if let Some(errors) = response.errors {
        anyhow::bail!("GraphQL errors: {:?}", errors);
    }

    let data = response
        .data
        .ok_or_else(|| anyhow::anyhow!("No data in response"))?;

    match data.run_or_error {
        RunOrError::Run(run) => match fmt {
            Some(f) => output::render(&run, f),
            None => {
                output::format_run_detail(
                    &run.run_id,
                    &run.job_name,
                    &format!("{:?}", run.status),
                    run.start_time,
                    run.end_time,
                    &run.run_config_yaml,
                );
                Ok(())
            }
        },
        RunOrError::RunNotFoundError(err) => anyhow::bail!("Run not found: {}", err.message),
        RunOrError::Other => anyhow::bail!("Unexpected response type from API"),
    }
}

// Events command structures
#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "EventConnection")]
#[cynic(schema_module = "crate::schema::schema")]
struct EventConnection {
    events: Vec<DagsterRunEvent>,
}

#[derive(cynic::InlineFragments, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "DagsterRunEvent")]
#[cynic(schema_module = "crate::schema::schema")]
#[serde(tag = "eventType")]
enum DagsterRunEvent {
    ExecutionStepStartEvent(ExecutionStepStartEvent),
    ExecutionStepSuccessEvent(ExecutionStepSuccessEvent),
    ExecutionStepFailureEvent(ExecutionStepFailureEvent),
    LogMessageEvent(LogMessageEvent),
    LogsCapturedEvent(LogsCapturedEvent),
    MaterializationEvent(MaterializationEvent),
    EngineEvent(EngineEvent),
    RunStartEvent(RunStartEvent),
    RunSuccessEvent(RunSuccessEvent),
    RunFailureEvent(RunFailureEvent),
    #[cynic(fallback)]
    Other,
}

impl DagsterRunEvent {
    fn to_table_row(&self) -> (String, String, String, String, String) {
        match self {
            Self::ExecutionStepStartEvent(e) => (
                e.timestamp.clone(),
                "StepStart".into(),
                format!("{:?}", e.level),
                e.step_key.clone().unwrap_or_default(),
                e.message.clone(),
            ),
            Self::ExecutionStepSuccessEvent(e) => (
                e.timestamp.clone(),
                "StepSuccess".into(),
                format!("{:?}", e.level),
                e.step_key.clone().unwrap_or_default(),
                e.message.clone(),
            ),
            Self::ExecutionStepFailureEvent(e) => (
                e.timestamp.clone(),
                "StepFailure".into(),
                format!("{:?}", e.level),
                e.step_key.clone().unwrap_or_default(),
                e.message.clone(),
            ),
            Self::LogMessageEvent(e) => (
                e.timestamp.clone(),
                "Log".into(),
                format!("{:?}", e.level),
                e.step_key.clone().unwrap_or_default(),
                e.message.clone(),
            ),
            Self::LogsCapturedEvent(e) => (
                e.timestamp.clone(),
                "LogsCaptured".into(),
                format!("{:?}", e.level),
                e.step_key.clone().unwrap_or_default(),
                e.message.clone(),
            ),
            Self::MaterializationEvent(e) => (
                e.timestamp.clone(),
                "Materialization".into(),
                format!("{:?}", e.level),
                e.step_key.clone().unwrap_or_default(),
                e.message.clone(),
            ),
            Self::EngineEvent(e) => (
                e.timestamp.clone(),
                "Engine".into(),
                format!("{:?}", e.level),
                e.step_key.clone().unwrap_or_default(),
                e.message.clone(),
            ),
            Self::RunStartEvent(e) => (
                e.timestamp.clone(),
                "RunStart".into(),
                format!("{:?}", e.level),
                String::new(),
                e.message.clone(),
            ),
            Self::RunSuccessEvent(e) => (
                e.timestamp.clone(),
                "RunSuccess".into(),
                format!("{:?}", e.level),
                String::new(),
                e.message.clone(),
            ),
            Self::RunFailureEvent(e) => (
                e.timestamp.clone(),
                "RunFailure".into(),
                format!("{:?}", e.level),
                String::new(),
                e.message.clone(),
            ),
            Self::Other => (String::new(), "Unknown".into(), String::new(), String::new(), String::new()),
        }
    }
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct ExecutionStepStartEvent {
    #[cynic(rename = "runId")]
    run_id: String,
    message: String,
    timestamp: String,
    level: LogLevel,
    #[cynic(rename = "stepKey")]
    step_key: Option<String>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct ExecutionStepSuccessEvent {
    #[cynic(rename = "runId")]
    run_id: String,
    message: String,
    timestamp: String,
    level: LogLevel,
    #[cynic(rename = "stepKey")]
    step_key: Option<String>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct ExecutionStepFailureEvent {
    #[cynic(rename = "runId")]
    run_id: String,
    message: String,
    timestamp: String,
    level: LogLevel,
    #[cynic(rename = "stepKey")]
    step_key: Option<String>,
    error: Option<PythonError>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct LogMessageEvent {
    #[cynic(rename = "runId")]
    run_id: String,
    message: String,
    timestamp: String,
    level: LogLevel,
    #[cynic(rename = "stepKey")]
    step_key: Option<String>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct LogsCapturedEvent {
    #[cynic(rename = "runId")]
    run_id: String,
    message: String,
    timestamp: String,
    level: LogLevel,
    #[cynic(rename = "stepKey")]
    step_key: Option<String>,
    #[cynic(rename = "fileKey")]
    file_key: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct MaterializationEvent {
    #[cynic(rename = "runId")]
    run_id: String,
    message: String,
    timestamp: String,
    level: LogLevel,
    #[cynic(rename = "stepKey")]
    step_key: Option<String>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct EngineEvent {
    #[cynic(rename = "runId")]
    run_id: String,
    message: String,
    timestamp: String,
    level: LogLevel,
    #[cynic(rename = "stepKey")]
    step_key: Option<String>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct RunStartEvent {
    #[cynic(rename = "runId")]
    run_id: String,
    message: String,
    timestamp: String,
    level: LogLevel,
    #[cynic(rename = "pipelineName")]
    pipeline_name: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct RunSuccessEvent {
    #[cynic(rename = "runId")]
    run_id: String,
    message: String,
    timestamp: String,
    level: LogLevel,
    #[cynic(rename = "pipelineName")]
    pipeline_name: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct RunFailureEvent {
    #[cynic(rename = "runId")]
    run_id: String,
    message: String,
    timestamp: String,
    level: LogLevel,
    #[cynic(rename = "pipelineName")]
    pipeline_name: String,
    error: Option<PythonError>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct PythonError {
    message: String,
}

#[derive(cynic::Enum, Clone, Copy, Debug)]
#[cynic(schema = "dagster", graphql_type = "LogLevel")]
#[cynic(schema_module = "crate::schema::schema")]
enum LogLevel {
    #[cynic(rename = "CRITICAL")]
    Critical,
    #[cynic(rename = "ERROR")]
    Error,
    #[cynic(rename = "INFO")]
    Info,
    #[cynic(rename = "WARNING")]
    Warning,
    #[cynic(rename = "DEBUG")]
    Debug,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "Run")]
#[cynic(schema_module = "crate::schema::schema")]
struct RunWithEvents {
    #[cynic(rename = "eventConnection")]
    event_connection: EventConnection,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "RunOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum RunOrErrorEvents {
    Run(RunWithEvents),
    RunNotFoundError(RunNotFoundError),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct RunEventsQueryVariables {
    run_id: cynic::Id,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "RunEventsQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct RunEventsQuery {
    #[arguments(runId: $run_id)]
    #[cynic(rename = "runOrError")]
    run_or_error: RunOrErrorEvents,
}

pub async fn get_events(
    token: &str,
    api_url: &str,
    run_id: String,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let operation = RunEventsQuery::build(RunEventsQueryVariables {
        run_id: cynic::Id::new(run_id),
    });

    let client = reqwest::Client::new();
    let response = client
        .post(api_url)
        .header("Authorization", format!("Bearer {}", token))
        .run_graphql(operation)
        .await?;

    if let Some(errors) = response.errors {
        anyhow::bail!("GraphQL errors: {:?}", errors);
    }

    let data = response
        .data
        .ok_or_else(|| anyhow::anyhow!("No data in response"))?;

    match data.run_or_error {
        RunOrErrorEvents::Run(run) => match fmt {
            Some(f) => output::render(&run.event_connection.events, f),
            None => {
                let rows: Vec<_> = run
                    .event_connection
                    .events
                    .iter()
                    .map(|e| e.to_table_row())
                    .collect();
                output::format_events_table(&rows);
                Ok(())
            }
        },
        RunOrErrorEvents::RunNotFoundError(err) => {
            anyhow::bail!("Run not found: {}", err.message)
        }
        RunOrErrorEvents::Other => anyhow::bail!("Unexpected response type from API"),
    }
}

// Logs command structures
#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "CapturedLogs")]
#[cynic(schema_module = "crate::schema::schema")]
struct CapturedLogs {
    stdout: Option<String>,
    stderr: Option<String>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "Run",
    variables = "RunLogsQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct RunWithLogs {
    #[arguments(fileKey: $file_key)]
    #[cynic(rename = "capturedLogs")]
    captured_logs: CapturedLogs,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "RunOrError",
    variables = "RunLogsQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
enum RunOrErrorLogs {
    Run(RunWithLogs),
    RunNotFoundError(RunNotFoundError),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct RunLogsQueryVariables {
    run_id: cynic::Id,
    file_key: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "RunLogsQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct RunLogsQuery {
    #[arguments(runId: $run_id)]
    #[cynic(rename = "runOrError")]
    run_or_error: RunOrErrorLogs,
}

pub async fn get_logs(
    token: &str,
    api_url: &str,
    run_id: String,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    // First, fetch events to find LogsCapturedEvent
    let events_operation = RunEventsQuery::build(RunEventsQueryVariables {
        run_id: cynic::Id::new(run_id.clone()),
    });

    let client = reqwest::Client::new();
    let events_response = client
        .post(api_url)
        .header("Authorization", format!("Bearer {}", token))
        .run_graphql(events_operation)
        .await?;

    if let Some(errors) = events_response.errors {
        anyhow::bail!("GraphQL errors: {:?}", errors);
    }

    let events_data = events_response
        .data
        .ok_or_else(|| anyhow::anyhow!("No data in response"))?;

    let file_key = match events_data.run_or_error {
        RunOrErrorEvents::Run(run) => run
            .event_connection
            .events
            .iter()
            .find_map(|event| {
                if let DagsterRunEvent::LogsCapturedEvent(log_event) = event {
                    Some(log_event.file_key.clone())
                } else {
                    None
                }
            })
            .ok_or_else(|| anyhow::anyhow!("No LogsCapturedEvent found for this run"))?,
        RunOrErrorEvents::RunNotFoundError(err) => {
            anyhow::bail!("Run not found: {}", err.message)
        }
        RunOrErrorEvents::Other => anyhow::bail!("Unexpected response type from API"),
    };

    // Now fetch the captured logs using the file_key
    let logs_operation = RunLogsQuery::build(RunLogsQueryVariables {
        run_id: cynic::Id::new(run_id),
        file_key,
    });

    let logs_response = client
        .post(api_url)
        .header("Authorization", format!("Bearer {}", token))
        .run_graphql(logs_operation)
        .await?;

    if let Some(errors) = logs_response.errors {
        anyhow::bail!("GraphQL errors: {:?}", errors);
    }

    let logs_data = logs_response
        .data
        .ok_or_else(|| anyhow::anyhow!("No data in response"))?;

    match logs_data.run_or_error {
        RunOrErrorLogs::Run(run) => match fmt {
            Some(f) => output::render(&run.captured_logs, f),
            None => {
                output::format_logs_raw(
                    run.captured_logs.stdout.as_deref(),
                    run.captured_logs.stderr.as_deref(),
                );
                Ok(())
            }
        },
        RunOrErrorLogs::RunNotFoundError(err) => {
            anyhow::bail!("Run not found: {}", err.message)
        }
        RunOrErrorLogs::Other => anyhow::bail!("Unexpected response type from API"),
    }
}
