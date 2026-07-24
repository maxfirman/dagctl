use anyhow::Result;
use chrono::{DateTime, FixedOffset, Local, NaiveDate, Utc};
use serde::Serialize;

use crate::output::{self, OutputFormat};

// ═══════════════════════════════════════════════════════════════════════════════
// Metric name mapping: friendly CLI names → internal GraphQL metric names
// ═══════════════════════════════════════════════════════════════════════════════

const METRIC_ALIASES: &[(&str, &str)] = &[
    ("dagster-credits", "__dagster_dagster_credits"),
    ("compute-duration", "__dagster_execution_time_ms"),
    (
        "compute-duration-per-asset",
        "__dagster_execution_time_per_asset_ms",
    ),
    ("run-duration", "__dagster_run_duration_ms"),
    ("materializations", "__dagster_materializations"),
    ("observations", "__dagster_observations"),
    ("step-failures", "__dagster_step_failures"),
    ("step-retries", "__dagster_step_retries"),
    ("retry-duration", "__dagster_retry_duration_ms"),
    ("failures", "__dagster_failed_to_materialize"),
    ("freshness-failures", "__dagster_freshness_failures"),
    ("freshness-pass-rate", "__dagster_freshness_pass_rate"),
    ("freshness-warnings", "__dagster_freshness_warnings"),
    ("check-errors", "__dagster_asset_check_errors"),
    ("check-warnings", "__dagster_asset_check_warnings"),
    ("check-success-rate", "__dagster_asset_check_success_rate"),
    ("success-rate", "__dagster_asset_success_rate"),
    ("time-to-resolution", "__dagster_asset_time_to_resolution"),
    ("snowflake-credits", "__cost_snowflake_credits"),
];

/// Resolve a user-provided metric name to the internal GraphQL metric name.
/// Accepts both friendly aliases (e.g. "dagster-credits") and raw internal names.
pub fn resolve_metric_name(input: &str) -> String {
    for (alias, internal) in METRIC_ALIASES {
        if input.eq_ignore_ascii_case(alias) {
            return internal.to_string();
        }
    }
    // Pass through raw metric names as-is
    input.to_string()
}

/// List all known friendly metric aliases.
#[allow(dead_code)]
pub fn list_metric_aliases() -> &'static [(&'static str, &'static str)] {
    METRIC_ALIASES
}

// ═══════════════════════════════════════════════════════════════════════════════
// Time range parsing
// ═══════════════════════════════════════════════════════════════════════════════

/// Parse a duration string like "24h", "7d", "30d", "120d" into seconds.
pub fn parse_duration_to_seconds(input: &str) -> Result<i64> {
    let input = input.trim().to_lowercase();
    if let Some(hours) = input.strip_suffix('h') {
        let h: i64 = hours
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid duration: '{}'", input))?;
        return Ok(h * 3600);
    }
    if let Some(days) = input.strip_suffix('d') {
        let d: i64 = days
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid duration: '{}'", input))?;
        return Ok(d * 86400);
    }
    if let Some(weeks) = input.strip_suffix('w') {
        let w: i64 = weeks
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid duration: '{}'", input))?;
        return Ok(w * 604800);
    }
    anyhow::bail!(
        "Invalid duration '{}'. Use format like '24h', '7d', '30d', '4w'",
        input
    )
}

/// Parse a date string (ISO 8601 or YYYY-MM-DD) into a Unix timestamp (seconds as f64).
pub fn parse_date_to_timestamp(s: &str) -> Result<f64> {
    if let Ok(dt) = DateTime::<FixedOffset>::parse_from_rfc3339(s) {
        return Ok(dt.timestamp() as f64);
    }
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let local = date.and_hms_opt(0, 0, 0).unwrap();
        let dt = local
            .and_local_timezone(Local)
            .single()
            .ok_or_else(|| anyhow::anyhow!("Ambiguous local time for date: {}", s))?;
        return Ok(dt.timestamp() as f64);
    }
    anyhow::bail!(
        "Invalid date format '{}'. Use YYYY-MM-DD or ISO 8601 (e.g. 2026-05-01T10:30:00Z)",
        s
    )
}

/// Resolve the time range from --last, --since, --until flags.
/// Returns (after, before) as Unix timestamps in seconds.
pub fn resolve_time_range(
    last: &Option<String>,
    since: &Option<String>,
    until: &Option<String>,
) -> Result<(f64, f64)> {
    let now = Utc::now().timestamp() as f64;

    match (last, since, until) {
        (Some(duration), None, None) => {
            let secs = parse_duration_to_seconds(duration)?;
            Ok((now - secs as f64, now))
        }
        (None, Some(s), None) => {
            let after = parse_date_to_timestamp(s)?;
            Ok((after, now))
        }
        (None, None, Some(u)) => {
            // Default to 7 days before the until date
            let before = parse_date_to_timestamp(u)?;
            Ok((before - 604800.0, before))
        }
        (None, Some(s), Some(u)) => {
            let after = parse_date_to_timestamp(s)?;
            let before = parse_date_to_timestamp(u)?;
            Ok((after, before))
        }
        (None, None, None) => {
            // Default: last 7 days
            Ok((now - 604800.0, now))
        }
        _ => anyhow::bail!("Cannot combine --last with --since/--until"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Clap value enums
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum Granularity {
    Hourly,
    Daily,
    Weekly,
    Monthly,
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum AggregationFunction {
    Sum,
    Average,
    P75,
    P90,
    P95,
    P99,
    Max,
    Min,
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum MetricsEntityType {
    Jobs,
    Assets,
    #[value(name = "asset-groups")]
    AssetGroups,
    Deployments,
}

// ═══════════════════════════════════════════════════════════════════════════════
// GraphQL query/response types (using raw reqwest, not cynic, because the
// Insights API uses Float scalars for timestamps and dynamic metric names
// that don't map cleanly to cynic's typed approach)
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug, Serialize, serde::Deserialize)]
pub struct ReportingMetricsResponse {
    pub timestamps: Vec<f64>,
    pub metrics: Vec<ReportingEntryResponse>,
}

#[derive(Debug, Serialize, serde::Deserialize)]
pub struct ReportingEntryResponse {
    pub entity: serde_json::Value,
    #[serde(rename = "aggregateValue")]
    pub aggregate_value: f64,
    #[serde(rename = "aggregateValueChange")]
    pub aggregate_value_change: Option<AggregateValueChangeResponse>,
    pub values: Vec<Option<f64>>,
}

#[derive(Debug, Serialize, serde::Deserialize)]
pub struct AggregateValueChangeResponse {
    pub change: f64,
    #[serde(rename = "isNewlyAvailable")]
    pub is_newly_available: bool,
}

#[derive(Debug, Serialize, serde::Deserialize)]
pub struct MetricTypeResponse {
    #[serde(rename = "metricName")]
    pub metric_name: String,
    #[serde(rename = "displayName")]
    pub display_name: String,
    pub category: Option<String>,
    #[serde(rename = "unitType")]
    pub unit_type: Option<String>,
    pub description: Option<String>,
    pub visible: Option<bool>,
}

#[derive(Debug, Serialize, serde::Deserialize)]
pub struct InsightsRunEntryResponse {
    pub value: f64,
    #[serde(rename = "runId")]
    pub run_id: String,
    pub timestamp: f64,
}

#[derive(Debug, Serialize, serde::Deserialize)]
pub struct InsightsRunLevelMetricsResponse {
    #[serde(rename = "runsWithData")]
    pub runs_with_data: Vec<InsightsRunEntryResponse>,
}

#[derive(Debug, Serialize, serde::Deserialize)]
pub struct ReportingMetadataResponse {
    #[serde(rename = "downsamplingRate")]
    pub downsampling_rate: Option<f64>,
    #[serde(rename = "latestDataTimestamp")]
    pub latest_data_timestamp: Option<f64>,
    #[serde(rename = "availableMetadataKeys")]
    pub available_metadata_keys: Option<Vec<String>>,
}

// ═══════════════════════════════════════════════════════════════════════════════
// GraphQL query helpers
// ═══════════════════════════════════════════════════════════════════════════════

fn build_metrics_selector(
    after: f64,
    before: f64,
    metric_name: &str,
    granularity: &Granularity,
    aggregation: &Option<AggregationFunction>,
) -> String {
    let gran = match granularity {
        Granularity::Hourly => "HOURLY",
        Granularity::Daily => "DAILY",
        Granularity::Weekly => "WEEKLY",
        Granularity::Monthly => "MONTHLY",
    };
    let agg_str = match aggregation {
        Some(AggregationFunction::Sum) => ", aggregationFunction: SUM",
        Some(AggregationFunction::Average) => ", aggregationFunction: AVERAGE",
        Some(AggregationFunction::P75) => ", aggregationFunction: P75",
        Some(AggregationFunction::P90) => ", aggregationFunction: P90",
        Some(AggregationFunction::P95) => ", aggregationFunction: P95",
        Some(AggregationFunction::P99) => ", aggregationFunction: P99",
        Some(AggregationFunction::Max) => ", aggregationFunction: MAX",
        Some(AggregationFunction::Min) => ", aggregationFunction: MIN",
        None => "",
    };
    format!(
        "{{ after: {}, before: {}, metricName: \"{}\", granularity: {}{} }}",
        after, before, metric_name, gran, agg_str
    )
}

async fn execute_graphql(token: &str, api_url: &str, query: &str) -> Result<serde_json::Value> {
    let client = reqwest::Client::new();
    let response = client
        .post(api_url)
        .header("Content-Type", "application/json")
        .header("Dagster-Cloud-Api-Token", token)
        .json(&serde_json::json!({ "query": query }))
        .send()
        .await?;

    let body: serde_json::Value = response.json().await?;

    if let Some(errors) = body.get("errors") {
        anyhow::bail!("GraphQL errors: {}", errors);
    }

    body.get("data")
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("No data in response"))
}

/// Fetch the current deployment ID from the API.
async fn fetch_current_deployment_id(token: &str, api_url: &str) -> Result<i64> {
    let query = r#"{ currentDeployment { deploymentId } }"#;
    let data = execute_graphql(token, api_url, query).await?;
    data.get("currentDeployment")
        .and_then(|v| v.get("deploymentId"))
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow::anyhow!("Could not determine current deployment ID"))
}

// ═══════════════════════════════════════════════════════════════════════════════
// Public command functions
// ═══════════════════════════════════════════════════════════════════════════════

/// List available metrics for a given entity type.
pub async fn list_metrics(
    token: &str,
    api_url: &str,
    entity_type: &Option<MetricsEntityType>,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    let query_field = match entity_type {
        Some(MetricsEntityType::Assets) => "metricTypesForAsset",
        Some(MetricsEntityType::AssetGroups) => "metricTypesForAssetGroup",
        Some(MetricsEntityType::Deployments) => "metricTypesForDeployment",
        _ => "metricTypesForJob",
    };

    let query = format!(
        "{{ {}(metricsStoreType: VICTORIA_METRICS) {{ ... on MetricTypeList {{ metricTypes {{ metricName displayName category unitType description visible }} }} ... on UnauthorizedError {{ message }} ... on PythonError {{ message }} }} }}",
        query_field
    );

    let data = execute_graphql(token, api_url, &query).await?;

    let metric_types: Vec<MetricTypeResponse> = serde_json::from_value(
        data.get(query_field)
            .and_then(|v| v.get("metricTypes"))
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Unexpected response format"))?,
    )?;

    // Filter to visible metrics only
    let visible: Vec<&MetricTypeResponse> = metric_types
        .iter()
        .filter(|m| m.visible.unwrap_or(false))
        .collect();

    match fmt {
        Some(f) => output::render(&visible, f),
        None => {
            output::format_insights_metrics_table(&visible);
            Ok(())
        }
    }
}

/// Show deployment insights metadata.
pub async fn get_info(token: &str, api_url: &str, fmt: &Option<OutputFormat>) -> Result<()> {
    let query = r#"{ reportingMetadata { downsamplingRate latestDataTimestamp availableMetadataKeys } metricsTimeRanges { timeRanges } }"#;

    let data = execute_graphql(token, api_url, query).await?;

    let metadata: ReportingMetadataResponse = serde_json::from_value(
        data.get("reportingMetadata")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Unexpected response format"))?,
    )?;

    let time_ranges: Vec<String> = data
        .get("metricsTimeRanges")
        .and_then(|v| v.get("timeRanges"))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    #[derive(Serialize)]
    struct InfoOutput {
        latest_data_timestamp: Option<f64>,
        latest_data_time: String,
        downsampling_rate: Option<f64>,
        available_time_ranges: Vec<String>,
        available_metadata_keys: Vec<String>,
    }

    let info = InfoOutput {
        latest_data_timestamp: metadata.latest_data_timestamp,
        latest_data_time: metadata
            .latest_data_timestamp
            .map(|ts| output::format_timestamp(Some(ts)))
            .unwrap_or_else(|| "-".into()),
        downsampling_rate: metadata.downsampling_rate,
        available_time_ranges: time_ranges,
        available_metadata_keys: metadata.available_metadata_keys.unwrap_or_default(),
    };

    match fmt {
        Some(f) => output::render(&info, f),
        None => {
            output::format_insights_info(
                &info.latest_data_time,
                info.downsampling_rate,
                &info.available_time_ranges,
            );
            Ok(())
        }
    }
}

/// Query metrics by job.
#[allow(clippy::too_many_arguments)]
pub async fn metrics_by_job(
    token: &str,
    api_url: &str,
    metric: &str,
    last: &Option<String>,
    since: &Option<String>,
    until: &Option<String>,
    granularity: &Granularity,
    aggregation: &Option<AggregationFunction>,
    limit: Option<i32>,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    let metric_name = resolve_metric_name(metric);
    let (after, before) = resolve_time_range(last, since, until)?;
    let selector = build_metrics_selector(after, before, &metric_name, granularity, aggregation);

    let filter = build_job_filter(limit);

    let query = format!(
        r#"{{ reportingMetricsByJob(metricsSelector: {}, metricsFilter: {}, metricsStoreType: VICTORIA_METRICS) {{ ... on ReportingMetrics {{ timestamps metrics {{ entity {{ ... on ReportingJob {{ jobName codeLocationName repositoryName }} }} aggregateValue aggregateValueChange {{ change isNewlyAvailable }} values }} }} ... on ReportingInputError {{ message }} ... on PythonError {{ message }} ... on UnauthorizedError {{ message }} }} }}"#,
        selector, filter
    );

    let data = execute_graphql(token, api_url, &query).await?;
    let field = data
        .get("reportingMetricsByJob")
        .ok_or_else(|| anyhow::anyhow!("Unexpected response format"))?;

    if let Some(msg) = field.get("message") {
        anyhow::bail!("API error: {}", msg.as_str().unwrap_or("unknown"));
    }

    let response: ReportingMetricsResponse = serde_json::from_value(field.clone())?;
    render_metrics_response(&response, "JOB", "CODE LOCATION", metric, fmt)
}

/// Query metrics by asset.
#[allow(clippy::too_many_arguments)]
pub async fn metrics_by_asset(
    token: &str,
    api_url: &str,
    metric: &str,
    last: &Option<String>,
    since: &Option<String>,
    until: &Option<String>,
    granularity: &Granularity,
    aggregation: &Option<AggregationFunction>,
    limit: Option<i32>,
    code_location: &Option<String>,
    group: &Option<String>,
    selection: &Option<String>,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    let asset_selection = build_asset_selection(code_location, group, selection)?;
    let metric_name = resolve_metric_name(metric);
    let (after, before) = resolve_time_range(last, since, until)?;
    let selector = build_metrics_selector(after, before, &metric_name, granularity, aggregation);

    let filter = build_asset_filter(limit, &asset_selection);

    let query = format!(
        r#"{{ reportingMetricsByAsset(metricsSelector: {}, metricsFilter: {}, metricsStoreType: VICTORIA_METRICS) {{ ... on ReportingMetrics {{ timestamps metrics {{ entity {{ ... on ReportingAsset {{ assetKey {{ path }} assetGroup codeLocationName }} }} aggregateValue aggregateValueChange {{ change isNewlyAvailable }} values }} }} ... on ReportingInputError {{ message }} ... on PythonError {{ message }} ... on UnauthorizedError {{ message }} }} }}"#,
        selector, filter
    );

    let data = execute_graphql(token, api_url, &query).await?;
    let field = data
        .get("reportingMetricsByAsset")
        .ok_or_else(|| anyhow::anyhow!("Unexpected response format"))?;

    if let Some(msg) = field.get("message") {
        anyhow::bail!("API error: {}", msg.as_str().unwrap_or("unknown"));
    }

    let response: ReportingMetricsResponse = serde_json::from_value(field.clone())?;
    render_metrics_response(&response, "ASSET", "CODE LOCATION", metric, fmt)
}

/// Query metrics by asset group.
#[allow(clippy::too_many_arguments)]
pub async fn metrics_by_asset_group(
    token: &str,
    api_url: &str,
    metric: &str,
    last: &Option<String>,
    since: &Option<String>,
    until: &Option<String>,
    granularity: &Granularity,
    aggregation: &Option<AggregationFunction>,
    limit: Option<i32>,
    code_location: &Option<String>,
    selection: &Option<String>,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    let asset_selection = build_asset_selection(code_location, &None, selection)?;
    let metric_name = resolve_metric_name(metric);
    let (after, before) = resolve_time_range(last, since, until)?;
    let selector = build_metrics_selector(after, before, &metric_name, granularity, aggregation);

    let filter = build_asset_filter(limit, &asset_selection);

    let query = format!(
        r#"{{ reportingMetricsByAssetGroup(metricsSelector: {}, metricsFilter: {}, metricsStoreType: VICTORIA_METRICS) {{ ... on ReportingMetrics {{ timestamps metrics {{ entity {{ ... on ReportingAssetGroup {{ groupName codeLocationName repositoryName }} }} aggregateValue aggregateValueChange {{ change isNewlyAvailable }} values }} }} ... on ReportingInputError {{ message }} ... on PythonError {{ message }} ... on UnauthorizedError {{ message }} }} }}"#,
        selector, filter
    );

    let data = execute_graphql(token, api_url, &query).await?;
    let field = data
        .get("reportingMetricsByAssetGroup")
        .ok_or_else(|| anyhow::anyhow!("Unexpected response format"))?;

    if let Some(msg) = field.get("message") {
        anyhow::bail!("API error: {}", msg.as_str().unwrap_or("unknown"));
    }

    let response: ReportingMetricsResponse = serde_json::from_value(field.clone())?;
    render_metrics_response(&response, "GROUP", "CODE LOCATION", metric, fmt)
}

/// Query metrics by deployment.
#[allow(clippy::too_many_arguments)]
pub async fn metrics_by_deployment(
    token: &str,
    api_url: &str,
    metric: &str,
    last: &Option<String>,
    since: &Option<String>,
    until: &Option<String>,
    granularity: &Granularity,
    aggregation: &Option<AggregationFunction>,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    let metric_name = resolve_metric_name(metric);
    let (after, before) = resolve_time_range(last, since, until)?;
    let selector = build_metrics_selector(after, before, &metric_name, granularity, aggregation);

    // Fetch the current deployment ID to filter correctly
    let deployment_id = fetch_current_deployment_id(token, api_url).await?;

    let query = format!(
        r#"{{ reportingMetricsByDeployment(metricsSelector: {}, metricsFilter: {{ deploymentIds: [{}], branchDeployments: false }}, metricsStoreType: VICTORIA_METRICS) {{ ... on ReportingMetrics {{ timestamps metrics {{ entity {{ ... on DagsterCloudDeployment {{ deploymentName }} }} aggregateValue aggregateValueChange {{ change isNewlyAvailable }} values }} }} ... on ReportingInputError {{ message }} ... on PythonError {{ message }} ... on UnauthorizedError {{ message }} }} }}"#,
        selector, deployment_id
    );

    let data = execute_graphql(token, api_url, &query).await?;
    let field = data
        .get("reportingMetricsByDeployment")
        .ok_or_else(|| anyhow::anyhow!("Unexpected response format"))?;

    if let Some(msg) = field.get("message") {
        anyhow::bail!("API error: {}", msg.as_str().unwrap_or("unknown"));
    }

    let response: ReportingMetricsResponse = serde_json::from_value(field.clone())?;
    render_metrics_response(&response, "DEPLOYMENT", "", metric, fmt)
}

/// Query per-run metrics for a specific job.
pub async fn job_run_metrics(
    token: &str,
    api_url: &str,
    metric: &str,
    job: &str,
    code_location: &str,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    let metric_name = resolve_metric_name(metric);

    let query = format!(
        r#"{{ runLevelMetricsForJob(metricsSelector: {{ metricName: "{}" }}, job: {{ jobName: "{}", codeLocationName: "{}", repositoryName: "__repository__" }}) {{ ... on InsightsRunLevelMetrics {{ runsWithData {{ value runId timestamp }} }} ... on ReportingInputError {{ message }} ... on PythonError {{ message }} ... on UnauthorizedError {{ message }} }} }}"#,
        metric_name, job, code_location
    );

    let data = execute_graphql(token, api_url, &query).await?;
    let field = data
        .get("runLevelMetricsForJob")
        .ok_or_else(|| anyhow::anyhow!("Unexpected response format"))?;

    if let Some(msg) = field.get("message") {
        anyhow::bail!("API error: {}", msg.as_str().unwrap_or("unknown"));
    }

    let response: InsightsRunLevelMetricsResponse = serde_json::from_value(field.clone())?;

    match fmt {
        Some(f) => output::render(&response.runs_with_data, f),
        None => {
            output::format_insights_run_metrics_table(&response.runs_with_data, metric);
            Ok(())
        }
    }
}

/// Query per-materialization metrics for a specific asset.
pub async fn asset_materialization_metrics(
    token: &str,
    api_url: &str,
    metric: &str,
    asset_key: &str,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    let metric_name = resolve_metric_name(metric);
    let path_parts: Vec<&str> = asset_key.split('/').collect();
    let path_json: String = path_parts
        .iter()
        .map(|p| format!("\"{}\"", p))
        .collect::<Vec<_>>()
        .join(", ");

    let query = format!(
        r#"{{ materializationLevelMetricsForAsset(metricsSelector: {{ metricName: "{}" }}, assetKey: {{ assetKey: {{ path: [{}] }} }}) {{ ... on InsightsRunLevelMetrics {{ runsWithData {{ value runId timestamp }} }} ... on ReportingInputError {{ message }} ... on PythonError {{ message }} ... on UnauthorizedError {{ message }} }} }}"#,
        metric_name, path_json
    );

    let data = execute_graphql(token, api_url, &query).await?;
    let field = data
        .get("materializationLevelMetricsForAsset")
        .ok_or_else(|| anyhow::anyhow!("Unexpected response format"))?;

    if let Some(msg) = field.get("message") {
        anyhow::bail!("API error: {}", msg.as_str().unwrap_or("unknown"));
    }

    let response: InsightsRunLevelMetricsResponse = serde_json::from_value(field.clone())?;

    match fmt {
        Some(f) => output::render(&response.runs_with_data, f),
        None => {
            output::format_insights_run_metrics_table(&response.runs_with_data, metric);
            Ok(())
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Filter builders
// ═══════════════════════════════════════════════════════════════════════════════

fn build_job_filter(limit: Option<i32>) -> String {
    let limit_val = limit.unwrap_or(10);
    format!("{{ limit: {} }}", limit_val)
}

/// Build the asset selection string from convenience flags or raw selection.
/// Fails fast if --selection is combined with --code-location or --group.
pub fn build_asset_selection(
    code_location: &Option<String>,
    group: &Option<String>,
    selection: &Option<String>,
) -> Result<Option<String>> {
    let has_convenience_filters = code_location.is_some() || group.is_some();

    if selection.is_some() && has_convenience_filters {
        anyhow::bail!(
            "Cannot combine --selection with --code-location or --group. Use --selection alone for custom queries."
        );
    }

    if let Some(sel) = selection {
        return Ok(Some(sel.clone()));
    }

    if !has_convenience_filters {
        return Ok(None);
    }

    // Build DSL from convenience flags, combining with "and"
    let mut parts: Vec<String> = Vec::new();
    if let Some(loc) = code_location {
        parts.push(format!("code_location:\"{}\"", loc));
    }
    if let Some(g) = group {
        parts.push(format!("group:\"{}\"", g));
    }

    Ok(Some(parts.join(" and ")))
}

fn build_asset_filter(limit: Option<i32>, asset_selection: &Option<String>) -> String {
    let limit_val = limit.unwrap_or(10);
    match asset_selection {
        Some(sel) => {
            let escaped = sel.replace('\\', "\\\\").replace('"', "\\\"");
            format!(
                "{{ assetSelection: \"{}\", limit: {} }}",
                escaped, limit_val
            )
        }
        None => format!("{{ limit: {} }}", limit_val),
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Response rendering
// ═══════════════════════════════════════════════════════════════════════════════

/// Extract the entity name from the JSON entity object.
fn extract_entity_name(entity: &serde_json::Value) -> (String, String) {
    // Job entity
    if let Some(name) = entity.get("jobName").and_then(|v| v.as_str()) {
        let loc = entity
            .get("codeLocationName")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        return (name.to_string(), loc.to_string());
    }
    // Asset entity
    if let Some(key) = entity.get("assetKey").and_then(|v| v.get("path")) {
        let path_parts: Vec<&str> = key
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        let loc = entity
            .get("codeLocationName")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        return (path_parts.join("/"), loc.to_string());
    }
    // Asset group entity
    if let Some(name) = entity.get("groupName").and_then(|v| v.as_str()) {
        let loc = entity
            .get("codeLocationName")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        return (name.to_string(), loc.to_string());
    }
    // Deployment entity
    if let Some(name) = entity.get("deploymentName").and_then(|v| v.as_str()) {
        return (name.to_string(), String::new());
    }
    ("unknown".to_string(), String::new())
}

fn render_metrics_response(
    response: &ReportingMetricsResponse,
    entity_header: &str,
    secondary_header: &str,
    metric: &str,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    // For JSON/YAML, render the full response including time series
    #[derive(Serialize)]
    struct FullOutput {
        metric: String,
        timestamps: Vec<f64>,
        entries: Vec<MetricEntry>,
    }

    #[derive(Serialize)]
    struct MetricEntry {
        name: String,
        code_location: String,
        aggregate_value: f64,
        change_pct: f64,
        values: Vec<Option<f64>>,
    }

    let entries: Vec<MetricEntry> = response
        .metrics
        .iter()
        .map(|m| {
            let (name, loc) = extract_entity_name(&m.entity);
            MetricEntry {
                name,
                code_location: loc,
                aggregate_value: m.aggregate_value,
                change_pct: m
                    .aggregate_value_change
                    .as_ref()
                    .map(|c| c.change)
                    .unwrap_or(0.0),
                values: m.values.clone(),
            }
        })
        .collect();

    match fmt {
        Some(f) => {
            let output = FullOutput {
                metric: metric.to_string(),
                timestamps: response.timestamps.clone(),
                entries,
            };
            output::render(&output, f)
        }
        None => {
            let rows: Vec<output::InsightsMetricRow> = entries
                .iter()
                .map(|e| output::InsightsMetricRow {
                    name: e.name.clone(),
                    secondary: e.code_location.clone(),
                    aggregate_value: e.aggregate_value,
                    change_pct: e.change_pct,
                })
                .collect();
            output::format_insights_table(&rows, entity_header, secondary_header, metric);
            Ok(())
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Unit tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_metric_name_alias() {
        assert_eq!(
            resolve_metric_name("dagster-credits"),
            "__dagster_dagster_credits"
        );
        assert_eq!(
            resolve_metric_name("compute-duration"),
            "__dagster_execution_time_ms"
        );
        assert_eq!(
            resolve_metric_name("snowflake-credits"),
            "__cost_snowflake_credits"
        );
    }

    #[test]
    fn test_resolve_metric_name_case_insensitive() {
        assert_eq!(
            resolve_metric_name("Dagster-Credits"),
            "__dagster_dagster_credits"
        );
        assert_eq!(
            resolve_metric_name("COMPUTE-DURATION"),
            "__dagster_execution_time_ms"
        );
    }

    #[test]
    fn test_resolve_metric_name_passthrough() {
        assert_eq!(resolve_metric_name("__meta_num_rows"), "__meta_num_rows");
        assert_eq!(resolve_metric_name("custom_metric"), "custom_metric");
    }

    #[test]
    fn test_parse_duration_hours() {
        assert_eq!(parse_duration_to_seconds("24h").unwrap(), 86400);
        assert_eq!(parse_duration_to_seconds("1h").unwrap(), 3600);
        assert_eq!(parse_duration_to_seconds("48h").unwrap(), 172800);
    }

    #[test]
    fn test_parse_duration_days() {
        assert_eq!(parse_duration_to_seconds("7d").unwrap(), 604800);
        assert_eq!(parse_duration_to_seconds("30d").unwrap(), 2592000);
        assert_eq!(parse_duration_to_seconds("120d").unwrap(), 10368000);
    }

    #[test]
    fn test_parse_duration_weeks() {
        assert_eq!(parse_duration_to_seconds("1w").unwrap(), 604800);
        assert_eq!(parse_duration_to_seconds("4w").unwrap(), 2419200);
    }

    #[test]
    fn test_parse_duration_case_insensitive() {
        assert_eq!(parse_duration_to_seconds("7D").unwrap(), 604800);
        assert_eq!(parse_duration_to_seconds("24H").unwrap(), 86400);
    }

    #[test]
    fn test_parse_duration_with_whitespace() {
        assert_eq!(parse_duration_to_seconds(" 7d ").unwrap(), 604800);
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration_to_seconds("7x").is_err());
        assert!(parse_duration_to_seconds("abc").is_err());
        assert!(parse_duration_to_seconds("").is_err());
        assert!(parse_duration_to_seconds("d").is_err());
    }

    #[test]
    fn test_parse_date_to_timestamp_iso8601() {
        let ts = parse_date_to_timestamp("2026-07-01T00:00:00Z").unwrap();
        assert_eq!(ts, 1782864000.0);
    }

    #[test]
    fn test_parse_date_to_timestamp_iso8601_with_offset() {
        let ts = parse_date_to_timestamp("2026-07-01T01:00:00+01:00").unwrap();
        assert_eq!(ts, 1782864000.0);
    }

    #[test]
    fn test_parse_date_to_timestamp_date_only() {
        // Date-only parsing depends on local timezone, just check it doesn't error
        let ts = parse_date_to_timestamp("2026-07-01");
        assert!(ts.is_ok());
    }

    #[test]
    fn test_parse_date_to_timestamp_invalid() {
        assert!(parse_date_to_timestamp("not-a-date").is_err());
        assert!(parse_date_to_timestamp("2026-13-01").is_err());
    }

    #[test]
    fn test_resolve_time_range_last() {
        let (after, before) = resolve_time_range(&Some("7d".to_string()), &None, &None).unwrap();
        let diff = before - after;
        assert!((diff - 604800.0).abs() < 1.0);
    }

    #[test]
    fn test_resolve_time_range_since_until() {
        let (after, before) = resolve_time_range(
            &None,
            &Some("2026-07-01T00:00:00Z".to_string()),
            &Some("2026-07-08T00:00:00Z".to_string()),
        )
        .unwrap();
        assert_eq!(after, 1782864000.0);
        assert_eq!(before, 1783468800.0);
    }

    #[test]
    fn test_resolve_time_range_default_7d() {
        let (after, before) = resolve_time_range(&None, &None, &None).unwrap();
        let diff = before - after;
        assert!((diff - 604800.0).abs() < 1.0);
    }

    #[test]
    fn test_resolve_time_range_conflict() {
        let result = resolve_time_range(
            &Some("7d".to_string()),
            &Some("2026-07-01T00:00:00Z".to_string()),
            &None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_entity_name_job() {
        let entity = serde_json::json!({
            "jobName": "my_job",
            "codeLocationName": "my-location",
            "repositoryName": "__repository__"
        });
        let (name, loc) = extract_entity_name(&entity);
        assert_eq!(name, "my_job");
        assert_eq!(loc, "my-location");
    }

    #[test]
    fn test_extract_entity_name_asset() {
        let entity = serde_json::json!({
            "assetKey": { "path": ["prefix", "my_asset"] },
            "assetGroup": "my_group",
            "codeLocationName": "my-location"
        });
        let (name, loc) = extract_entity_name(&entity);
        assert_eq!(name, "prefix/my_asset");
        assert_eq!(loc, "my-location");
    }

    #[test]
    fn test_extract_entity_name_asset_group() {
        let entity = serde_json::json!({
            "groupName": "my_group",
            "codeLocationName": "my-location",
            "repositoryName": "__repository__"
        });
        let (name, loc) = extract_entity_name(&entity);
        assert_eq!(name, "my_group");
        assert_eq!(loc, "my-location");
    }

    #[test]
    fn test_extract_entity_name_deployment() {
        let entity = serde_json::json!({
            "deploymentName": "prod"
        });
        let (name, loc) = extract_entity_name(&entity);
        assert_eq!(name, "prod");
        assert_eq!(loc, "");
    }

    #[test]
    fn test_build_job_filter_defaults() {
        let filter = build_job_filter(None);
        assert_eq!(filter, "{ limit: 10 }");
    }

    #[test]
    fn test_build_job_filter_with_limit() {
        let filter = build_job_filter(Some(5));
        assert_eq!(filter, "{ limit: 5 }");
    }

    #[test]
    fn test_build_asset_selection_from_code_location() {
        let sel = build_asset_selection(&Some("dp-dagster".to_string()), &None, &None).unwrap();
        assert_eq!(sel, Some("code_location:\"dp-dagster\"".to_string()));
    }

    #[test]
    fn test_build_asset_selection_from_group() {
        let sel = build_asset_selection(&None, &Some("ism".to_string()), &None).unwrap();
        assert_eq!(sel, Some("group:\"ism\"".to_string()));
    }

    #[test]
    fn test_build_asset_selection_combined_with_and() {
        let sel = build_asset_selection(
            &Some("dp-dagster".to_string()),
            &Some("ism".to_string()),
            &None,
        )
        .unwrap();
        assert_eq!(
            sel,
            Some("code_location:\"dp-dagster\" and group:\"ism\"".to_string())
        );
    }

    #[test]
    fn test_build_asset_selection_raw_passthrough() {
        let sel = build_asset_selection(
            &None,
            &None,
            &Some("key:\"dp_model_db/release/fdp_*\"".to_string()),
        )
        .unwrap();
        assert_eq!(sel, Some("key:\"dp_model_db/release/fdp_*\"".to_string()));
    }

    #[test]
    fn test_build_asset_selection_none_when_no_filters() {
        let sel = build_asset_selection(&None, &None, &None).unwrap();
        assert_eq!(sel, None);
    }

    #[test]
    fn test_build_asset_selection_conflict_errors() {
        let result = build_asset_selection(
            &Some("dp-dagster".to_string()),
            &None,
            &Some("key:\"x\"".to_string()),
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Cannot combine --selection")
        );
    }

    #[test]
    fn test_build_asset_filter_with_selection() {
        let sel = Some("code_location:\"dp-dagster\"".to_string());
        let filter = build_asset_filter(Some(20), &sel);
        assert!(filter.contains("assetSelection"));
        assert!(filter.contains("dp-dagster"));
        assert!(filter.contains("limit: 20"));
    }

    #[test]
    fn test_build_asset_filter_no_selection() {
        let filter = build_asset_filter(None, &None);
        assert_eq!(filter, "{ limit: 10 }");
    }

    #[test]
    fn test_list_metric_aliases_not_empty() {
        let aliases = list_metric_aliases();
        assert!(!aliases.is_empty());
        // All aliases should map to strings starting with __
        for (_, internal) in aliases {
            assert!(internal.starts_with("__"));
        }
    }
}
