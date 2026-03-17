use anyhow::Result;
use serde::Serialize;

use crate::output::{self, OutputFormat};

// --- List jobs via workspace query ---

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "CloudQuery")]
#[cynic(schema_module = "crate::schema::schema")]
struct WorkspaceJobsQuery {
    #[cynic(rename = "workspaceOrError")]
    workspace_or_error: WorkspaceOrError,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "WorkspaceOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum WorkspaceOrError {
    Workspace(Workspace),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct Workspace {
    #[cynic(rename = "locationEntries")]
    location_entries: Vec<WorkspaceLocationEntry>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct WorkspaceLocationEntry {
    name: String,
    #[cynic(rename = "locationOrLoadError")]
    location_or_load_error: Option<RepositoryLocationOrLoadError>,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "RepositoryLocationOrLoadError")]
#[cynic(schema_module = "crate::schema::schema")]
enum RepositoryLocationOrLoadError {
    RepositoryLocation(RepositoryLocation),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct RepositoryLocation {
    repositories: Vec<Repository>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct Repository {
    #[allow(dead_code)]
    name: String,
    jobs: Vec<JobSummary>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "Job")]
#[cynic(schema_module = "crate::schema::schema")]
struct JobSummary {
    name: String,
    #[cynic(rename = "isJob")]
    is_job: bool,
    schedules: Vec<ScheduleName>,
    sensors: Vec<SensorName>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "Schedule")]
#[cynic(schema_module = "crate::schema::schema")]
struct ScheduleName {
    name: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "Sensor")]
#[cynic(schema_module = "crate::schema::schema")]
struct SensorName {
    name: String,
}

#[derive(Serialize)]
struct JobListEntry {
    name: String,
    code_location: String,
    schedules: Vec<String>,
    sensors: Vec<String>,
}

pub async fn list_jobs(
    token: &str,
    api_url: &str,
    code_location: Option<String>,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let response = reqwest::Client::new()
        .post(api_url)
        .header("Authorization", format!("Bearer {}", token))
        .run_graphql(WorkspaceJobsQuery::build(()))
        .await?;

    if let Some(errors) = response.errors {
        anyhow::bail!("GraphQL errors: {:?}", errors);
    }

    let data = response
        .data
        .ok_or_else(|| anyhow::anyhow!("No data in response"))?;

    match data.workspace_or_error {
        WorkspaceOrError::Workspace(ws) => {
            let mut entries: Vec<JobListEntry> = Vec::new();
            for loc in ws.location_entries {
                if let Some(ref filter) = code_location
                    && &loc.name != filter
                {
                    continue;
                }
                if let Some(RepositoryLocationOrLoadError::RepositoryLocation(rl)) =
                    loc.location_or_load_error
                {
                    for repo in rl.repositories {
                        for job in repo.jobs {
                            if !job.is_job {
                                continue;
                            }
                            entries.push(JobListEntry {
                                name: job.name,
                                code_location: loc.name.clone(),
                                schedules: job.schedules.into_iter().map(|s| s.name).collect(),
                                sensors: job.sensors.into_iter().map(|s| s.name).collect(),
                            });
                        }
                    }
                }
            }
            match fmt {
                Some(f) => output::render(&entries, f),
                None => {
                    let rows: Vec<_> = entries
                        .iter()
                        .map(|e| {
                            (
                                e.name.clone(),
                                e.code_location.clone(),
                                e.schedules.len(),
                                e.sensors.len(),
                            )
                        })
                        .collect();
                    output::format_jobs_table(&rows);
                    Ok(())
                }
            }
        }
        WorkspaceOrError::Other => anyhow::bail!("Unexpected response type from API"),
    }
}

// --- Job detail via pipelineOrError ---

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct JobQueryVariables {
    params: PipelineSelector,
}

#[derive(cynic::InputObject, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct PipelineSelector {
    #[cynic(rename = "pipelineName")]
    pipeline_name: String,
    #[cynic(rename = "repositoryName")]
    repository_name: String,
    #[cynic(rename = "repositoryLocationName")]
    repository_location_name: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "JobQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct JobQuery {
    #[arguments(params: $params)]
    #[cynic(rename = "pipelineOrError")]
    pipeline_or_error: PipelineOrError,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "PipelineOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum PipelineOrError {
    Pipeline(JobDetail),
    PipelineNotFoundError(PipelineNotFoundError),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct PipelineNotFoundError {
    message: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "Pipeline")]
#[cynic(schema_module = "crate::schema::schema")]
struct JobDetail {
    name: String,
    description: Option<String>,
    #[cynic(rename = "isJob")]
    is_job: bool,
    tags: Vec<PipelineTag>,
    schedules: Vec<ScheduleInfo>,
    sensors: Vec<SensorInfo>,
    repository: RepositoryRef,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct PipelineTag {
    key: String,
    value: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "Schedule")]
#[cynic(schema_module = "crate::schema::schema")]
struct ScheduleInfo {
    name: String,
    #[cynic(rename = "cronSchedule")]
    cron_schedule: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "Sensor")]
#[cynic(schema_module = "crate::schema::schema")]
struct SensorInfo {
    name: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "Repository")]
#[cynic(schema_module = "crate::schema::schema")]
struct RepositoryRef {
    name: String,
    location: LocationRef,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "RepositoryLocation")]
#[cynic(schema_module = "crate::schema::schema")]
struct LocationRef {
    name: String,
}

pub async fn get_job(
    token: &str,
    api_url: &str,
    name: String,
    code_location: &str,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    let repo_name = resolve_repo_name(token, api_url, code_location).await?;

    use cynic::{QueryBuilder, http::ReqwestExt};

    let operation = JobQuery::build(JobQueryVariables {
        params: PipelineSelector {
            pipeline_name: name,
            repository_name: repo_name,
            repository_location_name: code_location.to_string(),
        },
    });

    let response = reqwest::Client::new()
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

    match data.pipeline_or_error {
        PipelineOrError::Pipeline(job) => match fmt {
            Some(f) => output::render(&job, f),
            None => {
                let schedules: Vec<_> = job
                    .schedules
                    .iter()
                    .map(|s| format!("{} ({})", s.name, s.cron_schedule))
                    .collect();
                let sensors: Vec<_> = job.sensors.iter().map(|s| s.name.clone()).collect();
                let tags: Vec<_> = job
                    .tags
                    .iter()
                    .map(|t| format!("{}={}", t.key, t.value))
                    .collect();
                output::format_job_detail(
                    &job.name,
                    &job.repository.location.name,
                    job.description.as_deref().unwrap_or(""),
                    &schedules,
                    &sensors,
                    &tags,
                );
                Ok(())
            }
        },
        PipelineOrError::PipelineNotFoundError(err) => {
            anyhow::bail!("Job not found: {}", err.message)
        }
        PipelineOrError::Other => anyhow::bail!("Unexpected response type from API"),
    }
}

// --- Resolve repo name from a specific code location ---

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct LocationEntryQueryVariables {
    name: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "LocationEntryQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct LocationEntryQuery {
    #[arguments(name: $name)]
    #[cynic(rename = "workspaceLocationEntryOrError")]
    workspace_location_entry_or_error: Option<LocationEntryOrError>,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "WorkspaceLocationEntryOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum LocationEntryOrError {
    WorkspaceLocationEntry(LocationEntryDetail),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "WorkspaceLocationEntry")]
#[cynic(schema_module = "crate::schema::schema")]
struct LocationEntryDetail {
    #[cynic(rename = "locationOrLoadError")]
    location_or_load_error: Option<LocationOrLoadError>,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "RepositoryLocationOrLoadError")]
#[cynic(schema_module = "crate::schema::schema")]
enum LocationOrLoadError {
    RepositoryLocation(RepoLocationForLookup),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "RepositoryLocation")]
#[cynic(schema_module = "crate::schema::schema")]
struct RepoLocationForLookup {
    repositories: Vec<RepoName>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "Repository")]
#[cynic(schema_module = "crate::schema::schema")]
struct RepoName {
    name: String,
}

async fn resolve_repo_name(token: &str, api_url: &str, code_location: &str) -> Result<String> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let response = reqwest::Client::new()
        .post(api_url)
        .header("Authorization", format!("Bearer {}", token))
        .run_graphql(LocationEntryQuery::build(LocationEntryQueryVariables {
            name: code_location.to_string(),
        }))
        .await?;

    if let Some(errors) = response.errors {
        anyhow::bail!("GraphQL errors: {:?}", errors);
    }

    let data = response
        .data
        .ok_or_else(|| anyhow::anyhow!("No data in response"))?;

    let entry = data
        .workspace_location_entry_or_error
        .ok_or_else(|| anyhow::anyhow!("Code location '{}' not found", code_location))?;

    match entry {
        LocationEntryOrError::WorkspaceLocationEntry(e) => match e.location_or_load_error {
            Some(LocationOrLoadError::RepositoryLocation(rl)) => rl
                .repositories
                .into_iter()
                .next()
                .map(|r| r.name)
                .ok_or_else(|| {
                    anyhow::anyhow!("No repositories in code location '{}'", code_location)
                }),
            _ => anyhow::bail!("Code location '{}' is not loaded", code_location),
        },
        LocationEntryOrError::Other => {
            anyhow::bail!("Error loading code location '{}'", code_location)
        }
    }
}
