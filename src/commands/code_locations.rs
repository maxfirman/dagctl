use anyhow::Result;
use serde::Serialize;

use crate::output::{self, OutputFormat};

// --- Shared types ---

#[derive(cynic::Enum, Clone, Copy, Debug)]
#[cynic(schema = "dagster", graphql_type = "RepositoryLocationLoadStatus")]
#[cynic(schema_module = "crate::schema::schema")]
enum RepositoryLocationLoadStatus {
    Loading,
    Loaded,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct RepositoryMetadata {
    key: String,
    value: String,
}

// --- List query ---

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "CloudQuery")]
#[cynic(schema_module = "crate::schema::schema")]
struct WorkspaceQuery {
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
    #[cynic(rename = "loadStatus")]
    load_status: RepositoryLocationLoadStatus,
    #[cynic(rename = "updatedTimestamp")]
    updated_timestamp: f64,
    #[cynic(rename = "displayMetadata")]
    display_metadata: Vec<RepositoryMetadata>,
}

#[derive(Serialize)]
struct CodeLocationSummary {
    name: String,
    load_status: String,
    updated_timestamp: f64,
    display_metadata: Vec<MetadataEntry>,
}

#[derive(Serialize)]
struct MetadataEntry {
    key: String,
    value: String,
}

fn to_metadata(m: Vec<RepositoryMetadata>) -> Vec<MetadataEntry> {
    m.into_iter()
        .map(|m| MetadataEntry {
            key: m.key,
            value: m.value,
        })
        .collect()
}

pub async fn list_code_locations(
    token: &str,
    api_url: &str,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let response = reqwest::Client::new()
        .post(api_url)
        .header("Authorization", format!("Bearer {}", token))
        .run_graphql(WorkspaceQuery::build(()))
        .await?;

    if let Some(errors) = response.errors {
        anyhow::bail!("GraphQL errors: {:?}", errors);
    }

    let data = response
        .data
        .ok_or_else(|| anyhow::anyhow!("No data in response"))?;

    match data.workspace_or_error {
        WorkspaceOrError::Workspace(ws) => {
            let entries: Vec<CodeLocationSummary> = ws
                .location_entries
                .into_iter()
                .map(|e| CodeLocationSummary {
                    name: e.name,
                    load_status: format!("{:?}", e.load_status),
                    updated_timestamp: e.updated_timestamp,
                    display_metadata: to_metadata(e.display_metadata),
                })
                .collect();
            match fmt {
                Some(f) => output::render(&entries, f),
                None => {
                    let rows: Vec<_> = entries
                        .iter()
                        .map(|e| (e.name.clone(), e.load_status.clone(), e.updated_timestamp))
                        .collect();
                    output::format_code_locations_table(&rows);
                    Ok(())
                }
            }
        }
        WorkspaceOrError::Other => anyhow::bail!("Unexpected response type from API"),
    }
}

// --- Detail query ---

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct CodeLocationQueryVariables {
    name: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "CodeLocationQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct CodeLocationQuery {
    #[arguments(name: $name)]
    #[cynic(rename = "workspaceLocationEntryOrError")]
    workspace_location_entry_or_error: Option<WorkspaceLocationEntryOrError>,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "WorkspaceLocationEntryOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum WorkspaceLocationEntryOrError {
    WorkspaceLocationEntry(WorkspaceLocationEntryDetail),
    PythonError(PythonError),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct PythonError {
    message: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "WorkspaceLocationEntry")]
#[cynic(schema_module = "crate::schema::schema")]
struct WorkspaceLocationEntryDetail {
    name: String,
    #[cynic(rename = "loadStatus")]
    load_status: RepositoryLocationLoadStatus,
    #[cynic(rename = "updatedTimestamp")]
    updated_timestamp: f64,
    #[cynic(rename = "displayMetadata")]
    display_metadata: Vec<RepositoryMetadata>,
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
    #[cynic(rename = "dagsterLibraryVersions")]
    dagster_library_versions: Option<Vec<DagsterLibraryVersion>>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct Repository {
    name: String,
    jobs: Vec<JobName>,
    schedules: Vec<ScheduleName>,
    sensors: Vec<SensorName>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "Job")]
#[cynic(schema_module = "crate::schema::schema")]
struct JobName {
    #[allow(dead_code)]
    name: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "Schedule")]
#[cynic(schema_module = "crate::schema::schema")]
struct ScheduleName {
    #[allow(dead_code)]
    name: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "Sensor")]
#[cynic(schema_module = "crate::schema::schema")]
struct SensorName {
    #[allow(dead_code)]
    name: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct DagsterLibraryVersion {
    name: String,
    version: String,
}

// Output structs

#[derive(Serialize)]
struct CodeLocationDetail {
    name: String,
    load_status: String,
    updated_timestamp: f64,
    display_metadata: Vec<MetadataEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    repositories: Option<Vec<RepositorySummary>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dagster_library_versions: Option<Vec<LibraryVersion>>,
}

#[derive(Serialize)]
struct RepositorySummary {
    name: String,
    jobs_count: usize,
    schedules_count: usize,
    sensors_count: usize,
}

#[derive(Serialize)]
struct LibraryVersion {
    name: String,
    version: String,
}

pub async fn get_code_location(
    token: &str,
    api_url: &str,
    name: String,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let operation = CodeLocationQuery::build(CodeLocationQueryVariables { name: name.clone() });

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

    let entry = data
        .workspace_location_entry_or_error
        .ok_or_else(|| anyhow::anyhow!("Code location '{}' not found", name))?;

    match entry {
        WorkspaceLocationEntryOrError::WorkspaceLocationEntry(e) => {
            let (repositories, library_versions) = match e.location_or_load_error {
                Some(RepositoryLocationOrLoadError::RepositoryLocation(loc)) => {
                    let repos: Vec<RepositorySummary> = loc
                        .repositories
                        .into_iter()
                        .map(|r| RepositorySummary {
                            name: r.name,
                            jobs_count: r.jobs.len(),
                            schedules_count: r.schedules.len(),
                            sensors_count: r.sensors.len(),
                        })
                        .collect();
                    let libs = loc.dagster_library_versions.map(|vs| {
                        vs.into_iter()
                            .map(|v| LibraryVersion {
                                name: v.name,
                                version: v.version,
                            })
                            .collect()
                    });
                    (Some(repos), libs)
                }
                _ => (None, None),
            };

            let detail = CodeLocationDetail {
                name: e.name,
                load_status: format!("{:?}", e.load_status),
                updated_timestamp: e.updated_timestamp,
                display_metadata: to_metadata(e.display_metadata),
                repositories,
                dagster_library_versions: library_versions,
            };

            match fmt {
                Some(f) => output::render(&detail, f),
                None => {
                    let repos: Vec<_> = detail
                        .repositories
                        .as_ref()
                        .map(|rs| {
                            rs.iter()
                                .map(|r| {
                                    (
                                        r.name.clone(),
                                        r.jobs_count,
                                        r.schedules_count,
                                        r.sensors_count,
                                    )
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    let libs: Vec<_> = detail
                        .dagster_library_versions
                        .as_ref()
                        .map(|ls| {
                            ls.iter()
                                .map(|l| (l.name.clone(), l.version.clone()))
                                .collect()
                        })
                        .unwrap_or_default();
                    output::format_code_location_detail(
                        &detail.name,
                        &detail.load_status,
                        detail.updated_timestamp,
                        &repos,
                        &libs,
                    );
                    Ok(())
                }
            }
        }
        WorkspaceLocationEntryOrError::PythonError(err) => {
            anyhow::bail!("Error loading code location: {}", err.message)
        }
        WorkspaceLocationEntryOrError::Other => {
            anyhow::bail!("Unexpected response type from API")
        }
    }
}
