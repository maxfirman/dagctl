use anyhow::Result;
use serde::Serialize;

use crate::output::{self, OutputFormat};

// --- Shared types ---

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetKey {
    path: Vec<String>,
}

#[derive(cynic::InputObject, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetKeyInput {
    path: Vec<String>,
}

pub fn parse_asset_key(key: &str) -> Vec<String> {
    key.split('/').map(|s| s.to_string()).collect()
}

pub fn format_asset_key(path: &[String]) -> String {
    path.join("/")
}

// --- List assets ---

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "CloudQuery")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetNodesQuery {
    #[cynic(rename = "assetNodes")]
    asset_nodes: Vec<AssetNodeSummary>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "AssetNode")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetNodeSummary {
    #[cynic(rename = "assetKey")]
    asset_key: AssetKey,
    #[cynic(rename = "groupName")]
    group_name: String,
    kinds: Vec<String>,
    #[cynic(rename = "isPartitioned")]
    is_partitioned: bool,
    repository: AssetRepository,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "Repository")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetRepository {
    location: AssetLocation,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "RepositoryLocation")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetLocation {
    name: String,
}

#[derive(Serialize)]
struct AssetListEntry {
    key: String,
    group: String,
    code_location: String,
    kinds: Vec<String>,
    partitioned: bool,
}

pub async fn list_assets(
    token: &str,
    api_url: &str,
    group: Option<String>,
    code_location: Option<String>,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let response = reqwest::Client::new()
        .post(api_url)
        .header("Authorization", format!("Bearer {}", token))
        .run_graphql(AssetNodesQuery::build(()))
        .await?;

    if let Some(errors) = response.errors {
        anyhow::bail!("GraphQL errors: {:?}", errors);
    }

    let data = response
        .data
        .ok_or_else(|| anyhow::anyhow!("No data in response"))?;

    let entries: Vec<AssetListEntry> = data
        .asset_nodes
        .into_iter()
        .filter(|n| {
            if let Some(ref g) = group
                && &n.group_name != g
            {
                return false;
            }
            if let Some(ref loc) = code_location
                && &n.repository.location.name != loc
            {
                return false;
            }
            true
        })
        .map(|n| AssetListEntry {
            key: format_asset_key(&n.asset_key.path),
            group: n.group_name,
            code_location: n.repository.location.name,
            kinds: n.kinds,
            partitioned: n.is_partitioned,
        })
        .collect();

    match fmt {
        Some(f) => output::render(&entries, f),
        None => {
            let rows: Vec<_> = entries
                .iter()
                .map(|e| {
                    (
                        e.key.clone(),
                        e.group.clone(),
                        e.code_location.clone(),
                        e.kinds.join(", "),
                        if e.partitioned {
                            "partitioned".into()
                        } else {
                            "".into()
                        },
                    )
                })
                .collect();
            output::format_assets_table(&rows);
            Ok(())
        }
    }
}

// --- Asset detail ---

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetNodeQueryVariables {
    #[cynic(rename = "assetKey")]
    asset_key: AssetKeyInput,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "AssetNodeQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetNodeQuery {
    #[arguments(assetKey: $asset_key)]
    #[cynic(rename = "assetNodeOrError")]
    asset_node_or_error: AssetNodeOrError,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "AssetNodeOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetNodeOrError {
    AssetNode(Box<AssetNodeDetail>),
    AssetNotFoundError(AssetNotFoundError),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetNotFoundError {
    message: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetNode")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetNodeDetail {
    #[cynic(rename = "assetKey")]
    asset_key: AssetKey,
    #[cynic(rename = "groupName")]
    group_name: String,
    description: Option<String>,
    kinds: Vec<String>,
    #[cynic(rename = "isPartitioned")]
    is_partitioned: bool,
    #[cynic(rename = "jobNames")]
    job_names: Vec<String>,
    #[cynic(rename = "dependencyKeys")]
    dependency_keys: Vec<AssetKey>,
    #[cynic(rename = "dependedByKeys")]
    depended_by_keys: Vec<AssetKey>,
    repository: AssetRepository,
    owners: Vec<AssetOwner>,
    tags: Vec<DefinitionTag>,
    #[cynic(rename = "automationCondition")]
    automation_condition: Option<AutomationCondition>,
    #[cynic(rename = "targetingInstigators")]
    targeting_instigators: Vec<Instigator>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct DefinitionTag {
    key: String,
    value: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AutomationCondition {
    label: Option<String>,
    #[cynic(rename = "expandedLabel")]
    expanded_label: Vec<String>,
}

#[derive(cynic::InlineFragments, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "Instigator")]
#[cynic(schema_module = "crate::schema::schema")]
enum Instigator {
    Schedule(InstigatorSchedule),
    Sensor(InstigatorSensor),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "Schedule")]
#[cynic(schema_module = "crate::schema::schema")]
struct InstigatorSchedule {
    name: String,
    #[cynic(rename = "cronSchedule")]
    cron_schedule: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "Sensor")]
#[cynic(schema_module = "crate::schema::schema")]
struct InstigatorSensor {
    name: String,
    #[cynic(rename = "sensorType")]
    sensor_type: SensorType,
}

#[derive(cynic::Enum, Clone, Copy, Debug)]
#[cynic(schema = "dagster", graphql_type = "SensorType")]
#[cynic(schema_module = "crate::schema::schema")]
enum SensorType {
    Standard,
    RunStatus,
    Asset,
    MultiAsset,
    FreshnessPolicy,
    AutoMaterialize,
    Automation,
    Unknown,
}

#[derive(cynic::InlineFragments, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetOwner")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetOwner {
    UserAssetOwner(UserAssetOwner),
    TeamAssetOwner(TeamAssetOwner),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct UserAssetOwner {
    email: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct TeamAssetOwner {
    team: String,
}

pub async fn get_asset(
    token: &str,
    api_url: &str,
    key: String,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let path = parse_asset_key(&key);

    let operation = AssetNodeQuery::build(AssetNodeQueryVariables {
        asset_key: AssetKeyInput { path },
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

    match data.asset_node_or_error {
        AssetNodeOrError::AssetNode(node) => match fmt {
            Some(f) => output::render(&node, f),
            None => {
                let deps: Vec<_> = node
                    .dependency_keys
                    .iter()
                    .map(|k| format_asset_key(&k.path))
                    .collect();
                let dependents: Vec<_> = node
                    .depended_by_keys
                    .iter()
                    .map(|k| format_asset_key(&k.path))
                    .collect();
                let owners: Vec<_> = node
                    .owners
                    .iter()
                    .map(|o| match o {
                        AssetOwner::UserAssetOwner(u) => u.email.clone(),
                        AssetOwner::TeamAssetOwner(t) => format!("team:{}", t.team),
                        AssetOwner::Other => "unknown".into(),
                    })
                    .collect();
                let mut sensors = Vec::new();
                let mut schedules = Vec::new();
                for i in &node.targeting_instigators {
                    match i {
                        Instigator::Sensor(s) => {
                            sensors.push(format!("{} ({:?})", s.name, s.sensor_type))
                        }
                        Instigator::Schedule(s) => {
                            schedules.push(format!("{} ({})", s.name, s.cron_schedule))
                        }
                        Instigator::Other => {}
                    }
                }
                let automation = node
                    .automation_condition
                    .as_ref()
                    .and_then(|ac| ac.label.clone())
                    .unwrap_or_default();
                let tags: Vec<_> = node
                    .tags
                    .iter()
                    .map(|t| {
                        if t.value.is_empty() {
                            t.key.clone()
                        } else {
                            format!("{}={}", t.key, t.value)
                        }
                    })
                    .collect();
                output::format_asset_detail(&output::AssetDetail {
                    key: &format_asset_key(&node.asset_key.path),
                    group: &node.group_name,
                    code_location: &node.repository.location.name,
                    description: node.description.as_deref().unwrap_or(""),
                    kinds: &node.kinds,
                    partitioned: node.is_partitioned,
                    dependencies: &deps,
                    dependents: &dependents,
                    jobs: &node.job_names,
                    owners: &owners,
                    automation_condition: &automation,
                    sensors: &sensors,
                    schedules: &schedules,
                    tags: &tags,
                });
                Ok(())
            }
        },
        AssetNodeOrError::AssetNotFoundError(err) => {
            anyhow::bail!("Asset not found: {}", err.message)
        }
        AssetNodeOrError::Other => anyhow::bail!("Unexpected response type from API"),
    }
}
