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
    #[cynic(rename = "opName")]
    op_name: Option<String>,
    #[cynic(rename = "opVersion")]
    op_version: Option<String>,
    #[cynic(rename = "jobNames")]
    job_names: Vec<String>,
    #[cynic(rename = "dependencyKeys")]
    dependency_keys: Vec<AssetKey>,
    #[cynic(rename = "dependedByKeys")]
    depended_by_keys: Vec<AssetKey>,
    repository: AssetRepository,
    owners: Vec<AssetOwner>,
    tags: Vec<DefinitionTag>,
    #[cynic(rename = "metadataEntries")]
    metadata_entries: Vec<MetadataEntryFragment>,
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

#[derive(cynic::InlineFragments, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "MetadataEntry")]
#[cynic(schema_module = "crate::schema::schema")]
enum MetadataEntryFragment {
    TextMetadataEntry(TextMetadataEntry),
    UrlMetadataEntry(UrlMetadataEntry),
    PathMetadataEntry(PathMetadataEntry),
    JsonMetadataEntry(JsonMetadataEntry),
    IntMetadataEntry(IntMetadataEntry),
    FloatMetadataEntry(FloatMetadataEntry),
    BoolMetadataEntry(BoolMetadataEntry),
    MarkdownMetadataEntry(MarkdownMetadataEntry),
    #[cynic(fallback)]
    Other,
}

impl MetadataEntryFragment {
    fn label(&self) -> &str {
        match self {
            Self::TextMetadataEntry(e) => &e.label,
            Self::UrlMetadataEntry(e) => &e.label,
            Self::PathMetadataEntry(e) => &e.label,
            Self::JsonMetadataEntry(e) => &e.label,
            Self::IntMetadataEntry(e) => &e.label,
            Self::FloatMetadataEntry(e) => &e.label,
            Self::BoolMetadataEntry(e) => &e.label,
            Self::MarkdownMetadataEntry(e) => &e.label,
            Self::Other => "",
        }
    }

    fn value(&self) -> String {
        match self {
            Self::TextMetadataEntry(e) => e.text.clone(),
            Self::UrlMetadataEntry(e) => e.url.clone(),
            Self::PathMetadataEntry(e) => e.path.clone(),
            Self::JsonMetadataEntry(e) => e.json_string.clone(),
            Self::IntMetadataEntry(e) => e.int_repr.clone(),
            Self::FloatMetadataEntry(e) => e.float_repr.clone(),
            Self::BoolMetadataEntry(e) => e.bool_value.map(|b| b.to_string()).unwrap_or_default(),
            Self::MarkdownMetadataEntry(e) => e.md_str.clone(),
            Self::Other => String::new(),
        }
    }
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct TextMetadataEntry {
    label: String,
    text: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct UrlMetadataEntry {
    label: String,
    url: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct PathMetadataEntry {
    label: String,
    path: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct JsonMetadataEntry {
    label: String,
    #[cynic(rename = "jsonString")]
    json_string: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct IntMetadataEntry {
    label: String,
    #[cynic(rename = "intRepr")]
    int_repr: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct FloatMetadataEntry {
    label: String,
    #[cynic(rename = "floatRepr")]
    float_repr: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct BoolMetadataEntry {
    label: String,
    #[cynic(rename = "boolValue")]
    bool_value: Option<bool>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct MarkdownMetadataEntry {
    label: String,
    #[cynic(rename = "mdStr")]
    md_str: String,
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
                let metadata: Vec<_> = node
                    .metadata_entries
                    .iter()
                    .filter(|m| !m.label().is_empty())
                    .map(|m| (m.label().to_string(), m.value()))
                    .collect();
                output::format_asset_detail(&output::AssetDetail {
                    key: &format_asset_key(&node.asset_key.path),
                    group: &node.group_name,
                    code_location: &node.repository.location.name,
                    description: node.description.as_deref().unwrap_or(""),
                    kinds: &node.kinds,
                    partitioned: node.is_partitioned,
                    computed_by: node.op_name.as_deref().unwrap_or(""),
                    code_version: node.op_version.as_deref().unwrap_or(""),
                    dependencies: &deps,
                    dependents: &dependents,
                    jobs: &node.job_names,
                    owners: &owners,
                    automation_condition: &automation,
                    sensors: &sensors,
                    schedules: &schedules,
                    tags: &tags,
                    metadata: &metadata,
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

// --- Asset events ---

#[derive(cynic::Enum, Clone, Copy, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "AssetEventHistoryEventTypeSelector"
)]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetEventHistoryEventTypeSelector {
    #[cynic(rename = "MATERIALIZATION")]
    Materialization,
    #[cynic(rename = "FAILED_TO_MATERIALIZE")]
    FailedToMaterialize,
    #[cynic(rename = "OBSERVATION")]
    Observation,
}

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetEventsQueryVariables {
    #[cynic(rename = "assetKey")]
    asset_key: AssetKeyInput,
    limit: i32,
    #[cynic(rename = "eventTypeSelectors")]
    event_type_selectors: Vec<AssetEventHistoryEventTypeSelector>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "AssetEventsQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetEventsQuery {
    #[arguments(assetKey: $asset_key)]
    #[cynic(rename = "assetOrError")]
    asset_or_error: AssetOrError,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "AssetOrError",
    variables = "AssetEventsQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetOrError {
    Asset(AssetWithEvents),
    AssetNotFoundError(AssetNotFoundError),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "Asset",
    variables = "AssetEventsQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetWithEvents {
    #[arguments(limit: $limit, eventTypeSelectors: $event_type_selectors)]
    #[cynic(rename = "assetEventHistory")]
    asset_event_history: AssetResultEventHistoryConnection,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetResultEventHistoryConnection {
    results: Vec<AssetResultEventType>,
}

#[derive(cynic::InlineFragments, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetResultEventType")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetResultEventType {
    MaterializationEvent(AssetMaterializationEvent),
    ObservationEvent(AssetObservationEvent),
    FailedToMaterializeEvent(AssetFailedToMaterializeEvent),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "MaterializationEvent")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetMaterializationEvent {
    #[cynic(rename = "runId")]
    run_id: String,
    timestamp: String,
    message: String,
    partition: Option<String>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "ObservationEvent")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetObservationEvent {
    #[cynic(rename = "runId")]
    run_id: String,
    timestamp: String,
    message: String,
    partition: Option<String>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "FailedToMaterializeEvent")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetFailedToMaterializeEvent {
    #[cynic(rename = "runId")]
    run_id: String,
    timestamp: String,
    message: String,
    partition: Option<String>,
}

pub async fn get_asset_events(
    token: &str,
    api_url: &str,
    key: String,
    limit: Option<i32>,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let operation = AssetEventsQuery::build(AssetEventsQueryVariables {
        asset_key: AssetKeyInput {
            path: parse_asset_key(&key),
        },
        limit: limit.unwrap_or(25),
        event_type_selectors: vec![
            AssetEventHistoryEventTypeSelector::Materialization,
            AssetEventHistoryEventTypeSelector::Observation,
            AssetEventHistoryEventTypeSelector::FailedToMaterialize,
        ],
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

    match data.asset_or_error {
        AssetOrError::Asset(asset) => {
            let events = asset.asset_event_history.results;
            match fmt {
                Some(f) => output::render(&events, f),
                None => {
                    let rows: Vec<_> = events
                        .iter()
                        .map(|e| match e {
                            AssetResultEventType::MaterializationEvent(m) => (
                                m.timestamp.clone(),
                                "Materialization".into(),
                                m.run_id.clone(),
                                m.partition.clone().unwrap_or_default(),
                                m.message.clone(),
                            ),
                            AssetResultEventType::ObservationEvent(o) => (
                                o.timestamp.clone(),
                                "Observation".into(),
                                o.run_id.clone(),
                                o.partition.clone().unwrap_or_default(),
                                o.message.clone(),
                            ),
                            AssetResultEventType::FailedToMaterializeEvent(f) => (
                                f.timestamp.clone(),
                                "FailedToMaterialize".into(),
                                f.run_id.clone(),
                                f.partition.clone().unwrap_or_default(),
                                f.message.clone(),
                            ),
                            AssetResultEventType::Other => (
                                String::new(),
                                "Unknown".into(),
                                String::new(),
                                String::new(),
                                String::new(),
                            ),
                        })
                        .collect();
                    output::format_asset_events_table(&rows);
                    Ok(())
                }
            }
        }
        AssetOrError::AssetNotFoundError(err) => {
            anyhow::bail!("Asset not found: {}", err.message)
        }
        AssetOrError::Other => anyhow::bail!("Unexpected response type from API"),
    }
}

// --- Asset partitions ---

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "AssetNodeQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetPartitionsQuery {
    #[arguments(assetKey: $asset_key)]
    #[cynic(rename = "assetNodeOrError")]
    asset_node_or_error: AssetNodeOrErrorPartitions,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "AssetNodeOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetNodeOrErrorPartitions {
    AssetNode(AssetNodePartitions),
    AssetNotFoundError(AssetNotFoundError),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetNode")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetNodePartitions {
    #[cynic(rename = "isPartitioned")]
    is_partitioned: bool,
    #[cynic(rename = "partitionStats")]
    partition_stats: Option<PartitionStats>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct PartitionStats {
    #[cynic(rename = "numMaterialized")]
    num_materialized: i32,
    #[cynic(rename = "numPartitions")]
    num_partitions: i32,
    #[cynic(rename = "numFailed")]
    num_failed: i32,
    #[cynic(rename = "numMaterializing")]
    num_materializing: i32,
}

pub async fn get_asset_partitions(
    token: &str,
    api_url: &str,
    key: String,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let operation = AssetPartitionsQuery::build(AssetNodeQueryVariables {
        asset_key: AssetKeyInput {
            path: parse_asset_key(&key),
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

    match data.asset_node_or_error {
        AssetNodeOrErrorPartitions::AssetNode(node) => {
            if !node.is_partitioned {
                anyhow::bail!("Asset '{}' is not partitioned", key);
            }
            let stats = node
                .partition_stats
                .ok_or_else(|| anyhow::anyhow!("No partition stats available"))?;
            match fmt {
                Some(f) => output::render(&stats, f),
                None => {
                    output::format_asset_partitions_table(
                        stats.num_partitions,
                        stats.num_materialized,
                        stats.num_failed,
                        stats.num_materializing,
                    );
                    Ok(())
                }
            }
        }
        AssetNodeOrErrorPartitions::AssetNotFoundError(err) => {
            anyhow::bail!("Asset not found: {}", err.message)
        }
        AssetNodeOrErrorPartitions::Other => anyhow::bail!("Unexpected response type from API"),
    }
}

// --- Asset checks (list) ---

#[derive(cynic::Enum, Clone, Copy, Debug)]
#[cynic(schema = "dagster", graphql_type = "AssetCheckExecutionResolvedStatus")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetCheckExecutionResolvedStatus {
    InProgress,
    Succeeded,
    Failed,
    ExecutionFailed,
    Skipped,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "AssetNodeQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetChecksQuery {
    #[arguments(assetKey: $asset_key)]
    #[cynic(rename = "assetNodeOrError")]
    asset_node_or_error: AssetNodeOrErrorChecks,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "AssetNodeOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetNodeOrErrorChecks {
    AssetNode(AssetNodeWithChecks),
    AssetNotFoundError(AssetNotFoundError),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "AssetNode")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetNodeWithChecks {
    #[cynic(rename = "assetChecksOrError")]
    asset_checks_or_error: AssetChecksOrError,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "AssetChecksOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetChecksOrError {
    AssetChecks(AssetChecks),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetChecks {
    checks: Vec<AssetCheckSummary>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetCheck")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckSummary {
    name: String,
    description: Option<String>,
    blocking: bool,
    #[cynic(rename = "executionForLatestMaterialization")]
    execution_for_latest_materialization: Option<AssetCheckExecutionSummary>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetCheckExecution")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckExecutionSummary {
    status: AssetCheckExecutionResolvedStatus,
    #[cynic(rename = "runId")]
    run_id: String,
    timestamp: f64,
}

pub async fn get_asset_checks(
    token: &str,
    api_url: &str,
    key: String,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let operation = AssetChecksQuery::build(AssetNodeQueryVariables {
        asset_key: AssetKeyInput {
            path: parse_asset_key(&key),
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

    match data.asset_node_or_error {
        AssetNodeOrErrorChecks::AssetNode(node) => match node.asset_checks_or_error {
            AssetChecksOrError::AssetChecks(checks) => match fmt {
                Some(f) => output::render(&checks.checks, f),
                None => {
                    let rows: Vec<_> = checks
                        .checks
                        .iter()
                        .map(|c| {
                            let (status, run_id, ts) =
                                if let Some(ref exec) = c.execution_for_latest_materialization {
                                    (
                                        format!("{:?}", exec.status),
                                        exec.run_id.clone(),
                                        output::format_timestamp(Some(exec.timestamp)),
                                    )
                                } else {
                                    (String::new(), String::new(), String::new())
                                };
                            (
                                c.name.clone(),
                                if c.blocking { "Yes" } else { "No" }.into(),
                                status,
                                run_id,
                                ts,
                                c.description.clone().unwrap_or_default(),
                            )
                        })
                        .collect();
                    output::format_asset_checks_table(&rows);
                    Ok(())
                }
            },
            AssetChecksOrError::Other => {
                anyhow::bail!("Asset checks unavailable (migration or upgrade required)")
            }
        },
        AssetNodeOrErrorChecks::AssetNotFoundError(err) => {
            anyhow::bail!("Asset not found: {}", err.message)
        }
        AssetNodeOrErrorChecks::Other => anyhow::bail!("Unexpected response type from API"),
    }
}

// --- Asset check (detail) ---

#[derive(cynic::Enum, Clone, Copy, Debug)]
#[cynic(schema = "dagster", graphql_type = "AssetCheckSeverity")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetCheckSeverity {
    Warn,
    Error,
}

#[derive(cynic::Enum, Clone, Copy, Debug)]
#[cynic(schema = "dagster", graphql_type = "AssetCheckCanExecuteIndividually")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetCheckCanExecuteIndividually {
    CanExecute,
    RequiresMaterialization,
    NeedsUserCodeUpgrade,
}

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckDetailQueryVariables {
    #[cynic(rename = "assetKey")]
    asset_key: AssetKeyInput,
    #[cynic(rename = "checkName")]
    check_name: String,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "AssetCheckDetailQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckDetailQuery {
    #[arguments(assetKey: $asset_key)]
    #[cynic(rename = "assetNodeOrError")]
    asset_node_or_error: AssetNodeOrErrorCheckDetail,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "AssetNodeOrError",
    variables = "AssetCheckDetailQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetNodeOrErrorCheckDetail {
    AssetNode(AssetNodeWithCheckDetail),
    AssetNotFoundError(AssetNotFoundError),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "AssetNode",
    variables = "AssetCheckDetailQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetNodeWithCheckDetail {
    #[arguments(checkName: $check_name)]
    #[cynic(rename = "assetCheckOrError")]
    asset_check_or_error: AssetCheckOrError,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "AssetCheckOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetCheckOrError {
    AssetCheck(AssetCheckDetail),
    AssetCheckNotFoundError(AssetCheckNotFoundError),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckNotFoundError {
    message: String,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetCheck")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckDetail {
    name: String,
    description: Option<String>,
    blocking: bool,
    #[cynic(rename = "jobNames")]
    job_names: Vec<String>,
    #[cynic(rename = "canExecuteIndividually")]
    can_execute_individually: AssetCheckCanExecuteIndividually,
    #[cynic(rename = "automationCondition")]
    automation_condition: Option<AutomationCondition>,
    #[cynic(rename = "executionForLatestMaterialization")]
    execution_for_latest_materialization: Option<AssetCheckExecutionDetail>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetCheckExecution")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckExecutionDetail {
    status: AssetCheckExecutionResolvedStatus,
    #[cynic(rename = "runId")]
    run_id: String,
    timestamp: f64,
    evaluation: Option<AssetCheckEvaluation>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckEvaluation {
    severity: AssetCheckSeverity,
    success: bool,
    description: Option<String>,
}

pub async fn get_asset_check(
    token: &str,
    api_url: &str,
    key: String,
    check_name: &str,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let operation = AssetCheckDetailQuery::build(AssetCheckDetailQueryVariables {
        asset_key: AssetKeyInput {
            path: parse_asset_key(&key),
        },
        check_name: check_name.to_string(),
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
        AssetNodeOrErrorCheckDetail::AssetNode(node) => match node.asset_check_or_error {
            AssetCheckOrError::AssetCheck(check) => match fmt {
                Some(f) => output::render(&check, f),
                None => {
                    let (status, run_id, ts, severity, success) =
                        if let Some(ref exec) = check.execution_for_latest_materialization {
                            let (sev, suc) = if let Some(ref eval) = exec.evaluation {
                                (format!("{:?}", eval.severity), eval.success.to_string())
                            } else {
                                (String::new(), String::new())
                            };
                            (
                                format!("{:?}", exec.status),
                                exec.run_id.clone(),
                                output::format_timestamp(Some(exec.timestamp)),
                                sev,
                                suc,
                            )
                        } else {
                            (
                                String::new(),
                                String::new(),
                                String::new(),
                                String::new(),
                                String::new(),
                            )
                        };
                    let automation = check
                        .automation_condition
                        .as_ref()
                        .and_then(|ac| ac.label.clone())
                        .unwrap_or_default();
                    output::format_asset_check_detail(&output::AssetCheckDetail {
                        name: &check.name,
                        description: check.description.as_deref().unwrap_or(""),
                        blocking: check.blocking,
                        jobs: &check.job_names,
                        can_execute_individually: &format!("{:?}", check.can_execute_individually),
                        automation_condition: &automation,
                        latest_status: &status,
                        latest_run_id: &run_id,
                        latest_timestamp: &ts,
                        latest_severity: &severity,
                        latest_success: &success,
                    });
                    Ok(())
                }
            },
            AssetCheckOrError::AssetCheckNotFoundError(err) => {
                anyhow::bail!("Asset check not found: {}", err.message)
            }
            AssetCheckOrError::Other => {
                anyhow::bail!("Asset check unavailable (migration or upgrade required)")
            }
        },
        AssetNodeOrErrorCheckDetail::AssetNotFoundError(err) => {
            anyhow::bail!("Asset not found: {}", err.message)
        }
        AssetNodeOrErrorCheckDetail::Other => {
            anyhow::bail!("Unexpected response type from API")
        }
    }
}

// --- Asset check executions ---

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckExecutionsQueryVariables {
    #[cynic(rename = "assetKey")]
    asset_key: AssetKeyInput,
    #[cynic(rename = "checkName")]
    check_name: String,
    limit: i32,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "AssetCheckExecutionsQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckExecutionsQuery {
    #[arguments(assetKey: $asset_key, checkName: $check_name, limit: $limit)]
    #[cynic(rename = "assetCheckExecutions")]
    asset_check_executions: Vec<AssetCheckExecutionRow>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetCheckExecution")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckExecutionRow {
    status: AssetCheckExecutionResolvedStatus,
    #[cynic(rename = "runId")]
    run_id: String,
    timestamp: f64,
    partition: Option<String>,
    evaluation: Option<AssetCheckEvaluationSummary>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetCheckEvaluation")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckEvaluationSummary {
    severity: AssetCheckSeverity,
}

pub async fn get_asset_check_executions(
    token: &str,
    api_url: &str,
    key: String,
    check_name: &str,
    limit: Option<i32>,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let operation = AssetCheckExecutionsQuery::build(AssetCheckExecutionsQueryVariables {
        asset_key: AssetKeyInput {
            path: parse_asset_key(&key),
        },
        check_name: check_name.to_string(),
        limit: limit.unwrap_or(25),
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

    let executions = data.asset_check_executions;
    match fmt {
        Some(f) => output::render(&executions, f),
        None => {
            let rows: Vec<_> = executions
                .iter()
                .map(|e| {
                    let severity = e
                        .evaluation
                        .as_ref()
                        .map(|ev| format!("{:?}", ev.severity))
                        .unwrap_or_default();
                    (
                        output::format_timestamp(Some(e.timestamp)),
                        format!("{:?}", e.status),
                        e.run_id.clone(),
                        e.partition.clone().unwrap_or_default(),
                        severity,
                    )
                })
                .collect();
            output::format_asset_check_executions_table(&rows);
            Ok(())
        }
    }
}
