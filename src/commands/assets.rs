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

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum AssetHealthStatusFilter {
    Healthy,
    Warning,
    Degraded,
    Unknown,
    #[value(name = "not-applicable")]
    NotApplicable,
}

// Resolve group -> (repositoryName, codeLocationName) via searchFieldValues

#[derive(cynic::Enum, Clone, Copy, Debug)]
#[cynic(schema = "dagster", graphql_type = "SearchAssetDefFields")]
#[cynic(schema_module = "crate::schema::schema")]
enum SearchAssetDefFields {
    OrganizationId,
    DeploymentId,
    Name,
    AssetKey,
    Group,
    GroupAddress,
    Kind,
    CodeLocationVersion,
    RepositoryName,
    LocationName,
    RepoAddress,
    Owners,
    Tags,
    Description,
    Columns,
    ColumnNames,
    ColumnTags,
    TableName,
    SerializationVersion,
}

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct SearchFieldValuesVariables {
    #[cynic(rename = "fieldValueQuery")]
    field_value_query: String,
    field: Option<SearchAssetDefFields>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "SearchFieldValuesVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct SearchFieldValuesQuery {
    #[arguments(fieldValueQuery: $field_value_query, field: $field)]
    #[cynic(rename = "searchFieldValues")]
    search_field_values: SearchResultsCountsOrError,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "SearchResultsCountsOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum SearchResultsCountsOrError {
    SearchResultCountsByDimension(SearchResultCountsByDimension),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct SearchResultCountsByDimension {
    groups: Vec<AssetGroupSearchResultCount>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetGroupSearchResultCount {
    #[cynic(rename = "repositoryName")]
    repository_name: String,
    #[cynic(rename = "codeLocationName")]
    code_location_name: String,
    group: String,
}

// assetNodes query (unfiltered)
#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "CloudQuery")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetNodesQuery {
    #[cynic(rename = "assetNodes")]
    asset_nodes: Vec<AssetNodeSummary>,
}

// assetNodes query (filtered by group)
#[derive(cynic::InputObject, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetGroupSelector {
    #[cynic(rename = "groupName")]
    group_name: String,
    #[cynic(rename = "repositoryName")]
    repository_name: String,
    #[cynic(rename = "repositoryLocationName")]
    repository_location_name: String,
}

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetNodesFilteredVariables {
    group: Option<AssetGroupSelector>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "AssetNodesFilteredVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetNodesFilteredQuery {
    #[arguments(group: $group)]
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

// Health queries

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "CloudQuery")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetsHealthQuery {
    #[cynic(rename = "assetsOrError")]
    assets_or_error: AssetsOrErrorList,
}

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetsHealthFilteredVariables {
    #[cynic(rename = "assetKeys")]
    asset_keys: Option<Vec<AssetKeyInput>>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "AssetsHealthFilteredVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetsHealthFilteredQuery {
    #[arguments(assetKeys: $asset_keys)]
    #[cynic(rename = "assetsOrError")]
    assets_or_error: AssetsOrErrorList,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "AssetsOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetsOrErrorList {
    AssetConnection(AssetConnection),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetConnection {
    nodes: Vec<AssetHealthNode>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "Asset")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetHealthNode {
    key: AssetKey,
    #[cynic(rename = "assetHealth")]
    asset_health: Option<AssetHealthSummary>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetHealth")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetHealthSummary {
    #[cynic(rename = "assetHealth")]
    asset_health: AssetHealthStatus,
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
    health: String,
}

pub async fn list_assets(
    token: &str,
    api_url: &str,
    group: Option<String>,
    code_location: Option<String>,
    health_filter: Vec<AssetHealthStatusFilter>,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};
    use std::collections::HashMap;

    let client = reqwest::Client::new();
    let auth = format!("Bearer {}", token);

    // Resolve group selector if --group is provided
    let group_selectors: Vec<AssetGroupSelector> = if let Some(ref g) = group {
        let resp = client
            .post(api_url)
            .header("Authorization", &auth)
            .run_graphql(SearchFieldValuesQuery::build(SearchFieldValuesVariables {
                field_value_query: g.clone(),
                field: Some(SearchAssetDefFields::Group),
            }))
            .await?;
        let data = resp
            .data
            .ok_or_else(|| anyhow::anyhow!("No data in response"))?;
        match data.search_field_values {
            SearchResultsCountsOrError::SearchResultCountsByDimension(d) => d
                .groups
                .into_iter()
                .filter(|r| {
                    &r.group == g
                        && code_location
                            .as_ref()
                            .is_none_or(|loc| &r.code_location_name == loc)
                })
                .map(|r| AssetGroupSelector {
                    group_name: r.group,
                    repository_name: r.repository_name,
                    repository_location_name: r.code_location_name,
                })
                .collect(),
            SearchResultsCountsOrError::Other => vec![],
        }
    } else {
        vec![]
    };

    // Fetch asset nodes — use server-side filter if we resolved group selectors
    let mut all_nodes = Vec::new();
    if !group_selectors.is_empty() {
        // Fetch each group selector (usually 1-2)
        for gs in group_selectors {
            let resp = client
                .post(api_url)
                .header("Authorization", &auth)
                .run_graphql(AssetNodesFilteredQuery::build(
                    AssetNodesFilteredVariables { group: Some(gs) },
                ))
                .await?;
            if let Some(data) = resp.data {
                all_nodes.extend(data.asset_nodes);
            }
        }
    } else if group.is_some() {
        // Group was specified but not found — return empty
    } else {
        let resp = client
            .post(api_url)
            .header("Authorization", &auth)
            .run_graphql(AssetNodesQuery::build(()))
            .await?;
        if let Some(errors) = resp.errors {
            anyhow::bail!("GraphQL errors: {:?}", errors);
        }
        all_nodes = resp
            .data
            .ok_or_else(|| anyhow::anyhow!("No data in response"))?
            .asset_nodes;
    }

    // Client-side filter for code_location when no group filter was used
    if group.is_none()
        && let Some(ref loc) = code_location
    {
        all_nodes.retain(|n| &n.repository.location.name == loc);
    }

    // Fetch health — use filtered keys if we have a subset
    let asset_keys: Option<Vec<AssetKeyInput>> = if group.is_some() || code_location.is_some() {
        Some(
            all_nodes
                .iter()
                .map(|n| AssetKeyInput {
                    path: n.asset_key.path.clone(),
                })
                .collect(),
        )
    } else {
        None
    };

    let health_map: HashMap<String, String> = if let Some(keys) = asset_keys {
        let resp = client
            .post(api_url)
            .header("Authorization", &auth)
            .run_graphql(AssetsHealthFilteredQuery::build(
                AssetsHealthFilteredVariables {
                    asset_keys: Some(keys),
                },
            ))
            .await?;
        extract_health_map(resp.data)
    } else {
        let resp = client
            .post(api_url)
            .header("Authorization", &auth)
            .run_graphql(AssetsHealthQuery::build(()))
            .await?;
        extract_health_map(resp.data)
    };

    let entries: Vec<AssetListEntry> = all_nodes
        .into_iter()
        .filter_map(|n| {
            let key = format_asset_key(&n.asset_key.path);
            let health_str = health_map.get(&key).cloned().unwrap_or_default();
            if !health_filter.is_empty() {
                let matches = health_filter.iter().any(|f| match f {
                    AssetHealthStatusFilter::Healthy => health_str == "Healthy",
                    AssetHealthStatusFilter::Warning => health_str == "Warning",
                    AssetHealthStatusFilter::Degraded => health_str == "Degraded",
                    AssetHealthStatusFilter::Unknown => health_str == "Unknown",
                    AssetHealthStatusFilter::NotApplicable => health_str == "NotApplicable",
                });
                if !matches {
                    return None;
                }
            }
            Some(AssetListEntry {
                key,
                group: n.group_name,
                code_location: n.repository.location.name,
                kinds: n.kinds,
                partitioned: n.is_partitioned,
                health: health_str,
            })
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
                        e.health.clone(),
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

fn extract_health_map<T: HasAssetsOrError>(
    data: Option<T>,
) -> std::collections::HashMap<String, String> {
    data.and_then(|d| d.into_nodes())
        .unwrap_or_default()
        .into_iter()
        .map(|n| {
            let key = format_asset_key(&n.key.path);
            let status = n
                .asset_health
                .map(|h| format!("{:?}", h.asset_health))
                .unwrap_or_default();
            (key, status)
        })
        .collect()
}

trait HasAssetsOrError {
    fn into_nodes(self) -> Option<Vec<AssetHealthNode>>;
}

impl HasAssetsOrError for AssetsHealthQuery {
    fn into_nodes(self) -> Option<Vec<AssetHealthNode>> {
        match self.assets_or_error {
            AssetsOrErrorList::AssetConnection(c) => Some(c.nodes),
            AssetsOrErrorList::Other => None,
        }
    }
}

impl HasAssetsOrError for AssetsHealthFilteredQuery {
    fn into_nodes(self) -> Option<Vec<AssetHealthNode>> {
        match self.assets_or_error {
            AssetsOrErrorList::AssetConnection(c) => Some(c.nodes),
            AssetsOrErrorList::Other => None,
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
    #[arguments(assetKey: $asset_key)]
    #[cynic(rename = "assetOrError")]
    asset_or_error_health: AssetOrErrorHealth,
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

// --- Asset health (fetched alongside detail) ---

#[derive(cynic::Enum, Clone, Copy, Debug)]
#[cynic(schema = "dagster", graphql_type = "AssetHealthStatus")]
#[cynic(schema_module = "crate::schema::schema")]
pub enum AssetHealthStatus {
    Healthy,
    Warning,
    Degraded,
    Unknown,
    NotApplicable,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(schema = "dagster", graphql_type = "AssetOrError")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetOrErrorHealth {
    Asset(AssetWithHealth),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(schema = "dagster", graphql_type = "Asset")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetWithHealth {
    #[cynic(rename = "assetHealth")]
    asset_health: Option<AssetHealth>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetHealth {
    #[cynic(rename = "assetHealth")]
    asset_health: AssetHealthStatus,
    #[cynic(rename = "materializationStatus")]
    materialization_status: AssetHealthStatus,
    #[cynic(rename = "materializationStatusMetadata")]
    materialization_status_metadata: Option<AssetHealthMaterializationMeta>,
    #[cynic(rename = "assetChecksStatus")]
    asset_checks_status: AssetHealthStatus,
    #[cynic(rename = "assetChecksStatusMetadata")]
    asset_checks_status_metadata: Option<AssetHealthCheckMeta>,
    #[cynic(rename = "freshnessStatus")]
    freshness_status: AssetHealthStatus,
    #[cynic(rename = "freshnessStatusMetadata")]
    freshness_status_metadata: Option<AssetHealthFreshnessMeta>,
}

#[derive(cynic::InlineFragments, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetHealthMaterializationMeta")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetHealthMaterializationMeta {
    AssetHealthMaterializationDegradedPartitionedMeta(
        AssetHealthMaterializationDegradedPartitionedMeta,
    ),
    AssetHealthMaterializationHealthyPartitionedMeta(
        AssetHealthMaterializationHealthyPartitionedMeta,
    ),
    AssetHealthMaterializationDegradedNotPartitionedMeta(
        AssetHealthMaterializationDegradedNotPartitionedMeta,
    ),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetHealthMaterializationDegradedPartitionedMeta {
    #[cynic(rename = "numFailedPartitions")]
    num_failed_partitions: i32,
    #[cynic(rename = "numMissingPartitions")]
    num_missing_partitions: i32,
    #[cynic(rename = "totalNumPartitions")]
    total_num_partitions: i32,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetHealthMaterializationHealthyPartitionedMeta {
    #[cynic(rename = "numMissingPartitions")]
    num_missing_partitions: i32,
    #[cynic(rename = "totalNumPartitions")]
    total_num_partitions: i32,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetHealthMaterializationDegradedNotPartitionedMeta {
    #[cynic(rename = "failedRunId")]
    failed_run_id: Option<String>,
}

#[derive(cynic::InlineFragments, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetHealthCheckMeta")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetHealthCheckMeta {
    AssetHealthCheckDegradedMeta(AssetHealthCheckDegradedMeta),
    AssetHealthCheckWarningMeta(AssetHealthCheckWarningMeta),
    AssetHealthCheckUnknownMeta(AssetHealthCheckUnknownMeta),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetHealthCheckDegradedMeta {
    #[cynic(rename = "numFailedChecks")]
    num_failed_checks: i32,
    #[cynic(rename = "numWarningChecks")]
    num_warning_checks: i32,
    #[cynic(rename = "totalNumChecks")]
    total_num_checks: i32,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetHealthCheckWarningMeta {
    #[cynic(rename = "numWarningChecks")]
    num_warning_checks: i32,
    #[cynic(rename = "totalNumChecks")]
    total_num_checks: i32,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetHealthCheckUnknownMeta {
    #[cynic(rename = "numNotExecutedChecks")]
    num_not_executed_checks: i32,
    #[cynic(rename = "totalNumChecks")]
    total_num_checks: i32,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetHealthFreshnessMeta {
    #[cynic(rename = "lastMaterializedTimestamp")]
    last_materialized_timestamp: Option<f64>,
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
        AssetNodeOrError::AssetNode(node) => {
            let health = match data.asset_or_error_health {
                AssetOrErrorHealth::Asset(a) => a.asset_health,
                AssetOrErrorHealth::Other => None,
            };
            match fmt {
                Some(f) => {
                    #[derive(Serialize)]
                    struct AssetWithHealth<'a> {
                        #[serde(flatten)]
                        node: &'a AssetNodeDetail,
                        health: &'a Option<AssetHealth>,
                    }
                    output::render(
                        &AssetWithHealth {
                            node: &node,
                            health: &health,
                        },
                        f,
                    )
                }
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
                    let health_overall = health
                        .as_ref()
                        .map(|h| format!("{:?}", h.asset_health))
                        .unwrap_or_default();
                    let health_mat = health
                    .as_ref()
                    .map(|h| {
                        let status = format!("{:?}", h.materialization_status);
                        match &h.materialization_status_metadata {
                            Some(AssetHealthMaterializationMeta::AssetHealthMaterializationDegradedPartitionedMeta(m)) => {
                                format!("{status} (failed in {}/{} partitions, {} missing)",
                                    m.num_failed_partitions, m.total_num_partitions, m.num_missing_partitions)
                            }
                            Some(AssetHealthMaterializationMeta::AssetHealthMaterializationHealthyPartitionedMeta(m)) => {
                                if m.num_missing_partitions > 0 {
                                    format!("{status} ({} missing of {} partitions)",
                                        m.num_missing_partitions, m.total_num_partitions)
                                } else {
                                    format!("{status} ({} partitions)", m.total_num_partitions)
                                }
                            }
                            Some(AssetHealthMaterializationMeta::AssetHealthMaterializationDegradedNotPartitionedMeta(m)) => {
                                match &m.failed_run_id {
                                    Some(id) => format!("{status} (failed run: {id})"),
                                    None => status,
                                }
                            }
                            _ => status,
                        }
                    })
                    .unwrap_or_default();
                    let health_checks = health
                        .as_ref()
                        .map(|h| {
                            let status = format!("{:?}", h.asset_checks_status);
                            match &h.asset_checks_status_metadata {
                                Some(AssetHealthCheckMeta::AssetHealthCheckDegradedMeta(m)) => {
                                    format!(
                                        "{status} ({} failed, {} warning of {} checks)",
                                        m.num_failed_checks,
                                        m.num_warning_checks,
                                        m.total_num_checks
                                    )
                                }
                                Some(AssetHealthCheckMeta::AssetHealthCheckWarningMeta(m)) => {
                                    format!(
                                        "{status} ({} warning of {} checks)",
                                        m.num_warning_checks, m.total_num_checks
                                    )
                                }
                                Some(AssetHealthCheckMeta::AssetHealthCheckUnknownMeta(m)) => {
                                    format!(
                                        "{status} ({} not executed of {} checks)",
                                        m.num_not_executed_checks, m.total_num_checks
                                    )
                                }
                                _ => status,
                            }
                        })
                        .unwrap_or_default();
                    let health_freshness = health
                        .as_ref()
                        .map(|h| {
                            let status = format!("{:?}", h.freshness_status);
                            match &h.freshness_status_metadata {
                                Some(m) => match m.last_materialized_timestamp {
                                    Some(ts) => format!(
                                        "{status} (last materialized: {})",
                                        output::format_timestamp(Some(ts))
                                    ),
                                    None => status,
                                },
                                None => status,
                            }
                        })
                        .unwrap_or_default();
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
                        health: &health_overall,
                        health_materialization: &health_mat,
                        health_checks: &health_checks,
                        health_freshness: &health_freshness,
                    });
                    Ok(())
                }
            }
        }
        AssetNodeOrError::AssetNotFoundError(err) => {
            anyhow::bail!("Asset not found: {}", err.message)
        }
        AssetNodeOrError::Other => anyhow::bail!("Unexpected response type from API"),
    }
}

// --- Asset events ---

#[derive(cynic::Enum, Clone, Copy, Debug, PartialEq)]
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
    partitions: Option<Vec<String>>,
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
    #[arguments(limit: $limit, eventTypeSelectors: $event_type_selectors, partitions: $partitions)]
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

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum AssetEventTypeFilter {
    Materialization,
    Observation,
    #[value(name = "failed-to-materialize")]
    FailedToMaterialize,
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum AssetEventStatusFilter {
    Success,
    Failure,
}

fn resolve_event_selectors(
    event_type: &[AssetEventTypeFilter],
    status: &[AssetEventStatusFilter],
) -> Vec<AssetEventHistoryEventTypeSelector> {
    // Each user-facing type maps to GraphQL selectors:
    //   materialization -> [MATERIALIZATION, FAILED_TO_MATERIALIZE]
    //   observation -> [OBSERVATION]
    //   failed-to-materialize -> [FAILED_TO_MATERIALIZE]
    // Status then filters: success keeps non-failure selectors, failure keeps FAILED_TO_MATERIALIZE
    let types: Vec<AssetEventTypeFilter> = if event_type.is_empty() {
        vec![
            AssetEventTypeFilter::Materialization,
            AssetEventTypeFilter::Observation,
        ]
    } else {
        event_type.to_vec()
    };

    let mut selectors = Vec::new();
    let want_success = status.is_empty()
        || status
            .iter()
            .any(|s| matches!(s, AssetEventStatusFilter::Success));
    let want_failure = status.is_empty()
        || status
            .iter()
            .any(|s| matches!(s, AssetEventStatusFilter::Failure));

    for t in &types {
        match t {
            AssetEventTypeFilter::Materialization => {
                if want_success {
                    selectors.push(AssetEventHistoryEventTypeSelector::Materialization);
                }
                if want_failure {
                    selectors.push(AssetEventHistoryEventTypeSelector::FailedToMaterialize);
                }
            }
            AssetEventTypeFilter::Observation => {
                if want_success {
                    selectors.push(AssetEventHistoryEventTypeSelector::Observation);
                }
            }
            AssetEventTypeFilter::FailedToMaterialize => {
                if want_failure {
                    selectors.push(AssetEventHistoryEventTypeSelector::FailedToMaterialize);
                }
            }
        }
    }
    selectors.dedup();
    selectors
}

#[allow(clippy::too_many_arguments)]
pub async fn get_asset_events(
    token: &str,
    api_url: &str,
    key: String,
    limit: Option<i32>,
    event_type: Vec<AssetEventTypeFilter>,
    status: Vec<AssetEventStatusFilter>,
    partition: Option<String>,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let selectors = resolve_event_selectors(&event_type, &status);

    if selectors.is_empty() {
        // Filters exclude all event types (e.g. --type observation --status failure)
        return match fmt {
            Some(f) => output::render(&Vec::<AssetResultEventType>::new(), f),
            None => {
                output::format_asset_events_table(&[]);
                Ok(())
            }
        };
    }

    let operation = AssetEventsQuery::build(AssetEventsQueryVariables {
        asset_key: AssetKeyInput {
            path: parse_asset_key(&key),
        },
        limit: limit.unwrap_or(25),
        event_type_selectors: selectors,
        partitions: partition.map(|p| vec![p]),
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
                                "Success".into(),
                                m.run_id.clone(),
                                m.partition.clone().unwrap_or_default(),
                                m.message.clone(),
                            ),
                            AssetResultEventType::ObservationEvent(o) => (
                                o.timestamp.clone(),
                                "Observation".into(),
                                "Success".into(),
                                o.run_id.clone(),
                                o.partition.clone().unwrap_or_default(),
                                o.message.clone(),
                            ),
                            AssetResultEventType::FailedToMaterializeEvent(f) => (
                                f.timestamp.clone(),
                                "FailedToMaterialize".into(),
                                "Failure".into(),
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

// --- Asset event detail ---

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetEventDetailQueryVariables {
    #[cynic(rename = "assetKey")]
    asset_key: AssetKeyInput,
    #[cynic(rename = "afterTimestampMillis")]
    after_timestamp_millis: Option<String>,
    #[cynic(rename = "beforeTimestampMillis")]
    before_timestamp_millis: Option<String>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "AssetEventDetailQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetEventDetailQuery {
    #[arguments(assetKey: $asset_key)]
    #[cynic(rename = "assetOrError")]
    asset_or_error: AssetOrErrorDetail,
}

#[derive(cynic::InlineFragments, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "AssetOrError",
    variables = "AssetEventDetailQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetOrErrorDetail {
    Asset(AssetWithEventDetail),
    AssetNotFoundError(AssetNotFoundError),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "Asset",
    variables = "AssetEventDetailQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetWithEventDetail {
    #[arguments(limit: 1, eventTypeSelectors: ["MATERIALIZATION", "OBSERVATION", "FAILED_TO_MATERIALIZE"], afterTimestampMillis: $after_timestamp_millis, beforeTimestampMillis: $before_timestamp_millis)]
    #[cynic(rename = "assetEventHistory")]
    asset_event_history: AssetEventDetailConnection,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetResultEventHistoryConnection")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetEventDetailConnection {
    results: Vec<AssetEventDetailType>,
}

#[derive(cynic::InlineFragments, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetResultEventType")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetEventDetailType {
    MaterializationEvent(MaterializationEventDetail),
    ObservationEvent(ObservationEventDetail),
    FailedToMaterializeEvent(FailedToMaterializeEventDetail),
    #[cynic(fallback)]
    Other,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "MaterializationEvent")]
#[cynic(schema_module = "crate::schema::schema")]
struct MaterializationEventDetail {
    #[cynic(rename = "runId")]
    run_id: String,
    timestamp: String,
    message: String,
    partition: Option<String>,
    #[cynic(rename = "stepKey")]
    step_key: Option<String>,
    description: Option<String>,
    label: Option<String>,
    #[cynic(rename = "metadataEntries")]
    metadata_entries: Vec<MetadataEntryFragment>,
    tags: Vec<EventTag>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "ObservationEvent")]
#[cynic(schema_module = "crate::schema::schema")]
struct ObservationEventDetail {
    #[cynic(rename = "runId")]
    run_id: String,
    timestamp: String,
    message: String,
    partition: Option<String>,
    #[cynic(rename = "stepKey")]
    step_key: Option<String>,
    description: Option<String>,
    label: Option<String>,
    #[cynic(rename = "metadataEntries")]
    metadata_entries: Vec<MetadataEntryFragment>,
    tags: Vec<EventTag>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "FailedToMaterializeEvent")]
#[cynic(schema_module = "crate::schema::schema")]
struct FailedToMaterializeEventDetail {
    #[cynic(rename = "runId")]
    run_id: String,
    timestamp: String,
    message: String,
    partition: Option<String>,
    #[cynic(rename = "stepKey")]
    step_key: Option<String>,
    description: Option<String>,
    label: Option<String>,
    #[cynic(rename = "metadataEntries")]
    metadata_entries: Vec<MetadataEntryFragment>,
    tags: Vec<EventTag>,
    #[cynic(rename = "materializationFailureReason")]
    materialization_failure_reason: AssetMaterializationFailureReason,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster")]
#[cynic(schema_module = "crate::schema::schema")]
struct EventTag {
    key: String,
    value: String,
}

#[derive(cynic::Enum, Clone, Copy, Debug)]
#[cynic(schema = "dagster", graphql_type = "AssetMaterializationFailureReason")]
#[cynic(schema_module = "crate::schema::schema")]
enum AssetMaterializationFailureReason {
    FailedToMaterialize,
    UpstreamFailedToMaterialize,
    RunTerminated,
    Unknown,
}

pub async fn get_asset_event(
    token: &str,
    api_url: &str,
    key: String,
    timestamp: &str,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    // Timestamp is already in milliseconds, matching get asset-events output
    let ts_millis: i64 = timestamp
        .parse::<i64>()
        .map_err(|_| anyhow::anyhow!("Invalid timestamp: {}", timestamp))?;

    let operation = AssetEventDetailQuery::build(AssetEventDetailQueryVariables {
        asset_key: AssetKeyInput {
            path: parse_asset_key(&key),
        },
        after_timestamp_millis: Some((ts_millis - 1).to_string()),
        before_timestamp_millis: Some((ts_millis + 1).to_string()),
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
        AssetOrErrorDetail::Asset(asset) => {
            let event = asset
                .asset_event_history
                .results
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("No event found at timestamp {}", timestamp))?;
            match fmt {
                Some(f) => output::render(&event, f),
                None => {
                    match &event {
                        AssetEventDetailType::MaterializationEvent(e) => {
                            format_event_detail(
                                "Materialization",
                                "Success",
                                &e.run_id,
                                &e.timestamp,
                                e.partition.as_deref(),
                                e.step_key.as_deref(),
                                e.description.as_deref(),
                                e.label.as_deref(),
                                &e.metadata_entries,
                                &e.tags,
                                None,
                            );
                        }
                        AssetEventDetailType::ObservationEvent(e) => {
                            format_event_detail(
                                "Observation",
                                "Success",
                                &e.run_id,
                                &e.timestamp,
                                e.partition.as_deref(),
                                e.step_key.as_deref(),
                                e.description.as_deref(),
                                e.label.as_deref(),
                                &e.metadata_entries,
                                &e.tags,
                                None,
                            );
                        }
                        AssetEventDetailType::FailedToMaterializeEvent(e) => {
                            format_event_detail(
                                "FailedToMaterialize",
                                "Failure",
                                &e.run_id,
                                &e.timestamp,
                                e.partition.as_deref(),
                                e.step_key.as_deref(),
                                e.description.as_deref(),
                                e.label.as_deref(),
                                &e.metadata_entries,
                                &e.tags,
                                Some(&format!("{:?}", e.materialization_failure_reason)),
                            );
                        }
                        AssetEventDetailType::Other => {}
                    }
                    Ok(())
                }
            }
        }
        AssetOrErrorDetail::AssetNotFoundError(err) => {
            anyhow::bail!("Asset not found: {}", err.message)
        }
        AssetOrErrorDetail::Other => anyhow::bail!("Unexpected response type from API"),
    }
}

#[allow(clippy::too_many_arguments)]
fn format_event_detail(
    event_type: &str,
    status: &str,
    run_id: &str,
    timestamp: &str,
    partition: Option<&str>,
    step_key: Option<&str>,
    description: Option<&str>,
    label: Option<&str>,
    metadata: &[MetadataEntryFragment],
    tags: &[EventTag],
    failure_reason: Option<&str>,
) {
    output::format_asset_event_detail(&output::AssetEventDetail {
        event_type,
        status,
        run_id,
        timestamp,
        partition: partition.unwrap_or(""),
        step_key: step_key.unwrap_or(""),
        description: description.unwrap_or(""),
        label: label.unwrap_or(""),
        metadata: &metadata
            .iter()
            .filter(|m| !m.label().is_empty())
            .map(|m| (m.label().to_string(), m.value()))
            .collect::<Vec<_>>(),
        tags: &tags
            .iter()
            .map(|t| {
                if t.value.is_empty() {
                    t.key.clone()
                } else {
                    format!("{}={}", t.key, t.value)
                }
            })
            .collect::<Vec<_>>(),
        failure_reason: failure_reason.unwrap_or(""),
    });
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
                    let (status, run_id, ts, severity, result) =
                        if let Some(ref exec) = check.execution_for_latest_materialization {
                            let (sev, res) = if let Some(ref eval) = exec.evaluation {
                                (
                                    format!("{:?}", eval.severity),
                                    if eval.success { "Pass" } else { "Fail" }.into(),
                                )
                            } else {
                                (String::new(), String::new())
                            };
                            (
                                format!("{:?}", exec.status),
                                exec.run_id.clone(),
                                output::format_timestamp(Some(exec.timestamp)),
                                sev,
                                res,
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
                        severity: &severity,
                        latest_status: &status,
                        latest_run_id: &run_id,
                        latest_timestamp: &ts,
                        latest_result: &result,
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

#[derive(clap::ValueEnum, Clone, Debug, PartialEq)]
pub enum CheckExecutionStatusFilter {
    #[value(name = "in-progress")]
    InProgress,
    Succeeded,
    Failed,
    #[value(name = "execution-failed")]
    ExecutionFailed,
    Skipped,
}

#[derive(cynic::QueryVariables, Debug)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckExecutionsQueryVariables {
    #[cynic(rename = "assetKey")]
    asset_key: AssetKeyInput,
    #[cynic(rename = "checkName")]
    check_name: String,
    limit: i32,
    cursor: Option<String>,
}

#[derive(cynic::QueryFragment, Debug)]
#[cynic(
    schema = "dagster",
    graphql_type = "CloudQuery",
    variables = "AssetCheckExecutionsQueryVariables"
)]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckExecutionsQuery {
    #[arguments(assetKey: $asset_key, checkName: $check_name, limit: $limit, cursor: $cursor)]
    #[cynic(rename = "assetCheckExecutions")]
    asset_check_executions: Vec<AssetCheckExecutionRow>,
}

#[derive(cynic::QueryFragment, Debug, Serialize)]
#[cynic(schema = "dagster", graphql_type = "AssetCheckExecution")]
#[cynic(schema_module = "crate::schema::schema")]
struct AssetCheckExecutionRow {
    id: String,
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
    status: Vec<CheckExecutionStatusFilter>,
    fmt: &Option<OutputFormat>,
) -> Result<()> {
    use cynic::{QueryBuilder, http::ReqwestExt};

    let desired = limit.unwrap_or(25) as usize;
    let page_size = desired as i32;
    let asset_path = parse_asset_key(&key);
    let mut collected: Vec<AssetCheckExecutionRow> = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let operation = AssetCheckExecutionsQuery::build(AssetCheckExecutionsQueryVariables {
            asset_key: AssetKeyInput {
                path: asset_path.clone(),
            },
            check_name: check_name.to_string(),
            limit: page_size,
            cursor: cursor.clone(),
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

        let page = data.asset_check_executions;
        let page_len = page.len();

        if page.is_empty() {
            break;
        }

        cursor = page.last().map(|e| e.id.clone());

        if status.is_empty() {
            collected.extend(page);
        } else {
            collected.extend(
                page.into_iter()
                    .filter(|e| matches_status(&status, &e.status)),
            );
        }

        if collected.len() >= desired || page_len < page_size as usize {
            break;
        }
    }

    collected.truncate(desired);

    match fmt {
        Some(f) => output::render(&collected, f),
        None => {
            let rows: Vec<_> = collected
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

fn matches_status(
    filters: &[CheckExecutionStatusFilter],
    status: &AssetCheckExecutionResolvedStatus,
) -> bool {
    filters.iter().any(|f| match f {
        CheckExecutionStatusFilter::InProgress => {
            matches!(status, AssetCheckExecutionResolvedStatus::InProgress)
        }
        CheckExecutionStatusFilter::Succeeded => {
            matches!(status, AssetCheckExecutionResolvedStatus::Succeeded)
        }
        CheckExecutionStatusFilter::Failed => {
            matches!(status, AssetCheckExecutionResolvedStatus::Failed)
        }
        CheckExecutionStatusFilter::ExecutionFailed => {
            matches!(status, AssetCheckExecutionResolvedStatus::ExecutionFailed)
        }
        CheckExecutionStatusFilter::Skipped => {
            matches!(status, AssetCheckExecutionResolvedStatus::Skipped)
        }
    })
}
