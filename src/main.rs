mod auth;
mod commands;
mod config;
mod output;
mod schema;

use clap::{CommandFactory, Parser, Subcommand};
use commands::runs::RunStatusFilter;
use output::OutputFormat;

#[derive(Parser)]
#[command(name = "dagctl")]
#[command(version)]
#[command(about = "CLI for Dagster GraphQL API")]
struct Cli {
    #[arg(long, global = true)]
    token: Option<String>,

    #[arg(long, global = true)]
    organization: Option<String>,

    #[arg(long, global = true)]
    deployment: Option<String>,

    /// GitHub API base URL (for proxies/mirrors)
    #[arg(long, global = true)]
    github_url: Option<String>,

    /// Output format (default: table)
    #[arg(short = 'o', long = "output", global = true)]
    output: Option<OutputFormat>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Display one or many resources
    Get {
        #[command(subcommand)]
        resource: GetResource,
    },
    /// Manage the GraphQL schema
    Schema {
        #[command(subcommand)]
        action: SchemaCommands,
    },
    /// Print diagnostic info (version, config, API connectivity)
    Debug,
    /// Generate shell completion
    Completion { shell: clap_complete::Shell },
    /// Manage dagctl itself
    #[command(name = "self")]
    SelfCmd {
        #[command(subcommand)]
        action: SelfCommands,
    },
}

#[derive(Subcommand)]
enum GetResource {
    /// List runs
    Runs {
        #[arg(long)]
        limit: Option<i32>,
        /// Filter by status (e.g. success, failure, started, queued, canceled)
        #[arg(long, value_delimiter = ',')]
        status: Option<Vec<RunStatusFilter>>,
        /// Filter by job name
        #[arg(long)]
        job: Option<String>,
        /// Filter by user who launched the run
        #[arg(long)]
        launched_by: Option<String>,
        /// Filter by partition
        #[arg(long)]
        partition: Option<String>,
        /// Filter by tag (key=value)
        #[arg(long, value_delimiter = ',')]
        tags: Option<Vec<String>>,
    },
    /// Show details of a specific run
    Run { run_id: String },
    /// Get events for a run
    #[command(name = "run-events")]
    RunEvents { run_id: String },
    /// Get captured logs for a run
    #[command(name = "run-logs")]
    RunLogs { run_id: String },
    /// List code locations
    #[command(name = "code-locations")]
    CodeLocations,
    /// Show details of a specific code location
    #[command(name = "code-location")]
    CodeLocation { name: String },
    /// List jobs
    Jobs {
        #[arg(long)]
        code_location: Option<String>,
    },
    /// Show details of a specific job
    Job {
        name: String,
        #[arg(long, required = true)]
        code_location: String,
    },
    /// List assets
    Assets {
        #[arg(long)]
        group: Option<String>,
        #[arg(long)]
        code_location: Option<String>,
        /// Filter by health status (comma-separated: healthy,warning,degraded,unknown,not-applicable)
        #[arg(long, value_delimiter = ',')]
        health: Vec<commands::assets::AssetHealthStatusFilter>,
    },
    /// Show details of a specific asset (use slash-separated key, e.g. my_prefix/my_asset)
    Asset { key: String },
    /// Show details of a specific asset event by timestamp
    #[command(name = "asset-event")]
    AssetEvent { key: String, timestamp: String },
    /// Get event history for an asset (materializations, observations, failures)
    #[command(name = "asset-events")]
    AssetEvents {
        key: String,
        #[arg(long)]
        limit: Option<i32>,
        /// Filter by event type (comma-separated: materialization,observation,failed-to-materialize)
        #[arg(long = "type", value_delimiter = ',')]
        event_type: Vec<commands::assets::AssetEventTypeFilter>,
        /// Filter by status (comma-separated: success,failure)
        #[arg(long, value_delimiter = ',')]
        status: Vec<commands::assets::AssetEventStatusFilter>,
        /// Filter by partition
        #[arg(long)]
        partition: Option<String>,
        /// Only events after this date (ISO 8601 or YYYY-MM-DD)
        #[arg(long)]
        since: Option<String>,
        /// Only events before this date (ISO 8601 or YYYY-MM-DD)
        #[arg(long)]
        until: Option<String>,
    },
    /// Get partition status summary for an asset
    #[command(name = "asset-partitions")]
    AssetPartitions { key: String },
    /// Show upstream/downstream dependency graph for an asset
    #[command(name = "asset-lineage")]
    AssetLineage {
        key: String,
        /// Number of hops to traverse (default: 1)
        #[arg(long, default_value_t = 1)]
        depth: i32,
    },
    /// List asset checks with latest execution status
    #[command(name = "asset-checks")]
    AssetChecks { key: String },
    /// Show details of a specific asset check
    #[command(name = "asset-check")]
    AssetCheck { key: String, check: String },
    /// List historic executions for an asset check
    #[command(name = "asset-check-executions")]
    AssetCheckExecutions {
        key: String,
        check: String,
        #[arg(long)]
        limit: Option<i32>,
        /// Filter by status (comma-separated: in-progress,succeeded,failed,execution-failed,skipped)
        #[arg(long, value_delimiter = ',')]
        status: Vec<commands::assets::CheckExecutionStatusFilter>,
    },
    /// List available insight metrics
    #[command(name = "insights-metrics")]
    InsightsMetrics {
        /// Entity type to list metrics for (jobs, assets, asset-groups, deployments)
        #[arg(long, name = "for")]
        entity_type: Option<commands::insights::MetricsEntityType>,
    },
    /// Show deployment insights metadata (latest data timestamp, available ranges)
    #[command(name = "insights-info")]
    InsightsInfo,
    /// Query metrics by job
    #[command(name = "insights-by-job")]
    InsightsByJob {
        /// Metric to query (e.g. dagster-credits, compute-duration, snowflake-credits)
        #[arg(long)]
        metric: String,
        /// Time range duration (e.g. 24h, 7d, 30d, 120d)
        #[arg(long)]
        last: Option<String>,
        /// Start date (ISO 8601 or YYYY-MM-DD)
        #[arg(long)]
        since: Option<String>,
        /// End date (ISO 8601 or YYYY-MM-DD)
        #[arg(long)]
        until: Option<String>,
        /// Time granularity (hourly, daily, weekly, monthly)
        #[arg(long, default_value = "daily")]
        granularity: commands::insights::Granularity,
        /// Aggregation function (sum, average, p75, p90, p95, p99, max, min)
        #[arg(long)]
        aggregation: Option<commands::insights::AggregationFunction>,
        /// Max entities to return
        #[arg(long)]
        limit: Option<i32>,
    },
    /// Query metrics by asset
    #[command(name = "insights-by-asset")]
    InsightsByAsset {
        /// Metric to query (e.g. dagster-credits, compute-duration, snowflake-credits)
        #[arg(long)]
        metric: String,
        /// Time range duration (e.g. 24h, 7d, 30d, 120d)
        #[arg(long)]
        last: Option<String>,
        /// Start date (ISO 8601 or YYYY-MM-DD)
        #[arg(long)]
        since: Option<String>,
        /// End date (ISO 8601 or YYYY-MM-DD)
        #[arg(long)]
        until: Option<String>,
        /// Time granularity (hourly, daily, weekly, monthly)
        #[arg(long, default_value = "daily")]
        granularity: commands::insights::Granularity,
        /// Aggregation function (sum, average, p75, p90, p95, p99, max, min)
        #[arg(long)]
        aggregation: Option<commands::insights::AggregationFunction>,
        /// Max entities to return
        #[arg(long)]
        limit: Option<i32>,
        /// Filter to a specific code location
        #[arg(long)]
        code_location: Option<String>,
        /// Filter to a specific asset group
        #[arg(long)]
        group: Option<String>,
        /// Raw asset selection query (Dagster selection DSL). Conflicts with --code-location/--group.
        #[arg(long)]
        selection: Option<String>,
    },
    /// Query metrics by asset group
    #[command(name = "insights-by-asset-group")]
    InsightsByAssetGroup {
        /// Metric to query (e.g. dagster-credits, compute-duration, snowflake-credits)
        #[arg(long)]
        metric: String,
        /// Time range duration (e.g. 24h, 7d, 30d, 120d)
        #[arg(long)]
        last: Option<String>,
        /// Start date (ISO 8601 or YYYY-MM-DD)
        #[arg(long)]
        since: Option<String>,
        /// End date (ISO 8601 or YYYY-MM-DD)
        #[arg(long)]
        until: Option<String>,
        /// Time granularity (hourly, daily, weekly, monthly)
        #[arg(long, default_value = "daily")]
        granularity: commands::insights::Granularity,
        /// Aggregation function (sum, average, p75, p90, p95, p99, max, min)
        #[arg(long)]
        aggregation: Option<commands::insights::AggregationFunction>,
        /// Max entities to return
        #[arg(long)]
        limit: Option<i32>,
        /// Filter to a specific code location
        #[arg(long)]
        code_location: Option<String>,
        /// Raw asset selection query (Dagster selection DSL). Conflicts with --code-location.
        #[arg(long)]
        selection: Option<String>,
    },
    /// Query metrics by deployment
    #[command(name = "insights-by-deployment")]
    InsightsByDeployment {
        /// Metric to query (e.g. dagster-credits, compute-duration, snowflake-credits)
        #[arg(long)]
        metric: String,
        /// Time range duration (e.g. 24h, 7d, 30d, 120d)
        #[arg(long)]
        last: Option<String>,
        /// Start date (ISO 8601 or YYYY-MM-DD)
        #[arg(long)]
        since: Option<String>,
        /// End date (ISO 8601 or YYYY-MM-DD)
        #[arg(long)]
        until: Option<String>,
        /// Time granularity (hourly, daily, weekly, monthly)
        #[arg(long, default_value = "daily")]
        granularity: commands::insights::Granularity,
        /// Aggregation function (sum, average, p75, p90, p95, p99, max, min)
        #[arg(long)]
        aggregation: Option<commands::insights::AggregationFunction>,
    },
    /// Query per-run metrics for a specific job
    #[command(name = "insights-job-runs")]
    InsightsJobRuns {
        /// Metric to query
        #[arg(long)]
        metric: String,
        /// Job name
        #[arg(long)]
        job: String,
        /// Code location name
        #[arg(long)]
        code_location: String,
    },
    /// Query per-materialization metrics for a specific asset
    #[command(name = "insights-asset-materializations")]
    InsightsAssetMaterializations {
        /// Metric to query
        #[arg(long)]
        metric: String,
        /// Asset key (slash-separated, e.g. prefix/my_asset)
        #[arg(long)]
        asset: String,
    },
}

#[derive(Subcommand)]
enum SchemaCommands {
    /// Download the Dagster GraphQL schema
    Download,
}

#[derive(Subcommand)]
enum SelfCommands {
    /// Update dagctl to the latest release
    Update,
    /// Generate a Kiro SKILL.md file
    Skill {
        /// Write to ~/.kiro/skills/dagctl/SKILL.md instead of stdout
        #[arg(long)]
        install: bool,
    },
}

fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> anyhow::Result<()> {
    match &cli.command {
        Commands::SelfCmd {
            action: SelfCommands::Update,
        } => return commands::update::run_update(auth::resolve_github_url(cli.github_url)),
        Commands::SelfCmd {
            action: SelfCommands::Skill { install },
        } => return commands::skill::run_skill(*install),
        _ => {}
    }

    if let Commands::Completion { shell } = &cli.command {
        clap_complete::generate(
            *shell,
            &mut Cli::command(),
            "dagctl",
            &mut std::io::stdout(),
        );
        return Ok(());
    }

    let token = auth::resolve_token(cli.token)?;
    let organization = auth::resolve_organization(cli.organization)?;
    let deployment = auth::resolve_deployment(cli.deployment);
    let api_url = auth::build_api_url(&organization, deployment.as_deref());
    let fmt = cli.output;

    match cli.command {
        Commands::Schema { action } => match action {
            SchemaCommands::Download => {
                commands::schema::download_schema(&token, &api_url)?;
                Ok(())
            }
        },
        Commands::Get { resource } => match resource {
            GetResource::Runs {
                limit,
                status,
                job,
                launched_by,
                partition,
                tags,
            } => tokio::runtime::Runtime::new()?.block_on(async {
                commands::runs::list_runs(
                    &token,
                    &api_url,
                    limit,
                    &status,
                    &job,
                    &launched_by,
                    &partition,
                    &tags,
                    &fmt,
                )
                .await
            }),
            GetResource::Run { run_id } => tokio::runtime::Runtime::new()?
                .block_on(async { commands::runs::get_run(&token, &api_url, run_id, &fmt).await }),
            GetResource::RunEvents { run_id } => tokio::runtime::Runtime::new()?.block_on(async {
                commands::runs::get_events(&token, &api_url, run_id, &fmt).await
            }),
            GetResource::RunLogs { run_id } => tokio::runtime::Runtime::new()?
                .block_on(async { commands::runs::get_logs(&token, &api_url, run_id, &fmt).await }),
            GetResource::CodeLocations => tokio::runtime::Runtime::new()?.block_on(async {
                commands::code_locations::list_code_locations(&token, &api_url, &fmt).await
            }),
            GetResource::CodeLocation { name } => tokio::runtime::Runtime::new()?.block_on(async {
                commands::code_locations::get_code_location(&token, &api_url, name, &fmt).await
            }),
            GetResource::Jobs { code_location } => {
                tokio::runtime::Runtime::new()?.block_on(async {
                    commands::jobs::list_jobs(&token, &api_url, code_location, &fmt).await
                })
            }
            GetResource::Job {
                name,
                code_location,
            } => tokio::runtime::Runtime::new()?.block_on(async {
                commands::jobs::get_job(&token, &api_url, name, &code_location, &fmt).await
            }),
            GetResource::Assets {
                group,
                code_location,
                health,
            } => tokio::runtime::Runtime::new()?.block_on(async {
                commands::assets::list_assets(&token, &api_url, group, code_location, health, &fmt)
                    .await
            }),
            GetResource::Asset { key } => tokio::runtime::Runtime::new()?
                .block_on(async { commands::assets::get_asset(&token, &api_url, key, &fmt).await }),
            GetResource::AssetEvent { key, timestamp } => {
                tokio::runtime::Runtime::new()?.block_on(async {
                    commands::assets::get_asset_event(&token, &api_url, key, &timestamp, &fmt).await
                })
            }
            GetResource::AssetEvents {
                key,
                limit,
                event_type,
                status,
                partition,
                since,
                until,
            } => tokio::runtime::Runtime::new()?.block_on(async {
                commands::assets::get_asset_events(
                    &token, &api_url, key, limit, event_type, status, partition, since, until, &fmt,
                )
                .await
            }),
            GetResource::AssetPartitions { key } => {
                tokio::runtime::Runtime::new()?.block_on(async {
                    commands::assets::get_asset_partitions(&token, &api_url, key, &fmt).await
                })
            }
            GetResource::AssetLineage { key, depth } => {
                tokio::runtime::Runtime::new()?.block_on(async {
                    commands::assets::get_asset_lineage(&token, &api_url, key, depth, &fmt).await
                })
            }
            GetResource::AssetChecks { key } => tokio::runtime::Runtime::new()?.block_on(async {
                commands::assets::get_asset_checks(&token, &api_url, key, &fmt).await
            }),
            GetResource::AssetCheck { key, check } => {
                tokio::runtime::Runtime::new()?.block_on(async {
                    commands::assets::get_asset_check(&token, &api_url, key, &check, &fmt).await
                })
            }
            GetResource::AssetCheckExecutions {
                key,
                check,
                limit,
                status,
            } => tokio::runtime::Runtime::new()?.block_on(async {
                commands::assets::get_asset_check_executions(
                    &token, &api_url, key, &check, limit, status, &fmt,
                )
                .await
            }),
            GetResource::InsightsMetrics { entity_type } => tokio::runtime::Runtime::new()?
                .block_on(async {
                    commands::insights::list_metrics(&token, &api_url, &entity_type, &fmt).await
                }),
            GetResource::InsightsInfo => tokio::runtime::Runtime::new()?
                .block_on(async { commands::insights::get_info(&token, &api_url, &fmt).await }),
            GetResource::InsightsByJob {
                metric,
                last,
                since,
                until,
                granularity,
                aggregation,
                limit,
            } => tokio::runtime::Runtime::new()?.block_on(async {
                commands::insights::metrics_by_job(
                    &token,
                    &api_url,
                    &metric,
                    &last,
                    &since,
                    &until,
                    &granularity,
                    &aggregation,
                    limit,
                    &fmt,
                )
                .await
            }),
            GetResource::InsightsByAsset {
                metric,
                last,
                since,
                until,
                granularity,
                aggregation,
                limit,
                code_location,
                group,
                selection,
            } => tokio::runtime::Runtime::new()?.block_on(async {
                commands::insights::metrics_by_asset(
                    &token,
                    &api_url,
                    &metric,
                    &last,
                    &since,
                    &until,
                    &granularity,
                    &aggregation,
                    limit,
                    &code_location,
                    &group,
                    &selection,
                    &fmt,
                )
                .await
            }),
            GetResource::InsightsByAssetGroup {
                metric,
                last,
                since,
                until,
                granularity,
                aggregation,
                limit,
                code_location,
                selection,
            } => tokio::runtime::Runtime::new()?.block_on(async {
                commands::insights::metrics_by_asset_group(
                    &token,
                    &api_url,
                    &metric,
                    &last,
                    &since,
                    &until,
                    &granularity,
                    &aggregation,
                    limit,
                    &code_location,
                    &selection,
                    &fmt,
                )
                .await
            }),
            GetResource::InsightsByDeployment {
                metric,
                last,
                since,
                until,
                granularity,
                aggregation,
            } => tokio::runtime::Runtime::new()?.block_on(async {
                commands::insights::metrics_by_deployment(
                    &token,
                    &api_url,
                    &metric,
                    &last,
                    &since,
                    &until,
                    &granularity,
                    &aggregation,
                    &fmt,
                )
                .await
            }),
            GetResource::InsightsJobRuns {
                metric,
                job,
                code_location,
            } => tokio::runtime::Runtime::new()?.block_on(async {
                commands::insights::job_run_metrics(
                    &token,
                    &api_url,
                    &metric,
                    &job,
                    &code_location,
                    &fmt,
                )
                .await
            }),
            GetResource::InsightsAssetMaterializations { metric, asset } => {
                tokio::runtime::Runtime::new()?.block_on(async {
                    commands::insights::asset_materialization_metrics(
                        &token, &api_url, &metric, &asset, &fmt,
                    )
                    .await
                })
            }
        },
        Commands::Debug => tokio::runtime::Runtime::new()?.block_on(async {
            commands::debug::run_debug(&token, &organization, deployment.as_deref(), &api_url).await
        }),
        Commands::SelfCmd { .. } => unreachable!(),
        Commands::Completion { .. } => unreachable!(),
    }
}
