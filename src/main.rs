mod auth;
mod commands;
mod config;
mod output;
mod schema;

use clap::{CommandFactory, Parser, Subcommand};
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
    },
    /// Get partition status summary for an asset
    #[command(name = "asset-partitions")]
    AssetPartitions { key: String },
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
}

fn main() {
    let cli = Cli::parse();

    if let Err(e) = run(cli) {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn run(cli: Cli) -> anyhow::Result<()> {
    if let Commands::SelfCmd {
        action: SelfCommands::Update,
    } = &cli.command
    {
        return commands::update::run_update();
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
            GetResource::Runs { limit } => tokio::runtime::Runtime::new()?
                .block_on(async { commands::runs::list_runs(&token, &api_url, limit, &fmt).await }),
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
            GetResource::AssetEvents {
                key,
                limit,
                event_type,
                status,
                partition,
            } => tokio::runtime::Runtime::new()?.block_on(async {
                commands::assets::get_asset_events(
                    &token, &api_url, key, limit, event_type, status, partition, &fmt,
                )
                .await
            }),
            GetResource::AssetPartitions { key } => {
                tokio::runtime::Runtime::new()?.block_on(async {
                    commands::assets::get_asset_partitions(&token, &api_url, key, &fmt).await
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
            GetResource::AssetCheckExecutions { key, check, limit } => {
                tokio::runtime::Runtime::new()?.block_on(async {
                    commands::assets::get_asset_check_executions(
                        &token, &api_url, key, &check, limit, &fmt,
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
