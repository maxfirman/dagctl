mod auth;
mod commands;
mod config;
mod schema;

use clap::{CommandFactory, Parser, Subcommand};

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
    /// Get events for a run
    Events {
        run_id: String,
    },
    /// Get captured logs for a run
    Logs {
        run_id: String,
    },
    Schema {
        #[command(subcommand)]
        action: SchemaCommands,
    },
    Debug,
    /// Generate shell completions
    #[command(hide = true)]
    Completions {
        shell: clap_complete::Shell,
    },
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
    Run {
        run_id: String,
    },
    /// List code locations
    #[command(name = "code-locations")]
    CodeLocations,
    /// Show details of a specific code location
    #[command(name = "code-location")]
    CodeLocation {
        name: String,
    },
}

#[derive(Subcommand)]
enum SchemaCommands {
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

    if let Commands::Completions { shell } = &cli.command {
        clap_complete::generate(*shell, &mut Cli::command(), "dagctl", &mut std::io::stdout());
        return Ok(());
    }

    let token = auth::resolve_token(cli.token)?;
    let organization = auth::resolve_organization(cli.organization)?;
    let deployment = auth::resolve_deployment(cli.deployment);
    let api_url = auth::build_api_url(&organization, deployment.as_deref());

    match cli.command {
        Commands::Schema { action } => match action {
            SchemaCommands::Download => {
                commands::schema::download_schema(&token, &api_url)?;
                Ok(())
            }
        },
        Commands::Get { resource } => match resource {
            GetResource::Runs { limit } => tokio::runtime::Runtime::new()?
                .block_on(async { commands::runs::list_runs(&token, &api_url, limit).await }),
            GetResource::Run { run_id } => tokio::runtime::Runtime::new()?
                .block_on(async { commands::runs::get_run(&token, &api_url, run_id).await }),
            GetResource::CodeLocations => tokio::runtime::Runtime::new()?
                .block_on(async { commands::code_locations::list_code_locations(&token, &api_url).await }),
            GetResource::CodeLocation { name } => tokio::runtime::Runtime::new()?
                .block_on(async { commands::code_locations::get_code_location(&token, &api_url, name).await }),
        },
        Commands::Events { run_id } => tokio::runtime::Runtime::new()?
            .block_on(async { commands::runs::get_events(&token, &api_url, run_id).await }),
        Commands::Logs { run_id } => tokio::runtime::Runtime::new()?
            .block_on(async { commands::runs::get_logs(&token, &api_url, run_id).await }),
        Commands::Debug => tokio::runtime::Runtime::new()?.block_on(async {
            commands::debug::run_debug(&token, &organization, deployment.as_deref(), &api_url).await
        }),
        Commands::SelfCmd { .. } => unreachable!(),
        Commands::Completions { .. } => unreachable!(),
    }
}
