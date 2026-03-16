mod auth;
mod commands;
mod config;
mod schema;

use clap::{Parser, Subcommand};

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
    Schema {
        #[command(subcommand)]
        action: SchemaCommands,
    },
    Runs {
        #[command(subcommand)]
        action: RunsCommands,
    },
    Debug,
}

#[derive(Subcommand)]
enum SchemaCommands {
    Download,
}

#[derive(Subcommand)]
enum RunsCommands {
    List {
        #[arg(long)]
        limit: Option<i32>,
    },
    Get {
        run_id: String,
    },
    Events {
        run_id: String,
    },
    Logs {
        run_id: String,
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
        Commands::Runs { action } => match action {
            RunsCommands::List { limit } => tokio::runtime::Runtime::new()?
                .block_on(async { commands::runs::list_runs(&token, &api_url, limit).await }),
            RunsCommands::Get { run_id } => tokio::runtime::Runtime::new()?
                .block_on(async { commands::runs::get_run(&token, &api_url, run_id).await }),
            RunsCommands::Events { run_id } => tokio::runtime::Runtime::new()?
                .block_on(async { commands::runs::get_events(&token, &api_url, run_id).await }),
            RunsCommands::Logs { run_id } => tokio::runtime::Runtime::new()?
                .block_on(async { commands::runs::get_logs(&token, &api_url, run_id).await }),
        },
        Commands::Debug => tokio::runtime::Runtime::new()?.block_on(async {
            commands::debug::run_debug(&token, &organization, deployment.as_deref(), &api_url)
                .await
        }),
    }
}
