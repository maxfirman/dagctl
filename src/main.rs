mod commands;
mod auth;
mod config;
mod schema;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "dagster-cli")]
#[command(about = "CLI for Dagster GraphQL API")]
struct Cli {
    #[arg(long, global = true)]
    token: Option<String>,
    
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
    match cli.command {
        Commands::Schema { action } => match action {
            SchemaCommands::Download => {
                let token = auth::resolve_token(cli.token)?;
                commands::schema::download_schema(&token)?;
                Ok(())
            }
        },
        Commands::Runs { action } => {
            let token = auth::resolve_token(cli.token)?;
            match action {
                RunsCommands::List { limit } => {
                    tokio::runtime::Runtime::new()?.block_on(async {
                        commands::runs::list_runs(&token, limit).await
                    })
                }
                RunsCommands::Get { run_id } => {
                    tokio::runtime::Runtime::new()?.block_on(async {
                        commands::runs::get_run(&token, run_id).await
                    })
                }
                RunsCommands::Events { run_id } => {
                    tokio::runtime::Runtime::new()?.block_on(async {
                        commands::runs::get_events(&token, run_id).await
                    })
                }
                RunsCommands::Logs { run_id } => {
                    tokio::runtime::Runtime::new()?.block_on(async {
                        commands::runs::get_logs(&token, run_id).await
                    })
                }
            }
        }
    }
}
