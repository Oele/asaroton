//! The asaroton Matrix homeserver binary.

#![allow(unreachable_pub)]

mod config;
mod import_synapse;
mod livekit_config;
mod server;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "asaroton", version, about = "A modern Matrix homeserver")]
struct Cli {
    /// Path to the configuration file.
    #[arg(short, long, env = "ASAROTON_CONFIG", default_value = "asaroton.toml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the server (default).
    Serve,
    /// Run database migrations and exit.
    Migrate,
    /// Print the server's signing key ID and public key.
    ShowKey,
    /// Store the plain text of older messages for substring search (see `search.mode`).
    SearchBackfill,
    /// Copy a Synapse homeserver's data into this (empty) database.
    ImportSynapse(import_synapse::ImportArgs),
    /// Print a complete `livekit.yaml` for the SFU described by `[rtc.livekit]`.
    LivekitConfig,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = config::Config::load(&cli.config)?;
    if matches!(cli.command, Some(Command::LivekitConfig)) {
        return livekit_config::print(&config, &cli.config);
    }
    server::init_tracing(&config);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        match cli.command.unwrap_or(Command::Serve) {
            Command::Serve => server::serve(config).await,
            Command::Migrate => server::migrate_only(config).await,
            Command::ShowKey => server::show_key(config).await,
            Command::SearchBackfill => server::search_backfill(config).await,
            Command::ImportSynapse(args) => import_synapse::run(config, args).await,
            Command::LivekitConfig => unreachable!("handled before the runtime starts"),
        }
    })
}
