//! The asaroton Matrix homeserver binary.

#![allow(unreachable_pub)]
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

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

fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    let config = match config::Config::load(&cli.config) {
        Ok(config) => config,
        Err(e) => return failed_before_logging(&e),
    };
    if matches!(cli.command, Some(Command::LivekitConfig)) {
        return match livekit_config::print(&config, &cli.config) {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(e) => failed_before_logging(&e),
        };
    }
    server::init_tracing(&config);
    let result = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(anyhow::Error::from)
        .and_then(|runtime| {
            let result = runtime.block_on(async move {
                match cli.command.unwrap_or(Command::Serve) {
                    Command::Serve => server::serve(config).await,
                    Command::Migrate => server::migrate_only(config).await,
                    Command::ShowKey => server::show_key(config).await,
                    Command::SearchBackfill => server::search_backfill(config).await,
                    Command::ImportSynapse(args) => import_synapse::run(config, args).await,
                    Command::LivekitConfig => unreachable!("handled before the runtime starts"),
                }
            });
            // What had to be finished is finished by now (a server that stops waits for its
            // requests itself). Dropping the runtime would wait for every blocking call that
            // is still out, and a name lookup for a server that does not answer can be out
            // for half a minute: the process would not end when it is told to.
            runtime.shutdown_background();
            result
        });
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            // Through the logger, with every cause: a server that cannot start (database
            // unreachable, a migration that does not apply, a port in use) otherwise leaves
            // nothing with a level in the log, and whoever reads it by level sees a clean one.
            tracing::error!(error = %format!("{e:#}"), "asaroton stopped with an error");
            std::process::ExitCode::FAILURE
        }
    }
}

/// Reports an error from before the logger exists (it is set up from the configuration).
/// Under systemd the line gets the prefix that the journal reads as priority "err".
fn failed_before_logging(e: &anyhow::Error) -> std::process::ExitCode {
    let prefix = if std::env::var_os("JOURNAL_STREAM").is_some() {
        "<3>"
    } else {
        ""
    };
    eprintln!("{prefix}ERROR asaroton: {e:#}");
    std::process::ExitCode::FAILURE
}
