//! Startup wiring: logging, database, services, listeners.

use std::sync::Arc;

use anyhow::Context;
use tessera_service::Services;
use tessera_store::{PostgresStore, Store};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::Config;

/// Initialise `tracing`. `RUST_LOG` overrides the configured filter.
pub fn init_tracing(config: &Config) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(config.logging.filter.clone()));
    let registry = tracing_subscriber::registry().with(filter);
    if config.logging.json {
        registry
            .with(tracing_subscriber::fmt::layer().json())
            .init();
    } else {
        registry.with(tracing_subscriber::fmt::layer()).init();
    }
}

async fn connect_store(config: &Config) -> anyhow::Result<Arc<PostgresStore>> {
    let store = PostgresStore::connect(&config.database_url, config.database_max_connections)
        .await
        .context("connecting to PostgreSQL")?;
    store.migrate().await.context("running migrations")?;
    Ok(Arc::new(store))
}

fn service_config(config: &Config) -> tessera_service::Config {
    let public_base_url = if config.listeners.client.tls().is_some() {
        format!("https://{}", config.server_name)
    } else {
        format!("http://{}", config.listeners.client.bind)
    };
    tessera_service::Config {
        server_name: config.server_name.clone(),
        default_room_version: config.rooms.default_version.clone(),
        registration_enabled: config.registration.enabled,
        registration_shared_secret: config.registration.shared_secret.clone(),
        registration_requires_token: config.registration.require_token,
        allow_guests: config.registration.allow_guests,
        federation_disabled: config.federation.disabled,
        trusted_key_servers: config.federation.trusted_key_servers.clone(),
        rate_limiting: config.rate_limiting.enabled,
        public_base_url,
        software_version: env!("CARGO_PKG_VERSION").to_owned(),
    }
}

pub async fn migrate_only(config: Config) -> anyhow::Result<()> {
    connect_store(&config).await?;
    tracing::info!("migrations applied");
    Ok(())
}

pub async fn show_key(config: Config) -> anyhow::Result<()> {
    let store = connect_store(&config).await?;
    let services = Services::new(service_config(&config), store).await?;
    println!(
        "{} {}",
        services.signing_key.key_id(),
        services.signing_key.public_key_base64()
    );
    Ok(())
}

pub async fn serve(config: Config) -> anyhow::Result<()> {
    // Several dependencies enable both rustls backends; pick one explicitly or TLS panics.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    std::fs::create_dir_all(&config.data_dir)
        .with_context(|| format!("creating data dir {}", config.data_dir.display()))?;
    let store = connect_store(&config).await?;
    let services = Services::new(service_config(&config), store).await?;
    tracing::info!(server_name = %config.server_name, key = %services.signing_key.key_id(), "tessera starting");

    let app = tessera_api::build_router(Arc::clone(&services));

    let mut tasks = tokio::task::JoinSet::new();
    if config.listeners.client.enabled {
        tasks.spawn(listen(
            config.listeners.client.clone(),
            app.clone(),
            "client",
        ));
    }
    if config.listeners.federation.enabled && !config.federation.disabled {
        tasks.spawn(listen(
            config.listeners.federation.clone(),
            app,
            "federation",
        ));
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => tracing::info!("shutting down"),
        res = tasks.join_next() => {
            if let Some(res) = res {
                res.context("listener task panicked")??;
            }
        }
    }
    Ok(())
}

async fn listen(
    listener: crate::config::Listener,
    app: axum::Router,
    name: &'static str,
) -> anyhow::Result<()> {
    let app = app.into_make_service_with_connect_info::<std::net::SocketAddr>();
    match listener.tls() {
        Some((cert, key)) => {
            let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
                .await
                .with_context(|| {
                    format!(
                        "loading TLS cert {} / key {}",
                        cert.display(),
                        key.display()
                    )
                })?;
            tracing::info!(%name, bind = %listener.bind, "listening (https)");
            axum_server::bind_rustls(listener.bind, tls)
                .serve(app)
                .await?;
        }
        None => {
            tracing::info!(%name, bind = %listener.bind, "listening (http)");
            axum_server::bind(listener.bind).serve(app).await?;
        }
    }
    Ok(())
}
