//! Startup wiring: logging, database, services, federation, listeners.

use std::sync::Arc;

use anyhow::Context;
use tessera_api::App;
use tessera_federation::{Federation, FederationConfig, hooks::Hooks};
use tessera_service::Services;
use tessera_store::{EventStore, PostgresStore, Store};
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

pub async fn connect_store(config: &Config) -> anyhow::Result<Arc<PostgresStore>> {
    let store = PostgresStore::connect(&config.database_url, config.database_max_connections)
        .await
        .context("connecting to PostgreSQL")?;
    store.migrate().await.context("running migrations")?;
    store.set_search_plaintext(config.search.mode != crate::config::SearchMode::Word);
    Ok(Arc::new(store))
}

/// `tessera search-backfill`: store the plain text of every searchable event that lacks it, in
/// batches, so substring search covers history from before it was enabled.
pub async fn search_backfill(config: Config) -> anyhow::Result<()> {
    if config.search.mode == crate::config::SearchMode::Word {
        anyhow::bail!("search.mode is \"word\"; set it to \"substring\" or \"both\" first");
    }
    let store = connect_store(&config).await?;
    let (mut after, mut total, mut batches) = (i64::MIN, 0usize, 0u64);
    loop {
        let (written, last) = store.search_backfill_batch(after, 2000).await?;
        total += written;
        batches += 1;
        match last {
            Some(so) => after = so,
            None => break,
        }
        if batches % 25 == 0 {
            println!("{total} rows written, at stream position {after}");
        }
    }
    println!("done: {total} rows written");
    Ok(())
}

fn service_config(config: &Config) -> anyhow::Result<tessera_service::Config> {
    let public_base_url = match &config.public_base_url {
        Some(url) => url.trim_end_matches('/').to_owned(),
        None if config.listeners.client.tls().is_some() => {
            format!("https://{}", config.server_name)
        }
        None => format!("http://{}", config.listeners.client.bind),
    };
    let mut trusted = config.federation.trusted_key_servers.clone();
    if trusted.is_empty()
        && let Ok(m) = ruma::OwnedServerName::try_from("matrix.org")
    {
        trusted.push(m);
    }
    Ok(tessera_service::Config {
        server_name: config.server_name.clone(),
        default_room_version: config.rooms.default_version.clone(),
        registration_enabled: config.registration.enabled,
        registration_shared_secret: config.registration.shared_secret.clone(),
        registration_requires_token: config.registration.require_token,
        allow_guests: config.registration.allow_guests,
        federation_disabled: config.federation.disabled,
        trusted_key_servers: trusted,
        rate_limits: {
            let rl = &config.rate_limiting;
            let rate = |r: &crate::config::RateConfig| tessera_service::config::Rate {
                per_second: r.per_second,
                burst: r.burst,
            };
            tessera_service::config::RateLimits {
                enabled: rl.enabled,
                trust_x_forwarded_for: rl.trust_x_forwarded_for,
                logins: rate(&rl.logins),
                registrations: rate(&rl.registrations),
                messages: rate(&rl.messages),
                joins: rate(&rl.joins),
                invites: rate(&rl.invites),
            }
        },
        admins: config
            .admins
            .iter()
            .map(|a| {
                ruma::OwnedUserId::try_from(a.as_str())
                    .map_err(|e| anyhow::anyhow!("invalid admin user ID {a}: {e}"))
            })
            .collect::<anyhow::Result<Vec<_>>>()?,
        public_base_url,
        well_known_server: config.federation.well_known_server.clone(),
        search: tessera_service::config::SearchSettings {
            mode: match config.search.mode {
                crate::config::SearchMode::Word => tessera_store::SearchMode::Word,
                crate::config::SearchMode::Substring => tessera_store::SearchMode::Substring,
                crate::config::SearchMode::Both => tessera_store::SearchMode::Both,
            },
            min_length: config.search.substring_min_length,
        },
        rtc: config
            .rtc
            .livekit
            .as_ref()
            .map(|lk| tessera_service::config::LivekitSettings {
                url: lk.url.trim_end_matches('/').to_owned(),
                api_url: lk.api_url(),
                api_key: lk.api_key.clone(),
                api_secret: lk.api_secret.clone(),
                remote_can_publish: lk.remote_can_publish,
            }),
        software_version: env!("CARGO_PKG_VERSION").to_owned(),
        media_dir: config.data_dir.join("media"),
        appservices: load_appservices(config)?,
        server_notices: config.server_notices.enabled.then(|| {
            tessera_service::config::ServerNotices {
                user_localpart: config.server_notices.user_localpart.clone(),
                display_name: config.server_notices.display_name.clone(),
            }
        }),
        mas: mas_settings(config)?,
    })
}

fn mas_settings(config: &Config) -> anyhow::Result<Option<tessera_service::config::MasSettings>> {
    if !config.mas.enabled {
        return Ok(None);
    }
    let secret = match (&config.mas.secret, &config.mas.secret_file) {
        (Some(s), None) => s.clone(),
        (None, Some(path)) => std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading mas.secret_file {}: {e}", path.display()))?
            .trim()
            .to_owned(),
        (Some(_), Some(_)) => anyhow::bail!("set either mas.secret or mas.secret_file, not both"),
        (None, None) => anyhow::bail!("mas.enabled requires mas.secret or mas.secret_file"),
    };
    if secret.is_empty() {
        anyhow::bail!("the MAS shared secret is empty");
    }
    Ok(Some(tessera_service::config::MasSettings {
        endpoint: config.mas.endpoint.clone(),
        secret,
    }))
}

fn federation_config(config: &Config) -> FederationConfig {
    let mut extra = Vec::new();
    for path in &config.federation.extra_ca_certs {
        match std::fs::read(path) {
            Ok(pem) => extra.push(pem),
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "could not read extra CA certificate");
            }
        }
    }
    FederationConfig {
        extra_ca_certs_pem: extra,
        insecure_http_servers: config.federation.insecure_http_servers.clone(),
        danger_accept_invalid_certs: config.federation.danger_accept_invalid_certs,
    }
}

pub async fn migrate_only(config: Config) -> anyhow::Result<()> {
    connect_store(&config).await?;
    tracing::info!("migrations applied");
    Ok(())
}

pub async fn show_key(config: Config) -> anyhow::Result<()> {
    let store = connect_store(&config).await?;
    let services = Services::new(service_config(&config)?, store).await?;
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
    let services = Services::new(service_config(&config)?, store).await?;
    tracing::info!(server_name = %config.server_name, key = %services.signing_key.key_id(), "tessera starting");

    let federation = if config.federation.disabled {
        None
    } else {
        let fed = Federation::new(Arc::clone(&services), federation_config(&config))
            .context("starting federation")?;
        services.set_federation(Arc::new(Hooks(Arc::clone(&fed))));
        Some(fed)
    };
    tessera_service::delayed_events::start_worker(Arc::clone(&services));
    tessera_service::rtc::delegation::start_worker(Arc::clone(&services));
    if config.retention.enabled {
        tessera_service::admin::start_retention(
            Arc::clone(&services),
            std::time::Duration::from_secs(config.retention.interval_secs.max(60)),
            config.retention.default_max_lifetime_ms,
        );
    }
    // Configured admins get the flag (they must have registered already).
    for admin in &services.config.admins {
        match services.store.get_user(admin).await? {
            Some(u) if !u.is_admin => services.store.set_admin(admin, true).await?,
            Some(_) => {}
            None => tracing::warn!(user = %admin, "configured admin does not exist yet"),
        }
    }
    let app = tessera_api::build_router(Arc::new(App {
        limiter: tessera_api::ratelimit::Limiter::default(),
        services: Arc::clone(&services),
        federation,
    }));

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

/// Read every appservice registration named in the config.
fn load_appservices(
    config: &Config,
) -> anyhow::Result<Vec<tessera_service::appservice::Registration>> {
    let mut files: Vec<std::path::PathBuf> = config.appservices.registration_files.clone();
    if let Some(dir) = &config.appservices.registration_dir
        && let Ok(entries) = std::fs::read_dir(dir)
    {
        let mut found: Vec<std::path::PathBuf> = entries
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| {
                p.extension()
                    .and_then(|e| e.to_str())
                    .is_some_and(|e| e == "yaml" || e == "yml")
            })
            .collect();
        found.sort();
        files.extend(found);
    }
    let mut out = Vec::new();
    for f in files {
        let text = std::fs::read_to_string(&f)
            .map_err(|e| anyhow::anyhow!("reading appservice registration {}: {e}", f.display()))?;
        let reg: tessera_service::appservice::Registration = serde_yaml::from_str(&text)
            .map_err(|e| anyhow::anyhow!("parsing appservice registration {}: {e}", f.display()))?;
        tracing::info!(id = %reg.id, file = %f.display(), "loaded appservice registration");
        out.push(reg);
    }
    Ok(out)
}
