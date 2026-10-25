//! Server signing keys: publishing ours, fetching and caching other servers', and building
//! the public-key maps that signature verification needs.

use std::{collections::BTreeMap, time::Duration};

use ruma::{
    CanonicalJsonObject, MilliSecondsSinceUnixEpoch, OwnedServerName, OwnedServerSigningKeyId,
    ServerName, UInt,
    api::federation::discovery::{
        OldVerifyKey, ServerSigningKeys, VerifyKey, get_remote_server_keys,
    },
    serde::Base64,
    signatures::{PublicKeyMap, PublicKeySet},
};
use tessera_store::{RemoteVerifyKey, now_ts};

use crate::{Error, Federation, Result};

/// How long our published keys are valid for. Spec recommends at most a week.
fn our_key_validity() -> Duration {
    Duration::from_hours(24 * 7)
}

impl Federation {
    /// Our `/_matrix/key/v2/server` response: current and expired keys, signed by us.
    pub async fn our_server_keys(&self) -> Result<CanonicalJsonObject> {
        let services = &self.services;
        let valid_until =
            now_ts() + i64::try_from(our_key_validity().as_millis()).unwrap_or(i64::MAX);
        let mut verify_keys = BTreeMap::new();
        verify_keys.insert(
            services.signing_key.key_id().clone(),
            VerifyKey::new(services.signing_key.public_key()),
        );
        let mut old_verify_keys = BTreeMap::new();
        for k in services.store.local_signing_keys().await? {
            if let Some(expired) = k.expired_ts
                && let Ok(key) = tessera_core::SigningKey::from_base64_seed(
                    &k.seed_b64,
                    k.key_id.as_str().trim_start_matches("ed25519:"),
                )
            {
                old_verify_keys.insert(
                    k.key_id.clone(),
                    OldVerifyKey::new(ms(expired), key.public_key()),
                );
            }
        }
        let mut keys = ServerSigningKeys::new(services.config.server_name.clone(), ms(valid_until));
        keys.verify_keys = verify_keys;
        keys.old_verify_keys = old_verify_keys;
        let mut obj = tessera_core::canonical::from_serialize(&keys)?;
        services
            .signing_key
            .sign_json(&services.config.server_name, &mut obj)?;
        Ok(obj)
    }

    /// Public keys for `server_name` that are valid at `at_ts` (or the latest we have), fetching
    /// from the server or a notary when the cache cannot satisfy the request.
    pub async fn public_keys_for(
        &self,
        server_name: &ServerName,
        key_ids: &[OwnedServerSigningKeyId],
        min_valid_until: i64,
    ) -> Result<PublicKeySet> {
        if server_name == self.services.config.server_name {
            let mut set = PublicKeySet::new();
            set.insert(
                self.services.signing_key.key_id().to_string(),
                self.services.signing_key.public_key(),
            );
            return Ok(set);
        }
        let wanted: Vec<String> = key_ids.iter().map(ToString::to_string).collect();
        let mut cached = if wanted.is_empty() {
            self.services.store.remote_verify_keys(server_name).await?
        } else {
            self.services
                .store
                .remote_verify_keys_for(server_name, &wanted)
                .await?
        };
        let satisfied = |keys: &[RemoteVerifyKey]| {
            !keys.is_empty()
                && wanted
                    .iter()
                    .all(|id| keys.iter().any(|k| k.key_id.as_str() == id))
                && keys
                    .iter()
                    .all(|k| k.valid_until_ts >= min_valid_until || k.expired_ts.is_some())
        };
        if !satisfied(&cached) {
            self.fetch_server_keys(server_name, min_valid_until).await?;
            cached = if wanted.is_empty() {
                self.services.store.remote_verify_keys(server_name).await?
            } else {
                self.services
                    .store
                    .remote_verify_keys_for(server_name, &wanted)
                    .await?
            };
        }
        if cached.is_empty() {
            return Err(Error::NoKeys(server_name.to_owned()));
        }
        let mut set = PublicKeySet::new();
        for k in cached {
            if let Ok(b) = Base64::parse(&k.verify_key_b64) {
                set.insert(k.key_id.to_string(), b);
            }
        }
        Ok(set)
    }

    /// A one-server `PublicKeyMap`.
    pub async fn public_key_map_for(
        &self,
        server_name: &ServerName,
        key_ids: &[OwnedServerSigningKeyId],
        min_valid_until: i64,
    ) -> Result<PublicKeyMap> {
        let set = self
            .public_keys_for(server_name, key_ids, min_valid_until)
            .await?;
        let mut map = PublicKeyMap::new();
        map.insert(server_name.to_string(), set);
        Ok(map)
    }

    /// Fetch keys directly from the server, falling back to the configured notaries, verify the
    /// response signature and cache the keys.
    pub async fn fetch_server_keys(
        &self,
        server_name: &ServerName,
        min_valid_until: i64,
    ) -> Result<()> {
        let direct = self.fetch_keys_direct(server_name).await;
        match direct {
            Ok(keys) => {
                self.store_fetched_keys(server_name, server_name, keys)
                    .await?;
                return Ok(());
            }
            Err(e) => {
                tracing::debug!(server = %server_name, error = %e, "direct key fetch failed, trying notaries");
            }
        }
        let mut last_err = Error::NoKeys(server_name.to_owned());
        for notary in &self.services.config.trusted_key_servers {
            if notary == server_name {
                continue;
            }
            match self
                .fetch_keys_from_notary(notary, server_name, min_valid_until)
                .await
            {
                Ok(list) => {
                    for keys in list {
                        self.store_fetched_keys(server_name, notary, keys).await?;
                    }
                    return Ok(());
                }
                Err(e) => last_err = e,
            }
        }
        Err(last_err)
    }

    async fn fetch_keys_direct(&self, server_name: &ServerName) -> Result<CanonicalJsonObject> {
        let resp = self
            .client
            .get_json(server_name, "/_matrix/key/v2/server")
            .await?;
        tessera_core::canonical::from_json_value(resp).map_err(Into::into)
    }

    async fn fetch_keys_from_notary(
        &self,
        notary: &ServerName,
        server_name: &ServerName,
        min_valid_until: i64,
    ) -> Result<Vec<CanonicalJsonObject>> {
        let req =
            get_remote_server_keys::v2::Request::new(server_name.to_owned(), ms(min_valid_until));
        let resp = self.client.send_unauthenticated(notary, req).await?;
        resp.server_keys
            .into_iter()
            .map(|raw| tessera_core::canonical::from_str(raw.json().get()).map_err(Into::into))
            .collect()
    }

    /// Verify a `ServerSigningKeys` response against itself and store its keys.
    async fn store_fetched_keys(
        &self,
        server_name: &ServerName,
        fetched_from: &ServerName,
        keys: CanonicalJsonObject,
    ) -> Result<()> {
        let parsed: ServerSigningKeys =
            serde_json::from_value(tessera_core::canonical::to_json_value(keys.clone()))?;
        if parsed.server_name != server_name {
            return Err(Error::Protocol(format!(
                "key response for {} was for {}",
                server_name, parsed.server_name
            )));
        }
        // The response must be signed by one of the keys it advertises.
        let mut set = PublicKeySet::new();
        for (id, k) in &parsed.verify_keys {
            set.insert(id.to_string(), k.key.clone());
        }
        let mut map = PublicKeyMap::new();
        map.insert(server_name.to_string(), set);
        tessera_core::signing::verify_json(&map, &keys)
            .map_err(|e| Error::Protocol(format!("server key response not validly signed: {e}")))?;

        // Spec: cache for at most half the remaining validity period.
        let valid_until = i64::try_from(u64::from(parsed.valid_until_ts.0)).unwrap_or(i64::MAX);
        let now = now_ts();
        let effective_valid_until = now + (valid_until - now).max(0) / 2;
        let key_json = tessera_core::canonical::to_string(&keys);
        let mut rows = Vec::new();
        for (id, k) in &parsed.verify_keys {
            rows.push(RemoteVerifyKey {
                server_name: server_name.to_owned(),
                key_id: id.clone(),
                verify_key_b64: k.key.encode(),
                valid_until_ts: effective_valid_until,
                expired_ts: None,
                fetched_ts: now,
                fetched_from: fetched_from.to_owned(),
                key_json: key_json.clone(),
            });
        }
        for (id, k) in &parsed.old_verify_keys {
            rows.push(RemoteVerifyKey {
                server_name: server_name.to_owned(),
                key_id: id.clone(),
                verify_key_b64: k.key.encode(),
                valid_until_ts: effective_valid_until,
                expired_ts: Some(i64::try_from(u64::from(k.expired_ts.0)).unwrap_or(i64::MAX)),
                fetched_ts: now,
                fetched_from: fetched_from.to_owned(),
                key_json: key_json.clone(),
            });
        }
        self.services.store.store_remote_verify_keys(rows).await?;
        Ok(())
    }

    /// Notary: the cached key responses we hold for a server (`POST /key/v2/query`).
    pub async fn notary_keys_for(
        &self,
        server_name: &ServerName,
        min_valid_until: i64,
    ) -> Result<Vec<CanonicalJsonObject>> {
        if server_name == self.services.config.server_name {
            return Ok(vec![self.our_server_keys().await?]);
        }
        let mut cached = self.services.store.remote_verify_keys(server_name).await?;
        if cached.is_empty() || cached.iter().all(|k| k.valid_until_ts < min_valid_until) {
            let _ = self.fetch_server_keys(server_name, min_valid_until).await;
            cached = self.services.store.remote_verify_keys(server_name).await?;
        }
        // All rows from one fetch share the same signed response JSON; de-duplicate.
        let mut seen = std::collections::BTreeSet::new();
        let mut out = Vec::new();
        for k in cached {
            if seen.insert(k.key_json.clone())
                && let Ok(obj) = tessera_core::canonical::from_str(&k.key_json)
            {
                out.push(obj);
            }
        }
        Ok(out)
    }
}

fn ms(ts: i64) -> MilliSecondsSinceUnixEpoch {
    MilliSecondsSinceUnixEpoch(UInt::try_from(ts.max(0)).unwrap_or(UInt::MAX))
}

/// Servers whose keys an incoming event needs, with the event's own timestamp as the
/// minimum validity.
pub fn event_signature_servers(event: &CanonicalJsonObject) -> Vec<OwnedServerName> {
    event
        .get("signatures")
        .and_then(|s| s.as_object())
        .map(|m| {
            m.keys()
                .filter_map(|k| OwnedServerName::try_from(k.as_str()).ok())
                .collect()
        })
        .unwrap_or_default()
}
