//! Server signing keys: publishing ours, fetching and caching other servers', and building
//! the public-key maps that signature verification needs.

use std::{collections::BTreeMap, time::Duration};

use asaroton_store::{RemoteVerifyKey, now_ts};
use ruma::{
    CanonicalJsonObject, MilliSecondsSinceUnixEpoch, OwnedServerName, OwnedServerSigningKeyId,
    ServerName, UInt,
    api::federation::discovery::{
        OldVerifyKey, ServerSigningKeys, VerifyKey, get_remote_server_keys,
    },
    serde::Base64,
    signatures::{PublicKeyMap, PublicKeySet},
};

use crate::{Error, Federation, Result};

/// How long a key that could not be found is not looked for again.
const KEY_MISS_TTL: Duration = Duration::from_mins(10);

/// Check that a key document carries a valid signature of the notary it came from, made with
/// one of the notary's keys that we know.
fn signed_by_notary(
    document: &CanonicalJsonObject,
    notary: &ServerName,
    notary_keys: &PublicKeySet,
) -> Result<()> {
    let mut to_verify = document.clone();
    let mut checkable = false;
    if let Some(ruma::CanonicalJsonValue::Object(signatures)) = to_verify.get_mut("signatures") {
        signatures.retain(|entity, _| entity == notary.as_str());
        if let Some(ruma::CanonicalJsonValue::Object(by_key)) = signatures.get_mut(notary.as_str())
        {
            by_key.retain(|key_id, _| notary_keys.contains_key(key_id));
            checkable = !by_key.is_empty();
        }
    }
    if !checkable {
        return Err(Error::Protocol(format!(
            "no signature of {notary} made with a key we know"
        )));
    }
    let mut map = PublicKeyMap::new();
    map.insert(notary.to_string(), notary_keys.clone());
    asaroton_core::signing::verify_json(&map, &to_verify)
        .map_err(|e| Error::Protocol(format!("signature of {notary} does not verify: {e}")))
}

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
                && let Ok(key) = asaroton_core::SigningKey::from_base64_seed(
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
        let mut obj = asaroton_core::canonical::from_serialize(&keys)?;
        services
            .signing_key
            .sign_json(&services.config.server_name, &mut obj)?;
        Ok(obj)
    }

    /// Public keys of `server_name` for checking a signature made at `min_valid_until`.
    ///
    /// `key_ids` names the keys the signature at hand was made with. A server changes keys
    /// over time, and an old event is signed with the key of its day: asking only for "the
    /// server's keys" finds the current one and leaves the old event unverifiable. When a
    /// named key is not cached, the server itself is asked first and then the notaries,
    /// which keep the keys they have seen, also of servers that no longer publish them or
    /// no longer exist. A key that had expired before `min_valid_until` is not returned.
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
        let mut cached = self.cached_keys(server_name, &wanted).await?;
        let satisfied = |keys: &[RemoteVerifyKey]| {
            !keys.is_empty()
                && wanted
                    .iter()
                    .all(|id| keys.iter().any(|k| k.key_id.as_str() == id))
                && keys
                    .iter()
                    .all(|k| k.valid_until_ts >= min_valid_until || k.expired_ts.is_some())
        };
        if !satisfied(&cached) && !self.gave_up_recently(server_name, &wanted, &cached) {
            let fetched = self
                .fetch_server_keys(server_name, &wanted, min_valid_until)
                .await;
            cached = self.cached_keys(server_name, &wanted).await?;
            for id in &wanted {
                let key = (server_name.to_owned(), id.clone());
                if cached.iter().any(|k| k.key_id.as_str() == id) {
                    self.key_misses.remove(&key);
                } else if fetched.is_ok() {
                    // Somebody answered and the key was not among what they have. (When
                    // nobody could be reached the next event simply tries again.)
                    self.key_misses.insert(key, std::time::Instant::now());
                }
            }
            if cached.is_empty() {
                fetched?;
            }
        }
        // A retired key only vouches for what was signed while it was in use.
        cached.retain(|k| {
            k.expired_ts
                .is_none_or(|expired| expired >= min_valid_until)
        });
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

    async fn cached_keys(
        &self,
        server_name: &ServerName,
        wanted: &[String],
    ) -> Result<Vec<RemoteVerifyKey>> {
        Ok(if wanted.is_empty() {
            self.services.store.remote_verify_keys(server_name).await?
        } else {
            self.services
                .store
                .remote_verify_keys_for(server_name, wanted)
                .await?
        })
    }

    /// Whether the keys still missing were all looked for a moment ago without success.
    /// Without this every event signed with an unobtainable key would start a round of
    /// requests to its server and the notaries.
    fn gave_up_recently(
        &self,
        server_name: &ServerName,
        wanted: &[String],
        cached: &[RemoteVerifyKey],
    ) -> bool {
        let mut missing = wanted
            .iter()
            .filter(|id| !cached.iter().any(|k| k.key_id.as_str() == *id))
            .peekable();
        missing.peek().is_some()
            && missing.all(|id| {
                self.key_misses
                    .get(&(server_name.to_owned(), id.clone()))
                    .is_some_and(|at| at.elapsed() < KEY_MISS_TTL)
            })
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

    /// Fetch keys from the server itself and, when that fails or does not yield every key in
    /// `wanted`, from the configured notaries. Each response is verified and cached.
    pub async fn fetch_server_keys(
        &self,
        server_name: &ServerName,
        wanted: &[String],
        min_valid_until: i64,
    ) -> Result<()> {
        let mut found_any = false;
        let mut last_err = Error::NoKeys(server_name.to_owned());
        match self.fetch_keys_direct(server_name).await {
            Ok(keys) => {
                self.store_fetched_keys(server_name, server_name, keys)
                    .await?;
                found_any = true;
                if self.has_keys(server_name, wanted).await? {
                    return Ok(());
                }
                tracing::debug!(server = %server_name, ?wanted, "the server does not publish this key any more, asking notaries");
            }
            Err(e) => {
                tracing::debug!(server = %server_name, error = %e, "direct key fetch failed, trying notaries");
            }
        }
        for notary in &self.services.config.trusted_key_servers {
            if notary == server_name {
                continue;
            }
            match self
                .fetch_keys_from_notary(notary, server_name, min_valid_until)
                .await
            {
                Ok(list) => {
                    // What a notary says about another server counts because the notary
                    // signs it. Its own keys come from the notary itself.
                    let notary_keys = match self.notary_own_keys(notary).await {
                        Ok(keys) => keys,
                        Err(e) => {
                            tracing::warn!(%notary, error = %e, "cannot check this notary's answers: its own keys are unavailable");
                            last_err = e;
                            continue;
                        }
                    };
                    // A notary returns every key response it holds for the server; one that
                    // does not check out must not cost us the others.
                    for keys in list {
                        if let Err(e) = signed_by_notary(&keys, notary, &notary_keys) {
                            tracing::warn!(server = %server_name, %notary, error = %e, "ignoring a key response that the notary has not validly signed");
                            continue;
                        }
                        match self.store_fetched_keys(server_name, notary, keys).await {
                            Ok(()) => found_any = true,
                            Err(e) => {
                                tracing::debug!(server = %server_name, %notary, error = %e, "ignoring a key response from the notary");
                            }
                        }
                    }
                    if found_any && self.has_keys(server_name, wanted).await? {
                        return Ok(());
                    }
                }
                Err(e) => last_err = e,
            }
        }
        if found_any { Ok(()) } else { Err(last_err) }
    }

    /// Whether every key in `wanted` is cached (trivially so when none is named).
    async fn has_keys(&self, server_name: &ServerName, wanted: &[String]) -> Result<bool> {
        if wanted.is_empty() {
            return Ok(true);
        }
        let cached = self
            .services
            .store
            .remote_verify_keys_for(server_name, wanted)
            .await?;
        Ok(wanted
            .iter()
            .all(|id| cached.iter().any(|k| k.key_id.as_str() == id)))
    }

    async fn fetch_keys_direct(&self, server_name: &ServerName) -> Result<CanonicalJsonObject> {
        let resp = self
            .client
            .get_json(server_name, "/_matrix/key/v2/server")
            .await?;
        asaroton_core::canonical::from_json_value(resp).map_err(Into::into)
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
            .map(|raw| asaroton_core::canonical::from_str(raw.json().get()).map_err(Into::into))
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
            serde_json::from_value(asaroton_core::canonical::to_json_value(keys.clone()))?;
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
        // A notary passes the response on with its own signature (and those of notaries it
        // got it from) added; only the origin server's signature is checked here.
        let mut to_verify = keys.clone();
        if let Some(ruma::CanonicalJsonValue::Object(sigs)) = to_verify.get_mut("signatures") {
            sigs.retain(|entity, _| entity == server_name.as_str());
        }
        asaroton_core::signing::verify_json(&map, &to_verify)
            .map_err(|e| Error::Protocol(format!("server key response not validly signed: {e}")))?;

        // Spec: cache for at most half the remaining validity period.
        let valid_until = i64::try_from(u64::from(parsed.valid_until_ts.0)).unwrap_or(i64::MAX);
        let now = now_ts();
        let effective_valid_until = now + (valid_until - now).max(0) / 2;
        // A response that has run out is what a notary keeps of a key the server has since
        // replaced. It is stored with the validity it really had: enough for events signed
        // back then, and a newer event signed with it prompts a fresh look at the server.
        // (The store never lowers a validity it already holds, so a notary's stale copy of a
        // key that is still current does no harm.)
        let stored_valid_until = if valid_until <= now {
            valid_until
        } else {
            effective_valid_until
        };
        let key_json = asaroton_core::canonical::to_string(&keys);
        let mut rows = Vec::new();
        for (id, k) in &parsed.verify_keys {
            rows.push(RemoteVerifyKey {
                server_name: server_name.to_owned(),
                key_id: id.clone(),
                verify_key_b64: k.key.encode(),
                valid_until_ts: stored_valid_until,
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
            let _ = self
                .fetch_server_keys(server_name, &[], min_valid_until)
                .await;
            cached = self.services.store.remote_verify_keys(server_name).await?;
        }
        // All rows from one fetch share the same signed response JSON; de-duplicate. Each
        // document goes out with our own signature added: that is what makes it a notary's
        // answer, and servers that check it (Synapse does) refuse a document without one.
        let mut seen = std::collections::BTreeSet::new();
        let mut out = Vec::new();
        for k in cached {
            if seen.insert(k.key_json.clone())
                && let Ok(mut obj) = asaroton_core::canonical::from_str(&k.key_json)
            {
                self.services
                    .signing_key
                    .sign_json(&self.services.config.server_name, &mut obj)?;
                out.push(obj);
            }
        }
        Ok(out)
    }

    /// The keys of a notary itself, for checking its signature on what it hands out. Only
    /// the notary is asked: who vouches for a notary's key is not a question for a notary.
    async fn notary_own_keys(&self, notary: &ServerName) -> Result<PublicKeySet> {
        let now = now_ts();
        let mut cached = self.services.store.remote_verify_keys(notary).await?;
        if cached.is_empty() || cached.iter().all(|k| k.valid_until_ts < now) {
            let keys = self.fetch_keys_direct(notary).await?;
            self.store_fetched_keys(notary, notary, keys).await?;
            cached = self.services.store.remote_verify_keys(notary).await?;
        }
        let mut set = PublicKeySet::new();
        for k in cached {
            if let Ok(b) = Base64::parse(&k.verify_key_b64) {
                set.insert(k.key_id.to_string(), b);
            }
        }
        Ok(set)
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
