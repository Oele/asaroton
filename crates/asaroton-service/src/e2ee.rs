//! End-to-end encryption support: device keys, one-time/fallback keys, cross-signing keys,
//! key signatures, the device-list change stream, and to-device messaging.
//!
//! The server never interprets key material; it stores and serves signed JSON blobs.

use std::collections::BTreeMap;

use asaroton_store::OneTimeKeyRow;
use ruma::{DeviceId, OwnedDeviceId, OwnedServerName, OwnedUserId, UserId};
use serde_json::{Value, json};

use crate::{Error, Notification, Result, Services};

/// Result of `/keys/query` for the users we know about.
#[derive(Debug, Default)]
pub struct KeyQueryResult {
    pub device_keys: BTreeMap<OwnedUserId, BTreeMap<OwnedDeviceId, Value>>,
    pub master_keys: BTreeMap<OwnedUserId, Value>,
    pub self_signing_keys: BTreeMap<OwnedUserId, Value>,
    pub user_signing_keys: BTreeMap<OwnedUserId, Value>,
}

fn split_key_id(id: &str) -> Result<(String, String)> {
    let (alg, name) = id
        .split_once(':')
        .ok_or_else(|| Error::bad_json(format!("invalid key id {id}")))?;
    Ok((alg.to_owned(), name.to_owned()))
}

impl Services {
    /// `/keys/upload`.
    pub async fn upload_keys(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        device_keys: Option<Value>,
        one_time_keys: &BTreeMap<String, Value>,
        fallback_keys: &BTreeMap<String, Value>,
    ) -> Result<BTreeMap<String, u64>> {
        if let Some(dk) = device_keys {
            let uid = dk.get("user_id").and_then(|v| v.as_str());
            let did = dk.get("device_id").and_then(|v| v.as_str());
            if uid != Some(user_id.as_str()) || did != Some(device_id.as_str()) {
                return Err(Error::invalid_param(
                    "Device keys do not belong to the authenticated device.",
                ));
            }
            let valid = dk
                .get("algorithms")
                .is_some_and(serde_json::Value::is_array)
                && dk.get("keys").is_some_and(serde_json::Value::is_object);
            if !valid {
                return Err(Error::bad_json("Device keys are missing required fields."));
            }
            let existing = self.store.get_device_keys(user_id, device_id).await?;
            if existing.as_ref() != Some(&dk) {
                self.store.set_device_keys(user_id, device_id, &dk).await?;
                self.notify_device_list_change(user_id, Some(device_id))
                    .await?;
            }
        }
        if !one_time_keys.is_empty() {
            let rows = one_time_keys
                .iter()
                .map(|(id, key)| {
                    let (algorithm, key_id) = split_key_id(id)?;
                    Ok(OneTimeKeyRow {
                        algorithm,
                        key_id,
                        key: key.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            self.store
                .add_one_time_keys(user_id, device_id, &rows)
                .await?;
        }
        if !fallback_keys.is_empty() {
            let rows = fallback_keys
                .iter()
                .map(|(id, key)| {
                    let (algorithm, key_id) = split_key_id(id)?;
                    Ok(OneTimeKeyRow {
                        algorithm,
                        key_id,
                        key: key.clone(),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            self.store
                .set_fallback_keys(user_id, device_id, &rows)
                .await?;
        }
        self.one_time_key_counts(user_id, device_id).await
    }

    /// Unclaimed one-time keys per algorithm (always includes `signed_curve25519`).
    pub async fn one_time_key_counts(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<BTreeMap<String, u64>> {
        let mut counts: BTreeMap<String, u64> = BTreeMap::new();
        for (alg, n) in self.store.count_one_time_keys(user_id, device_id).await? {
            counts.insert(alg, u64::try_from(n).unwrap_or(0));
        }
        counts.entry("signed_curve25519".to_owned()).or_insert(0);
        Ok(counts)
    }

    /// Algorithms with an unused fallback key.
    pub async fn unused_fallback_key_algorithms(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
    ) -> Result<Vec<String>> {
        Ok(self
            .store
            .unused_fallback_algorithms(user_id, device_id)
            .await?)
    }

    /// `/keys/query` for local users (remote users go through federation later).
    pub async fn query_keys(
        &self,
        requester: &UserId,
        users: &BTreeMap<OwnedUserId, Vec<OwnedDeviceId>>,
    ) -> Result<KeyQueryResult> {
        let mut out = KeyQueryResult::default();
        for (user, devices) in users {
            if !self.is_local(user) {
                continue;
            }
            let mut all = self.store.all_device_keys(user).await?;
            if !devices.is_empty() {
                all.retain(|(d, _)| devices.contains(d));
            }
            let mut per_user = BTreeMap::new();
            for (device_id, mut keys) in all {
                // Merge uploaded cross-signing signatures and the device display name.
                for (signer, key_id, sig) in
                    self.store.key_signatures(user, device_id.as_str()).await?
                {
                    if signer != *user {
                        continue;
                    }
                    keys["signatures"][signer.as_str()][key_id] = json!(sig);
                }
                if let Some(d) = self.store.get_device(user, &device_id).await?
                    && let Some(name) = d.display_name
                {
                    keys["unsigned"]["device_display_name"] = json!(name);
                }
                per_user.insert(device_id, keys);
            }
            out.device_keys.insert(user.clone(), per_user);

            if let Some(mut k) = self.store.get_cross_signing_key(user, "master").await? {
                for (signer, key_id, sig) in self.store.key_signatures(user, "master").await? {
                    // Other users' signatures on a master key are only shown to the signer.
                    if signer == *user || signer == requester {
                        k["signatures"][signer.as_str()][key_id] = json!(sig);
                    }
                }
                out.master_keys.insert(user.clone(), k);
            }
            if let Some(k) = self
                .store
                .get_cross_signing_key(user, "self_signing")
                .await?
            {
                out.self_signing_keys.insert(user.clone(), k);
            }
            if user == requester
                && let Some(k) = self
                    .store
                    .get_cross_signing_key(user, "user_signing")
                    .await?
            {
                out.user_signing_keys.insert(user.clone(), k);
            }
        }
        Ok(out)
    }

    /// `/keys/claim`: `user -> device -> algorithm` to `user -> device -> key_id -> key`.
    pub async fn claim_keys(
        &self,
        wanted: &BTreeMap<OwnedUserId, BTreeMap<OwnedDeviceId, String>>,
    ) -> Result<BTreeMap<OwnedUserId, BTreeMap<OwnedDeviceId, BTreeMap<String, Value>>>> {
        let mut out: BTreeMap<OwnedUserId, BTreeMap<OwnedDeviceId, BTreeMap<String, Value>>> =
            BTreeMap::new();
        for (user, devices) in wanted {
            if !self.is_local(user) {
                continue;
            }
            for (device, algorithm) in devices {
                if let Some(k) = self
                    .store
                    .claim_one_time_key(user, device, algorithm)
                    .await?
                {
                    out.entry(user.clone())
                        .or_default()
                        .entry(device.clone())
                        .or_default()
                        .insert(format!("{}:{}", k.algorithm, k.key_id), k.key);
                }
            }
        }
        Ok(out)
    }

    /// `/keys/device_signing/upload`.
    pub async fn upload_cross_signing_keys(
        &self,
        user_id: &UserId,
        master: Option<Value>,
        self_signing: Option<Value>,
        user_signing: Option<Value>,
    ) -> Result<()> {
        let mut changed = false;
        for (ty, key) in [
            ("master", master),
            ("self_signing", self_signing),
            ("user_signing", user_signing),
        ] {
            if let Some(k) = key {
                if k.get("user_id").and_then(|v| v.as_str()) != Some(user_id.as_str()) {
                    return Err(Error::invalid_param(format!("{ty} key user_id mismatch")));
                }
                self.store.set_cross_signing_key(user_id, ty, &k).await?;
                changed = true;
            }
        }
        if changed {
            self.notify_device_list_change(user_id, None).await?;
        }
        Ok(())
    }

    /// `/keys/signatures/upload`: `user -> key_id -> signed object`.
    pub async fn upload_signatures(
        &self,
        signer: &UserId,
        signed: &BTreeMap<OwnedUserId, BTreeMap<String, Value>>,
    ) -> Result<BTreeMap<OwnedUserId, BTreeMap<String, Value>>> {
        let mut failures: BTreeMap<OwnedUserId, BTreeMap<String, Value>> = BTreeMap::new();
        let mut changed_users: Vec<OwnedUserId> = Vec::new();
        for (target_user, keys) in signed {
            for (target_key, obj) in keys {
                let Some(sigs) = obj
                    .get("signatures")
                    .and_then(|s| s.get(signer.as_str()))
                    .and_then(|s| s.as_object())
                else {
                    failures.entry(target_user.clone()).or_default().insert(
                        target_key.clone(),
                        json!({ "errcode": "M_INVALID_SIGNATURE", "error": "No signature from the uploading user." }),
                    );
                    continue;
                };
                // Normalise the target: a device id, or a cross-signing key by its ed25519 key id.
                let target = if self
                    .store
                    .get_device_keys(target_user, target_key.as_str().into())
                    .await?
                    .is_some()
                {
                    target_key.clone()
                } else {
                    match self
                        .cross_signing_key_type_for(target_user, target_key)
                        .await?
                    {
                        Some(t) => t,
                        None => {
                            failures.entry(target_user.clone()).or_default().insert(
                                target_key.clone(),
                                json!({ "errcode": "M_NOT_FOUND", "error": "Unknown key." }),
                            );
                            continue;
                        }
                    }
                };
                for (key_id, sig) in sigs {
                    if let Some(sig) = sig.as_str() {
                        self.store
                            .add_key_signature(target_user, &target, signer, key_id, sig)
                            .await?;
                    }
                }
                if !changed_users.contains(target_user) {
                    changed_users.push(target_user.clone());
                }
            }
        }
        for u in changed_users {
            self.notify_device_list_change(&u, None).await?;
        }
        Ok(failures)
    }

    /// Map a cross-signing public key id (`ed25519:...` or bare key) to its type.
    async fn cross_signing_key_type_for(
        &self,
        user: &UserId,
        key_id: &str,
    ) -> Result<Option<String>> {
        for ty in ["master", "self_signing", "user_signing"] {
            if let Some(k) = self.store.get_cross_signing_key(user, ty).await?
                && let Some(keys) = k.get("keys").and_then(|k| k.as_object())
                && keys
                    .iter()
                    .any(|(id, v)| id == key_id || v.as_str() == Some(key_id))
            {
                return Ok(Some(ty.to_owned()));
            }
        }
        Ok(None)
    }

    /// Record that a user's device list changed and wake their syncs.
    pub async fn notify_device_list_change(
        &self,
        user_id: &UserId,
        device_id: Option<&DeviceId>,
    ) -> Result<()> {
        let stream_id = self
            .store
            .add_device_list_change(user_id, device_id)
            .await?;
        self.notifier.notify(Notification::DeviceLists {
            user_id: user_id.to_owned(),
        });
        if self.is_local(user_id) {
            let servers = self.remote_servers_sharing_rooms_with(user_id).await?;
            match device_id {
                Some(device_id) => {
                    let keys = self.store.get_device_keys(user_id, device_id).await?;
                    let device = self.store.get_device(user_id, device_id).await?;
                    let deleted = keys.is_none() && device.is_none();
                    let mut content = json!({
                        "user_id": user_id, "device_id": device_id, "stream_id": stream_id, "prev_id": [],
                        "deleted": deleted,
                    });
                    if let Some(n) = device.and_then(|d| d.display_name) {
                        content["device_display_name"] = json!(n);
                    }
                    if let Some(k) = keys {
                        content["keys"] = k;
                    }
                    self.send_edu_to(
                        servers,
                        json!({ "edu_type": "m.device_list_update", "content": content }),
                    );
                }
                None => {
                    let mut content = json!({ "user_id": user_id });
                    if let Some(k) = self.store.get_cross_signing_key(user_id, "master").await? {
                        content["master_key"] = k;
                    }
                    if let Some(k) = self
                        .store
                        .get_cross_signing_key(user_id, "self_signing")
                        .await?
                    {
                        content["self_signing_key"] = k;
                    }
                    self.send_edu_to(
                        servers,
                        json!({ "edu_type": "m.signing_key_update", "content": content }),
                    );
                }
            }
        }
        Ok(())
    }

    /// Users whose device lists changed in `(since, to]` and share a room with `user`, split
    /// into `changed` and `left` per the sync spec (left = no longer share any room).
    pub async fn device_list_changes_for(
        &self,
        user: &UserId,
        since: i64,
        to: i64,
    ) -> Result<(Vec<OwnedUserId>, Vec<OwnedUserId>)> {
        let changed = self.store.device_list_changes_since(since, to).await?;
        if changed.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let my_rooms = self.store.rooms_for_user(user, "join").await?;
        let mut shared = Vec::new();
        for u in changed {
            if u == user {
                shared.push(u);
                continue;
            }
            let their_rooms = self.store.rooms_for_user(&u, "join").await?;
            if their_rooms.iter().any(|r| my_rooms.contains(r)) {
                shared.push(u);
            }
        }
        Ok((shared, Vec::new()))
    }

    /// `/sendToDevice`.
    pub async fn send_to_device(
        &self,
        sender: &UserId,
        event_type: &str,
        messages: &BTreeMap<OwnedUserId, BTreeMap<String, Value>>,
    ) -> Result<()> {
        // Remote recipients get one m.direct_to_device EDU per server.
        let mut by_server: BTreeMap<OwnedServerName, serde_json::Map<String, Value>> =
            BTreeMap::new();
        for (user, per_device) in messages {
            if !self.is_local(user) {
                let entry = by_server.entry(user.server_name().to_owned()).or_default();
                let devices: serde_json::Map<String, Value> = per_device
                    .iter()
                    .map(|(d, c)| (d.clone(), c.clone()))
                    .collect();
                entry.insert(user.to_string(), Value::Object(devices));
                continue;
            }
            for (device, content) in per_device {
                let targets: Vec<OwnedDeviceId> = if device == "*" {
                    self.store
                        .list_devices(user)
                        .await?
                        .into_iter()
                        .map(|d| d.device_id)
                        .collect()
                } else {
                    vec![OwnedDeviceId::from(device.as_str())]
                };
                for d in targets {
                    self.store
                        .add_to_device_message(user, &d, sender, event_type, content)
                        .await?;
                }
            }
            self.notifier.notify(Notification::User {
                user_id: user.clone(),
            });
        }
        for (server, msgs) in by_server {
            self.send_edu_to(
                vec![server],
                json!({
                    "edu_type": "m.direct_to_device",
                    "content": {
                        "sender": sender, "type": event_type,
                        "message_id": asaroton_auth::token::generate_session_id(),
                        "messages": msgs,
                    },
                }),
            );
        }
        Ok(())
    }
}

impl Services {
    /// Send `m.device_list_update` for every device of `user` to `servers` (used when a
    /// membership change makes them share a room for the first time).
    pub async fn send_device_lists_to(
        &self,
        user: &UserId,
        servers: Vec<OwnedServerName>,
    ) -> Result<()> {
        if servers.is_empty() || !self.is_local(user) {
            return Ok(());
        }
        let stream_id = self.store.max_device_list_stream_id().await?;
        let keys: std::collections::HashMap<_, _> = self
            .store
            .all_device_keys(user)
            .await?
            .into_iter()
            .collect();
        for device in self.store.list_devices(user).await? {
            let mut content = json!({
                "user_id": user, "device_id": device.device_id, "stream_id": stream_id, "prev_id": [],
            });
            if let Some(k) = keys.get(&device.device_id) {
                content["keys"] = k.clone();
            }
            if let Some(n) = device.display_name {
                content["device_display_name"] = json!(n);
            }
            self.send_edu_to(
                servers.clone(),
                json!({ "edu_type": "m.device_list_update", "content": content }),
            );
        }
        Ok(())
    }
}
