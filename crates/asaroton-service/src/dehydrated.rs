//! Dehydrated devices (MSC3814).
//!
//! Room keys go to devices. A user whose devices are all signed out when a message is sent
//! has no device the key could go to, and cannot read that message after signing in again.
//! A dehydrated device is a device the client leaves with the server for that time: its
//! keys are published like any device's, so senders share room keys with it, and what they
//! send waits here. The client that signs in next fetches the device (its private part is
//! encrypted with a key only the user has), reads the waiting to-device messages through it
//! and takes the room keys over.
//!
//! To the rest of the server it is a device like any other, without an access token. A
//! user has at most one.

use std::collections::BTreeMap;

use ruma::{DeviceId, OwnedDeviceId, UserId};
use serde_json::{Value, json};

use crate::{Error, Result, Services};

/// To-device messages in one answer of [`Services::dehydrated_device_events`].
const EVENTS_PER_ANSWER: usize = 100;

fn none() -> Error {
    Error::not_found("No dehydrated device available.")
}

impl Services {
    /// Stores a dehydrated device in the place of the one the user had.
    #[allow(clippy::too_many_arguments)]
    pub async fn put_dehydrated_device(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        display_name: Option<&str>,
        device_data: &Value,
        device_keys: Value,
        one_time_keys: &BTreeMap<String, Value>,
        fallback_keys: &BTreeMap<String, Value>,
    ) -> Result<()> {
        // One at a time for a user: two that replace the device at once would each leave
        // theirs behind.
        let _turn = self.txn_turn(user_id, None, "dehydrated_device").await;
        let old = self.store.dehydrated_device(user_id).await?;
        let replaces_itself = old.as_ref().is_some_and(|(id, _)| id == device_id);
        if !replaces_itself && self.store.get_device(user_id, device_id).await?.is_some() {
            // Otherwise a client could turn a device that someone is signed in on into one
            // whose to-device messages anybody with the account can read out.
            return Err(Error::invalid_param(
                "This device ID belongs to a device that is in use.",
            ));
        }
        if let Some((old, _)) = old
            && !replaces_itself
        {
            self.store
                .delete_to_device_messages_up_to(user_id, &old, i64::MAX)
                .await?;
            self.delete_device_fully(user_id, &old).await?;
        }
        self.store
            .create_device(user_id, device_id, display_name)
            .await?;
        self.store
            .set_dehydrated_device(user_id, device_id, device_data)
            .await?;
        // Publishes the device: from here on senders share room keys with it.
        self.upload_keys(
            user_id,
            device_id,
            Some(device_keys),
            one_time_keys,
            fallback_keys,
        )
        .await?;
        Ok(())
    }

    /// The user's dehydrated device and its data.
    pub async fn get_dehydrated_device(&self, user_id: &UserId) -> Result<(OwnedDeviceId, Value)> {
        self.store
            .dehydrated_device(user_id)
            .await?
            .ok_or_else(none)
    }

    /// Removes the user's dehydrated device, with its keys and what was waiting for it.
    pub async fn delete_dehydrated_device(&self, user_id: &UserId) -> Result<OwnedDeviceId> {
        let _turn = self.txn_turn(user_id, None, "dehydrated_device").await;
        let (device_id, _) = self
            .store
            .dehydrated_device(user_id)
            .await?
            .ok_or_else(none)?;
        self.store
            .delete_to_device_messages_up_to(user_id, &device_id, i64::MAX)
            .await?;
        self.delete_device_fully(user_id, &device_id).await?;
        Ok(device_id)
    }

    /// A page of the to-device messages that wait for the dehydrated device, and where the
    /// next page starts. Asking for a page deletes the ones before it: the client has them.
    /// An empty page is the end.
    pub async fn dehydrated_device_events(
        &self,
        user_id: &UserId,
        device_id: &DeviceId,
        next_batch: Option<&str>,
    ) -> Result<(Vec<Value>, String)> {
        let dehydrated = self.store.dehydrated_device(user_id).await?;
        if dehydrated.is_none_or(|(id, _)| id != device_id) {
            return Err(Error::forbidden(
                "You can only fetch messages for your dehydrated device.",
            ));
        }
        let since = match next_batch {
            Some(token) => token
                .parse::<i64>()
                .map_err(|_| Error::invalid_param("Unknown next_batch."))?,
            None => 0,
        };
        if since > 0 {
            self.store
                .delete_to_device_messages_up_to(user_id, device_id, since)
                .await?;
        }
        let up_to = self.store.stream_positions().await?.to_device;
        let messages = self
            .store
            .to_device_messages(user_id, device_id, since, up_to, EVENTS_PER_ANSWER)
            .await?;
        let next = messages.last().map_or(since, |m| m.stream_id);
        let events = messages
            .into_iter()
            .map(|m| json!({ "type": m.event_type, "sender": m.sender, "content": m.content }))
            .collect();
        Ok((events, next.to_string()))
    }
}
