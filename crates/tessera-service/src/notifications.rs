//! Unread notification counts: push rules evaluated against the events a user has not read
//! yet, overall and per thread.

use std::collections::BTreeMap;

use ruma::{
    OwnedEventId, RoomId, UserId,
    events::StateEventType,
    push::{Action, HighlightTweakValue, PushConditionPowerLevelsCtx, PushConditionRoomCtx, Tweak},
    room_version_rules::RoomPowerLevelsRules,
    serde::Raw,
};
use serde_json::Value;
use tessera_store::{Direction, StreamOrdering};

use crate::{Result, Services};

/// Notification and highlight counts.
#[derive(Debug, Clone, Default)]
pub struct Counts {
    pub notification_count: u64,
    pub highlight_count: u64,
}

/// Unread counts for a room: the main timeline, every thread with unread events, and the
/// total over both.
#[derive(Debug, Clone, Default)]
pub struct UnreadCounts {
    pub total: Counts,
    pub main: Counts,
    pub threads: BTreeMap<OwnedEventId, Counts>,
}

/// How many events after the user's read position are considered.
const MAX_SCANNED: usize = 2000;

impl Services {
    /// Unread counts for `user` in `room_id`, considering events up to `upto` (inclusive).
    pub async fn unread_counts(
        &self,
        room_id: &RoomId,
        user: &UserId,
        upto: StreamOrdering,
    ) -> Result<UnreadCounts> {
        // Read positions: unthreaded receipts cover everything, "main" the main timeline,
        // and a thread receipt only that thread.
        let mut unthreaded: Option<StreamOrdering> = None;
        let mut main: Option<StreamOrdering> = None;
        let mut threads: BTreeMap<String, StreamOrdering> = BTreeMap::new();
        for r in self.store.receipts_for_room(room_id, 0).await? {
            if r.user_id != user || !matches!(r.receipt_type.as_str(), "m.read" | "m.read.private")
            {
                continue;
            }
            let Some(ev) = self.store.get_event(&r.event_id).await? else {
                continue;
            };
            let pos = ev.stream_ordering;
            let bump = |slot: &mut Option<StreamOrdering>| {
                if slot.is_none_or(|s| s < pos) {
                    *slot = Some(pos);
                }
            };
            match r.thread_id.as_deref() {
                None => bump(&mut unthreaded),
                Some("main") => bump(&mut main),
                Some(t) => {
                    let e = threads.entry(t.to_owned()).or_insert(pos);
                    if *e < pos {
                        *e = pos;
                    }
                }
            }
        }
        let mut floor = [unthreaded, main]
            .into_iter()
            .flatten()
            .chain(threads.values().copied())
            .min();
        if floor.is_none() {
            floor = self
                .store
                .get_membership(room_id, user)
                .await?
                .map(|m| m.stream_ordering.saturating_sub(1));
        }
        let floor = floor.unwrap_or(0);
        let mut out = UnreadCounts::default();
        if floor >= upto {
            return Ok(out);
        }
        let events = self
            .store
            .room_events_in_range(room_id, floor, Some(upto), Direction::Forwards, MAX_SCANNED)
            .await?;
        if events.is_empty() {
            return Ok(out);
        }

        // Push rule context.
        let ctx = self.room_context(room_id).await?;
        let joined = self.store.members(room_id, Some("join")).await?;
        let display_name = ctx
            .get(StateEventType::RoomMember, user.as_str())
            .and_then(|p| serde_json::from_str::<Value>(p.content().get()).ok())
            .and_then(|c| {
                c.get("displayname")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| user.localpart().to_owned());
        let mut push_ctx = PushConditionRoomCtx::new(
            room_id.to_owned(),
            ruma::UInt::try_from(joined.len()).unwrap_or(ruma::UInt::MAX),
            user.to_owned(),
            display_name,
        );
        if let Some(pl) = ctx.get(StateEventType::RoomPowerLevels, "")
            && let Ok(content) = serde_json::from_str::<
                ruma::events::room::power_levels::RoomPowerLevelsEventContent,
            >(pl.content().get())
        {
            push_ctx.power_levels = Some(PushConditionPowerLevelsCtx::new(
                content.users,
                content.users_default,
                content.notifications,
                RoomPowerLevelsRules::new(
                    &ctx.rules.authorization,
                    crate::rooms::power::creators(&ctx.state, &ctx.rules),
                ),
            ));
        }
        let ruleset = self.push_rules(user).await?;

        for ev in events {
            if ev.pdu.sender() == user {
                continue;
            }
            let content: Value = serde_json::from_str(ev.pdu.content().get()).unwrap_or_default();
            let thread_root = content
                .get("m.relates_to")
                .filter(|r| r.get("rel_type").and_then(Value::as_str) == Some("m.thread"))
                .and_then(|r| r.get("event_id").and_then(Value::as_str))
                .and_then(|id| OwnedEventId::try_from(id).ok());
            let pos = ev.stream_ordering;
            let read = unthreaded.is_some_and(|u| pos <= u)
                || match &thread_root {
                    None => main.is_some_and(|m| pos <= m),
                    Some(root) => threads.get(root.as_str()).is_some_and(|t| pos <= *t),
                };
            if read {
                continue;
            }
            let Ok(raw) = serde_json::value::to_raw_value(&ev.pdu.to_client_json()) else {
                continue;
            };
            let raw: Raw<Value> = Raw::from_json(raw);
            let actions = ruleset.get_actions(&raw, &push_ctx).await;
            let notify = actions.iter().any(|a| matches!(a, Action::Notify));
            if !notify {
                continue;
            }
            let highlight = actions.iter().any(|a| {
                matches!(
                    a,
                    Action::SetTweak(Tweak::Highlight(HighlightTweakValue::Yes))
                )
            });
            let add = |c: &mut Counts| {
                c.notification_count += 1;
                if highlight {
                    c.highlight_count += 1;
                }
            };
            add(&mut out.total);
            match thread_root {
                None => add(&mut out.main),
                Some(root) => add(out.threads.entry(root).or_default()),
            }
        }
        Ok(out)
    }
}
