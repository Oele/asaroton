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

/// Counts already computed for a (room, user), so a sync only evaluates the events that
/// arrived since the previous one.
#[derive(Clone, Debug)]
pub struct UnreadCacheEntry {
    floor: StreamOrdering,
    unthreaded: Option<StreamOrdering>,
    main: Option<StreamOrdering>,
    threads: BTreeMap<String, StreamOrdering>,
    rules_stream: i64,
    counted_upto: StreamOrdering,
    counts: UnreadCounts,
}

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
        for r in self.store.user_receipts(room_id, user).await? {
            if !matches!(r.receipt_type.as_str(), "m.read" | "m.read.private") {
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
        if floor >= upto {
            return Ok(UnreadCounts::default());
        }
        // Resume from the cached counts when nothing they depend on has changed.
        let rules_stream = self.push_rules_stream_id(user).await?;
        let key = (room_id.to_owned(), user.to_owned());
        let cached = self
            .unread_cache
            .get(&key)
            .filter(|c| {
                c.floor == floor
                    && c.unthreaded == unthreaded
                    && c.main == main
                    && c.threads == threads
                    && c.rules_stream == rules_stream
                    && c.counted_upto <= upto
            })
            .map(|c| c.clone());
        let (start, mut out) = match cached {
            Some(c) => (c.counted_upto, c.counts),
            None => (floor, UnreadCounts::default()),
        };
        let events = if start >= upto {
            Vec::new()
        } else {
            self.store
                .room_events_in_range(room_id, start, Some(upto), Direction::Forwards, MAX_SCANNED)
                .await?
        };
        let remember = |counts: &UnreadCounts| {
            if self.unread_cache.len() > 200_000 {
                self.unread_cache.clear();
            }
            self.unread_cache.insert(
                key.clone(),
                UnreadCacheEntry {
                    floor,
                    unthreaded,
                    main,
                    threads: threads.clone(),
                    rules_stream,
                    counted_upto: upto,
                    counts: counts.clone(),
                },
            );
        };
        if events.is_empty() {
            remember(&out);
            return Ok(out);
        }

        let (ruleset, push_ctx) = self.push_rule_context(room_id, user).await?;

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
        remember(&out);
        Ok(out)
    }
}

impl Services {
    /// The user's push ruleset and the room context push conditions are evaluated against.
    pub async fn push_rule_context(
        &self,
        room_id: &RoomId,
        user: &UserId,
    ) -> Result<(ruma::push::Ruleset, PushConditionRoomCtx)> {
        // Only the create event, the power levels and the user's own membership matter, so
        // the full room state is not loaded.
        let info = self
            .store
            .get_room(room_id)
            .await?
            .ok_or_else(|| crate::Error::not_found("Room not found."))?;
        let rules = tessera_core::room_version::rules(&info.room_version)?;
        let keys = [
            (StateEventType::RoomCreate, String::new()),
            (StateEventType::RoomPowerLevels, String::new()),
            (StateEventType::RoomMember, user.to_string()),
        ];
        let mut state: crate::rooms::power::StatePdus = std::collections::HashMap::new();
        for se in self.store.current_state_pdus_for(room_id, &keys).await? {
            if let Some((ty, key)) = se.pdu.state_tuple() {
                state.insert((ty, key.to_owned()), se.pdu.clone());
            }
        }
        let joined = self.store.joined_member_count(room_id).await?;
        let display_name = state
            .get(&(StateEventType::RoomMember, user.to_string()))
            .and_then(|p| serde_json::from_str::<Value>(p.content().get()).ok())
            .and_then(|c| {
                c.get("displayname")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| user.localpart().to_owned());
        let (store, subscriber, subscriber_room) =
            (self.store.clone(), user.to_owned(), room_id.to_owned());
        let mut push_ctx = PushConditionRoomCtx::new(
            room_id.to_owned(),
            ruma::UInt::try_from(joined.max(0)).unwrap_or(ruma::UInt::MAX),
            user.to_owned(),
            display_name,
        )
        .with_has_thread_subscription_fn(move |root: &ruma::EventId| {
            let (store, user, room, root) = (
                store.clone(),
                subscriber.clone(),
                subscriber_room.clone(),
                root.to_owned(),
            );
            Box::pin(async move {
                store
                    .get_thread_subscription(&user, &room, &root)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|s| s.subscribed)
            })
        });
        if let Some(pl) = state.get(&(StateEventType::RoomPowerLevels, String::new()))
            && let Ok(content) = serde_json::from_str::<
                ruma::events::room::power_levels::RoomPowerLevelsEventContent,
            >(pl.content().get())
        {
            push_ctx.power_levels = Some(PushConditionPowerLevelsCtx::new(
                content.users,
                content.users_default,
                content.notifications,
                RoomPowerLevelsRules::new(
                    &rules.authorization,
                    crate::rooms::power::creators(&state, &rules),
                ),
            ));
        }
        let ruleset = self.push_rules(user).await?;
        Ok((ruleset, push_ctx))
    }

    /// Evaluate the user's push rules for one event: `None` when it does not notify,
    /// otherwise whether it is a highlight.
    pub async fn push_actions_for(
        &self,
        ruleset: &ruma::push::Ruleset,
        push_ctx: &PushConditionRoomCtx,
        ev: &tessera_store::StoredEvent,
    ) -> Option<bool> {
        let raw = serde_json::value::to_raw_value(&ev.pdu.to_client_json()).ok()?;
        let raw: Raw<Value> = Raw::from_json(raw);
        let actions = ruleset.get_actions(&raw, push_ctx).await;
        if !actions.iter().any(|a| matches!(a, Action::Notify)) {
            return None;
        }
        Some(actions.iter().any(|a| {
            matches!(
                a,
                Action::SetTweak(Tweak::Highlight(HighlightTweakValue::Yes))
            )
        }))
    }
}
