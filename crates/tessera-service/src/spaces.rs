//! Spaces: room summaries and the `/hierarchy` walk, locally and over federation.

use std::collections::{BTreeSet, VecDeque};

use ruma::{OwnedRoomId, OwnedServerName, RoomId, ServerName, UserId, events::StateEventType};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{Error, Result, Services, rooms::pipeline::RoomContext};

/// Default and maximum page size for `/hierarchy`.
const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;

/// A pending room in the hierarchy walk.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Pending {
    room: OwnedRoomId,
    depth: usize,
    via: Vec<OwnedServerName>,
}

/// Pagination state, carried in the `next_batch` token.
#[derive(Debug, Serialize, Deserialize)]
struct Cursor {
    queue: Vec<Pending>,
    seen: Vec<OwnedRoomId>,
    suggested_only: bool,
    max_depth: Option<usize>,
}

fn state_str(ctx: &RoomContext, ty: StateEventType, field: &str) -> Option<String> {
    ctx.get(ty, "")
        .and_then(|p| serde_json::from_str::<Value>(p.content().get()).ok())
        .and_then(|v| v.get(field).and_then(Value::as_str).map(str::to_owned))
}

fn join_rule_summary(ctx: &RoomContext) -> (String, Vec<OwnedRoomId>) {
    let content = ctx
        .get(StateEventType::RoomJoinRules, "")
        .and_then(|p| serde_json::from_str::<Value>(p.content().get()).ok());
    let rule = content
        .as_ref()
        .and_then(|v| {
            v.get("join_rule")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "invite".to_owned());
    let allowed = content
        .as_ref()
        .and_then(|v| v.get("allow").and_then(Value::as_array))
        .map(|rules| {
            rules
                .iter()
                .filter(|r| r.get("type").and_then(Value::as_str) == Some("m.room_membership"))
                .filter_map(|r| r.get("room_id").and_then(Value::as_str))
                .filter_map(|r| OwnedRoomId::try_from(r).ok())
                .collect()
        })
        .unwrap_or_default();
    (rule, allowed)
}

impl Services {
    /// The spec's room summary for a local room (name, topic, counts, join rule, ...).
    pub async fn room_summary_json(&self, room_id: &RoomId) -> Result<Value> {
        let ctx = self.room_context(room_id).await?;
        let joined = self.store.joined_member_count(room_id).await?;
        let (join_rule, allowed) = join_rule_summary(&ctx);
        let mut v = json!({
            "room_id": room_id,
            "num_joined_members": joined,
            "join_rule": join_rule,
            "world_readable": state_str(&ctx, StateEventType::RoomHistoryVisibility, "history_visibility").as_deref() == Some("world_readable"),
            "guest_can_join": state_str(&ctx, StateEventType::RoomGuestAccess, "guest_access").as_deref() == Some("can_join"),
        });
        let opt = [
            ("name", StateEventType::RoomName, "name"),
            ("topic", StateEventType::RoomTopic, "topic"),
            (
                "canonical_alias",
                StateEventType::RoomCanonicalAlias,
                "alias",
            ),
            ("avatar_url", StateEventType::RoomAvatar, "url"),
            ("room_type", StateEventType::RoomCreate, "type"),
            ("encryption", StateEventType::RoomEncryption, "algorithm"),
            ("room_version", StateEventType::RoomCreate, "room_version"),
        ];
        for (key, ty, field) in opt {
            if let Some(s) = state_str(&ctx, ty, field) {
                v[key] = json!(s);
            }
        }
        if !allowed.is_empty() {
            v["allowed_room_ids"] = json!(allowed);
        }
        Ok(v)
    }

    /// `m.space.child` events of a room with a non-empty `via`, as stripped events, in the
    /// spec's order (`order` key, then age).
    pub async fn space_children(&self, room_id: &RoomId) -> Result<Vec<Value>> {
        let mut children: Vec<(String, u64, Value)> = Vec::new();
        for ev in self.store.current_state_pdus(room_id).await? {
            if ev.pdu.event_type() != &ruma::events::TimelineEventType::SpaceChild {
                continue;
            }
            let content: Value = serde_json::from_str(ev.pdu.content().get()).unwrap_or_default();
            let via_ok = content
                .get("via")
                .and_then(Value::as_array)
                .is_some_and(|v| !v.is_empty());
            if !via_ok {
                continue;
            }
            let order = content
                .get("order")
                .and_then(Value::as_str)
                .filter(|o| o.len() <= 50 && o.chars().all(|c| ('\x20'..='\x7e').contains(&c)))
                .map_or_else(|| "\u{7f}".to_owned(), str::to_owned);
            let ts = u64::from(ev.pdu.origin_server_ts().0);
            children.push((
                order,
                ts,
                json!({
                    "type": "m.space.child",
                    "state_key": ev.pdu.state_key().unwrap_or(""),
                    "content": content,
                    "sender": ev.pdu.sender(),
                    "origin_server_ts": ts,
                }),
            ));
        }
        children.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        Ok(children.into_iter().map(|c| c.2).collect())
    }

    /// Whether a local user may see a room in a hierarchy: a member, or the room is public,
    /// knockable, world readable, or restricted to a room the user is in.
    pub async fn room_accessible_to_user(&self, room_id: &RoomId, user: &UserId) -> Result<bool> {
        use ruma::events::room::member::MembershipState;
        if let Some(m) = self.store.get_membership(room_id, user).await?
            && matches!(
                m.membership,
                MembershipState::Join | MembershipState::Invite
            )
        {
            return Ok(true);
        }
        let ctx = self.room_context(room_id).await?;
        let (rule, allowed) = join_rule_summary(&ctx);
        if matches!(rule.as_str(), "public" | "knock" | "knock_restricted")
            || state_str(
                &ctx,
                StateEventType::RoomHistoryVisibility,
                "history_visibility",
            )
            .as_deref()
                == Some("world_readable")
        {
            return Ok(true);
        }
        if rule == "restricted" {
            for r in allowed {
                if self.is_joined(&r, user).await? {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Whether a remote server may see a room: public, knockable, world readable, or a member
    /// of that server is in it.
    pub async fn room_accessible_to_server(
        &self,
        room_id: &RoomId,
        server: &ServerName,
    ) -> Result<bool> {
        let ctx = self.room_context(room_id).await?;
        let (rule, allowed) = join_rule_summary(&ctx);
        if matches!(rule.as_str(), "public" | "knock" | "knock_restricted")
            || state_str(
                &ctx,
                StateEventType::RoomHistoryVisibility,
                "history_visibility",
            )
            .as_deref()
                == Some("world_readable")
        {
            return Ok(true);
        }
        // Restricted rooms: a server with a member in one of the allowed rooms may see it.
        if rule == "restricted" {
            for r in &allowed {
                if self.store.room_exists(r).await?
                    && self
                        .store
                        .members(r, Some("join"))
                        .await?
                        .iter()
                        .any(|m| m.user_id.server_name() == server)
                {
                    return Ok(true);
                }
            }
        }
        Ok(self.store.members(room_id, None).await?.iter().any(|m| {
            m.user_id.server_name() == server
                && matches!(
                    m.membership,
                    ruma::events::room::member::MembershipState::Join
                        | ruma::events::room::member::MembershipState::Invite
                )
        }))
    }

    /// Whether we hold the room's state (we are, or were, in it).
    async fn room_is_local(&self, room_id: &RoomId) -> Result<bool> {
        Ok(self.store.room_exists(room_id).await?
            && !self.store.current_state(room_id).await?.is_empty())
    }

    /// `GET /rooms/{roomId}/hierarchy`: a breadth-first page of the space tree.
    pub async fn hierarchy(
        &self,
        user: &UserId,
        root: &OwnedRoomId,
        suggested_only: bool,
        max_depth: Option<usize>,
        limit: Option<usize>,
        from: Option<&str>,
    ) -> Result<(Vec<Value>, Option<String>)> {
        let limit = limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
        let (mut queue, mut seen, suggested_only, max_depth) = match from {
            Some(token) => {
                let bytes = hex::decode(token)
                    .map_err(|_| Error::invalid_param("Invalid pagination token."))?;
                let c: Cursor = serde_json::from_slice(&bytes)
                    .map_err(|_| Error::invalid_param("Invalid pagination token."))?;
                (
                    VecDeque::from(c.queue),
                    c.seen.into_iter().collect::<BTreeSet<_>>(),
                    c.suggested_only,
                    c.max_depth,
                )
            }
            None => {
                let mut q = VecDeque::new();
                q.push_back(Pending {
                    room: root.clone(),
                    depth: 0,
                    via: Vec::new(),
                });
                (q, BTreeSet::new(), suggested_only, max_depth)
            }
        };
        if from.is_none() {
            // The root must be visible to the caller at all.
            if self.room_is_local(root).await? {
                if !self.room_accessible_to_user(root, user).await? {
                    return Err(Error::forbidden("You are not allowed to view this room."));
                }
            } else if let Some(server) = root.server_name() {
                queue[0].via.push(server.to_owned());
            } else {
                // A room we know nothing about and whose ID names no server: nowhere to ask.
                return Err(Error::not_found("Unknown room."));
            }
        }

        let mut rooms: Vec<Value> = Vec::new();
        // Summaries of remote rooms learned from a parent's federation response.
        let mut remote_cache: std::collections::HashMap<OwnedRoomId, Value> =
            std::collections::HashMap::new();
        while let Some(item) = queue.pop_front() {
            if rooms.len() >= limit {
                queue.push_front(item);
                break;
            }
            if !seen.insert(item.room.clone()) {
                continue;
            }
            let (summary, children): (Value, Vec<Value>) = if self.room_is_local(&item.room).await?
            {
                if !self.room_accessible_to_user(&item.room, user).await? {
                    continue;
                }
                let s = self.room_summary_json(&item.room).await?;
                // Only spaces have children worth listing or following.
                let c = if s.get("room_type").and_then(Value::as_str) == Some("m.space") {
                    self.space_children(&item.room).await?
                } else {
                    Vec::new()
                };
                (s, c)
            } else {
                let is_space = remote_cache
                    .get(&item.room)
                    .and_then(|s| s.get("room_type").and_then(Value::as_str))
                    == Some("m.space");
                let cached = remote_cache.get(&item.room).cloned();
                if let Some(s) = cached.filter(|_| !is_space) {
                    (s, Vec::new())
                } else {
                    let mut via = item.via.clone();
                    if let Some(server) = item.room.server_name()
                        && !via.iter().any(|s| *s == server)
                    {
                        via.push(server.to_owned());
                    }
                    let Ok(resp) = self
                        .federation()
                        .remote_hierarchy(&via, &item.room, suggested_only)
                        .await
                    else {
                        continue;
                    };
                    let Some(room) = resp.get("room").cloned() else {
                        continue;
                    };
                    for child in resp
                        .get("children")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        if let Some(id) = child
                            .get("room_id")
                            .and_then(Value::as_str)
                            .and_then(|r| OwnedRoomId::try_from(r).ok())
                        {
                            remote_cache.insert(id, child.clone());
                        }
                    }
                    let children = room
                        .get("children_state")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_default();
                    let mut summary = room;
                    if let Some(o) = summary.as_object_mut() {
                        o.remove("children_state");
                    }
                    (summary, children)
                }
            };
            // With `suggested_only`, only suggested links are reported and followed.
            let children: Vec<Value> = if suggested_only {
                children
                    .into_iter()
                    .filter(|c| {
                        c.get("content")
                            .and_then(|c| c.get("suggested"))
                            .and_then(Value::as_bool)
                            == Some(true)
                    })
                    .collect()
            } else {
                children
            };
            let descend = max_depth.is_none_or(|d| item.depth < d);
            if descend {
                // Depth first: the children go to the front of the queue, in order.
                let mut pending = Vec::new();
                for child in &children {
                    let content = child.get("content").cloned().unwrap_or_default();
                    let Some(child_id) = child
                        .get("state_key")
                        .and_then(Value::as_str)
                        .and_then(|k| OwnedRoomId::try_from(k).ok())
                    else {
                        continue;
                    };
                    let via: Vec<OwnedServerName> = content
                        .get("via")
                        .and_then(Value::as_array)
                        .map(|v| {
                            v.iter()
                                .filter_map(Value::as_str)
                                .filter_map(|s| OwnedServerName::try_from(s).ok())
                                .collect()
                        })
                        .unwrap_or_default();
                    pending.push(Pending {
                        room: child_id,
                        depth: item.depth + 1,
                        via,
                    });
                }
                for p in pending.into_iter().rev() {
                    queue.push_front(p);
                }
            }
            let mut entry = summary;
            entry["children_state"] = Value::Array(children);
            rooms.push(entry);
        }
        let next = if queue.is_empty() {
            None
        } else {
            let cursor = Cursor {
                queue: queue.into_iter().collect(),
                seen: seen.into_iter().collect(),
                suggested_only,
                max_depth,
            };
            Some(hex::encode(serde_json::to_vec(&cursor)?))
        };
        Ok((rooms, next))
    }

    /// The federation `/hierarchy/{roomId}` response: this room, the local children the
    /// origin may see, and the ones it may not.
    pub async fn federation_hierarchy(
        &self,
        origin: &ServerName,
        room_id: &RoomId,
        suggested_only: bool,
    ) -> Result<Value> {
        if !self.room_is_local(room_id).await? {
            return Err(Error::not_found("Unknown room."));
        }
        if !self.room_accessible_to_server(room_id, origin).await? {
            return Err(Error::not_found("Room is not accessible."));
        }
        let mut room = self.room_summary_json(room_id).await?;
        let children_state = self.space_children(room_id).await?;
        let mut children = Vec::new();
        let mut inaccessible = Vec::new();
        for child in &children_state {
            let content = child.get("content").cloned().unwrap_or_default();
            if suggested_only && content.get("suggested").and_then(Value::as_bool) != Some(true) {
                continue;
            }
            let Some(child_id) = child
                .get("state_key")
                .and_then(Value::as_str)
                .and_then(|k| OwnedRoomId::try_from(k).ok())
            else {
                continue;
            };
            if !self.room_is_local(&child_id).await? {
                continue;
            }
            if self.room_accessible_to_server(&child_id, origin).await? {
                let mut s = self.room_summary_json(&child_id).await?;
                s["children_state"] = Value::Array(self.space_children(&child_id).await?);
                children.push(s);
            } else {
                inaccessible.push(child_id);
            }
        }
        room["children_state"] = Value::Array(children_state);
        Ok(json!({
            "room": room,
            "children": children,
            "inaccessible_children": inaccessible,
        }))
    }
}

/// Matrix server ACL glob: `*` matches any run of characters, `?` one.
fn acl_glob(pattern: &str, server: &str) -> bool {
    fn rec(p: &[char], v: &[char]) -> bool {
        match (p.first(), v.first()) {
            (None, None) => true,
            (Some('*'), _) => rec(&p[1..], v) || (!v.is_empty() && rec(p, &v[1..])),
            (Some('?'), Some(_)) => rec(&p[1..], &v[1..]),
            (Some(a), Some(b)) if a.eq_ignore_ascii_case(b) => rec(&p[1..], &v[1..]),
            _ => false,
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = server.chars().collect();
    rec(&p, &v)
}

impl Services {
    /// Whether the room's `m.room.server_acl` lets `server` participate.
    pub async fn server_acl_allows(&self, room_id: &RoomId, server: &ServerName) -> Result<bool> {
        let Some(acl) = self
            .state_content(room_id, &StateEventType::RoomServerAcl, "")
            .await?
        else {
            return Ok(true);
        };
        Ok(acl_permits(&acl, server))
    }
}

/// Evaluate an ACL content object for a server name.
pub fn acl_permits(acl: &Value, server: &ServerName) -> bool {
    let host = server.host();
    let is_ip_literal = host.parse::<std::net::IpAddr>().is_ok()
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok();
    if is_ip_literal
        && !acl
            .get("allow_ip_literals")
            .and_then(Value::as_bool)
            .unwrap_or(true)
    {
        return false;
    }
    let list = |key: &str| -> Vec<String> {
        acl.get(key)
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    };
    if list("deny").iter().any(|p| acl_glob(p, host)) {
        return false;
    }
    list("allow").iter().any(|p| acl_glob(p, host))
}

#[cfg(test)]
mod acl_tests {
    use super::*;

    #[test]
    fn globs_and_lists() {
        let acl = json!({ "allow": ["*"], "deny": ["evil.example", "*.bad.example"], "allow_ip_literals": false });
        let ok = |s: &str| acl_permits(&acl, <&ServerName>::try_from(s).unwrap());
        assert!(ok("matrix.org"));
        assert!(!ok("evil.example"));
        assert!(!ok("sub.bad.example"));
        assert!(!ok("1.2.3.4"));
        assert!(!ok("1.2.3.4:8448"));
        let strict = json!({ "allow": ["good.example"] });
        assert!(acl_permits(
            &strict,
            <&ServerName>::try_from("good.example").unwrap()
        ));
        assert!(!acl_permits(
            &strict,
            <&ServerName>::try_from("other.example").unwrap()
        ));
    }
}
