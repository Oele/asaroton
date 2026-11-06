//! State resolution.

use ruma::{
    EventId, OwnedEventId,
    events::StateEventType,
    room_version_rules::{RoomVersionRules, StateResolutionVersion},
    state_res::{self, utils::event_id_set::EventIdSet},
};

use crate::{Error, Pdu, Result};

/// A room state: `(type, state_key)` to event ID.
pub type StateMap = state_res::StateMap<OwnedEventId>;

/// A set of event IDs, as used for auth chains.
pub type EventIdSetOwned = EventIdSet<OwnedEventId>;

/// Resolve several candidate states into one.
///
/// * `state_maps`: the state after each forward extremity (or the states to merge).
/// * `auth_chains`: the full auth chain of each state map, same order.
/// * `fetch_event`: looks up any event by ID (must cover all events in the maps and chains).
/// * `fetch_conflicted_state_subgraph`: for room version 12+, given the conflicted state set,
///   returns every event reachable from it via `auth_events`/`prev_events` that is itself a
///   state event in the conflict (see the spec, "conflicted state subgraph"). May return `None`
///   for older room versions.
pub fn resolve(
    rules: &RoomVersionRules,
    state_maps: &[StateMap],
    auth_chains: Vec<EventIdSetOwned>,
    fetch_event: impl Fn(&EventId) -> Option<Pdu>,
    fetch_conflicted_state_subgraph: impl Fn(
        &state_res::StateMap<Vec<OwnedEventId>>,
    ) -> Option<EventIdSetOwned>,
) -> Result<StateMap> {
    let StateResolutionVersion::V2(v2) = &rules.state_res else {
        return Err(Error::Auth("state resolution v1 is not implemented".into()));
    };
    Ok(state_res::resolve(
        &rules.authorization,
        v2,
        state_maps.iter(),
        auth_chains,
        fetch_event,
        fetch_conflicted_state_subgraph,
    )?)
}

/// Whether this room version needs the legacy (v1) state resolution algorithm.
pub fn needs_state_res_v1(rules: &RoomVersionRules) -> bool {
    matches!(rules.state_res, StateResolutionVersion::V1)
}

/// Convenience: build a `StateMap` from state PDUs.
pub fn state_map_from_pdus<'a>(pdus: impl IntoIterator<Item = &'a Pdu>) -> StateMap {
    let mut map = StateMap::new();
    for pdu in pdus {
        if let Some((ty, key)) = pdu.state_tuple() {
            map.insert((ty, key.to_owned()), pdu.event_id().to_owned());
        }
    }
    map
}

/// Look up a state event in a map, returning the `(type, state_key)` key form used by ruma.
pub fn state_key(ty: StateEventType, key: &str) -> (StateEventType, String) {
    (ty, key.to_owned())
}
