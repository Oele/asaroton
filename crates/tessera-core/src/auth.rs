//! Event format checks and authorization rules.
//!
//! Thin wrappers over `ruma-state-res` that take [`Pdu`] and this crate's error type. The
//! checks correspond to the spec's "checks performed on receipt of a PDU":
//!
//! 1. [`check_format`]: structural validity and size limits.
//! 2. [`check_state_independent`]: the `auth_events` are the right set, and for create events
//!    the create-specific rules.
//! 3. [`check_state_dependent`]: the event is allowed by a given state (its own auth events,
//!    the state before it, or the current state for soft-fail).

use std::collections::HashMap;

use ruma::{
    EventId, OwnedEventId,
    events::StateEventType,
    room_version_rules::RoomVersionRules,
    state_res::{
        auth_types_for_event, check_pdu_format, check_state_dependent_auth_rules,
        check_state_independent_auth_rules,
    },
};

use crate::{Error, Pdu, Result, state::StateMap};

/// Check the PDU's shape and size limits for its room version.
pub fn check_format(pdu: &Pdu, rules: &RoomVersionRules) -> Result<()> {
    check_pdu_format(pdu.raw(), &rules.event_format).map_err(Error::InvalidPdu)?;
    Ok(())
}

/// Size limits the spec puts on individual fields (bytes). Exceeding them makes an event
/// *rejected* like any other auth failure, so (as in the reference implementation) it is
/// kept as a rejected event that later events can still build on.
pub fn check_size_limits(pdu: &Pdu) -> Result<()> {
    let too_long = |what: &str, len: usize, max: usize| -> Result<()> {
        if len > max {
            Err(Error::InvalidPdu(format!(
                "`{what}` is too long ({len} > {max} bytes)"
            )))
        } else {
            Ok(())
        }
    };
    too_long("sender", pdu.sender().as_str().len(), 255)?;
    too_long("room_id", pdu.room_id().as_str().len(), 255)?;
    too_long("event_id", pdu.event_id().as_str().len(), 255)?;
    too_long("type", pdu.event_type().to_string().len(), 255)?;
    if let Some(key) = pdu.state_key() {
        too_long("state_key", key.len(), 255)?;
    }
    Ok(())
}

/// The `(type, state_key)` pairs that must make up the event's `auth_events`.
pub fn auth_types(pdu: &Pdu, rules: &RoomVersionRules) -> Result<Vec<(StateEventType, String)>> {
    auth_types_for_event(
        pdu.event_type(),
        pdu.sender(),
        pdu.state_key(),
        pdu.content(),
        &rules.authorization,
    )
    .map_err(Error::Auth)
}

/// Pick the auth events for a *new* event out of the current room state.
///
/// Returns the event IDs for every auth type present in `state`; missing entries are skipped
/// (e.g. no power levels event yet while creating a room).
pub fn select_auth_events(
    event_type: &ruma::events::TimelineEventType,
    sender: &ruma::UserId,
    state_key: Option<&str>,
    content: &serde_json::value::RawValue,
    state: &StateMap,
    rules: &RoomVersionRules,
) -> Result<Vec<OwnedEventId>> {
    let types = auth_types_for_event(event_type, sender, state_key, content, &rules.authorization)
        .map_err(Error::Auth)?;
    Ok(types
        .into_iter()
        .filter_map(|key| state.get(&key).cloned())
        .collect())
}

/// State-independent auth rules: the create event rules, and that `auth_events` contains
/// exactly the expected entries, all of which are known.
pub fn check_state_independent(
    pdu: &Pdu,
    rules: &RoomVersionRules,
    fetch_event: impl Fn(&EventId) -> Option<Pdu>,
) -> Result<()> {
    check_state_independent_auth_rules(&rules.authorization, pdu, fetch_event).map_err(Error::Auth)
}

/// State-dependent auth rules against an arbitrary state lookup.
pub fn check_state_dependent(
    pdu: &Pdu,
    rules: &RoomVersionRules,
    fetch_state: impl Fn(&StateEventType, &str) -> Option<Pdu>,
) -> Result<()> {
    check_state_dependent_auth_rules(&rules.authorization, pdu, fetch_state).map_err(Error::Auth)
}

/// State-dependent auth rules against a concrete map of state events.
pub fn check_against_state(
    pdu: &Pdu,
    rules: &RoomVersionRules,
    state: &HashMap<(StateEventType, String), Pdu>,
) -> Result<()> {
    check_state_dependent(pdu, rules, |ty, key| {
        state.get(&(ty.clone(), key.to_owned())).cloned()
    })
}

/// State-dependent auth rules using only the event's own `auth_events` as the state, which is
/// the spec's "passes authorization rules based on the event's auth events" step.
pub fn check_against_auth_events(
    pdu: &Pdu,
    rules: &RoomVersionRules,
    fetch_event: impl Fn(&EventId) -> Option<Pdu>,
) -> Result<()> {
    let mut state: HashMap<(StateEventType, String), Pdu> = HashMap::new();
    // Since room version 12 the create event is not listed in `auth_events`; it is implied by
    // the room ID instead.
    if !rules.event_format.allow_room_create_in_auth_events && !pdu.is_create() {
        let create_id = crate::pdu::create_event_id_for_room(pdu.room_id())?;
        let Some(create) = fetch_event(&create_id) else {
            return Err(Error::Auth(format!("create event {create_id} not found")));
        };
        state.insert((StateEventType::RoomCreate, String::new()), create);
    }
    for id in pdu.auth_events() {
        let Some(ev) = fetch_event(id) else {
            return Err(Error::Auth(format!("auth event {id} not found")));
        };
        if ev.rejected() {
            return Err(Error::Auth(format!("auth event {id} was rejected")));
        }
        if let Some((ty, key)) = ev.state_tuple() {
            state.insert((ty, key.to_owned()), ev.clone());
        }
    }
    check_against_state(pdu, rules, &state)
}
