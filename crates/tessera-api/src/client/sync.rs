//! `GET /sync` (classic).

use std::{collections::BTreeMap, time::Duration};

use ruma::{
    UInt,
    api::client::{
        filter::FilterDefinition,
        sync::sync_events::v3::{self as sync_v3, Filter},
    },
};
use tessera_service::sync::{SyncRequest, SyncToken};

use crate::{
    AppState, Error, Result, Ruma,
    client::rooms::{client_event, raw},
};

/// Default timeline limit when the filter does not say.
const DEFAULT_TIMELINE_LIMIT: usize = 10;

pub async fn sync(services: AppState, req: Ruma<sync_v3::Request>) -> Result<sync_v3::Response> {
    let sender = req.sender()?;
    let since = match req.body.since.as_deref() {
        None | Some("") => None,
        Some(s) => {
            Some(SyncToken::parse(s).ok_or_else(|| Error::invalid_param("Invalid since token."))?)
        }
    };

    // Timeline limit from the filter (inline or stored).
    let mut timeline_limit = DEFAULT_TIMELINE_LIMIT;
    let filter: Option<FilterDefinition> = match &req.body.filter {
        Some(Filter::FilterDefinition(f)) => Some(f.clone()),
        Some(Filter::FilterId(id)) => {
            let id: i64 = id
                .parse()
                .map_err(|_| Error::invalid_param("Invalid filter ID."))?;
            services
                .store
                .get_filter(&sender.user_id, id)
                .await?
                .map(serde_json::from_value)
                .transpose()
                .map_err(|e| Error::internal(e.to_string()))?
        }
        None => None,
        Some(_) => None,
    };
    if let Some(f) = &filter
        && let Some(l) = f.room.timeline.limit
    {
        timeline_limit = usize::try_from(u64::from(l)).unwrap_or(DEFAULT_TIMELINE_LIMIT);
    }

    let result = services
        .sync(SyncRequest {
            user_id: sender.user_id.clone(),
            since,
            timeout: req
                .body
                .timeout
                .unwrap_or(Duration::ZERO)
                .min(Duration::from_secs(300)),
            full_state: req.body.full_state,
            timeline_limit,
        })
        .await?;

    let mut resp = sync_v3::Response::new(result.next_batch.to_string());

    for (room_id, r) in result.join {
        let mut joined = sync_v3::JoinedRoom::new();
        joined.timeline.limited = r.limited;
        joined.timeline.prev_batch = r.prev_batch;
        joined.timeline.events = r
            .timeline
            .iter()
            .map(|e| raw(&client_event(e)))
            .collect::<Result<Vec<_>>>()?;
        let mut state = sync_v3::StateEvents::new();
        state.events = r
            .state
            .iter()
            .map(|e| raw(&client_event(e)))
            .collect::<Result<Vec<_>>>()?;
        joined.state = sync_v3::State::Before(state);
        joined.account_data.events = r.account_data.iter().map(raw).collect::<Result<Vec<_>>>()?;
        joined.ephemeral.events = r.ephemeral.iter().map(raw).collect::<Result<Vec<_>>>()?;
        joined.summary.joined_member_count =
            Some(UInt::try_from(r.joined_count).unwrap_or(UInt::MAX));
        joined.summary.invited_member_count =
            Some(UInt::try_from(r.invited_count).unwrap_or(UInt::MAX));
        joined.summary.heroes = r.heroes;
        resp.rooms.join.insert(room_id, joined);
    }
    for (room_id, r) in result.leave {
        let mut left = sync_v3::LeftRoom::new();
        left.timeline.limited = r.limited;
        left.timeline.prev_batch = r.prev_batch;
        left.timeline.events = r
            .timeline
            .iter()
            .map(|e| raw(&client_event(e)))
            .collect::<Result<Vec<_>>>()?;
        let mut state = sync_v3::StateEvents::new();
        state.events = r
            .state
            .iter()
            .map(|e| raw(&client_event(e)))
            .collect::<Result<Vec<_>>>()?;
        left.state = sync_v3::State::Before(state);
        resp.rooms.leave.insert(room_id, left);
    }
    for (room_id, r) in result.invite {
        let mut invited = sync_v3::InvitedRoom::new();
        invited.invite_state.events = r.invite_state.iter().map(raw).collect::<Result<Vec<_>>>()?;
        resp.rooms.invite.insert(room_id, invited);
    }
    resp.account_data.events = result
        .account_data
        .iter()
        .map(raw)
        .collect::<Result<Vec<_>>>()?;
    resp.device_one_time_keys_count = BTreeMap::new();
    Ok(resp)
}
