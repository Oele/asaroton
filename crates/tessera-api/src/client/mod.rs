//! Client-Server API handlers.

pub mod account;
pub mod discovery;
pub mod keys;
pub mod media;
pub mod misc;
pub mod push;
pub mod rooms;
pub mod session;
pub mod synapse_admin;
pub mod sync;

use axum::Router;

use crate::{AppState, route, route_with_extra_paths};

/// Register every client-server endpoint.
pub fn routes(router: Router<AppState>) -> Router<AppState> {
    use ruma::api::client as c;

    let r = router;
    // discovery
    let r = route::<c::discovery::get_supported_versions::Request, _, _>(r, discovery::versions);
    let r = r.route(
        "/_matrix/client/v3/capabilities",
        axum::routing::get(capabilities_handler),
    );
    let r = r.route(
        "/.well-known/matrix/client",
        axum::routing::get(discovery::well_known_client),
    );
    // session
    let r = route::<c::session::get_login_types::v3::Request, _, _>(r, session::login_types);
    let r = route::<c::session::login::v3::Request, _, _>(r, session::login);
    let r = route::<c::session::logout::v3::Request, _, _>(r, session::logout);
    let r = route::<c::session::logout_all::v3::Request, _, _>(r, session::logout_all);
    let r = route::<c::session::refresh_token::v3::Request, _, _>(r, session::refresh);
    // account
    let r = route::<c::account::register::v3::Request, _, _>(r, account::register);
    let r =
        route::<c::account::get_username_availability::v3::Request, _, _>(r, account::available);
    let r = route::<c::account::whoami::v3::Request, _, _>(r, account::whoami);
    let r = route::<c::account::change_password::v3::Request, _, _>(r, account::change_password);
    let r = route::<c::account::deactivate::v3::Request, _, _>(r, account::deactivate);
    // devices
    let r = route::<c::device::get_devices::v3::Request, _, _>(r, account::get_devices);
    let r = route::<c::device::get_device::v3::Request, _, _>(r, account::get_device);
    let r = route::<c::device::update_device::v3::Request, _, _>(r, account::update_device);
    let r = route::<c::device::delete_device::v3::Request, _, _>(r, account::delete_device);
    let r = route::<c::device::delete_devices::v3::Request, _, _>(r, account::delete_devices);
    // profile
    let r = route::<c::profile::get_profile::v3::Request, _, _>(r, account::get_profile);
    let r = route::<c::profile::get_display_name::v3::Request, _, _>(r, account::get_display_name);
    let r = route::<c::profile::set_display_name::v3::Request, _, _>(r, account::set_display_name);
    let r = route::<c::profile::get_avatar_url::v3::Request, _, _>(r, account::get_avatar_url);
    let r = route::<c::profile::set_avatar_url::v3::Request, _, _>(r, account::set_avatar_url);
    // rooms & membership
    let r = route::<c::room::create_room::v3::Request, _, _>(r, rooms::create);
    let r = route::<c::membership::join_room_by_id::v3::Request, _, _>(r, rooms::join_by_id);
    let r = route::<c::membership::join_room_by_id_or_alias::v3::Request, _, _>(
        r,
        rooms::join_by_id_or_alias,
    );
    let r = route::<c::membership::leave_room::v3::Request, _, _>(r, rooms::leave);
    let r = route::<c::membership::forget_room::v3::Request, _, _>(r, rooms::forget);
    let r = route::<c::membership::invite_user::v3::Request, _, _>(r, rooms::invite);
    let r = route::<c::membership::kick_user::v3::Request, _, _>(r, rooms::kick);
    let r = route::<c::membership::ban_user::v3::Request, _, _>(r, rooms::ban);
    let r = route::<c::membership::unban_user::v3::Request, _, _>(r, rooms::unban);
    let r = route::<c::membership::joined_rooms::v3::Request, _, _>(r, rooms::joined);
    let r = route::<c::membership::get_member_events::v3::Request, _, _>(r, rooms::members);
    let r = r
        .route(
            "/_matrix/client/r0/rooms/{room_id}/joined_members",
            axum::routing::get(joined_members_handler),
        )
        .route(
            "/_matrix/client/v3/rooms/{room_id}/joined_members",
            axum::routing::get(joined_members_handler),
        );
    // events
    let r = route::<c::message::send_message_event::v3::Request, _, _>(r, rooms::send_message);
    let r = route_with_extra_paths::<c::state::send_state_event::v3::Request, _, _>(
        r,
        &[
            "/_matrix/client/r0/rooms/{room_id}/state/{event_type}",
            "/_matrix/client/v3/rooms/{room_id}/state/{event_type}",
            "/_matrix/client/r0/rooms/{room_id}/state/{event_type}/",
            "/_matrix/client/v3/rooms/{room_id}/state/{event_type}/",
        ],
        rooms::send_state,
    );
    let r = route::<c::redact::redact_event::v3::Request, _, _>(r, rooms::redact);
    let r = route::<c::state::get_state_events::v3::Request, _, _>(r, rooms::state);
    let r = route_with_extra_paths::<c::state::get_state_event_for_key::v3::Request, _, _>(
        r,
        &[
            "/_matrix/client/r0/rooms/{room_id}/state/{event_type}",
            "/_matrix/client/v3/rooms/{room_id}/state/{event_type}",
            "/_matrix/client/r0/rooms/{room_id}/state/{event_type}/",
            "/_matrix/client/v3/rooms/{room_id}/state/{event_type}/",
        ],
        rooms::state_key,
    );
    // `/messages` defaults `dir` to backwards, as the reference implementation does.
    let r = r
        .route(
            "/_matrix/client/v3/rooms/{room_id}/messages",
            axum::routing::get(messages_handler),
        )
        .route(
            "/_matrix/client/r0/rooms/{room_id}/messages",
            axum::routing::get(messages_handler),
        );
    let r = route::<c::room::get_room_event::v3::Request, _, _>(r, rooms::context);
    let r = route::<c::context::get_context::v3::Request, _, _>(r, rooms::event_context);
    let r = route::<c::relations::get_relating_events::v1::Request, _, _>(r, rooms::relations);
    let r = route::<c::relations::get_relating_events_with_rel_type::v1::Request, _, _>(
        r,
        rooms::relations_with_rel_type,
    );
    let r = route::<
        c::relations::get_relating_events_with_rel_type_and_event_type::v1::Request,
        _,
        _,
    >(r, rooms::relations_with_rel_and_event_type);
    let r = route::<c::threads::get_threads::v1::Request, _, _>(r, rooms::threads);
    let r = route::<c::room::upgrade_room::v3::Request, _, _>(r, rooms::upgrade);
    let r = route::<c::room::report_content::v3::Request, _, _>(r, rooms::report_event);
    let r = route::<c::room::report_room::v3::Request, _, _>(r, rooms::report_room);
    let r = route::<c::reporting::report_user::v3::Request, _, _>(r, rooms::report_user);
    let r = r.route(
        "/_matrix/client/v3/search",
        axum::routing::post(search_handler),
    );
    let r = r.route(
        "/_matrix/client/r0/search",
        axum::routing::post(search_handler),
    );
    // aliases & directory
    let r = route::<c::alias::create_alias::v3::Request, _, _>(r, rooms::create_alias);
    let r = route::<c::alias::get_alias::v3::Request, _, _>(r, rooms::get_alias);
    let r = route::<c::alias::delete_alias::v3::Request, _, _>(r, rooms::delete_alias);
    let r = route::<c::room::aliases::v3::Request, _, _>(r, rooms::room_aliases);
    let r = route::<c::directory::get_room_visibility::v3::Request, _, _>(r, rooms::get_visibility);
    let r = route::<c::directory::set_room_visibility::v3::Request, _, _>(r, rooms::set_visibility);
    let r = route::<c::directory::get_public_rooms::v3::Request, _, _>(r, rooms::public_rooms);
    let r = route::<c::directory::get_public_rooms_filtered::v3::Request, _, _>(
        r,
        rooms::public_rooms_filtered,
    );
    // sync & misc
    let r = r
        .route("/_matrix/client/r0/sync", axum::routing::get(sync_handler))
        .route("/_matrix/client/v3/sync", axum::routing::get(sync_handler))
        .route(
            "/_synapse/admin/v1/register",
            axum::routing::get(synapse_admin::get_nonce).post(synapse_admin::register),
        );
    let r = route::<c::config::set_global_account_data::v3::Request, _, _>(
        r,
        misc::set_global_account_data,
    );
    let r = route::<c::config::set_room_account_data::v3::Request, _, _>(
        r,
        misc::set_room_account_data,
    );
    let r = route::<c::config::get_global_account_data::v3::Request, _, _>(
        r,
        misc::get_global_account_data,
    );
    let r = route::<c::config::get_room_account_data::v3::Request, _, _>(
        r,
        misc::get_room_account_data,
    );
    let r = route::<c::filter::create_filter::v3::Request, _, _>(r, misc::create_filter);
    let r = route::<c::filter::get_filter::v3::Request, _, _>(r, misc::get_filter);
    let r = route::<c::typing::create_typing_event::v3::Request, _, _>(r, misc::typing);
    let r = route::<c::receipt::create_receipt::v3::Request, _, _>(r, misc::receipt);
    let r = route::<c::read_marker::set_read_marker::v3::Request, _, _>(r, misc::read_markers);
    let r = route::<c::presence::set_presence::v3::Request, _, _>(r, misc::set_presence);
    let r = route::<c::presence::get_presence::v3::Request, _, _>(r, misc::get_presence);
    let r = route::<c::user_directory::search_users::v3::Request, _, _>(r, misc::search_users);
    let r = route::<c::thirdparty::get_protocols::v3::Request, _, _>(r, misc::thirdparty_protocols);
    let r = route::<c::voip::get_turn_server_info::v3::Request, _, _>(r, misc::turn_server);
    // media
    #[allow(deprecated)]
    let r = route::<c::media::get_media_config::v3::Request, _, _>(r, media::config);
    let r =
        route::<c::authenticated_media::get_media_config::v1::Request, _, _>(r, media::config_v1);
    let r = route::<c::media::create_content::v3::Request, _, _>(r, media::upload);
    #[allow(deprecated)]
    let r = route::<c::media::get_media_preview::v3::Request, _, _>(r, rooms::preview_url_legacy);
    let r = route::<c::authenticated_media::get_media_preview::v1::Request, _, _>(
        r,
        rooms::preview_url,
    );
    let r = route::<c::media::create_mxc_uri::v1::Request, _, _>(r, media::create);
    let r = r.route(
        "/_matrix/media/v3/upload/{server_name}/{media_id}",
        axum::routing::put(upload_async_handler),
    );
    let r = route::<c::authenticated_media::get_content::v1::Request, _, _>(r, media::download_v1);
    let r = route::<c::authenticated_media::get_content_as_filename::v1::Request, _, _>(
        r,
        media::download_v1_named,
    );
    let r = route::<c::authenticated_media::get_content_thumbnail::v1::Request, _, _>(
        r,
        media::thumbnail_v1,
    );
    #[allow(deprecated)]
    let r = route::<c::media::get_content::v3::Request, _, _>(r, media::download_legacy);
    #[allow(deprecated)]
    let r = route::<c::media::get_content_as_filename::v3::Request, _, _>(
        r,
        media::download_legacy_named,
    );
    #[allow(deprecated)]
    let r = route::<c::media::get_content_thumbnail::v3::Request, _, _>(r, media::thumbnail_legacy);
    // e2ee keys & to-device
    let r = route::<c::keys::upload_keys::v3::Request, _, _>(r, keys::upload);
    let r = route::<c::keys::get_keys::v3::Request, _, _>(r, keys::query);
    let r = route::<c::keys::claim_keys::v3::Request, _, _>(r, keys::claim);
    let r = route::<c::keys::get_key_changes::v3::Request, _, _>(r, keys::changes);
    let r = route::<c::keys::upload_signing_keys::v3::Request, _, _>(r, keys::upload_signing);
    let r = route::<c::keys::upload_signatures::v3::Request, _, _>(r, keys::upload_sigs);
    let r = route::<c::to_device::send_event_to_device::v3::Request, _, _>(r, keys::send_to_device);
    // push
    let r = route::<c::push::get_pushrules_all::v3::Request, _, _>(r, push::rules_all);
    let r = route::<c::push::get_pushrules_global_scope::v3::Request, _, _>(r, push::rules_global);
    let r = route::<c::push::get_pushrule::v3::Request, _, _>(r, push::get_rule);
    let r = route::<c::push::set_pushrule::v3::Request, _, _>(r, push::set_rule);
    let r = route::<c::push::delete_pushrule::v3::Request, _, _>(r, push::delete_rule);
    let r = route::<c::push::get_pushrule_enabled::v3::Request, _, _>(r, push::get_enabled);
    let r = route::<c::push::set_pushrule_enabled::v3::Request, _, _>(r, push::set_enabled);
    let r = route::<c::push::get_pushrule_actions::v3::Request, _, _>(r, push::get_actions);
    let r = route::<c::push::set_pushrule_actions::v3::Request, _, _>(r, push::set_actions);
    let r = route::<c::push::get_pushers::v3::Request, _, _>(r, push::get_pushers);
    route::<c::push::set_pusher::v3::Request, _, _>(r, push::set_pusher)
}

/// `/sync` returns hand-built JSON, so it bypasses the typed `route` helper.
async fn sync_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: crate::Ruma<ruma::api::client::sync::sync_events::v3::Request>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match sync::sync(state, req).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn joined_members_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: crate::Ruma<ruma::api::client::membership::joined_members::v3::Request>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match rooms::joined_members(state, req).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn upload_async_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: crate::Ruma<ruma::api::client::media::create_content_async::v3::Request>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match media::upload_async(state, req).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn capabilities_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: crate::Ruma<ruma::api::client::discovery::get_capabilities::v3::Request>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match discovery::capabilities(&state, &req) {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn messages_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    mut req: axum::extract::Request,
) -> axum::response::Response {
    use axum::{extract::FromRequest, response::IntoResponse};
    let has_dir = req
        .uri()
        .query()
        .is_some_and(|q| q.split('&').any(|kv| kv.starts_with("dir=")));
    if !has_dir {
        let path = req.uri().path().to_owned();
        let query = match req.uri().query() {
            Some(q) if !q.is_empty() => format!("{q}&dir=b"),
            _ => "dir=b".to_owned(),
        };
        if let Ok(uri) = format!("{path}?{query}").parse::<http::Uri>() {
            *req.uri_mut() = uri;
        }
    }
    let ruma = match crate::Ruma::<ruma::api::client::message::get_message_events::v3::Request>::from_request(req, &state).await {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    match rooms::messages(state, ruma).await {
        Ok(resp) => crate::extract::RumaResponse(resp).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn search_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: crate::Ruma<ruma::api::client::search::search_events::v3::Request>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match rooms::search(state, req).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}
