//! Client-Server API handlers.

pub mod account;
pub mod admin;
pub mod backup;
pub mod delayed_events;
pub mod discovery;
pub mod keys;
pub mod mas;
pub mod media;
pub mod misc;
pub mod push;
pub mod rooms;
pub mod rtc;
pub mod rtc_signal;
pub mod session;
pub mod sliding_sync;
pub mod synapse_admin;
pub mod sync;
pub mod thread_subscriptions;

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
    let r = r.route(
        "/.well-known/matrix/server",
        axum::routing::get(discovery::well_known_server),
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
    // Custom profile fields (v1.16). Static `displayname`/`avatar_url` routes above win.
    let r = r
        .route(
            "/_matrix/client/v3/profile/{user_id}/{key}",
            axum::routing::get(profile_field_get_handler)
                .put(account::set_profile_field)
                .delete(profile_field_delete_handler),
        )
        .route(
            "/_matrix/client/unstable/uk.tcpip.msc4133/profile/{user_id}/{key}",
            axum::routing::get(profile_field_get_handler)
                .put(account::set_profile_field)
                .delete(profile_field_delete_handler),
        )
        .route(
            "/_matrix/client/v1/mutual_rooms",
            axum::routing::get(misc::mutual_rooms),
        );
    // rooms & membership
    let r = route::<c::room::create_room::v3::Request, _, _>(r, rooms::create);
    let r = route::<c::membership::join_room_by_id::v3::Request, _, _>(r, rooms::join_by_id);
    let r = route::<c::membership::join_room_by_id_or_alias::v3::Request, _, _>(
        r,
        rooms::join_by_id_or_alias,
    );
    let r = route::<c::membership::leave_room::v3::Request, _, _>(r, rooms::leave);
    let r = route::<c::knock::knock_room::v3::Request, _, _>(r, rooms::knock);
    let r =
        route::<c::room::get_event_by_timestamp::v1::Request, _, _>(r, rooms::timestamp_to_event);
    let r = r
        .route(
            "/_matrix/client/v1/room_summary/{room_id_or_alias}",
            axum::routing::get(room_summary_handler),
        )
        .route(
            "/_matrix/client/unstable/im.nheko.summary/rooms/{room_id_or_alias}/summary",
            axum::routing::get(room_summary_handler),
        );
    let r = r.route(
        "/_matrix/client/v1/rooms/{room_id}/hierarchy",
        axum::routing::get(hierarchy_handler),
    );
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
    let r = crate::router::route_raw::<c::message::send_message_event::v3::Request, _, _>(
        r,
        &[],
        delayed_events::send_message,
    );
    let r = crate::router::route_raw::<c::state::send_state_event::v3::Request, _, _>(
        r,
        &[
            "/_matrix/client/r0/rooms/{room_id}/state/{event_type}",
            "/_matrix/client/v3/rooms/{room_id}/state/{event_type}",
            "/_matrix/client/r0/rooms/{room_id}/state/{event_type}/",
            "/_matrix/client/v3/rooms/{room_id}/state/{event_type}/",
        ],
        delayed_events::send_state,
    );
    // Thread subscriptions (MSC4306) and their change stream (MSC4308).
    let r = route::<c::threads::subscribe_thread::unstable::Request, _, _>(
        r,
        thread_subscriptions::subscribe,
    );
    let r = route::<c::threads::unsubscribe_thread::unstable::Request, _, _>(
        r,
        thread_subscriptions::unsubscribe,
    );
    let r = route::<c::threads::get_thread_subscription::unstable::Request, _, _>(
        r,
        thread_subscriptions::get,
    );
    let r = crate::router::route_raw::<
        c::threads::get_thread_subscriptions_changes::unstable::Request,
        _,
        _,
    >(r, &[], thread_subscriptions::changes);
    // Delayed events (MSC4140).
    let r = r.route(
        "/_matrix/client/unstable/org.matrix.msc4140/rooms/{room_id}/delayed_event/{event_type}/{txn_id}",
        axum::routing::put(delayed_events::schedule),
    );
    let r = crate::router::route_raw::<
        c::delayed_events::get_all_delayed_events::unstable::Request,
        _,
        _,
    >(r, &[], delayed_events::list);
    let r = r
        .route(
            "/_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delay_id}",
            axum::routing::post(delayed_events::update_with_body),
        )
        .route(
            "/_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delay_id}/{action}",
            axum::routing::post(delayed_events::update_with_path),
        );
    // MatrixRTC transports (MSC4143) and the LiveKit transport (MSC4195).
    let r = r
        .route(
            "/_matrix/client/v1/rtc/transports",
            axum::routing::get(rtc::transports),
        )
        .route(
            "/_matrix/client/unstable/org.matrix.msc4143/rtc/transports",
            axum::routing::get(rtc::transports),
        )
        .route(
            "/_matrix/client/v1/rtc/livekit/get_token",
            axum::routing::post(rtc::get_token),
        )
        .route(
            "/_matrix/client/v1/rtc/livekit/delegate_delayed_leave",
            axum::routing::post(rtc::delegate_delayed_leave),
        );
    let r = [
        ("get_token", axum::routing::post(rtc::get_token)),
        ("sfu/get", axum::routing::post(rtc::sfu_get)),
        (
            "delegate_delayed_leave",
            axum::routing::post(rtc::delegate_delayed_leave),
        ),
        ("sfu_webhook", axum::routing::post(rtc::sfu_webhook)),
    ]
    .into_iter()
    .fold(r, |r, (path, handler)| {
        r.route(&format!("{}/{path}", rtc::PREFIX), handler)
    });
    let r = r
        .route(
            &format!("{}/rtc", asaroton_service::rtc::signal::SIGNAL_PATH),
            axum::routing::get(rtc_signal::websocket),
        )
        .route(
            &format!(
                "{}/rtc/validate",
                asaroton_service::rtc::signal::SIGNAL_PATH
            ),
            axum::routing::get(rtc_signal::validate),
        );
    let r =
        route::<c::account::request_openid_token::v3::Request, _, _>(r, rtc::request_openid_token);
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
    let r = r
        .route(
            "/_matrix/client/v3/publicRooms",
            axum::routing::get(public_rooms_handler).post(public_rooms_filtered_handler),
        )
        .route(
            "/_matrix/client/r0/publicRooms",
            axum::routing::get(public_rooms_handler).post(public_rooms_filtered_handler),
        );
    // sync & misc
    let r = r
        .route("/_matrix/client/r0/sync", axum::routing::get(sync_handler))
        .route("/_matrix/client/v3/sync", axum::routing::get(sync_handler))
        .route(
            "/_synapse/admin/v1/register",
            axum::routing::get(synapse_admin::get_nonce).post(synapse_admin::register),
        )
        .route(
            "/_synapse/admin/v1/send_server_notice",
            axum::routing::post(synapse_admin::send_server_notice),
        )
        .route(
            "/_synapse/admin/v1/send_server_notice/{txn_id}",
            axum::routing::put(synapse_admin::send_server_notice),
        )
        .route(
            "/_synapse/admin/v1/server_version",
            axum::routing::get(admin::server_version),
        )
        .route(
            "/_synapse/admin/v2/users",
            axum::routing::get(admin::list_users),
        )
        .route(
            "/_synapse/admin/v2/users/{user_id}",
            axum::routing::get(admin::get_user).put(admin::put_user),
        )
        .route(
            "/_synapse/admin/v2/users/{user_id}/devices",
            axum::routing::get(admin::list_devices),
        )
        .route(
            "/_synapse/admin/v1/users/{user_id}/admin",
            axum::routing::get(admin::get_admin).put(admin::set_admin),
        )
        .route(
            "/_synapse/admin/v1/users/{user_id}/joined_rooms",
            axum::routing::get(admin::joined_rooms),
        )
        .route(
            "/_synapse/admin/v1/deactivate/{user_id}",
            axum::routing::post(admin::deactivate),
        )
        .route(
            "/_synapse/admin/v1/reset_password/{user_id}",
            axum::routing::post(admin::reset_password),
        )
        .route(
            "/_synapse/admin/v1/rooms",
            axum::routing::get(admin::list_rooms),
        )
        .route(
            "/_synapse/admin/v1/rooms/{room_id}",
            axum::routing::get(admin::get_room).delete(admin::delete_room),
        )
        .route(
            "/_synapse/admin/v1/rooms/{room_id}/members",
            axum::routing::get(admin::room_members),
        )
        .route(
            "/_synapse/admin/v1/rooms/{room_id}/state",
            axum::routing::get(admin::room_state),
        )
        .route(
            "/_synapse/admin/v1/purge_history/{room_id}",
            axum::routing::post(admin::purge_history),
        )
        .route(
            "/_synapse/admin/v1/purge_history/{room_id}/{event_id}",
            axum::routing::post(admin::purge_history_at),
        )
        .route(
            "/_synapse/admin/v1/purge_history_status/{purge_id}",
            axum::routing::get(admin::purge_status),
        )
        .route(
            "/_synapse/admin/v1/join/{room}",
            axum::routing::post(admin::force_join),
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
    let r = route::<c::tag::get_tags::v3::Request, _, _>(r, misc::get_tags);
    let r = route::<c::tag::create_tag::v3::Request, _, _>(r, misc::create_tag);
    let r = route::<c::tag::delete_tag::v3::Request, _, _>(r, misc::delete_tag);
    // Account data deletion (MSC3391).
    let r = r
        .route(
            "/_matrix/client/unstable/org.matrix.msc3391/user/{user_id}/account_data/{data_type}",
            axum::routing::delete(misc::delete_global_account_data),
        )
        .route(
            "/_matrix/client/unstable/org.matrix.msc3391/user/{user_id}/rooms/{room_id}/account_data/{data_type}",
            axum::routing::delete(misc::delete_room_account_data),
        );
    let r = route::<c::filter::create_filter::v3::Request, _, _>(r, misc::create_filter);
    let r = route::<c::filter::get_filter::v3::Request, _, _>(r, misc::get_filter);
    let r = route::<c::typing::create_typing_event::v3::Request, _, _>(r, misc::typing);
    let r = route::<c::receipt::create_receipt::v3::Request, _, _>(r, misc::receipt);
    let r = route::<c::read_marker::set_read_marker::v3::Request, _, _>(r, misc::read_markers);
    let r = route::<c::presence::set_presence::v3::Request, _, _>(r, misc::set_presence);
    let r = route::<c::presence::get_presence::v3::Request, _, _>(r, misc::get_presence);
    let r = route::<c::user_directory::search_users::v3::Request, _, _>(r, misc::search_users);
    let r = route::<c::appservice::request_ping::v1::Request, _, _>(r, misc::appservice_ping);
    let r = r
        .route(
            "/_matrix/client/v3/thirdparty/protocols",
            axum::routing::get(thirdparty_protocols_handler),
        )
        .route(
            "/_matrix/client/r0/thirdparty/protocols",
            axum::routing::get(thirdparty_protocols_handler),
        )
        .route(
            "/_matrix/client/v3/thirdparty/protocol/{protocol}",
            axum::routing::get(thirdparty_protocol_handler),
        )
        .route(
            "/_matrix/client/r0/thirdparty/protocol/{protocol}",
            axum::routing::get(thirdparty_protocol_handler),
        )
        .route(
            "/_matrix/client/v3/thirdparty/location/{protocol}",
            axum::routing::get(thirdparty_location_handler),
        )
        .route(
            "/_matrix/client/r0/thirdparty/location/{protocol}",
            axum::routing::get(thirdparty_location_handler),
        )
        .route(
            "/_matrix/client/v3/thirdparty/user/{protocol}",
            axum::routing::get(thirdparty_user_handler),
        )
        .route(
            "/_matrix/client/r0/thirdparty/user/{protocol}",
            axum::routing::get(thirdparty_user_handler),
        )
        .route(
            "/_matrix/client/v3/thirdparty/location",
            axum::routing::get(thirdparty_location_by_alias_handler),
        )
        .route(
            "/_matrix/client/r0/thirdparty/location",
            axum::routing::get(thirdparty_location_by_alias_handler),
        )
        .route(
            "/_matrix/client/v3/thirdparty/user",
            axum::routing::get(thirdparty_user_by_id_handler),
        )
        .route(
            "/_matrix/client/r0/thirdparty/user",
            axum::routing::get(thirdparty_user_by_id_handler),
        )
        .route(
            "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync",
            axum::routing::post(sliding_sync_handler),
        )
        // Matrix Authentication Service: discovery for clients, management API for MAS.
        .route(
            "/_matrix/client/v1/auth_metadata",
            axum::routing::get(mas::auth_metadata),
        )
        .route(
            "/_matrix/client/unstable/org.matrix.msc2965/auth_metadata",
            axum::routing::get(mas::auth_metadata),
        )
        .route(
            "/_matrix/client/unstable/org.matrix.msc2965/auth_issuer",
            axum::routing::get(mas::auth_issuer),
        )
        .route("/_synapse/mas/query_user", axum::routing::get(mas::query_user))
        .route(
            "/_synapse/mas/is_localpart_available",
            axum::routing::get(mas::is_localpart_available),
        )
        .route(
            "/_synapse/mas/provision_user",
            axum::routing::post(mas::provision_user),
        )
        .route("/_synapse/mas/delete_user", axum::routing::post(mas::delete_user))
        .route(
            "/_synapse/mas/reactivate_user",
            axum::routing::post(mas::reactivate_user),
        )
        .route(
            "/_synapse/mas/set_displayname",
            axum::routing::post(mas::set_displayname),
        )
        .route(
            "/_synapse/mas/unset_displayname",
            axum::routing::post(mas::unset_displayname),
        )
        .route(
            "/_synapse/mas/allow_cross_signing_reset",
            axum::routing::post(mas::allow_cross_signing_reset),
        )
        .route("/_synapse/mas/upsert_device", axum::routing::post(mas::upsert_device))
        .route("/_synapse/mas/delete_device", axum::routing::post(mas::delete_device))
        .route(
            "/_synapse/mas/update_device_display_name",
            axum::routing::post(mas::update_device_display_name),
        )
        .route("/_synapse/mas/sync_devices", axum::routing::post(mas::sync_devices));
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
    // key backups
    let r = route::<c::backup::create_backup_version::v3::Request, _, _>(r, backup::create_version);
    let r =
        route::<c::backup::get_latest_backup_info::v3::Request, _, _>(r, backup::latest_version);
    let r = route::<c::backup::get_backup_info::v3::Request, _, _>(r, backup::get_version);
    let r = route::<c::backup::update_backup_version::v3::Request, _, _>(r, backup::update_version);
    let r = route::<c::backup::delete_backup_version::v3::Request, _, _>(r, backup::delete_version);
    let r = route::<c::backup::add_backup_keys::v3::Request, _, _>(r, backup::put_keys);
    let r =
        route::<c::backup::add_backup_keys_for_room::v3::Request, _, _>(r, backup::put_room_keys);
    let r = route::<c::backup::add_backup_keys_for_session::v3::Request, _, _>(
        r,
        backup::put_session_key,
    );
    let r = route::<c::backup::get_backup_keys::v3::Request, _, _>(r, backup::get_keys);
    let r =
        route::<c::backup::get_backup_keys_for_room::v3::Request, _, _>(r, backup::get_room_keys);
    let r = route::<c::backup::get_backup_keys_for_session::v3::Request, _, _>(
        r,
        backup::get_session_key,
    );
    let r = route::<c::backup::delete_backup_keys::v3::Request, _, _>(r, backup::delete_keys);
    let r = route::<c::backup::delete_backup_keys_for_room::v3::Request, _, _>(
        r,
        backup::delete_room_keys,
    );
    let r = route::<c::backup::delete_backup_keys_for_session::v3::Request, _, _>(
        r,
        backup::delete_session_key,
    );
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
    let rel_filter = req
        .uri()
        .query()
        .and_then(|q| q.split('&').find_map(|kv| kv.strip_prefix("filter=")))
        .map(|raw| {
            // Form encoding: `+` is a space.
            percent_encoding::percent_decode_str(&raw.replace('+', " "))
                .decode_utf8_lossy()
                .into_owned()
        })
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .map(|f| rooms::RelTypeFilter::from_filter_json(&f))
        .unwrap_or_default();
    let ruma = match crate::Ruma::<ruma::api::client::message::get_message_events::v3::Request>::from_request(req, &state).await {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    match rooms::messages(state, ruma, rel_filter).await {
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

async fn hierarchy_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: crate::Ruma<ruma::api::client::space::get_hierarchy::v1::Request>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match rooms::hierarchy(state, req).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn room_summary_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: crate::Ruma<ruma::api::client::room::get_summary::v1::Request>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match rooms::room_summary(state, req).await {
        Ok(resp) => resp,
        Err(e) => e.into_response(),
    }
}

async fn public_rooms_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: crate::Ruma<ruma::api::client::directory::get_public_rooms::v3::Request>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match rooms::public_rooms(state, req).await {
        Ok(resp) => {
            let body = rooms::public_rooms_json(
                &resp.chunk,
                resp.total_room_count_estimate,
                resp.next_batch.as_deref(),
                resp.prev_batch.as_deref(),
            );
            let mut r = axum::Json(body).into_response();
            crate::extract::add_cors(r.headers_mut());
            r
        }
        Err(e) => e.into_response(),
    }
}

async fn public_rooms_filtered_handler(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: crate::Ruma<ruma::api::client::directory::get_public_rooms_filtered::v3::Request>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    match rooms::public_rooms_filtered(state, req).await {
        Ok(resp) => {
            let body = rooms::public_rooms_json(
                &resp.chunk,
                resp.total_room_count_estimate,
                resp.next_batch.as_deref(),
                resp.prev_batch.as_deref(),
            );
            let mut r = axum::Json(body).into_response();
            crate::extract::add_cors(r.headers_mut());
            r
        }
        Err(e) => e.into_response(),
    }
}

macro_rules! raw_json_handler {
    ($name:ident, $req:ty, $call:path) => {
        async fn $name(
            axum::extract::State(state): axum::extract::State<AppState>,
            req: crate::Ruma<$req>,
        ) -> axum::response::Response {
            use axum::response::IntoResponse;
            match $call(state, req).await {
                Ok(resp) => resp,
                Err(e) => e.into_response(),
            }
        }
    };
}

raw_json_handler!(
    thirdparty_protocols_handler,
    ruma::api::client::thirdparty::get_protocols::v3::Request,
    misc::thirdparty_protocols
);
raw_json_handler!(
    thirdparty_protocol_handler,
    ruma::api::client::thirdparty::get_protocol::v3::Request,
    misc::thirdparty_protocol
);
raw_json_handler!(
    thirdparty_location_handler,
    ruma::api::client::thirdparty::get_location_for_protocol::v3::Request,
    misc::thirdparty_location
);
raw_json_handler!(
    thirdparty_user_handler,
    ruma::api::client::thirdparty::get_user_for_protocol::v3::Request,
    misc::thirdparty_user
);
raw_json_handler!(
    thirdparty_location_by_alias_handler,
    ruma::api::client::thirdparty::get_location_for_room_alias::v3::Request,
    misc::thirdparty_location_by_alias
);
raw_json_handler!(
    thirdparty_user_by_id_handler,
    ruma::api::client::thirdparty::get_user_for_user_id::v3::Request,
    misc::thirdparty_user_by_id
);
raw_json_handler!(
    sliding_sync_handler,
    ruma::api::client::sync::sync_events::v5::Request,
    sliding_sync::sliding_sync
);
raw_json_handler!(
    profile_field_get_handler,
    ruma::api::client::profile::get_profile_field::v3::Request,
    account::get_profile_field
);
raw_json_handler!(
    profile_field_delete_handler,
    ruma::api::client::profile::delete_profile_field::v3::Request,
    account::delete_profile_field
);
