//! What a suspended account may still do (Matrix v1.13, "Account suspension").
//!
//! The specification leaves the choice to the server, with a list of what it SHOULD permit
//! and a list of what it recommends refusing. The line drawn here: a suspended account keeps
//! reading, its encryption, its own account and the way out; it does nothing that others in
//! its rooms (or on other servers) would see or receive. What it may not do is answered with
//! 403 `M_USER_SUSPENDED`.
//!
//! Every request with an access token passes [`check`] in the [`crate::Ruma`] extractor, so
//! that no endpoint can forget it; the few client endpoints that read their token themselves
//! (custom profile fields, scheduling a delayed event, a call's SFU token) refuse a suspended
//! account in their handler. The decision:
//!
//! - Reading (every `GET`) is allowed.
//! - A write is allowed, refused, or looked at, as [`RULES`] says. A write that is not in the
//!   table is refused, so that an endpoint added later stays closed to a suspended account
//!   until someone decides otherwise.
//! - Looked at ([`inspect`]): `/send` and `/redact` may redact the account's own events and
//!   nothing else (`/send` of any other event type is refused); `/state` may only set the
//!   account's own membership to `leave`; presence may change without a status message;
//!   public read receipts are kept as private ones, so that the account's unread counts stay
//!   right while others are shown nothing.
//!
//! Not done: the room the spec suggests for talking to the server's admins. Who is suspended
//! can still reach them by email or through `/.well-known/matrix/support`.

use std::{
    any::{Any, TypeId},
    collections::HashMap,
    sync::LazyLock,
};

use asaroton_auth::Sender;
use ruma::{
    EventId, RoomId,
    api::IncomingRequest,
    events::{MessageLikeEventType, StateEventType},
};

use crate::{AppState, Error, Result};

/// What a suspended account may do with a request that writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Rule {
    /// Allowed as it is.
    Allow,
    /// Refused with `M_USER_SUSPENDED`.
    Refuse,
    /// Decided by looking at the request ([`inspect`]).
    Inspect,
}

/// The writes a suspended account may make, may not make, or makes on conditions. Reading is
/// not listed: every `GET` is allowed. A write missing here is refused.
static RULES: LazyLock<HashMap<TypeId, Rule>> = LazyLock::new(|| {
    use Rule::{Allow, Inspect, Refuse};
    use ruma::api::client as c;
    /// An entry; only writes have one (reading is decided before the table is asked).
    fn write<T: IncomingRequest + 'static>(rule: Rule) -> (TypeId, Rule) {
        debug_assert!(
            T::METHOD != http::Method::GET,
            "{} is read with GET: it needs no entry",
            std::any::type_name::<T>()
        );
        (TypeId::of::<T>(), rule)
    }
    HashMap::from([
        // ---- Allowed: the way out, the account and its devices (spec: log out, delete
        // devices, deactivate, add administrative contacts).
        write::<c::session::login::v3::Request>(Allow),
        write::<c::session::logout::v3::Request>(Allow),
        write::<c::session::logout_all::v3::Request>(Allow),
        write::<c::session::refresh_token::v3::Request>(Allow),
        write::<c::account::register::v3::Request>(Allow),
        write::<c::account::deactivate::v3::Request>(Allow),
        write::<c::account::change_password::v3::Request>(Allow),
        write::<c::account::request_3pid_management_token_via_email::v3::Request>(Allow),
        write::<c::account::add_3pid::v3::Request>(Allow),
        // There is no identity server to unbind from: nothing happens.
        write::<c::account::unbind_3pid::v3::Request>(Allow),
        write::<c::device::update_device::v3::Request>(Allow),
        write::<c::device::delete_device::v3::Request>(Allow),
        write::<c::device::delete_devices::v3::Request>(Allow),
        // ---- Allowed: encryption (spec: verify devices, cross-signing, key backup). To-device
        // messages carry key sharing and verification, and cannot be told apart from others.
        write::<c::keys::upload_keys::v3::Request>(Allow),
        write::<c::keys::get_keys::v3::Request>(Allow),
        write::<c::keys::claim_keys::v3::Request>(Allow),
        write::<c::keys::upload_signing_keys::v3::Request>(Allow),
        write::<c::keys::upload_signatures::v3::Request>(Allow),
        write::<c::to_device::send_event_to_device::v3::Request>(Allow),
        write::<c::dehydrated_device::put_dehydrated_device::msc3814::Request>(Allow),
        write::<c::dehydrated_device::delete_dehydrated_device::msc3814::Request>(Allow),
        write::<c::dehydrated_device::get_events::msc3814::Request>(Allow),
        write::<c::backup::create_backup_version::v3::Request>(Allow),
        write::<c::backup::update_backup_version::v3::Request>(Allow),
        write::<c::backup::delete_backup_version::v3::Request>(Allow),
        write::<c::backup::add_backup_keys::v3::Request>(Allow),
        write::<c::backup::add_backup_keys_for_room::v3::Request>(Allow),
        write::<c::backup::add_backup_keys_for_session::v3::Request>(Allow),
        write::<c::backup::delete_backup_keys::v3::Request>(Allow),
        write::<c::backup::delete_backup_keys_for_room::v3::Request>(Allow),
        write::<c::backup::delete_backup_keys_for_session::v3::Request>(Allow),
        // ---- Allowed: reading, with requests that write nothing others see (spec: see and
        // receive messages). Leaving and rejecting invites (spec), forgetting a room left.
        write::<c::sync::sync_events::msc4186::Request>(Allow),
        write::<c::sync::sync_events::v4::Request>(Allow),
        write::<c::search::search_events::v3::Request>(Allow),
        write::<c::user_directory::search_users::v3::Request>(Allow),
        write::<c::directory::get_public_rooms_filtered::v3::Request>(Allow),
        write::<c::filter::create_filter::v3::Request>(Allow),
        write::<c::membership::leave_room::v3::Request>(Allow),
        write::<c::membership::forget_room::v3::Request>(Allow),
        // The account's own data and settings, seen by nobody else.
        write::<c::config::set_global_account_data::v3::Request>(Allow),
        write::<c::config::set_room_account_data::v3::Request>(Allow),
        write::<c::tag::create_tag::v3::Request>(Allow),
        write::<c::tag::delete_tag::v3::Request>(Allow),
        write::<c::push::set_pushrule::v3::Request>(Allow),
        write::<c::push::delete_pushrule::v3::Request>(Allow),
        write::<c::push::set_pushrule_enabled::v3::Request>(Allow),
        write::<c::push::set_pushrule_actions::v3::Request>(Allow),
        write::<c::push::set_pusher::v3::Request>(Allow),
        write::<c::threads::subscribe_thread::msc4306::Request>(Allow),
        write::<c::threads::unsubscribe_thread::msc4306::Request>(Allow),
        // Reports go to the server's admins, who may want to hear them.
        write::<c::room::report_content::v3::Request>(Allow),
        write::<c::room::report_room::v3::Request>(Allow),
        write::<c::reporting::report_user::v3::Request>(Allow),
        // An admin's tools stay an admin's (only appservices call the ping).
        write::<c::admin::lock_user::v1::Request>(Allow),
        write::<c::admin::suspend_user::v1::Request>(Allow),
        write::<c::appservice::request_ping::v1::Request>(Allow),
        // ---- Looked at (see `inspect`).
        write::<c::message::send_message_event::v3::Request>(Inspect),
        write::<c::redact::redact_event::v3::Request>(Inspect),
        write::<c::state::send_state_event::v3::Request>(Inspect),
        write::<c::presence::set_presence::v3::Request>(Inspect),
        write::<c::receipt::create_receipt::v3::Request>(Inspect),
        write::<c::read_marker::set_read_marker::v3::Request>(Inspect),
        // ---- Refused: what the spec recommends refusing. Joining and knocking (accepting an
        // invite is a join), inviting, sending (`/send`, looked at above), changing the
        // profile. Redacting others' events is refused in `inspect`.
        write::<c::membership::join_room_by_id::v3::Request>(Refuse),
        write::<c::membership::join_room_by_id_or_alias::v3::Request>(Refuse),
        write::<c::knock::knock_room::v3::Request>(Refuse),
        write::<c::membership::invite_user::v3::Request>(Refuse),
        write::<c::profile::set_display_name::v3::Request>(Refuse),
        write::<c::profile::set_avatar_url::v3::Request>(Refuse),
        write::<c::profile::delete_profile_field::v3::Request>(Refuse),
        // The spec's "change or add, but not remove" administrative contacts.
        write::<c::account::delete_3pid::v3::Request>(Refuse),
        // ---- Refused: what others would see or get. A new room (with its invitations, name,
        // directory entry) or an upgraded one; another member's membership; the room
        // directory and aliases; typing notifications.
        write::<c::room::create_room::v3::Request>(Refuse),
        write::<c::room::upgrade_room::v3::Request>(Refuse),
        write::<c::membership::kick_user::v3::Request>(Refuse),
        write::<c::membership::ban_user::v3::Request>(Refuse),
        write::<c::membership::unban_user::v3::Request>(Refuse),
        write::<c::alias::create_alias::v3::Request>(Refuse),
        write::<c::alias::delete_alias::v3::Request>(Refuse),
        write::<c::directory::set_room_visibility::v3::Request>(Refuse),
        write::<c::typing::create_typing_event::v3::Request>(Refuse),
        // Uploads: a suspended account has nowhere to post media, and should not be a place
        // to keep files for sharing elsewhere. (A new avatar is refused anyway.)
        write::<c::media::create_content::v3::Request>(Refuse),
        write::<c::media::create_content_async::v3::Request>(Refuse),
        write::<c::media::create_mxc_uri::v1::Request>(Refuse),
        // OpenID tokens prove the account to widgets, integration managers and calls' SFUs
        // (whose token endpoint refuses a suspended account as well).
        write::<c::account::request_openid_token::v3::Request>(Refuse),
    ])
});

/// Decides whether a suspended account may make this request; may rewrite it (receipts). A
/// sender that is not suspended passes untouched.
pub(crate) async fn check<T>(services: &AppState, sender: &Sender, request: &mut T) -> Result<()>
where
    T: IncomingRequest + Send + 'static,
{
    if !sender.suspended {
        return Ok(());
    }
    match rule::<T>() {
        Rule::Allow => Ok(()),
        Rule::Refuse => Err(Error::suspended()),
        Rule::Inspect => inspect(services, sender, request).await,
    }
}

/// The rule for a request type: reading is allowed, a write as [`RULES`] says.
fn rule<T: IncomingRequest + 'static>() -> Rule {
    if T::METHOD == http::Method::GET {
        return Rule::Allow;
    }
    RULES
        .get(&TypeId::of::<T>())
        .copied()
        .unwrap_or(Rule::Refuse)
}

/// The requests a suspended account may make on conditions. A type marked [`Rule::Inspect`]
/// that is not handled here is refused.
async fn inspect(
    services: &AppState,
    sender: &Sender,
    request: &mut (dyn Any + Send),
) -> Result<()> {
    use ruma::api::client as c;
    if let Some(req) = request.downcast_ref::<c::message::send_message_event::v3::Request>() {
        // Only a redaction (v1.18 lets one go through `/send`), of the account's own event.
        if req.event_type != MessageLikeEventType::RoomRedaction {
            return Err(Error::suspended());
        }
        let target = serde_json::from_str::<serde_json::Value>(req.body.json().get())
            .ok()
            .and_then(|c| c.get("redacts")?.as_str().map(str::to_owned))
            .and_then(|id| ruma::OwnedEventId::try_from(id).ok());
        return match target {
            Some(target) => own_event(services, sender, &req.room_id, &target).await,
            None => Err(Error::suspended()),
        };
    }
    if let Some(req) = request.downcast_ref::<c::redact::redact_event::v3::Request>() {
        return own_event(services, sender, &req.room_id, &req.event_id).await;
    }
    if let Some(req) = request.downcast_ref::<c::state::send_state_event::v3::Request>() {
        // Leaving through `/state`, as `/leave` would. Nothing else: state is what everyone
        // in the room sees (and a join this way is a join).
        let leaves = req.event_type == StateEventType::RoomMember
            && req.state_key == sender.user_id.as_str()
            && serde_json::from_str::<serde_json::Value>(req.body.json().get())
                .is_ok_and(|c| c.get("membership").and_then(|m| m.as_str()) == Some("leave"));
        return if leaves {
            Ok(())
        } else {
            Err(Error::suspended())
        };
    }
    if let Some(req) = request.downcast_ref::<c::presence::set_presence::v3::Request>() {
        // Online, away or offline, but no status message for others to read.
        return if req.status_msg.as_deref().is_none_or(str::is_empty) {
            Ok(())
        } else {
            Err(Error::suspended())
        };
    }
    if let Some(req) = request.downcast_mut::<c::receipt::create_receipt::v3::Request>() {
        use c::receipt::create_receipt::v3::ReceiptType;
        if req.receipt_type == ReceiptType::Read {
            req.receipt_type = ReceiptType::ReadPrivate;
        }
        return Ok(());
    }
    if let Some(req) = request.downcast_mut::<c::read_marker::set_read_marker::v3::Request>() {
        if let Some(public) = req.read_receipt.take() {
            req.private_read_receipt.get_or_insert(public);
        }
        return Ok(());
    }
    Err(Error::suspended())
}

/// Allowed when `event_id` is an event of the account's own in `room_id`; otherwise (another
/// member's, or one this server does not have) refused.
async fn own_event(
    services: &AppState,
    sender: &Sender,
    room_id: &RoomId,
    event_id: &EventId,
) -> Result<()> {
    let own = services
        .store
        .get_event(event_id)
        .await?
        .is_some_and(|e| e.pdu.sender() == sender.user_id && e.pdu.room_id() == room_id);
    if own { Ok(()) } else { Err(Error::suspended()) }
}

#[cfg(test)]
mod tests {
    use ruma::api::client as c;

    use super::{RULES, Rule, rule};

    #[test]
    fn what_the_spec_asks_to_permit_is_allowed() {
        // Log in and new sessions; log out and delete devices; deactivate; add contacts.
        assert_eq!(rule::<c::session::login::v3::Request>(), Rule::Allow);
        assert_eq!(rule::<c::session::logout::v3::Request>(), Rule::Allow);
        assert_eq!(rule::<c::session::logout_all::v3::Request>(), Rule::Allow);
        assert_eq!(rule::<c::device::delete_device::v3::Request>(), Rule::Allow);
        assert_eq!(
            rule::<c::device::delete_devices::v3::Request>(),
            Rule::Allow
        );
        assert_eq!(rule::<c::account::deactivate::v3::Request>(), Rule::Allow);
        assert_eq!(rule::<c::account::add_3pid::v3::Request>(), Rule::Allow);
        // Verification and cross-signing, key backup.
        assert_eq!(
            rule::<c::keys::upload_signing_keys::v3::Request>(),
            Rule::Allow
        );
        assert_eq!(
            rule::<c::keys::upload_signatures::v3::Request>(),
            Rule::Allow
        );
        assert_eq!(
            rule::<c::to_device::send_event_to_device::v3::Request>(),
            Rule::Allow
        );
        assert_eq!(
            rule::<c::backup::add_backup_keys::v3::Request>(),
            Rule::Allow
        );
        // Sync (sliding sync is a POST), leave and reject invites.
        assert_eq!(
            rule::<c::sync::sync_events::msc4186::Request>(),
            Rule::Allow
        );
        assert_eq!(
            rule::<c::membership::leave_room::v3::Request>(),
            Rule::Allow
        );
        // Own redactions, looked at.
        assert_eq!(
            rule::<c::redact::redact_event::v3::Request>(),
            Rule::Inspect
        );
        assert_eq!(
            rule::<c::message::send_message_event::v3::Request>(),
            Rule::Inspect
        );
    }

    #[test]
    fn what_the_spec_recommends_refusing_is_refused() {
        assert_eq!(
            rule::<c::membership::join_room_by_id::v3::Request>(),
            Rule::Refuse
        );
        assert_eq!(
            rule::<c::membership::join_room_by_id_or_alias::v3::Request>(),
            Rule::Refuse
        );
        assert_eq!(rule::<c::knock::knock_room::v3::Request>(), Rule::Refuse);
        assert_eq!(
            rule::<c::membership::invite_user::v3::Request>(),
            Rule::Refuse
        );
        assert_eq!(
            rule::<c::profile::set_display_name::v3::Request>(),
            Rule::Refuse
        );
        assert_eq!(
            rule::<c::profile::set_avatar_url::v3::Request>(),
            Rule::Refuse
        );
        assert_eq!(rule::<c::account::delete_3pid::v3::Request>(), Rule::Refuse);
    }

    #[test]
    fn reading_is_allowed_and_an_undecided_write_refused() {
        assert_eq!(rule::<c::sync::sync_events::v3::Request>(), Rule::Allow);
        assert_eq!(
            rule::<c::message::get_message_events::v3::Request>(),
            Rule::Allow
        );
        assert!(!RULES.contains_key(&std::any::TypeId::of::<
            c::room::get_event_by_timestamp::v1::Request,
        >()));
        assert_eq!(
            rule::<c::room::get_event_by_timestamp::v1::Request>(),
            Rule::Allow,
            "a GET needs no entry"
        );
        // A write that is in no list (this server does not serve it).
        assert_eq!(rule::<c::account::bind_3pid::v3::Request>(), Rule::Refuse);
    }

    #[test]
    fn the_table_holds_writes_only() {
        // Building it asserts that no entry is a GET (which would never be looked up).
        assert!(RULES.len() > 50);
    }
}
