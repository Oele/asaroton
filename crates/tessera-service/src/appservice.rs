//! Application services: registrations, namespaces and the checks the rest of the server
//! needs (who owns a user ID, which rooms an appservice cares about).

use std::sync::Arc;

use ruma::{OwnedUserId, RoomId, UserId};
use serde::Deserialize;

use crate::{Error, Result, Services};

/// A registration file (`id`, tokens, `url`, `sender_localpart`, `namespaces`).
#[derive(Debug, Clone, Deserialize)]
pub struct Registration {
    pub id: String,
    #[serde(default)]
    pub url: Option<String>,
    pub as_token: String,
    pub hs_token: String,
    pub sender_localpart: String,
    #[serde(default)]
    pub namespaces: Namespaces,
    #[serde(default)]
    pub rate_limited: Option<bool>,
    #[serde(default)]
    pub protocols: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Namespaces {
    #[serde(default)]
    pub users: Vec<NamespaceEntry>,
    #[serde(default)]
    pub aliases: Vec<NamespaceEntry>,
    #[serde(default)]
    pub rooms: Vec<NamespaceEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NamespaceEntry {
    #[serde(default)]
    pub exclusive: bool,
    pub regex: String,
}

/// A compiled namespace.
#[derive(Debug, Clone)]
pub struct Namespace {
    pub exclusive: bool,
    pub regex: regex::Regex,
}

impl Namespace {
    fn compile(e: &NamespaceEntry) -> Result<Self> {
        let regex = regex::Regex::new(&format!("^(?:{})$", e.regex))
            .map_err(|err| Error::internal(format!("appservice namespace regex: {err}")))?;
        Ok(Self {
            exclusive: e.exclusive,
            regex,
        })
    }
}

/// A loaded application service.
#[derive(Debug)]
pub struct Appservice {
    pub id: String,
    pub url: Option<String>,
    pub as_token: String,
    pub hs_token: String,
    pub sender: OwnedUserId,
    pub users: Vec<Namespace>,
    pub aliases: Vec<Namespace>,
    pub rooms: Vec<Namespace>,
    pub rate_limited: bool,
    pub protocols: Vec<String>,
}

impl Appservice {
    /// Whether the appservice may act as `user` (its sender, or a user in its namespaces).
    pub fn controls_user(&self, user: &UserId) -> bool {
        user == self.sender || self.users.iter().any(|n| n.regex.is_match(user.as_str()))
    }

    /// Whether `user` is in one of the appservice's *exclusive* user namespaces.
    pub fn owns_user(&self, user: &UserId) -> bool {
        self.users
            .iter()
            .any(|n| n.exclusive && n.regex.is_match(user.as_str()))
    }

    pub fn matches_alias(&self, alias: &str) -> bool {
        self.aliases.iter().any(|n| n.regex.is_match(alias))
    }

    pub fn owns_alias(&self, alias: &str) -> bool {
        self.aliases
            .iter()
            .any(|n| n.exclusive && n.regex.is_match(alias))
    }

    pub fn matches_room(&self, room_id: &RoomId) -> bool {
        self.rooms
            .iter()
            .any(|n| n.regex.is_match(room_id.as_str()))
    }
}

/// All configured appservices.
#[derive(Debug, Default)]
pub struct AppserviceRegistry {
    list: Vec<Arc<Appservice>>,
}

impl AppserviceRegistry {
    pub fn new(regs: &[Registration], server_name: &ruma::ServerName) -> Result<Self> {
        let mut list = Vec::new();
        for r in regs {
            let sender = OwnedUserId::try_from(format!("@{}:{server_name}", r.sender_localpart))
                .map_err(|e| Error::internal(format!("appservice sender: {e}")))?;
            let compile = |v: &[NamespaceEntry]| -> Result<Vec<Namespace>> {
                v.iter().map(Namespace::compile).collect()
            };
            list.push(Arc::new(Appservice {
                id: r.id.clone(),
                url: r.url.clone().filter(|u| !u.is_empty()),
                as_token: r.as_token.clone(),
                hs_token: r.hs_token.clone(),
                sender,
                users: compile(&r.namespaces.users)?,
                aliases: compile(&r.namespaces.aliases)?,
                rooms: compile(&r.namespaces.rooms)?,
                rate_limited: r.rate_limited.unwrap_or(true),
                protocols: r.protocols.clone().unwrap_or_default(),
            }));
        }
        Ok(Self { list })
    }

    pub fn all(&self) -> &[Arc<Appservice>] {
        &self.list
    }

    pub fn by_id(&self, id: &str) -> Option<Arc<Appservice>> {
        self.list.iter().find(|a| a.id == id).cloned()
    }

    pub fn by_token(&self, token: &str) -> Option<Arc<Appservice>> {
        self.list.iter().find(|a| a.as_token == token).cloned()
    }

    /// The appservice whose exclusive namespace covers `user`, if any.
    pub fn exclusive_owner_of_user(&self, user: &UserId) -> Option<Arc<Appservice>> {
        self.list.iter().find(|a| a.owns_user(user)).cloned()
    }

    pub fn exclusive_owner_of_alias(&self, alias: &str) -> Option<Arc<Appservice>> {
        self.list.iter().find(|a| a.owns_alias(alias)).cloned()
    }
}

impl Services {
    /// Whether an appservice cares about a room: a room namespace match, an alias match, or a
    /// member (joined or invited) it controls.
    pub async fn appservice_interested_in_room(
        &self,
        appservice: &Appservice,
        room_id: &RoomId,
    ) -> Result<bool> {
        if appservice.matches_room(room_id) {
            return Ok(true);
        }
        for m in self.store.members(room_id, None).await? {
            use ruma::events::room::member::MembershipState;
            if matches!(
                m.membership,
                MembershipState::Join | MembershipState::Invite
            ) && appservice.controls_user(&m.user_id)
            {
                return Ok(true);
            }
        }
        if !appservice.aliases.is_empty() {
            for a in self.store.aliases_for_room(room_id).await? {
                if appservice.matches_alias(a.alias.as_str()) {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Make sure a user an appservice acts as exists (created on first use, no password).
    pub async fn ensure_appservice_user(
        &self,
        appservice: &Appservice,
        user: &UserId,
    ) -> Result<()> {
        if !self.is_local(user) {
            return Err(Error::forbidden("User is not on this server."));
        }
        if !appservice.controls_user(user) {
            return Err(Error::matrix(
                ruma::api::error::ErrorKind::Exclusive,
                http::StatusCode::FORBIDDEN,
                "User is not in the appservice's namespace.",
            ));
        }
        if self.store.get_user(user).await?.is_none() {
            self.store
                .create_user(tessera_store::NewUser {
                    user_id: user.to_owned(),
                    password_hash: None,
                    display_name: None,
                    is_admin: false,
                    is_guest: false,
                    appservice_id: Some(appservice.id.clone()),
                })
                .await?;
        }
        Ok(())
    }

    /// Ask the appservices owning `user`'s namespace whether the user exists; the appservice
    /// is expected to register it during the query.
    pub async fn query_appservice_user(&self, user: &UserId) -> Result<bool> {
        for a in self.appservices.all() {
            if a.url.is_some()
                && a.controls_user(user)
                && a.sender != user
                && self.federation().appservice_query_user(&a.id, user).await?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Ask the appservices for an alias in their namespace; they create it during the query.
    pub async fn query_appservice_alias(&self, alias: &ruma::RoomAliasId) -> Result<bool> {
        for a in self.appservices.all() {
            if a.url.is_some()
                && a.matches_alias(alias.as_str())
                && self
                    .federation()
                    .appservice_query_alias(&a.id, alias)
                    .await?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
