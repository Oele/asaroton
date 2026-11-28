//! What state a sliding sync client asks for with a room (`required_state`), in a form that
//! holds both dialects: the unstable endpoint's `[type, state_key]` pairs (with the `*`, `$ME`
//! and `$LAZY` wildcards) and `/v4/sync`'s `include` / `exclude` / `lazy_members` (MSC4186).
//!
//! One list or subscription asks with one [`RequiredStateSpec`]; a room in several of them gets
//! their union, a [`RequiredState`].

use std::collections::BTreeSet;

use ruma::UserId;

/// Which state keys a matcher covers.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeyMatch {
    /// Every state key.
    Any,
    /// The requesting user's ID.
    Me,
    /// Exactly this one.
    Exact(String),
}

/// A `(type, state_key)` pattern. `ty: None` covers every type.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StateMatcher {
    pub ty: Option<String>,
    pub key: KeyMatch,
}

impl StateMatcher {
    fn matches(&self, ty: &str, key: &str, user: &UserId) -> bool {
        self.ty.as_deref().is_none_or(|t| t == ty)
            && match &self.key {
                KeyMatch::Any => true,
                KeyMatch::Me => key == user.as_str(),
                KeyMatch::Exact(k) => k == key,
            }
    }

    /// Whether this covers every membership event.
    fn covers_all_members(&self) -> bool {
        self.ty.as_deref().is_none_or(|t| t == MEMBER) && self.key == KeyMatch::Any
    }

    fn is_member_type(&self) -> bool {
        self.ty.as_deref() == Some(MEMBER)
    }
}

const MEMBER: &str = "m.room.member";

/// What one list or room subscription asks for.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequiredStateSpec {
    pub include: BTreeSet<StateMatcher>,
    pub exclude: BTreeSet<StateMatcher>,
    /// Members are loaded lazily: the memberships of the people the answer is about, and the
    /// user's own.
    pub lazy_members: bool,
    /// `/v4/sync`: with lazy members, an include without a type does not cover memberships
    /// (only one that names `m.room.member` does).
    pub lazy_narrows_wildcards: bool,
}

impl RequiredStateSpec {
    /// From the unstable endpoint's pairs: `*` for any type or key, `$ME` for the user's ID,
    /// and `$LAZY` (with `m.room.member` or `*`) for lazily loaded members.
    pub fn from_unstable<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
        let mut spec = Self::default();
        for (ty, key) in pairs {
            if key == "$LAZY" {
                if ty == MEMBER || ty == "*" {
                    spec.lazy_members = true;
                }
                continue;
            }
            spec.include.insert(StateMatcher {
                ty: (ty != "*").then(|| ty.to_owned()),
                key: match key {
                    "*" => KeyMatch::Any,
                    "$ME" => KeyMatch::Me,
                    other => KeyMatch::Exact(other.to_owned()),
                },
            });
        }
        spec
    }

    /// Whether `(ty, key)` is wanted. `lazy_member` says whose membership lazy loading wants,
    /// beside the user's own.
    fn matches(
        &self,
        ty: &str,
        key: &str,
        user: &UserId,
        lazy_member: &impl Fn(&UserId) -> bool,
    ) -> bool {
        if self.lazy_members
            && ty == MEMBER
            && (key == user.as_str() || <&UserId>::try_from(key).is_ok_and(lazy_member))
        {
            return true;
        }
        let narrowed = self.lazy_members && self.lazy_narrows_wildcards && ty == MEMBER;
        self.include
            .iter()
            .filter(|m| !narrowed || m.is_member_type())
            .any(|m| m.matches(ty, key, user))
            && !self.exclude.iter().any(|m| m.matches(ty, key, user))
    }

    /// Whether this asks for every membership on its own terms (then nobody's are lazy).
    ///
    /// On `/v4/sync` an include that names `m.room.member` without a `state_key` asks for
    /// every member, also with `lazy_members` (open question 1 of the v4 plan): only an
    /// include without a type is narrowed by lazy loading, as in MSC4186's `include: [{}]`.
    fn includes_all_members(&self) -> bool {
        self.exclude
            .iter()
            .all(|m| m.ty.as_deref().is_some_and(|t| t != MEMBER))
            && self.include.iter().any(|m| {
                m.covers_all_members()
                    && !(self.lazy_members && self.lazy_narrows_wildcards && m.ty.is_none())
            })
    }

    fn is_empty(&self) -> bool {
        self.include.is_empty() && !self.lazy_members
    }
}

/// A room's demand: the union of the specs of every list and subscription it is in.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RequiredState(BTreeSet<RequiredStateSpec>);

impl RequiredState {
    /// One spec's demand.
    pub fn of(spec: &RequiredStateSpec) -> Self {
        let mut r = Self::default();
        r.add(spec);
        r
    }

    pub fn add(&mut self, spec: &RequiredStateSpec) {
        if !spec.is_empty() {
            self.0.insert(spec.clone());
        }
    }

    pub fn extend(&mut self, other: &Self) {
        for spec in &other.0 {
            self.add(spec);
        }
    }

    /// Nothing asked for.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether a state event `(ty, key)` is wanted.
    pub fn matches(
        &self,
        ty: &str,
        key: &str,
        user: &UserId,
        lazy_member: impl Fn(&UserId) -> bool,
    ) -> bool {
        self.0
            .iter()
            .any(|s| s.matches(ty, key, user, &lazy_member))
    }

    /// Members are loaded lazily: some spec asks for that, and none asks for every membership
    /// anyway.
    pub fn loads_members_lazily(&self) -> bool {
        self.0.iter().any(|s| s.lazy_members)
            && !self.0.iter().any(RequiredStateSpec::includes_all_members)
    }

    /// Whether this asks for something `before` did not, so that the room's state is sent in
    /// full. Specs without excludes are compared by their patterns (so that dropping a pattern
    /// from a list is no expansion, as on the unstable endpoint), others as wholes.
    pub fn expands(&self, before: &Self) -> bool {
        let plain = |r: &Self| -> (BTreeSet<StateMatcher>, bool) {
            let mut include = BTreeSet::new();
            let mut lazy = false;
            for s in
                r.0.iter()
                    .filter(|s| s.exclude.is_empty() && !s.lazy_narrows_wildcards)
            {
                include.extend(s.include.iter().cloned());
                lazy |= s.lazy_members;
            }
            (include, lazy)
        };
        let (old_include, old_lazy) = plain(before);
        self.0.iter().any(|s| {
            if s.exclude.is_empty() && !s.lazy_narrows_wildcards {
                !s.include.is_subset(&old_include) || (s.lazy_members && !old_lazy)
            } else {
                !before.0.contains(s)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unstable(pairs: &[(&str, &str)]) -> RequiredState {
        let mut r = RequiredState::default();
        r.add(&RequiredStateSpec::from_unstable(pairs.iter().copied()));
        r
    }

    fn user() -> ruma::OwnedUserId {
        ruma::OwnedUserId::try_from("@me:x.test").unwrap()
    }

    #[test]
    fn unstable_pairs_match_as_before() {
        let me = user();
        let r = unstable(&[
            ("m.room.name", ""),
            ("m.room.member", "$ME"),
            ("org.x", "*"),
        ]);
        assert!(r.matches("m.room.name", "", &me, |_| false));
        assert!(!r.matches("m.room.name", "other", &me, |_| false));
        assert!(r.matches("m.room.member", "@me:x.test", &me, |_| false));
        assert!(!r.matches("m.room.member", "@you:x.test", &me, |_| false));
        assert!(r.matches("org.x", "anything", &me, |_| false));
        let all = unstable(&[("*", "*")]);
        assert!(all.matches("m.room.member", "@you:x.test", &me, |_| false));
        assert!(!all.loads_members_lazily());
    }

    #[test]
    fn lazy_members_want_the_people_the_answer_is_about() {
        let me = user();
        let r = unstable(&[("m.room.member", "$LAZY"), ("m.room.name", "")]);
        assert!(r.loads_members_lazily());
        let about = |u: &UserId| u.as_str() == "@you:x.test";
        assert!(r.matches("m.room.member", "@you:x.test", &me, about));
        assert!(r.matches("m.room.member", "@me:x.test", &me, about));
        assert!(!r.matches("m.room.member", "@them:x.test", &me, about));
        // A pattern for every membership switches lazy loading off.
        let mut both = r.clone();
        both.add(&RequiredStateSpec::from_unstable([("m.room.member", "*")]));
        assert!(!both.loads_members_lazily());
        let star = unstable(&[("*", "$LAZY")]);
        assert!(star.loads_members_lazily());
    }

    #[test]
    fn v4_lazy_members_narrow_a_typeless_include_and_excludes_apply() {
        let me = user();
        let spec = RequiredStateSpec {
            include: [StateMatcher {
                ty: None,
                key: KeyMatch::Any,
            }]
            .into(),
            exclude: [StateMatcher {
                ty: Some("m.room.topic".into()),
                key: KeyMatch::Any,
            }]
            .into(),
            lazy_members: true,
            lazy_narrows_wildcards: true,
        };
        let mut r = RequiredState::default();
        r.add(&spec);
        assert!(
            r.loads_members_lazily(),
            "include: [{{}}] with lazy members stays lazy"
        );
        assert!(r.matches("m.room.name", "", &me, |_| false));
        assert!(!r.matches("m.room.topic", "", &me, |_| false));
        assert!(!r.matches("m.room.member", "@them:x.test", &me, |_| false));
        assert!(r.matches("m.room.member", "@them:x.test", &me, |_| true));
    }

    #[test]
    fn v4_an_include_naming_the_member_type_asks_for_every_member() {
        let me = user();
        let spec = RequiredStateSpec {
            include: [
                StateMatcher {
                    ty: None,
                    key: KeyMatch::Any,
                },
                StateMatcher {
                    ty: Some("m.room.member".into()),
                    key: KeyMatch::Any,
                },
            ]
            .into(),
            exclude: BTreeSet::new(),
            lazy_members: true,
            lazy_narrows_wildcards: true,
        };
        let r = RequiredState::of(&spec);
        assert!(!r.loads_members_lazily());
        assert!(r.matches("m.room.member", "@them:x.test", &me, |_| false));
        // One named member beside lazy loading stays lazy, and gets that member.
        let one = RequiredStateSpec {
            include: [
                StateMatcher {
                    ty: None,
                    key: KeyMatch::Any,
                },
                StateMatcher {
                    ty: Some("m.room.member".into()),
                    key: KeyMatch::Exact("@friend:x.test".into()),
                },
            ]
            .into(),
            ..spec
        };
        let r = RequiredState::of(&one);
        assert!(r.loads_members_lazily());
        assert!(r.matches("m.room.member", "@friend:x.test", &me, |_| false));
        assert!(!r.matches("m.room.member", "@them:x.test", &me, |_| false));
        // A literal `*` or `$ME` is just a string on v4.
        let literal = RequiredState::of(&RequiredStateSpec {
            include: [StateMatcher {
                ty: Some("m.room.member".into()),
                key: KeyMatch::Exact("$ME".into()),
            }]
            .into(),
            ..RequiredStateSpec::default()
        });
        assert!(!literal.matches("m.room.member", "@me:x.test", &me, |_| false));
        assert!(literal.matches("m.room.member", "$ME", &me, |_| false));
    }

    #[test]
    fn dropping_a_pattern_is_no_expansion_adding_one_is() {
        let before = unstable(&[("m.room.name", ""), ("m.room.topic", "")]);
        assert!(!unstable(&[("m.room.name", "")]).expands(&before));
        assert!(unstable(&[("m.room.name", ""), ("m.room.avatar", "")]).expands(&before));
        assert!(unstable(&[("m.room.name", ""), ("m.room.member", "$LAZY")]).expands(&before));
        assert!(!RequiredState::default().expands(&before));
    }
}
