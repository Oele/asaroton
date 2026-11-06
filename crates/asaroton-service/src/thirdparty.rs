//! Third-party network lookups, answered by the appservices that bridge those networks.

use serde_json::{Map, Value, json};

use crate::{Error, Result, Services};

impl Services {
    /// All protocols known to any appservice, with the instances of every appservice merged.
    pub async fn thirdparty_protocols(&self) -> Result<Value> {
        let mut out: Map<String, Value> = Map::new();
        for a in self.appservices.all() {
            for name in &a.protocols {
                let path = format!("protocol/{}", urlencode(name));
                let Some(p) = self
                    .federation()
                    .appservice_thirdparty(&a.id, &path, &[])
                    .await?
                else {
                    continue;
                };
                merge_protocol(&mut out, name, p);
            }
        }
        Ok(Value::Object(out))
    }

    /// One protocol's definition.
    pub async fn thirdparty_protocol(&self, name: &str) -> Result<Value> {
        let mut out: Map<String, Value> = Map::new();
        for a in self.appservices.all() {
            if !a.protocols.iter().any(|p| p == name) {
                continue;
            }
            let path = format!("protocol/{}", urlencode(name));
            if let Some(p) = self
                .federation()
                .appservice_thirdparty(&a.id, &path, &[])
                .await?
            {
                merge_protocol(&mut out, name, p);
            }
        }
        out.remove(name)
            .ok_or_else(|| Error::not_found("Unknown protocol"))
    }

    /// Locations or users (`kind`) matching `fields`, from every appservice bridging
    /// `protocol` (or every appservice when no protocol is named).
    pub async fn thirdparty_lookup(
        &self,
        kind: &str,
        protocol: Option<&str>,
        fields: &[(String, String)],
    ) -> Result<Vec<Value>> {
        let path = match protocol {
            Some(p) => format!("{kind}/{}", urlencode(p)),
            None => kind.to_owned(),
        };
        let mut out = Vec::new();
        for a in self.appservices.all() {
            if let Some(p) = protocol
                && !a.protocols.iter().any(|x| x == p)
            {
                continue;
            }
            if protocol.is_none() && a.protocols.is_empty() {
                continue;
            }
            if let Some(Value::Array(items)) = self
                .federation()
                .appservice_thirdparty(&a.id, &path, fields)
                .await?
            {
                out.extend(items.into_iter().filter(Value::is_object));
            }
        }
        Ok(out)
    }
}

/// Merge one appservice's view of a protocol into the combined map: the first answer
/// defines the fields, later ones add their instances.
fn merge_protocol(out: &mut Map<String, Value>, name: &str, p: Value) {
    let Value::Object(mut p) = p else {
        return;
    };
    let instances = match p.remove("instances") {
        Some(Value::Array(i)) => i,
        _ => Vec::new(),
    };
    match out.get_mut(name) {
        Some(Value::Object(existing)) => {
            if let Some(Value::Array(all)) = existing.get_mut("instances") {
                all.extend(instances);
            }
        }
        _ => {
            p.insert("instances".into(), json!(instances));
            out.insert(name.to_owned(), Value::Object(p));
        }
    }
}

fn urlencode(s: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}
