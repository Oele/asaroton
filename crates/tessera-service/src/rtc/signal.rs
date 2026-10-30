//! The SFU signalling proxy: clients open their `LiveKit` WebSocket on tessera, which forwards
//! it to the SFU on the loopback interface. Besides sparing the admin a WebSocket rule in the
//! reverse proxy, this lets tessera correct one thing in the SFU's answers: `LiveKit` always
//! tells clients that its TURN-over-TLS relay listens on port 443, whatever port it was given
//! (its maintainers expect a load balancer there), so clients would never find a relay on
//! another port. Rewriting the port in the `turns:` URLs of the join and reconnect responses
//! lets the relay run on any port the server has free.
//!
//! The responses are protobuf messages. Only the generic wire format is interpreted here; every
//! field that is not a `turns:` URL under `ice_servers` is passed through byte for byte.

/// Path, under the client API, of the signalling proxy; `LiveKit` clients append `/rtc`.
pub const SIGNAL_PATH: &str = "/_matrix/client/unstable/io.element.msc4195/rtc/livekit/signal";

/// The WebSocket URL of the signalling proxy for a given public base URL.
pub fn signal_url(public_base_url: &str) -> String {
    let base = public_base_url.trim_end_matches('/');
    let ws = base
        .strip_prefix("https://")
        .map(|rest| format!("wss://{rest}"))
        .or_else(|| {
            base.strip_prefix("http://")
                .map(|rest| format!("ws://{rest}"))
        })
        .unwrap_or_else(|| base.to_owned());
    format!("{ws}{SIGNAL_PATH}")
}

// Field numbers from livekit_rtc.proto.
const SIGNAL_JOIN: u32 = 1;
const SIGNAL_RECONNECT: u32 = 19;
const JOIN_ICE_SERVERS: u32 = 5;
const RECONNECT_ICE_SERVERS: u32 = 1;
const ICE_SERVER_URLS: u32 = 1;

/// Rewrites the port of every `turns:` URL in a `SignalResponse` frame to `port`. Returns
/// `None` when nothing changed, including when the frame is not a message this understands.
pub fn rewrite_turn_port(frame: &[u8], port: u16) -> Option<Vec<u8>> {
    let mut changed = false;
    let out = map_fields(frame, &mut |num, data| match num {
        SIGNAL_JOIN => map_ice_container(data, JOIN_ICE_SERVERS, port, &mut changed),
        SIGNAL_RECONNECT => map_ice_container(data, RECONNECT_ICE_SERVERS, port, &mut changed),
        _ => None,
    })?;
    changed.then_some(out)
}

fn map_ice_container(
    data: &[u8],
    ice_field: u32,
    port: u16,
    changed: &mut bool,
) -> Option<Vec<u8>> {
    map_fields(data, &mut |num, inner| {
        (num == ice_field)
            .then(|| map_ice_server(inner, port, changed))
            .flatten()
    })
}

fn map_ice_server(data: &[u8], port: u16, changed: &mut bool) -> Option<Vec<u8>> {
    map_fields(data, &mut |num, url| {
        if num != ICE_SERVER_URLS {
            return None;
        }
        let url = std::str::from_utf8(url).ok()?;
        let rewritten = rewrite_url(url, port)?;
        *changed = true;
        Some(rewritten.into_bytes())
    })
}

/// `turns:host:443?transport=tcp` → `turns:host:<port>?transport=tcp`.
fn rewrite_url(url: &str, port: u16) -> Option<String> {
    let rest = url.strip_prefix("turns:")?;
    let (host_port, query) = rest
        .split_once('?')
        .map_or((rest, None), |(h, q)| (h, Some(q)));
    let (host, old_port) = host_port.rsplit_once(':')?;
    if old_port == port.to_string() || !old_port.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(match query {
        Some(q) => format!("turns:{host}:{port}?{q}"),
        None => format!("turns:{host}:{port}"),
    })
}

/// Replaces the payload of a length-delimited field, given its number and payload.
type FieldMapper<'a> = dyn FnMut(u32, &[u8]) -> Option<Vec<u8>> + 'a;

/// Re-encodes a protobuf message, giving `f` the chance to replace the payload of each
/// length-delimited field. Returns `None` if the bytes are not well-formed protobuf.
fn map_fields(data: &[u8], f: &mut FieldMapper<'_>) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(data.len() + 8);
    let mut pos = 0;
    while pos < data.len() {
        let start = pos;
        let (tag, n) = read_varint(&data[pos..])?;
        pos += n;
        let num = u32::try_from(tag >> 3).ok()?;
        match tag & 7 {
            0 => {
                let (_, n) = read_varint(&data[pos..])?;
                pos += n;
                out.extend_from_slice(&data[start..pos]);
            }
            1 => {
                pos = pos.checked_add(8).filter(|p| *p <= data.len())?;
                out.extend_from_slice(&data[start..pos]);
            }
            5 => {
                pos = pos.checked_add(4).filter(|p| *p <= data.len())?;
                out.extend_from_slice(&data[start..pos]);
            }
            2 => {
                let (len, n) = read_varint(&data[pos..])?;
                pos += n;
                let end = pos.checked_add(usize::try_from(len).ok()?)?;
                let payload = data.get(pos..end)?;
                match f(num, payload) {
                    Some(replaced) => {
                        write_varint(&mut out, tag);
                        write_varint(&mut out, replaced.len() as u64);
                        out.extend_from_slice(&replaced);
                    }
                    None => out.extend_from_slice(&data[start..end]),
                }
                pos = end;
            }
            _ => return None,
        }
    }
    Some(out)
}

fn read_varint(data: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    for (i, byte) in data.iter().enumerate().take(10) {
        value |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return Some((value, i + 1));
        }
    }
    None
}

#[allow(clippy::cast_possible_truncation)] // each pushed value is masked to seven bits
fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(((value & 0x7f) as u8) | 0x80);
        value >>= 7;
    }
    out.push((value & 0x7f) as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn len_field(num: u32, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        write_varint(&mut out, u64::from(num) << 3 | 2);
        write_varint(&mut out, payload.len() as u64);
        out.extend_from_slice(payload);
        out
    }

    fn varint_field(num: u32, value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        write_varint(&mut out, u64::from(num) << 3);
        write_varint(&mut out, value);
        out
    }

    fn join_frame(turns_url: &str) -> Vec<u8> {
        let ice = [
            len_field(1, turns_url.as_bytes()),
            len_field(1, b"turn:203.0.113.5:3478?transport=udp"),
            len_field(2, b"user"),
            len_field(3, b"secret"),
        ]
        .concat();
        let join = [
            len_field(4, b"1.13.11"),
            len_field(5, &ice),
            varint_field(6, 1),
            varint_field(10, 15),
        ]
        .concat();
        len_field(SIGNAL_JOIN, &join)
    }

    #[test]
    fn rewrites_only_the_turns_port() {
        let frame = join_frame("turns:sfu.example.org:443?transport=tcp");
        let out = rewrite_turn_port(&frame, 5349).expect("changed");
        assert_eq!(out, join_frame("turns:sfu.example.org:5349?transport=tcp"));
    }

    #[test]
    fn reconnect_responses_too() {
        let ice = len_field(1, b"turns:sfu.example.org:443?transport=tcp");
        let frame = len_field(
            SIGNAL_RECONNECT,
            &[len_field(1, &ice), varint_field(4, 7)].concat(),
        );
        let out = rewrite_turn_port(&frame, 5349).expect("changed");
        let ice = len_field(1, b"turns:sfu.example.org:5349?transport=tcp");
        assert_eq!(
            out,
            len_field(
                SIGNAL_RECONNECT,
                &[len_field(1, &ice), varint_field(4, 7)].concat()
            )
        );
    }

    #[test]
    fn leaves_other_frames_alone() {
        assert!(
            rewrite_turn_port(
                &join_frame("turns:sfu.example.org:5349?transport=tcp"),
                5349
            )
            .is_none()
        );
        assert!(rewrite_turn_port(&len_field(2, b"offer"), 5349).is_none());
        assert!(rewrite_turn_port(&[0xff, 0xff, 0xff], 5349).is_none());
    }

    #[test]
    fn signal_urls() {
        assert_eq!(
            signal_url("https://matrix.example.org/"),
            format!("wss://matrix.example.org{SIGNAL_PATH}")
        );
        assert_eq!(
            signal_url("http://localhost:8008"),
            format!("ws://localhost:8008{SIGNAL_PATH}")
        );
    }
}
