//! Interest propagation and cross-account delivery functions.
//!
//! Propagates LS+/LS-, RS+/RS-, and gateway interest changes to connected
//! leaf, route, and gateway peers. Also handles cross-account message delivery
//! and gateway reply rewriting.

#[cfg(any(feature = "hub", feature = "cluster", feature = "gateway"))]
use crate::nats_proto;
#[cfg(any(
    feature = "hub",
    feature = "cluster",
    feature = "gateway",
    feature = "accounts"
))]
use crate::server::ServerState;
#[cfg(any(
    feature = "hub",
    feature = "cluster",
    feature = "gateway",
    feature = "accounts"
))]
use crate::sub_list::DirectWriter;

#[cfg(feature = "gateway")]
use std::cell::RefCell;

#[cfg(feature = "accounts")]
use crate::handler::deliver_to_sub_inner;
#[cfg(any(feature = "accounts", feature = "gateway"))]
use crate::handler::WorkerCtx;
#[cfg(any(feature = "accounts", feature = "gateway"))]
use crate::types::HeaderMap;

/// Propagate LS+ (`is_sub=true`) or LS- (`is_sub=false`) to all inbound leaf connections.
///
/// Filtered by each leaf's publish permissions.
#[cfg(feature = "hub")]
pub(crate) fn propagate_leaf_interest(
    state: &ServerState,
    subject: &[u8],
    queue: Option<&[u8]>,
    is_sub: bool,
) {
    let writers = state.leaf_writers.read().unwrap();
    if writers.is_empty() {
        return;
    }
    let mut builder = nats_proto::MsgBuilder::new();
    let data = if is_sub {
        if let Some(q) = queue {
            builder.build_leaf_sub_queue(subject, q)
        } else {
            builder.build_leaf_sub(subject)
        }
    } else if let Some(q) = queue {
        builder.build_leaf_unsub_queue(subject, q)
    } else {
        builder.build_leaf_unsub(subject)
    };
    let subject_str = std::str::from_utf8(subject).unwrap_or("");
    for (_conn_id, (writer, perms)) in writers.iter() {
        if let Some(ref p) = perms {
            if !p.publish.is_allowed(subject_str) {
                continue;
            }
        }
        writer.write_raw(data);
        writer.notify();
    }
}

/// Send LS+ for all existing client subscriptions to a given leaf's DirectWriter.
///
/// Filters by the leaf's publish permissions — only sends LS+ for subjects that
/// the leaf is allowed to publish on (controls what the leaf can export).
#[cfg(feature = "hub")]
pub(crate) fn send_existing_subs(
    state: &ServerState,
    writer: &DirectWriter,
    leaf_perms: &Option<std::sync::Arc<crate::server::Permissions>>,
) {
    let mut builder = nats_proto::MsgBuilder::new();

    #[cfg(feature = "accounts")]
    {
        for account_sub in &state.account_subs {
            let subs = account_sub.read().unwrap();
            for (subject, queue) in subs.client_interests() {
                if let Some(ref p) = leaf_perms {
                    if !p.publish.is_allowed(subject) {
                        continue;
                    }
                }
                let data = if let Some(q) = queue {
                    builder.build_leaf_sub_queue(subject.as_bytes(), q.as_bytes())
                } else {
                    builder.build_leaf_sub(subject.as_bytes())
                };
                writer.write_raw(data);
            }
        }
    }
    #[cfg(not(feature = "accounts"))]
    {
        let subs = state.subs.read().unwrap();
        for (subject, queue) in subs.client_interests() {
            if let Some(ref p) = leaf_perms {
                if !p.publish.is_allowed(subject) {
                    continue;
                }
            }
            let data = if let Some(q) = queue {
                builder.build_leaf_sub_queue(subject.as_bytes(), q.as_bytes())
            } else {
                builder.build_leaf_sub(subject.as_bytes())
            };
            writer.write_raw(data);
        }
    }
    writer.notify();
}

/// Propagate RS+ (`is_sub=true`) or RS- (`is_sub=false`) to all inbound route connections.
#[cfg(feature = "cluster")]
pub(crate) fn propagate_route_interest(
    state: &ServerState,
    subject: &[u8],
    queue: Option<&[u8]>,
    is_sub: bool,
    #[cfg(feature = "accounts")] account: &[u8],
) {
    let writers = state.route_writers.read().unwrap();
    if writers.is_empty() {
        return;
    }
    let mut builder = nats_proto::MsgBuilder::new();
    let data = if is_sub {
        if let Some(q) = queue {
            builder.build_route_sub_queue(
                subject,
                q,
                #[cfg(feature = "accounts")]
                account,
            )
        } else {
            builder.build_route_sub(
                subject,
                #[cfg(feature = "accounts")]
                account,
            )
        }
    } else if let Some(q) = queue {
        builder.build_route_unsub_queue(
            subject,
            q,
            #[cfg(feature = "accounts")]
            account,
        )
    } else {
        builder.build_route_unsub(
            subject,
            #[cfg(feature = "accounts")]
            account,
        )
    };
    for writer in writers.values() {
        writer.write_raw(data);
        writer.notify();
    }
}

/// Send RS+ for all existing local subscriptions to a given route's DirectWriter.
#[cfg(feature = "cluster")]
pub(crate) fn send_existing_subs_to_route(state: &ServerState, writer: &DirectWriter) {
    let mut builder = nats_proto::MsgBuilder::new();

    #[cfg(feature = "accounts")]
    {
        for (idx, account_sub) in state.account_subs.iter().enumerate() {
            let acct = state
                .account_name(idx as crate::server::AccountId)
                .as_bytes();
            let subs = account_sub.read().unwrap();
            for (subject, queue) in subs.local_interests() {
                let data = if let Some(q) = queue {
                    builder.build_route_sub_queue(
                        subject.as_bytes(),
                        q.as_bytes(),
                        #[cfg(feature = "accounts")]
                        acct,
                    )
                } else {
                    builder.build_route_sub(
                        subject.as_bytes(),
                        #[cfg(feature = "accounts")]
                        acct,
                    )
                };
                writer.write_raw(data);
            }
        }
    }
    #[cfg(not(feature = "accounts"))]
    {
        let subs = state.subs.read().unwrap();
        for (subject, queue) in subs.local_interests() {
            let data = if let Some(q) = queue {
                builder.build_route_sub_queue(subject.as_bytes(), q.as_bytes())
            } else {
                builder.build_route_sub(subject.as_bytes())
            };
            writer.write_raw(data);
        }
    }
    writer.notify();
}

#[cfg(feature = "gateway")]
thread_local! {
    static GW_BUILDER: RefCell<nats_proto::MsgBuilder> = RefCell::new(nats_proto::MsgBuilder::new());
}

/// Propagate RS+ (`is_sub=true`) or RS- (`is_sub=false`) to all gateway connections.
///
/// Skipped for outbound gateways in Optimistic mode.
#[cfg(feature = "gateway")]
pub(crate) fn propagate_gateway_interest(
    state: &ServerState,
    subject: &[u8],
    queue: Option<&[u8]>,
    is_sub: bool,
    #[cfg(feature = "accounts")] account: &[u8],
) {
    use crate::server::GatewayInterestMode;

    let writers = state.gateway_writers.read().unwrap();
    if writers.is_empty() {
        return;
    }

    let gi = state.gateway_interest.read().unwrap();

    GW_BUILDER.with(|cell| {
        let mut builder = cell.borrow_mut();
        let data = if is_sub {
            if let Some(q) = queue {
                builder.build_route_sub_queue(
                    subject,
                    q,
                    #[cfg(feature = "accounts")]
                    account,
                )
            } else {
                builder.build_route_sub(
                    subject,
                    #[cfg(feature = "accounts")]
                    account,
                )
            }
        } else if let Some(q) = queue {
            builder.build_route_unsub_queue(
                subject,
                q,
                #[cfg(feature = "accounts")]
                account,
            )
        } else {
            builder.build_route_unsub(
                subject,
                #[cfg(feature = "accounts")]
                account,
            )
        };
        for (conn_id, writer) in writers.iter() {
            // Skip outbound gateways in Optimistic/Transitioning mode.
            if let Some(gis) = gi.get(conn_id) {
                if gis.mode == GatewayInterestMode::Optimistic
                    || gis.mode == GatewayInterestMode::Transitioning
                {
                    continue;
                }
            }
            writer.write_raw(data);
            writer.notify();
        }
    });
}

/// Send RS+ for all existing local subscriptions to a given gateway's DirectWriter.
#[cfg(feature = "gateway")]
pub(crate) fn send_existing_subs_to_gateway(state: &ServerState, writer: &DirectWriter) {
    let mut builder = nats_proto::MsgBuilder::new();

    #[cfg(feature = "accounts")]
    {
        for (idx, account_sub) in state.account_subs.iter().enumerate() {
            let acct = state
                .account_name(idx as crate::server::AccountId)
                .as_bytes();
            let subs = account_sub.read().unwrap();
            for (subject, queue) in subs.local_interests() {
                let data = if let Some(q) = queue {
                    builder.build_route_sub_queue(
                        subject.as_bytes(),
                        q.as_bytes(),
                        #[cfg(feature = "accounts")]
                        acct,
                    )
                } else {
                    builder.build_route_sub(
                        subject.as_bytes(),
                        #[cfg(feature = "accounts")]
                        acct,
                    )
                };
                writer.write_raw(data);
            }
        }
    }
    #[cfg(not(feature = "accounts"))]
    {
        let subs = state.subs.read().unwrap();
        for (subject, queue) in subs.local_interests() {
            let data = if let Some(q) = queue {
                builder.build_route_sub_queue(subject.as_bytes(), q.as_bytes())
            } else {
                builder.build_route_sub(subject.as_bytes())
            };
            writer.write_raw(data);
        }
    }
    writer.notify();
}

/// Forward a message to outbound gateways in optimistic mode.
///
/// Called after `deliver_to_subs` when gateway subs may not exist in SubList
/// (because the remote hasn't sent RS+ yet). In optimistic mode, we forward
/// unless the subject is in the gateway's negative interest set.
#[cfg(feature = "gateway")]
pub(crate) fn forward_to_optimistic_gateways(
    wctx: &mut WorkerCtx<'_>,
    subject: &[u8],
    subject_str: &str,
    reply: Option<&[u8]>,
    headers: Option<&HeaderMap>,
    payload: &[u8],
    #[cfg(feature = "accounts")] account: &[u8],
) {
    use crate::server::GatewayInterestMode;

    let gi = wctx.state.gateway_interest.read().unwrap();
    if gi.is_empty() {
        return;
    }

    let payload_len = payload.len() as u64;

    for gis in gi.values() {
        if gis.mode != GatewayInterestMode::Optimistic {
            continue;
        }
        // Skip if subject is in the negative interest set.
        if gis.ni.contains(subject_str) {
            continue;
        }
        // Check if a gateway sub already matched (delivered via SubList).
        // If the SubList already has a gateway sub for this subject, deliver_to_subs
        // already wrote to the writer, so skip to avoid duplicate delivery.
        // We check has_local_interest inverted — if a gateway sub exists in SubList
        // for this conn_id, the message was already delivered.
        // Actually, in optimistic mode we don't insert gateway subs into SubList,
        // so there's no duplication risk. Forward unconditionally (unless ni'd).

        // Rewrite reply with _GR_ prefix before forwarding across gateway.
        let gw_reply = rewrite_gateway_reply(reply, wctx.state);
        gis.writer.write_rmsg(
            subject,
            gw_reply.as_deref(),
            headers,
            payload,
            #[cfg(feature = "accounts")]
            account,
        );

        // Batch-accumulate notification (same as deliver_to_subs) instead of
        // per-message notify() to avoid one eventfd write syscall per PUB.
        let fd = gis.writer.event_raw_fd();
        if !wctx.pending_notify[..*wctx.pending_notify_count].contains(&fd)
            && *wctx.pending_notify_count < wctx.pending_notify.len()
        {
            wctx.pending_notify[*wctx.pending_notify_count] = fd;
            *wctx.pending_notify_count += 1;
        }

        *wctx.msgs_delivered += 1;
        *wctx.msgs_delivered_bytes += payload_len;
    }
}

/// Rewrite outbound reply: `reply` → `_GR_.<cluster_hash>.<server_hash>.reply`.
/// Returns `None` if the input reply is `None`.
#[cfg(feature = "gateway")]
pub(crate) fn rewrite_gateway_reply(
    reply: Option<&[u8]>,
    state: &ServerState,
) -> Option<bytes::Bytes> {
    let reply = reply?;
    // Don't double-rewrite
    if reply.starts_with(b"_GR_.") {
        return Some(bytes::Bytes::copy_from_slice(reply));
    }
    let prefix = &state.gateway_reply_prefix;
    let mut buf = Vec::with_capacity(prefix.len() + reply.len());
    buf.extend_from_slice(prefix);
    buf.extend_from_slice(reply);
    Some(bytes::Bytes::from(buf))
}

/// Unwrap inbound reply as a zero-copy `Bytes` sub-slice.
/// Returns a `Bytes::slice()` into the original buffer — no heap allocation.
#[cfg(feature = "gateway")]
pub(crate) fn unwrap_gateway_reply_bytes(reply: &bytes::Bytes) -> bytes::Bytes {
    if !reply.starts_with(b"_GR_.") {
        return reply.clone();
    }
    let after_prefix = &reply[5..];
    let dot1 = match memchr::memchr(b'.', after_prefix) {
        Some(i) => i,
        None => return reply.clone(),
    };
    let rest = &after_prefix[dot1 + 1..];
    let dot2 = match memchr::memchr(b'.', rest) {
        Some(i) => i,
        None => return reply.clone(),
    };
    let start = 5 + dot1 + 1 + dot2 + 1;
    reply.slice(start..)
}

/// Deliver a message to cross-account subscribers (worker context).
///
/// Called after same-account `deliver_to_subs()`. For each cross-account route
/// whose export pattern matches the published subject, delivers to the
/// destination account's SubList (optionally remapping the subject).
///
/// Single-hop: cross-account delivery does NOT recursively trigger more
/// cross-account forwarding.
#[cfg(feature = "accounts")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_cross_account(
    wctx: &mut WorkerCtx<'_>,
    subject: &[u8],
    subject_str: &str,
    reply: Option<&[u8]>,
    headers: Option<&HeaderMap>,
    payload: &[u8],
    src_account_id: crate::server::AccountId,
) -> Vec<(u64, u64)> {
    let routes = match wctx.state.cross_account_routes.get(src_account_id as usize) {
        Some(r) if !r.is_empty() => r,
        _ => return Vec::new(),
    };

    let payload_len = payload.len() as u64;
    let mut all_expired = Vec::new();

    for route in routes {
        if !crate::sub_list::subject_matches(&route.export_pattern, subject_str) {
            continue;
        }

        let (dst_subject_str, dst_subject_bytes);
        match &route.remap {
            Some(r) => {
                dst_subject_str =
                    crate::sub_list::remap_subject(&r.from_pattern, &r.to_pattern, subject_str);
                dst_subject_bytes = dst_subject_str.as_bytes();
            }
            None => {
                dst_subject_str = subject_str.to_string();
                dst_subject_bytes = subject;
            }
        };

        let dst_acct_name = wctx.state.account_name(route.dst_account_id).as_bytes();

        let subs = wctx.state.get_subs(route.dst_account_id).read().unwrap();
        let (_count, expired) = subs.for_each_match(&dst_subject_str, |sub| {
            let did_deliver = deliver_to_sub_inner(
                sub,
                dst_subject_bytes,
                reply,
                headers,
                payload,
                dst_acct_name,
            );
            if !did_deliver {
                return;
            }
            *wctx.msgs_delivered += 1;
            *wctx.msgs_delivered_bytes += payload_len;
            let fd = sub.writer.event_raw_fd();
            if fd != wctx.event_fd
                && !wctx.pending_notify[..*wctx.pending_notify_count].contains(&fd)
                && *wctx.pending_notify_count < wctx.pending_notify.len()
            {
                wctx.pending_notify[*wctx.pending_notify_count] = fd;
                *wctx.pending_notify_count += 1;
            }
        });
        drop(subs);
        all_expired.extend(expired);
    }

    all_expired
}

/// Deliver a message to cross-account subscribers (upstream/route reader thread).
///
/// Same as `deliver_cross_account` but for contexts outside the worker event loop.
/// Accumulates dirty writers for batch notification.
#[cfg(feature = "accounts")]
#[allow(clippy::too_many_arguments)]
pub(crate) fn deliver_cross_account_upstream(
    state: &ServerState,
    subject: &[u8],
    subject_str: &str,
    reply: Option<&[u8]>,
    headers: Option<&HeaderMap>,
    payload: &[u8],
    dirty_writers: &mut Vec<DirectWriter>,
    src_account_id: crate::server::AccountId,
) -> Vec<(u64, u64)> {
    let routes = match state.cross_account_routes.get(src_account_id as usize) {
        Some(r) if !r.is_empty() => r,
        _ => return Vec::new(),
    };

    let mut all_expired = Vec::new();

    for route in routes {
        if !crate::sub_list::subject_matches(&route.export_pattern, subject_str) {
            continue;
        }

        let (dst_subject_str, dst_subject_bytes_owned);
        match &route.remap {
            Some(r) => {
                dst_subject_str =
                    crate::sub_list::remap_subject(&r.from_pattern, &r.to_pattern, subject_str);
                dst_subject_bytes_owned = Some(dst_subject_str.as_bytes().to_vec());
            }
            None => {
                dst_subject_str = subject_str.to_string();
                dst_subject_bytes_owned = None;
            }
        };
        let dst_subject_bytes = dst_subject_bytes_owned.as_deref().unwrap_or(subject);

        #[allow(unused)]
        let dst_acct_name = state.account_name(route.dst_account_id).as_bytes();

        let subs = state.get_subs(route.dst_account_id).read().unwrap();
        let (_count, expired) = subs.for_each_match(&dst_subject_str, |sub| {
            #[cfg(feature = "cluster")]
            if sub.is_route {
                sub.writer
                    .write_rmsg(dst_subject_bytes, reply, headers, payload, dst_acct_name);
                dirty_writers.push(sub.writer.clone());
                return;
            }
            #[cfg(feature = "hub")]
            if sub.is_leaf {
                // Check subscribe permissions for leaf delivery.
                if let Some(ref perms) = sub.leaf_perms {
                    let subj = std::str::from_utf8(dst_subject_bytes).unwrap_or("");
                    if !perms.subscribe.is_allowed(subj) {
                        return;
                    }
                }
                sub.writer
                    .write_lmsg(dst_subject_bytes, reply, headers, payload);
            } else {
                sub.writer
                    .write_msg(dst_subject_bytes, &sub.sid_bytes, reply, headers, payload);
            }
            #[cfg(not(feature = "hub"))]
            sub.writer
                .write_msg(dst_subject_bytes, &sub.sid_bytes, reply, headers, payload);
            dirty_writers.push(sub.writer.clone());
        });
        drop(subs);
        all_expired.extend(expired);
    }

    all_expired
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "hub")]
    fn test_server_state() -> crate::server::ServerState {
        use std::collections::HashMap;
        use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize};

        crate::server::ServerState {
            info: Default::default(),
            auth: Default::default(),
            ping_interval_ms: AtomicU64::new(0),
            auth_timeout_ms: AtomicU64::new(0),
            max_pings_outstanding: AtomicU32::new(0),
            #[cfg(not(feature = "accounts"))]
            subs: std::sync::RwLock::new(crate::sub_list::SubList::new()),
            #[cfg(feature = "accounts")]
            account_subs: vec![std::sync::RwLock::new(crate::sub_list::SubList::new())],
            #[cfg(feature = "accounts")]
            account_registry: crate::server::AccountRegistry::new(&[]),
            #[cfg(feature = "accounts")]
            account_configs: Vec::new(),
            #[cfg(feature = "accounts")]
            cross_account_routes: Vec::new(),
            #[cfg(feature = "accounts")]
            reverse_imports: Vec::new(),
            #[cfg(feature = "leaf")]
            upstreams: std::sync::RwLock::new(Vec::new()),
            #[cfg(feature = "leaf")]
            upstream_txs: std::sync::RwLock::new(Vec::new()),
            has_subs: AtomicBool::new(false),
            buf_config: Default::default(),
            next_cid: AtomicU64::new(1),
            tls_config: None,
            active_connections: AtomicU64::new(0),
            max_connections: AtomicUsize::new(0),
            max_payload: AtomicUsize::new(1024 * 1024),
            max_control_line: AtomicUsize::new(4096),
            max_subscriptions: AtomicUsize::new(0),
            stats: Default::default(),
            leafnode_port: None,
            leaf_writers: std::sync::RwLock::new(HashMap::new()),
            leaf_auth: Default::default(),
            #[cfg(feature = "cluster")]
            route_writers: std::sync::RwLock::new(HashMap::new()),
            #[cfg(feature = "cluster")]
            cluster_port: None,
            #[cfg(feature = "cluster")]
            cluster_name: None,
            #[cfg(feature = "cluster")]
            cluster_seeds: Vec::new(),
            #[cfg(feature = "cluster")]
            route_peers: std::sync::Mutex::new(crate::server::RoutePeerRegistry {
                connected: HashMap::new(),
                known_urls: std::collections::HashSet::new(),
            }),
            #[cfg(feature = "cluster")]
            route_connect_tx: std::sync::Mutex::new(None),
            #[cfg(feature = "gateway")]
            gateway_writers: std::sync::RwLock::new(HashMap::new()),
            #[cfg(feature = "gateway")]
            gateway_port: None,
            #[cfg(feature = "gateway")]
            gateway_name: None,
            #[cfg(feature = "gateway")]
            gateway_remotes: Vec::new(),
            #[cfg(feature = "gateway")]
            gateway_peers: std::sync::Mutex::new(crate::server::GatewayPeerRegistry {
                connected: HashMap::new(),
                known_urls: std::collections::HashSet::new(),
            }),
            #[cfg(feature = "gateway")]
            gateway_connect_tx: std::sync::Mutex::new(None),
            #[cfg(feature = "gateway")]
            gateway_reply_prefix: b"_GR_.abc.def.".to_vec(),
            #[cfg(feature = "gateway")]
            cached_gateway_info: std::sync::Mutex::new(String::new()),
            #[cfg(feature = "gateway")]
            gateway_interest: std::sync::RwLock::new(HashMap::new()),
            #[cfg(feature = "gateway")]
            has_gateway_interest: AtomicBool::new(false),
            #[cfg(feature = "worker-affinity")]
            affinity: crate::server::AffinityMap::new(1),
        }
    }

    #[test]
    #[cfg(feature = "hub")]
    fn test_propagate_leaf_filters_by_publish_permission() {
        use std::sync::Arc;

        use crate::server::{Permission, Permissions};
        use crate::sub_list::DirectWriter;

        let state = test_server_state();
        let writer = DirectWriter::new_dummy();

        let perms = Permissions {
            publish: Permission {
                allow: Vec::new(),
                deny: vec!["secret.>".to_string()],
            },
            subscribe: Permission {
                allow: Vec::new(),
                deny: Vec::new(),
            },
        };
        state
            .leaf_writers
            .write()
            .unwrap()
            .insert(100, (writer.clone(), Some(Arc::new(perms))));

        // Denied subject should not be sent
        super::propagate_leaf_interest(&state, b"secret.data", None, true);
        assert!(
            writer.drain().is_none(),
            "denied subject should not be sent"
        );

        // Allowed subject should be sent
        super::propagate_leaf_interest(&state, b"public.data", None, true);
        let data = writer.drain().unwrap();
        assert_eq!(&data[..], b"LS+ public.data\r\n");
    }

    #[test]
    #[cfg(feature = "hub")]
    fn test_propagate_leaf_sends_to_all_leaves() {
        use crate::sub_list::DirectWriter;

        let state = test_server_state();
        let w1 = DirectWriter::new_dummy();
        let w2 = DirectWriter::new_dummy();

        {
            let mut writers = state.leaf_writers.write().unwrap();
            writers.insert(1, (w1.clone(), None));
            writers.insert(2, (w2.clone(), None));
        }

        super::propagate_leaf_interest(&state, b"foo.bar", None, true);

        assert_eq!(&w1.drain().unwrap()[..], b"LS+ foo.bar\r\n");
        assert_eq!(&w2.drain().unwrap()[..], b"LS+ foo.bar\r\n");
    }

    #[test]
    #[cfg(feature = "hub")]
    fn test_send_existing_subs_filters_permissions() {
        use std::sync::Arc;

        use crate::server::{Permission, Permissions};
        use crate::sub_list::{DirectWriter, Subscription};

        let state = test_server_state();

        {
            #[cfg(not(feature = "accounts"))]
            let mut subs = state.subs.write().unwrap();
            #[cfg(feature = "accounts")]
            let mut subs = state.account_subs[0].write().unwrap();
            subs.insert(Subscription::new_dummy(
                1,
                1,
                "public.events".to_string(),
                None,
            ));
            subs.insert(Subscription::new_dummy(
                1,
                2,
                "secret.data".to_string(),
                None,
            ));
        }

        let writer = DirectWriter::new_dummy();
        let perms = Permissions {
            publish: Permission {
                allow: Vec::new(),
                deny: vec!["secret.>".to_string()],
            },
            subscribe: Permission {
                allow: Vec::new(),
                deny: Vec::new(),
            },
        };

        super::send_existing_subs(&state, &writer, &Some(Arc::new(perms)));

        let data = writer.drain().unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert!(
            s.contains("LS+ public.events\r\n"),
            "allowed sub should be sent"
        );
        assert!(!s.contains("secret.data"), "denied sub should be filtered");
    }

    #[test]
    #[cfg(feature = "gateway")]
    fn test_rewrite_gateway_reply_adds_prefix() {
        let state = test_server_state_gw();
        let result = super::rewrite_gateway_reply(Some(b"reply"), &state);
        let r = result.unwrap();
        assert!(r.starts_with(b"_GR_."));
        assert!(r.ends_with(b".reply"));
    }

    #[test]
    #[cfg(feature = "gateway")]
    fn test_rewrite_gateway_reply_no_double_rewrite() {
        let state = test_server_state_gw();
        let already = b"_GR_.abc.def.reply";
        let result = super::rewrite_gateway_reply(Some(already), &state);
        let r = result.unwrap();
        assert_eq!(
            &r[..],
            already,
            "already-prefixed reply should not be rewritten"
        );
    }

    #[test]
    #[cfg(feature = "gateway")]
    fn test_unwrap_gateway_reply_strips_prefix() {
        let reply = bytes::Bytes::from_static(b"_GR_.abc.def.my.reply");
        let unwrapped = super::unwrap_gateway_reply_bytes(&reply);
        assert_eq!(&unwrapped[..], b"my.reply");
    }

    #[cfg(feature = "gateway")]
    fn test_server_state_gw() -> crate::server::ServerState {
        use std::collections::HashMap;
        use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize};

        crate::server::ServerState {
            info: Default::default(),
            auth: Default::default(),
            ping_interval_ms: AtomicU64::new(0),
            auth_timeout_ms: AtomicU64::new(0),
            max_pings_outstanding: AtomicU32::new(0),
            #[cfg(not(feature = "accounts"))]
            subs: std::sync::RwLock::new(crate::sub_list::SubList::new()),
            #[cfg(feature = "accounts")]
            account_subs: vec![std::sync::RwLock::new(crate::sub_list::SubList::new())],
            #[cfg(feature = "accounts")]
            account_registry: crate::server::AccountRegistry::new(&[]),
            #[cfg(feature = "accounts")]
            account_configs: Vec::new(),
            #[cfg(feature = "accounts")]
            cross_account_routes: Vec::new(),
            #[cfg(feature = "accounts")]
            reverse_imports: Vec::new(),
            #[cfg(feature = "leaf")]
            upstreams: std::sync::RwLock::new(Vec::new()),
            #[cfg(feature = "leaf")]
            upstream_txs: std::sync::RwLock::new(Vec::new()),
            has_subs: AtomicBool::new(false),
            buf_config: Default::default(),
            next_cid: AtomicU64::new(1),
            tls_config: None,
            active_connections: AtomicU64::new(0),
            max_connections: AtomicUsize::new(0),
            max_payload: AtomicUsize::new(1024 * 1024),
            max_control_line: AtomicUsize::new(4096),
            max_subscriptions: AtomicUsize::new(0),
            stats: Default::default(),
            #[cfg(feature = "hub")]
            leafnode_port: None,
            #[cfg(feature = "hub")]
            leaf_writers: std::sync::RwLock::new(HashMap::new()),
            #[cfg(feature = "hub")]
            leaf_auth: Default::default(),
            #[cfg(feature = "cluster")]
            route_writers: std::sync::RwLock::new(HashMap::new()),
            #[cfg(feature = "cluster")]
            cluster_port: None,
            #[cfg(feature = "cluster")]
            cluster_name: None,
            #[cfg(feature = "cluster")]
            cluster_seeds: Vec::new(),
            #[cfg(feature = "cluster")]
            route_peers: std::sync::Mutex::new(crate::server::RoutePeerRegistry {
                connected: HashMap::new(),
                known_urls: std::collections::HashSet::new(),
            }),
            #[cfg(feature = "cluster")]
            route_connect_tx: std::sync::Mutex::new(None),
            gateway_writers: std::sync::RwLock::new(HashMap::new()),
            gateway_port: None,
            gateway_name: None,
            gateway_remotes: Vec::new(),
            gateway_peers: std::sync::Mutex::new(crate::server::GatewayPeerRegistry {
                connected: HashMap::new(),
                known_urls: std::collections::HashSet::new(),
            }),
            gateway_connect_tx: std::sync::Mutex::new(None),
            gateway_reply_prefix: b"_GR_.abc.def.".to_vec(),
            cached_gateway_info: std::sync::Mutex::new(String::new()),
            gateway_interest: std::sync::RwLock::new(HashMap::new()),
            has_gateway_interest: AtomicBool::new(false),
            #[cfg(feature = "worker-affinity")]
            affinity: crate::server::AffinityMap::new(1),
        }
    }
}
