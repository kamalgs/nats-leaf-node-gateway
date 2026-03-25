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
