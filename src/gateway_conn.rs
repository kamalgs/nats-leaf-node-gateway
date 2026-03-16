// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! Outbound gateway connection manager for super-cluster support.
//!
//! Each outbound gateway connection spawns a reader thread that processes
//! RS+/RS-/RMSG from the remote cluster, and a writer thread that drains
//! the DirectWriter buffer to TCP.
//!
//! One-hop rule: messages received from a gateway are never re-forwarded
//! to other gateways (enforced by deliver_to_subs skipping gateway subs).

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use tracing::{debug, error, info, warn};

use crate::handler::{deliver_to_subs_upstream_inner, handle_expired_subs_upstream};
use crate::nats_proto::{self, GatewayOp, MsgBuilder};
use crate::server::{GatewayRemote, ServerState};
use crate::sub_list::{DirectWriter, Subscription};
use crate::upstream::Backoff;

/// Virtual connection ID range for outbound gateway connections.
/// Uses high IDs to avoid collision with inbound connection IDs and route IDs.
const GATEWAY_CONN_ID_BASE: u64 = 2 << 48;

/// Global counter for outbound gateway connection IDs.
static GATEWAY_CONN_COUNTER: AtomicU64 = AtomicU64::new(0);

fn next_gateway_conn_id() -> u64 {
    GATEWAY_CONN_ID_BASE + GATEWAY_CONN_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Manages all outbound gateway connections to remote clusters.
pub(crate) struct GatewayConnManager {
    shutdown: Arc<AtomicBool>,
}

impl GatewayConnManager {
    /// Spawn outbound gateway connections to all configured remote clusters.
    pub(crate) fn spawn(state: Arc<ServerState>) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));

        // Spawn per-remote supervisor threads (one outbound per remote cluster).
        for remote in &state.gateway_remotes {
            let st = Arc::clone(&state);
            let sd = Arc::clone(&shutdown);
            let remote = remote.clone();

            std::thread::Builder::new()
                .name(format!("gateway-{}", remote.name))
                .spawn(move || {
                    run_gateway_supervisor(remote, st, sd);
                })
                .expect("failed to spawn gateway supervisor");
        }

        Self { shutdown }
    }
}

impl Drop for GatewayConnManager {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

/// Supervisor loop for a single outbound gateway connection to a remote cluster.
fn run_gateway_supervisor(
    remote: GatewayRemote,
    state: Arc<ServerState>,
    shutdown: Arc<AtomicBool>,
) {
    let mut backoff = Backoff::new(Duration::from_millis(250), Duration::from_secs(30));
    let mut url_idx = 0;

    loop {
        if shutdown.load(Ordering::Acquire) {
            debug!(cluster = %remote.name, "gateway supervisor shutting down");
            return;
        }

        // Round-robin through remote URLs
        let url = &remote.urls[url_idx % remote.urls.len()];
        url_idx += 1;

        match connect_gateway(url, &remote.name, &state, &shutdown) {
            Ok(()) => {
                backoff.reset();
                if shutdown.load(Ordering::Acquire) {
                    return;
                }
                warn!(cluster = %remote.name, "gateway connection lost, will reconnect");
            }
            Err(e) => {
                if shutdown.load(Ordering::Acquire) {
                    return;
                }
                warn!(cluster = %remote.name, url, error = %e, "gateway connection failed");
            }
        }

        if shutdown.load(Ordering::Acquire) {
            return;
        }

        let delay = backoff.next_delay();
        debug!(
            cluster = %remote.name,
            delay_ms = delay.as_millis(),
            "reconnecting to gateway peer"
        );

        let end = std::time::Instant::now() + delay;
        while std::time::Instant::now() < end {
            if shutdown.load(Ordering::Acquire) {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// Parse a gateway URL like "nats://host:port" into a TCP address.
fn parse_gateway_url(url: &str) -> String {
    let stripped = url
        .strip_prefix("nats-gateway://")
        .or_else(|| url.strip_prefix("nats://"))
        .unwrap_or(url);

    if stripped.contains(':') {
        stripped.to_string()
    } else {
        format!("{stripped}:7222")
    }
}

/// Connect to a gateway peer, perform handshake, run reader/writer loop.
fn connect_gateway(
    url: &str,
    expected_name: &str,
    state: &Arc<ServerState>,
    shutdown: &Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let addr = parse_gateway_url(url);
    let tcp = TcpStream::connect(&addr)?;
    tcp.set_nodelay(true)?;

    let conn_id = next_gateway_conn_id();
    info!(addr = %addr, conn_id, cluster = expected_name, "outbound gateway connection established");

    let mut read_buf = BytesMut::with_capacity(state.buf_config.max_read_buf);
    let tcp_writer = tcp.try_clone()?;
    let tcp_shutdown = tcp.try_clone()?;

    // --- Read peer's INFO ---
    read_into_buf(&tcp, &mut read_buf)?;
    let peer_info = match nats_proto::try_parse_gateway_op(&mut read_buf)? {
        Some(GatewayOp::Info(info)) => {
            debug!(peer_id = %info.server_id, "received gateway INFO from peer");
            // Self-connect check
            if info.server_id == state.info.server_id {
                debug!("detected self-connect on gateway, closing");
                return Err("self-connect on gateway".into());
            }
            info
        }
        Some(other) => {
            return Err(format!("expected INFO from gateway peer, got: {other:?}").into());
        }
        None => {
            return Err("gateway peer closed connection before INFO".into());
        }
    };

    // Verify the remote gateway name matches expected
    #[cfg(feature = "gateway")]
    if let Some(ref gw_name) = peer_info.gateway {
        if gw_name != expected_name {
            return Err(format!(
                "gateway name mismatch: expected '{}', got '{}'",
                expected_name, gw_name
            )
            .into());
        }
    }

    // Process gateway_urls from peer INFO.
    #[cfg(feature = "gateway")]
    if let Some(ref urls) = peer_info.gateway_urls {
        if !urls.is_empty() {
            let tx = state.gateway_connect_tx.lock().unwrap();
            let mut peers = state.gateway_peers.lock().unwrap();
            for url in urls {
                if peers.known_urls.insert(url.clone()) {
                    if let Some(ref sender) = *tx {
                        let _ = sender.send(url.clone());
                    }
                }
            }
        }
    }

    let _ = &peer_info;

    // --- Send our INFO + CONNECT + PING ---
    {
        let mut w = io::BufWriter::new(&tcp_writer);
        w.write_all(build_gateway_info(state).as_bytes())?;
        w.write_all(build_gateway_connect(state, expected_name).as_bytes())?;
        w.write_all(b"PING\r\n")?;
        w.flush()?;
    }

    // --- Wait for PONG (process handshake ops) ---
    loop {
        match try_parse_or_read(&tcp, &mut read_buf)? {
            GatewayOp::Pong => {
                debug!("gateway handshake complete");
                break;
            }
            GatewayOp::Ping => {
                let mut w = io::BufWriter::new(&tcp_writer);
                w.write_all(b"PONG\r\n")?;
                w.flush()?;
            }
            GatewayOp::Info(_) => {}
            GatewayOp::Connect(_) => {}
            GatewayOp::RouteSub { .. } | GatewayOp::RouteUnsub { .. } => {}
            other => {
                return Err(format!("unexpected op during gateway handshake: {other:?}").into());
            }
        }
    }

    // --- Register peer in GatewayPeerRegistry ---
    {
        let mut peers = state.gateway_peers.lock().unwrap();
        peers
            .connected
            .entry(expected_name.to_string())
            .or_default()
            .insert(conn_id);
    }

    // --- Create DirectWriter for this gateway ---
    let direct_writer = DirectWriter::new_dummy();

    // Register in gateway_writers
    {
        let mut writers = state.gateway_writers.write().unwrap();
        writers.insert(conn_id, direct_writer.clone());
    }

    // --- Send RS+ for existing local subs (interest-only mode) ---
    {
        let mut w = io::BufWriter::new(&tcp_writer);
        let subs = state.subs.read().unwrap();
        let mut builder = MsgBuilder::new();
        for (subject, queue) in subs.local_interests() {
            let data = if let Some(q) = queue {
                builder.build_route_sub_queue(subject.as_bytes(), q.as_bytes())
            } else {
                builder.build_route_sub(subject.as_bytes())
            };
            w.write_all(data)?;
        }
        drop(subs);
        w.flush()?;
    }

    // --- Spawn writer thread ---
    let writer_dw = direct_writer.clone();
    let writer_shutdown = Arc::clone(shutdown);
    let writer_handle = std::thread::Builder::new()
        .name(format!("gateway-writer-{}", conn_id))
        .spawn(move || {
            run_gateway_writer(tcp_writer, writer_dw, writer_shutdown);
        })
        .expect("failed to spawn gateway writer");

    // --- Run reader loop ---
    let result = run_gateway_reader(&tcp, &mut read_buf, conn_id, state, &direct_writer);

    // --- Cleanup ---
    tcp_shutdown.shutdown(Shutdown::Both).ok();
    let _ = writer_handle.join();

    // Remove all gateway subs for this connection from SubList
    {
        let mut subs = state.subs.write().unwrap();
        subs.remove_conn(conn_id);
        state.has_subs.store(!subs.is_empty(), Ordering::Relaxed);
    }

    // Remove from gateway_writers
    {
        let mut writers = state.gateway_writers.write().unwrap();
        writers.remove(&conn_id);
    }

    // Remove from GatewayPeerRegistry
    {
        let mut peers = state.gateway_peers.lock().unwrap();
        if let Some(ids) = peers.connected.get_mut(expected_name) {
            ids.remove(&conn_id);
            if ids.is_empty() {
                peers.connected.remove(expected_name);
            }
        }
    }

    info!(
        conn_id,
        cluster = expected_name,
        "outbound gateway connection closed"
    );
    result
}

/// Writer thread: waits on DirectWriter's eventfd and flushes buffered data to TCP.
fn run_gateway_writer(tcp: TcpStream, dw: DirectWriter, shutdown: Arc<AtomicBool>) {
    let efd = dw.event_raw_fd();
    let mut pfds = [libc::pollfd {
        fd: efd,
        events: libc::POLLIN,
        revents: 0,
    }];
    let mut tcp_out = io::BufWriter::new(tcp);

    loop {
        if shutdown.load(Ordering::Relaxed) {
            // Final drain
            if let Some(data) = dw.drain() {
                let _ = tcp_out.write_all(&data);
                let _ = tcp_out.flush();
            }
            return;
        }

        pfds[0].revents = 0;
        let ret = unsafe { libc::poll(pfds.as_mut_ptr(), 1, 500) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            error!(error = %err, "gateway writer poll error");
            return;
        }

        if pfds[0].revents & libc::POLLIN != 0 {
            // Consume eventfd
            let mut val: u64 = 0;
            unsafe {
                libc::read(efd, &mut val as *mut u64 as *mut libc::c_void, 8);
            }
        }

        // Drain any pending data
        if let Some(data) = dw.drain() {
            if let Err(e) = tcp_out.write_all(&data) {
                debug!(error = %e, "gateway writer TCP error");
                return;
            }
            if let Err(e) = tcp_out.flush() {
                debug!(error = %e, "gateway writer flush error");
                return;
            }
        }
    }
}

/// Main reader loop for an outbound gateway connection.
fn run_gateway_reader(
    tcp: &TcpStream,
    read_buf: &mut BytesMut,
    conn_id: u64,
    state: &ServerState,
    direct_writer: &DirectWriter,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut dirty_writers: Vec<DirectWriter> = Vec::new();
    let mut gateway_sid_counter: u64 = 0;
    let mut gateway_sids: HashMap<(bytes::Bytes, Option<bytes::Bytes>), u64> = HashMap::new();
    let mut tmp = [0u8; 65536];

    loop {
        // Try to parse all ops already in the buffer
        while let Some(op) = nats_proto::try_parse_gateway_op(read_buf)? {
            handle_gateway_op(
                op,
                conn_id,
                state,
                direct_writer,
                tcp,
                &mut dirty_writers,
                &mut gateway_sid_counter,
                &mut gateway_sids,
            )?;
        }

        // Notify all dirty writers from this batch
        for w in dirty_writers.drain(..) {
            w.notify();
        }

        // Read more data from TCP
        let n = (&*tcp).read(&mut tmp)?;
        if n == 0 {
            return Ok(());
        }
        read_buf.extend_from_slice(&tmp[..n]);
    }
}

/// Handle a single gateway protocol operation from a peer.
#[allow(clippy::too_many_arguments)]
fn handle_gateway_op(
    op: GatewayOp,
    conn_id: u64,
    state: &ServerState,
    direct_writer: &DirectWriter,
    tcp: &TcpStream,
    dirty_writers: &mut Vec<DirectWriter>,
    gateway_sid_counter: &mut u64,
    gateway_sids: &mut HashMap<(bytes::Bytes, Option<bytes::Bytes>), u64>,
) -> io::Result<()> {
    use crate::handler::unwrap_gateway_reply;

    match op {
        GatewayOp::RouteSub { subject, queue, .. } => {
            *gateway_sid_counter += 1;
            let sid = *gateway_sid_counter;
            gateway_sids.insert((subject.clone(), queue.clone()), sid);

            let subject_str = unsafe { std::str::from_utf8_unchecked(&subject) };
            let queue_str = queue
                .as_ref()
                .map(|q| unsafe { std::str::from_utf8_unchecked(q) }.to_string());

            let sub = Subscription {
                conn_id,
                sid,
                sid_bytes: nats_proto::sid_to_bytes(sid),
                subject: subject_str.to_string(),
                queue: queue_str,
                writer: direct_writer.clone(),
                max_msgs: AtomicU64::new(0),
                delivered: AtomicU64::new(0),
                is_leaf: false,
                #[cfg(feature = "cluster")]
                is_route: false,
                is_gateway: true,
            };

            let mut subs = state.subs.write().unwrap();
            subs.insert(sub);
            state.has_subs.store(true, Ordering::Relaxed);

            debug!(conn_id, sid, subject = %subject_str, "outbound gateway sub");
        }
        GatewayOp::RouteUnsub { subject } => {
            // RS- doesn't carry queue; remove matching (subject, None) entry
            let key = (subject.clone(), None);
            if let Some(sid) = gateway_sids.remove(&key) {
                let mut subs = state.subs.write().unwrap();
                subs.remove(conn_id, sid);
                state.has_subs.store(!subs.is_empty(), Ordering::Relaxed);
            }
        }
        GatewayOp::RouteMsg {
            subject,
            reply,
            headers,
            payload,
            ..
        } => {
            let subject_str = unsafe { std::str::from_utf8_unchecked(&subject) };

            // Unwrap _GR_ reply prefix if present.
            let unwrapped_reply = reply.as_ref().map(|r| {
                let unwrapped = unwrap_gateway_reply(r);
                if unwrapped.len() != r.len() {
                    bytes::Bytes::copy_from_slice(unwrapped)
                } else {
                    r.clone()
                }
            });

            // One-hop: skip route subs and gateway subs — messages from a gateway peer
            // are never re-forwarded.
            let expired = deliver_to_subs_upstream_inner(
                state,
                &subject,
                subject_str,
                unwrapped_reply.as_deref(),
                headers.as_ref(),
                &payload,
                dirty_writers,
                #[cfg(feature = "cluster")]
                true, // skip_routes
                true, // skip_gateways
            );
            handle_expired_subs_upstream(&expired, state);
        }
        GatewayOp::Ping => {
            let mut w = io::BufWriter::new(tcp);
            w.write_all(b"PONG\r\n")?;
            w.flush()?;
        }
        GatewayOp::Pong => {}
        GatewayOp::Info(info) => {
            // Gossip: process gateway_urls from active-phase INFO updates.
            #[cfg(feature = "gateway")]
            if let Some(ref urls) = info.gateway_urls {
                if !urls.is_empty() {
                    let tx = state.gateway_connect_tx.lock().unwrap();
                    let mut peers = state.gateway_peers.lock().unwrap();
                    for url in urls {
                        if peers.known_urls.insert(url.clone()) {
                            if let Some(ref sender) = *tx {
                                let _ = sender.send(url.clone());
                            }
                        }
                    }
                }
            }
            let _ = info;
            debug!("received updated INFO from gateway peer");
        }
        GatewayOp::Connect(_) => {
            debug!("received CONNECT from gateway peer");
        }
    }
    Ok(())
}

/// Build INFO JSON for gateway protocol.
pub(crate) fn build_gateway_info(state: &ServerState) -> String {
    let gateway_name = state.gateway_name.as_deref().unwrap_or("default");
    let gateway_port = state.gateway_port.unwrap_or(0);

    // Collect known gateway URLs for gossip.
    let gateway_urls = {
        let peers = state.gateway_peers.lock().unwrap();
        let urls: Vec<&str> = peers.known_urls.iter().map(|s| s.as_str()).collect();
        if urls.is_empty() {
            String::new()
        } else {
            let items: Vec<String> = urls.iter().map(|u| format!("\"{}\"", u)).collect();
            format!(",\"gateway_urls\":[{}]", items.join(","))
        }
    };

    format!(
        "INFO {{\"server_id\":\"{}\",\"server_name\":\"{}\",\"version\":\"{}\",\
         \"host\":\"{}\",\"port\":{},\"max_payload\":{},\"proto\":1,\
         \"gateway\":\"{}\",\"gateway_port\":{}{}}}\r\n",
        state.info.server_id,
        state.info.server_name,
        state.info.version,
        state.info.host,
        state.info.port,
        state.info.max_payload,
        gateway_name,
        gateway_port,
        gateway_urls,
    )
}

/// Broadcast updated INFO to all connected gateway peers (gossip re-broadcast).
pub(crate) fn broadcast_gateway_info(state: &ServerState) {
    let info_line = build_gateway_info(state);
    let info_bytes = info_line.as_bytes();
    let writers = state.gateway_writers.read().unwrap();
    for writer in writers.values() {
        writer.write_raw(info_bytes);
        writer.notify();
    }
}

/// Build CONNECT JSON for gateway protocol.
fn build_gateway_connect(state: &ServerState, remote_name: &str) -> String {
    let gateway_name = state.gateway_name.as_deref().unwrap_or("default");
    format!(
        "CONNECT {{\"server_id\":\"{}\",\"name\":\"{}\",\
         \"gateway\":\"{}\",\"remote_gateway\":\"{}\"}}\r\n",
        state.info.server_id, state.info.server_name, gateway_name, remote_name,
    )
}

/// Read from TCP into the buffer, blocking until data is available.
fn read_into_buf(tcp: &TcpStream, buf: &mut BytesMut) -> io::Result<()> {
    let mut tmp = [0u8; 4096];
    let n = (&*tcp).read(&mut tmp)?;
    if n == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed",
        ));
    }
    buf.extend_from_slice(&tmp[..n]);
    Ok(())
}

/// Parse the next gateway op from the buffer, reading more data from TCP if needed.
fn try_parse_or_read(
    tcp: &TcpStream,
    buf: &mut BytesMut,
) -> Result<GatewayOp, Box<dyn std::error::Error>> {
    loop {
        if let Some(op) = nats_proto::try_parse_gateway_op(buf)? {
            return Ok(op);
        }
        read_into_buf(tcp, buf)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_gateway_url_with_scheme() {
        assert_eq!(parse_gateway_url("nats://host1:7222"), "host1:7222");
    }

    #[test]
    fn parse_gateway_url_bare() {
        assert_eq!(parse_gateway_url("host2:7222"), "host2:7222");
    }

    #[test]
    fn parse_gateway_url_default_port() {
        assert_eq!(parse_gateway_url("host3"), "host3:7222");
    }

    #[test]
    fn parse_gateway_url_gateway_scheme() {
        assert_eq!(parse_gateway_url("nats-gateway://host4:6222"), "host4:6222");
    }

    #[test]
    fn gateway_conn_id_is_high() {
        let id = next_gateway_conn_id();
        assert!(id >= GATEWAY_CONN_ID_BASE);
    }
}
