// Copyright 2024 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

//! N-worker epoll event loop.
//!
//! Each worker owns one epoll instance multiplexing many client connections.
//! DirectWriter notifies the *worker's* single eventfd, so fan-out to 100
//! connections on the same worker costs 1 eventfd write, not 100.

use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use bytes::{Buf, Bytes, BytesMut};
use tracing::{debug, info, warn};

use crate::nats_proto;
use crate::protocol::{AdaptiveBuf, ClientOp};
use crate::server::ServerState;
use crate::sub_list::{create_eventfd, DirectWriter, Subscription};
use crate::upstream::UpstreamCmd;

/// Sentinel key in epoll_event.u64 for the worker's eventfd.
const EVENT_FD_KEY: u64 = 0;

// --- Worker commands ---

pub(crate) enum WorkerCmd {
    NewConn {
        id: u64,
        stream: TcpStream,
        addr: SocketAddr,
    },
    Shutdown,
}

/// Handle for the acceptor to send connections and commands to a worker.
pub(crate) struct WorkerHandle {
    pub tx: mpsc::Sender<WorkerCmd>,
    event_fd: Arc<OwnedFd>,
}

impl WorkerHandle {
    /// Send a new connection to this worker and wake it.
    pub fn send_conn(&self, id: u64, stream: TcpStream, addr: SocketAddr) {
        let _ = self.tx.send(WorkerCmd::NewConn { id, stream, addr });
        self.wake();
    }

    /// Send shutdown command and wake the worker.
    pub fn shutdown(&self) {
        let _ = self.tx.send(WorkerCmd::Shutdown);
        self.wake();
    }

    fn wake(&self) {
        let val: u64 = 1;
        unsafe {
            libc::write(
                self.event_fd.as_raw_fd(),
                &val as *const u64 as *const libc::c_void,
                8,
            );
        }
    }
}

// --- Connection state machine ---

enum ConnPhase {
    /// INFO queued in write_buf, waiting to be flushed.
    SendInfo,
    /// INFO sent, waiting for CONNECT from client.
    WaitConnect,
    /// Normal operation.
    Active,
}

struct ClientState {
    fd: RawFd,
    _stream: TcpStream,
    read_buf: AdaptiveBuf,
    write_buf: BytesMut,
    direct_writer: DirectWriter,
    direct_buf: Arc<Mutex<BytesMut>>,
    has_pending: Arc<AtomicBool>,
    phase: ConnPhase,
    #[allow(dead_code)]
    conn_id: u64,
    upstream_tx: Option<mpsc::Sender<UpstreamCmd>>,
    /// Whether EPOLLOUT is currently registered for this fd.
    epoll_has_out: bool,
}

// --- Worker ---

pub(crate) struct Worker {
    epoll_fd: OwnedFd,
    event_fd: Arc<OwnedFd>,
    conns: HashMap<u64, ClientState>,
    fd_to_conn: HashMap<RawFd, u64>,
    rx: mpsc::Receiver<WorkerCmd>,
    state: Arc<ServerState>,
    info_line: Vec<u8>,
    shutdown: bool,
    /// Accumulated eventfd notifications. Flushed after processing a read batch.
    /// Deduplicates across multiple PUBs in the same read buffer.
    pending_notify: [RawFd; 16],
    pending_notify_count: usize,
}

impl Worker {
    /// Spawn a worker thread. Returns a handle for sending commands.
    pub(crate) fn spawn(
        index: usize,
        state: Arc<ServerState>,
    ) -> WorkerHandle {
        let (tx, rx) = mpsc::channel();
        let event_fd = Arc::new(create_eventfd());
        let handle = WorkerHandle {
            tx,
            event_fd: Arc::clone(&event_fd),
        };

        let info_json = serde_json::to_string(&state.info)
            .expect("failed to serialize server info");
        let info_line = format!("INFO {info_json}\r\n").into_bytes();

        std::thread::Builder::new()
            .name(format!("worker-{index}"))
            .spawn(move || {
                let epoll_fd = unsafe { libc::epoll_create1(0) };
                assert!(epoll_fd >= 0, "epoll_create1 failed");
                let epoll_fd = unsafe { OwnedFd::from_raw_fd(epoll_fd) };

                // Register eventfd with epoll (level-triggered)
                let mut ev = libc::epoll_event {
                    events: libc::EPOLLIN as u32,
                    u64: EVENT_FD_KEY,
                };
                let ret = unsafe {
                    libc::epoll_ctl(
                        epoll_fd.as_raw_fd(),
                        libc::EPOLL_CTL_ADD,
                        event_fd.as_raw_fd(),
                        &mut ev,
                    )
                };
                assert!(ret == 0, "epoll_ctl ADD eventfd failed");

                let mut worker = Worker {
                    epoll_fd,
                    event_fd,
                    conns: HashMap::new(),
                    fd_to_conn: HashMap::new(),
                    rx,
                    state,
                    info_line,
                    shutdown: false,
                    pending_notify: [-1; 16],
                    pending_notify_count: 0,
                };
                worker.run();
            })
            .expect("failed to spawn worker thread");

        handle
    }

    fn run(&mut self) {
        let mut events = vec![
            libc::epoll_event {
                events: 0,
                u64: 0,
            };
            256
        ];

        loop {
            let n = unsafe {
                libc::epoll_wait(
                    self.epoll_fd.as_raw_fd(),
                    events.as_mut_ptr(),
                    events.len() as i32,
                    -1,
                )
            };
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                warn!(error = %err, "epoll_wait failed");
                break;
            }

            for i in 0..n as usize {
                let ev = events[i];
                let key = ev.u64;

                if key == EVENT_FD_KEY {
                    self.handle_eventfd();
                } else {
                    let conn_id = key;
                    if ev.events & libc::EPOLLERR as u32 != 0 {
                        self.remove_conn(conn_id);
                        continue;
                    }
                    if ev.events & libc::EPOLLIN as u32 != 0 {
                        self.handle_read(conn_id);
                    }
                    if ev.events & libc::EPOLLOUT as u32 != 0 {
                        self.handle_write(conn_id);
                    }
                }
            }

            // Flush any pending DirectWriter data after processing all events.
            // This handles local delivery (pub on this worker → sub on this worker)
            // without needing an eventfd round-trip.
            self.flush_pending();

            if self.shutdown {
                break;
            }
        }
    }

    fn handle_eventfd(&mut self) {
        // Consume eventfd counter
        let mut val: u64 = 0;
        unsafe {
            libc::read(
                self.event_fd.as_raw_fd(),
                &mut val as *mut u64 as *mut libc::c_void,
                8,
            );
        }

        // Check for new connections / shutdown
        while let Ok(cmd) = self.rx.try_recv() {
            match cmd {
                WorkerCmd::NewConn { id, stream, addr } => {
                    self.add_conn(id, stream, addr);
                }
                WorkerCmd::Shutdown => {
                    self.shutdown = true;
                    return;
                }
            }
        }
        // Note: flush_pending is called after the event loop iteration in run(),
        // so we don't need to call it here.
    }

    fn add_conn(&mut self, id: u64, stream: TcpStream, addr: SocketAddr) {
        stream.set_nonblocking(true).ok();
        stream.set_nodelay(true).ok();
        let fd = stream.as_raw_fd();

        // Register with epoll (EPOLLIN, level-triggered)
        let mut ev = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: id,
        };
        let ret = unsafe {
            libc::epoll_ctl(
                self.epoll_fd.as_raw_fd(),
                libc::EPOLL_CTL_ADD,
                fd,
                &mut ev,
            )
        };
        if ret != 0 {
            warn!(id, error = %io::Error::last_os_error(), "epoll_ctl ADD failed");
            return;
        }

        let direct_buf = Arc::new(Mutex::new(BytesMut::with_capacity(65536)));
        let has_pending = Arc::new(AtomicBool::new(false));
        let direct_writer = DirectWriter::new(
            Arc::clone(&direct_buf),
            Arc::clone(&has_pending),
            Arc::clone(&self.event_fd),
        );

        let mut write_buf = BytesMut::with_capacity(4096);
        write_buf.extend_from_slice(&self.info_line);

        let client = ClientState {
            fd,
            _stream: stream,
            read_buf: AdaptiveBuf::new(self.state.buf_config.max_read_buf),
            write_buf,
            direct_writer,
            direct_buf,
            has_pending,
            phase: ConnPhase::SendInfo,
            conn_id: id,
            upstream_tx: None,
            epoll_has_out: false,
        };

        self.fd_to_conn.insert(fd, id);
        self.conns.insert(id, client);

        debug!(id, addr = %addr, "accepted connection on worker");

        // Try to flush INFO immediately
        self.try_flush_conn(id);
    }

    fn remove_conn(&mut self, conn_id: u64) {
        if let Some(client) = self.conns.remove(&conn_id) {
            unsafe {
                libc::epoll_ctl(
                    self.epoll_fd.as_raw_fd(),
                    libc::EPOLL_CTL_DEL,
                    client.fd,
                    std::ptr::null_mut(),
                );
            }
            self.fd_to_conn.remove(&client.fd);
            cleanup_conn(conn_id, &self.state);
            debug!(conn_id, "connection removed");
        }
    }

    /// Scan all connections for pending DirectWriter data, drain into write_buf,
    /// and try to flush to socket.
    fn flush_pending(&mut self) {
        let epoll_fd = self.epoll_fd.as_raw_fd();
        let mut to_remove: Vec<u64> = Vec::new();

        for (conn_id, client) in &mut self.conns {
            if !client.has_pending.load(Ordering::Acquire) {
                continue;
            }
            client.has_pending.store(false, Ordering::Relaxed);
            {
                let mut dbuf = client.direct_buf.lock().unwrap();
                if !dbuf.is_empty() {
                    client.write_buf.extend_from_slice(&dbuf);
                    dbuf.clear();
                }
            }

            // Inline flush to avoid borrow issues with self.try_flush_conn
            let mut error = false;
            while !client.write_buf.is_empty() {
                let n = unsafe {
                    libc::write(
                        client.fd,
                        client.write_buf.as_ptr() as *const libc::c_void,
                        client.write_buf.len(),
                    )
                };
                if n < 0 {
                    let err = io::Error::last_os_error();
                    if err.kind() == io::ErrorKind::WouldBlock {
                        if !client.epoll_has_out {
                            epoll_mod(epoll_fd, client.fd, *conn_id, true);
                            client.epoll_has_out = true;
                        }
                        break;
                    }
                    error = true;
                    break;
                }
                client.write_buf.advance(n as usize);
            }

            if error {
                to_remove.push(*conn_id);
                continue;
            }

            if client.write_buf.is_empty() && client.epoll_has_out {
                epoll_mod(epoll_fd, client.fd, *conn_id, false);
                client.epoll_has_out = false;
            }
        }

        for conn_id in to_remove {
            self.remove_conn(conn_id);
        }
    }

    /// Send accumulated eventfd notifications to remote workers, then clear.
    fn flush_notifications(&mut self) {
        let val: u64 = 1;
        for i in 0..self.pending_notify_count {
            unsafe {
                libc::write(
                    self.pending_notify[i],
                    &val as *const u64 as *const libc::c_void,
                    8,
                );
            }
        }
        self.pending_notify_count = 0;
    }

    /// Try to flush write_buf to the socket. Registers/removes EPOLLOUT as needed.
    fn try_flush_conn(&mut self, conn_id: u64) {
        let epoll_fd = self.epoll_fd.as_raw_fd();

        let client = match self.conns.get_mut(&conn_id) {
            Some(c) => c,
            None => return,
        };

        while !client.write_buf.is_empty() {
            let n = unsafe {
                libc::write(
                    client.fd,
                    client.write_buf.as_ptr() as *const libc::c_void,
                    client.write_buf.len(),
                )
            };
            if n < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::WouldBlock {
                    if !client.epoll_has_out {
                        epoll_mod(epoll_fd, client.fd, conn_id, true);
                        client.epoll_has_out = true;
                    }
                    return;
                }
                // Write error — remove connection
                let _ = client;
                self.remove_conn(conn_id);
                return;
            }
            client.write_buf.advance(n as usize);
        }

        // All data flushed
        if client.epoll_has_out {
            epoll_mod(epoll_fd, client.fd, conn_id, false);
            client.epoll_has_out = false;
        }

        // Phase transition: SendInfo → WaitConnect
        if matches!(client.phase, ConnPhase::SendInfo) {
            client.phase = ConnPhase::WaitConnect;
        }
    }

    fn handle_read(&mut self, conn_id: u64) {
        // Read from socket in a loop until WouldBlock to drain the kernel buffer.
        // This reduces the number of epoll_wait returns.
        loop {
            let client = match self.conns.get_mut(&conn_id) {
                Some(c) => c,
                None => return,
            };
            match client.read_buf.read_from_fd(client.fd) {
                Ok(0) => {
                    self.remove_conn(conn_id);
                    return;
                }
                Ok(n) => {
                    client.read_buf.after_read(n);
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => {
                    self.remove_conn(conn_id);
                    return;
                }
            }
        }

        // Parse and handle ops
        self.process_read_buf(conn_id);

        // Send batched eventfd notifications to remote workers.
        // This amortizes N_pubs * N_workers eventfd writes down to just N_workers.
        self.flush_notifications();
    }

    fn process_read_buf(&mut self, conn_id: u64) {
        loop {
            let phase = match self.conns.get(&conn_id) {
                Some(c) => match c.phase {
                    ConnPhase::SendInfo => return,
                    ConnPhase::WaitConnect => 1,
                    ConnPhase::Active => 2,
                },
                None => return,
            };

            if phase == 1 {
                // WaitConnect: parse CONNECT
                let op = {
                    let client = self.conns.get_mut(&conn_id).unwrap();
                    match nats_proto::try_parse_client_op(&mut *client.read_buf) {
                        Ok(op) => op,
                        Err(_) => {
                            self.remove_conn(conn_id);
                            return;
                        }
                    }
                };
                match op {
                    Some(ClientOp::Connect(_)) => {
                        let client = self.conns.get_mut(&conn_id).unwrap();
                        client.phase = ConnPhase::Active;
                        client.upstream_tx =
                            self.state.upstream_tx.read().unwrap().clone();
                        info!(conn_id, "client connected");
                    }
                    Some(_) => {
                        // Wrong op
                        if let Some(client) = self.conns.get_mut(&conn_id) {
                            client
                                .write_buf
                                .extend_from_slice(b"-ERR 'expected CONNECT'\r\n");
                        }
                        self.try_flush_conn(conn_id);
                        self.remove_conn(conn_id);
                        return;
                    }
                    None => return, // need more data
                }
            } else {
                // Active: parse client ops
                let can_skip = {
                    let client = self.conns.get(&conn_id).unwrap();
                    client.upstream_tx.is_none()
                        && !self.state.has_subs.load(Ordering::Relaxed)
                };

                let op = {
                    let client = self.conns.get_mut(&conn_id).unwrap();
                    let result = if can_skip {
                        nats_proto::try_skip_or_parse_client_op(&mut *client.read_buf)
                    } else {
                        nats_proto::try_parse_client_op(&mut *client.read_buf)
                    };
                    match result {
                        Ok(op) => op,
                        Err(_) => {
                            self.remove_conn(conn_id);
                            return;
                        }
                    }
                };

                match op {
                    Some(ClientOp::Pong) if can_skip => {
                        // Skipped PUB/HPUB, continue parsing
                        continue;
                    }
                    Some(op) => {
                        if self.handle_client_op(conn_id, op).is_err() {
                            self.remove_conn(conn_id);
                            return;
                        }
                    }
                    None => {
                        // No more complete ops — try_shrink and return
                        if let Some(client) = self.conns.get_mut(&conn_id) {
                            client.read_buf.try_shrink();
                        }
                        return;
                    }
                }
            }
        }
    }

    fn handle_write(&mut self, conn_id: u64) {
        self.try_flush_conn(conn_id);
    }

    fn handle_client_op(&mut self, conn_id: u64, op: ClientOp) -> io::Result<()> {
        match op {
            ClientOp::Ping => {
                if let Some(client) = self.conns.get_mut(&conn_id) {
                    client.write_buf.extend_from_slice(b"PONG\r\n");
                }
                self.try_flush_conn(conn_id);
            }
            ClientOp::Pong => {}
            ClientOp::Subscribe {
                sid,
                subject,
                queue_group,
            } => {
                let subject_str = bytes_to_str(&subject);
                let queue_str =
                    queue_group.as_ref().map(|q| bytes_to_str(q).to_string());

                let direct_writer = match self.conns.get(&conn_id) {
                    Some(c) => c.direct_writer.clone(),
                    None => return Ok(()),
                };

                let sub = Subscription {
                    conn_id,
                    sid,
                    sid_bytes: nats_proto::sid_to_bytes(sid),
                    subject: subject_str.to_string(),
                    queue: queue_str,
                    writer: direct_writer,
                };

                {
                    let mut subs = self.state.subs.write().unwrap();
                    subs.insert(sub);
                    self.state.has_subs.store(true, Ordering::Relaxed);
                }

                {
                    let mut upstream = self.state.upstream.write().unwrap();
                    if let Some(ref mut up) = *upstream {
                        if let Err(e) = up.add_interest(subject_str.to_string()) {
                            warn!(error = %e, "failed to add upstream interest");
                        }
                    }
                }

                debug!(conn_id, sid, subject = %subject_str, "client subscribed");
            }
            ClientOp::Unsubscribe { sid, max: _ } => {
                let removed = {
                    let mut subs = self.state.subs.write().unwrap();
                    let r = subs.remove(conn_id, sid);
                    self.state
                        .has_subs
                        .store(!subs.is_empty(), Ordering::Relaxed);
                    r
                };

                if let Some(removed) = removed {
                    let mut upstream = self.state.upstream.write().unwrap();
                    if let Some(ref mut up) = *upstream {
                        up.remove_interest(&removed.subject);
                    }
                    debug!(conn_id, sid, subject = %removed.subject, "client unsubscribed");
                }
            }
            ClientOp::Publish {
                subject,
                payload,
                respond,
                headers,
                ..
            } => {
                {
                    let my_event_fd = self.event_fd.as_raw_fd();
                    let pending_notify = &mut self.pending_notify;
                    let pending_count = &mut self.pending_notify_count;

                    let subject_str = bytes_to_str(&subject);
                    let subs = self.state.subs.read().unwrap();
                    subs.for_each_match(subject_str, |sub| {
                        sub.writer.write_msg(
                            &subject,
                            &sub.sid_bytes,
                            respond.as_deref(),
                            headers.as_ref(),
                            &payload,
                        );
                        // Skip notification for our own worker — flush_pending
                        // runs after the event loop iteration.
                        let fd = sub.writer.event_raw_fd();
                        if fd == my_event_fd {
                            return;
                        }
                        // Accumulate notification (deduplicated across entire batch).
                        if !pending_notify[..*pending_count].contains(&fd) {
                            if *pending_count < pending_notify.len() {
                                pending_notify[*pending_count] = fd;
                                *pending_count += 1;
                            }
                        }
                    });
                }

                let upstream_tx = self
                    .conns
                    .get(&conn_id)
                    .and_then(|c| c.upstream_tx.clone());
                if let Some(ref tx) = upstream_tx {
                    if let Err(e) = tx.send(UpstreamCmd::Publish {
                        subject,
                        reply: respond,
                        headers,
                        payload,
                    }) {
                        warn!(error = %e, "failed to forward publish to upstream");
                    }
                }
            }
            ClientOp::Connect(_) => {
                // Duplicate CONNECT, ignore
            }
        }
        Ok(())
    }
}

/// Modify epoll registration for a client socket.
fn epoll_mod(epoll_fd: RawFd, client_fd: RawFd, conn_id: u64, enable_out: bool) {
    let events = if enable_out {
        libc::EPOLLIN as u32 | libc::EPOLLOUT as u32
    } else {
        libc::EPOLLIN as u32
    };
    let mut ev = libc::epoll_event {
        events,
        u64: conn_id,
    };
    unsafe {
        libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_MOD, client_fd, &mut ev);
    }
}

/// Convert `Bytes` to `&str` without UTF-8 validation.
/// NATS subjects are restricted to ASCII printable characters.
#[inline]
fn bytes_to_str(b: &Bytes) -> &str {
    // SAFETY: NATS protocol subjects/reply-to are always ASCII
    unsafe { std::str::from_utf8_unchecked(b) }
}

fn cleanup_conn(id: u64, state: &ServerState) {
    let removed = {
        let mut subs = state.subs.write().unwrap();
        let r = subs.remove_conn(id);
        state
            .has_subs
            .store(!subs.is_empty(), Ordering::Relaxed);
        r
    };

    if !removed.is_empty() {
        let mut upstream = state.upstream.write().unwrap();
        if let Some(ref mut up) = *upstream {
            for sub in &removed {
                up.remove_interest(&sub.subject);
            }
        }
    }

    info!(id, "client cleaned up");
}
