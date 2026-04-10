//! Completion-based io_uring worker.
//!
//! Uses RECV/SEND SQEs for data I/O instead of epoll readiness + libc::read/write.
//! Protocol processing is shared with the epoll worker via [`WorkerCore`].
//!
//! **Token encoding**: the top 2 bits of the CQE `user_data` encode the operation
//! type, and the lower 62 bits carry the connection ID (or the sentinel
//! `EVENT_FD_KEY` for eventfd POLL notifications).
//!
//! | Bits 63..62 | Operation     |
//! |-------------|---------------|
//! | `0b00`      | POLL_ADD      |
//! | `0b01`      | RECV          |
//! | `0b10`      | SEND          |

use std::os::fd::AsRawFd;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Instant;

use bytes::{BufMut, BytesMut};
use io_uring::types::Fd;
use io_uring::{opcode, IoUring};
use rustc_hash::{FxHashMap, FxHashSet};
use tracing::{debug, warn};

use crate::core::server::ServerState;
use crate::core::worker::{WorkerCore, WorkerHandle, EVENT_FD_KEY};
use crate::sub_list::create_eventfd;

// ---------------------------------------------------------------------------
// Token encoding
// ---------------------------------------------------------------------------

const OP_POLL: u64 = 0 << 62;
const OP_RECV: u64 = 1 << 62;
const OP_SEND: u64 = 2 << 62;
const OP_MASK: u64 = 3u64 << 62;
const TOKEN_MASK: u64 = !(3u64 << 62);

/// Size of per-connection receive buffers.
const RECV_BUF_SIZE: usize = 65536;

// ---------------------------------------------------------------------------
// IoUringWorker
// ---------------------------------------------------------------------------

pub(crate) struct IoUringWorker {
    core: WorkerCore,
    ring: IoUring,

    /// Per-connection receive buffers. Each connection gets a pinned 64 KiB
    /// buffer that is registered with io_uring for RECV operations.
    recv_bufs: FxHashMap<u64, Box<[u8; RECV_BUF_SIZE]>>,

    /// Connection IDs that have an outstanding RECV SQE in the ring.
    recv_inflight: FxHashSet<u64>,

    /// Connection IDs that have an outstanding SEND SQE in the ring.
    /// Maps conn_id → the BytesMut that was submitted (for partial writes).
    send_inflight: FxHashMap<u64, BytesMut>,
}

impl IoUringWorker {
    /// Spawn a worker thread using io_uring. Returns a handle for sending commands.
    pub(crate) fn spawn(index: usize, state: Arc<ServerState>) -> WorkerHandle {
        let (tx, rx) = mpsc::channel();
        let event_fd = Arc::new(create_eventfd());
        let event_fd_clone = Arc::clone(&event_fd);

        let join_handle = std::thread::Builder::new()
            .name(format!("worker-{index}"))
            .spawn(move || {
                let ring = IoUring::new(256).expect("failed to create io_uring instance");

                // WorkerCore's io_fd is set to -1 so that `epoll_modify` calls from
                // protocol processing become harmless no-ops.
                let core = WorkerCore::new(index, state, event_fd, rx, -1);

                let mut worker = IoUringWorker {
                    core,
                    ring,
                    recv_bufs: FxHashMap::default(),
                    recv_inflight: FxHashSet::default(),
                    send_inflight: FxHashMap::default(),
                };

                // Submit a POLL_ADD on the eventfd so we wake up for WorkerCmds.
                worker.submit_poll_eventfd();
                worker.run();
            })
            .expect("failed to spawn io_uring worker thread");

        WorkerHandle {
            tx,
            event_fd: event_fd_clone,
            join_handle: Some(join_handle),
        }
    }

    // -----------------------------------------------------------------------
    // SQE submission helpers
    // -----------------------------------------------------------------------

    /// Submit a POLL_ADD SQE for the eventfd (level-triggered, multi-shot).
    fn submit_poll_eventfd(&mut self) {
        let fd = self.core.event_fd.as_raw_fd();
        let entry = opcode::PollAdd::new(Fd(fd), libc::POLLIN as u32)
            .multi(true)
            .build()
            .user_data(OP_POLL | EVENT_FD_KEY);
        unsafe {
            if self.ring.submission().push(&entry).is_err() {
                let _ = self.ring.submit();
                let _ = self.ring.submission().push(&entry);
            }
        }
    }

    /// Submit a RECV SQE for the given connection.
    fn submit_recv(&mut self, conn_id: u64) {
        if self.recv_inflight.contains(&conn_id) {
            return; // already in-flight
        }
        let fd = match self.core.conns.get(&conn_id) {
            Some(c) => c.fd,
            None => return,
        };
        // Ensure a recv buffer exists.
        let buf = self
            .recv_bufs
            .entry(conn_id)
            .or_insert_with(|| Box::new([0u8; RECV_BUF_SIZE]));
        let entry = opcode::Recv::new(Fd(fd), buf.as_mut_ptr(), RECV_BUF_SIZE as u32)
            .build()
            .user_data(OP_RECV | conn_id);
        unsafe {
            if self.ring.submission().push(&entry).is_err() {
                let _ = self.ring.submit();
                let _ = self.ring.submission().push(&entry);
            }
        }
        self.recv_inflight.insert(conn_id);
    }

    /// Submit a SEND SQE for the given connection's pending write data.
    ///
    /// Collects data from write_buf + direct_buf into a single BytesMut and
    /// submits it. On partial-write completion, the remaining bytes are
    /// re-submitted.
    fn submit_send(&mut self, conn_id: u64) {
        if self.send_inflight.contains_key(&conn_id) {
            return; // already in-flight
        }
        let client = match self.core.conns.get_mut(&conn_id) {
            Some(c) => c,
            None => return,
        };

        // Drain the MsgWriter direct buffer into write_buf.
        if client.has_pending.load(Ordering::Acquire) {
            client.has_pending.store(false, Ordering::Release);
            use crate::sub_list::DirectBuf;
            use crate::util::LockExt;
            let mut dbuf = client.direct_buf.lock_or_poison();
            if !dbuf.is_empty() {
                match &mut *dbuf {
                    DirectBuf::Text(b) => {
                        client.write_buf.extend_from_slice(b);
                        b.clear();
                    }
                    DirectBuf::Binary(seg_buf) => {
                        seg_buf.materialize_into(&mut client.write_buf);
                    }
                }
            }
        }

        if client.write_buf.is_empty() {
            return;
        }

        // Take the write_buf data for the SEND SQE.
        let data = client.write_buf.split();
        let fd = client.fd;
        let ptr = data.as_ptr();
        let len = data.len() as u32;

        let entry = opcode::Send::new(Fd(fd), ptr, len)
            .build()
            .user_data(OP_SEND | conn_id);
        unsafe {
            if self.ring.submission().push(&entry).is_err() {
                let _ = self.ring.submit();
                let _ = self.ring.submission().push(&entry);
            }
        }
        self.send_inflight.insert(conn_id, data);
    }

    // -----------------------------------------------------------------------
    // Completion handlers
    // -----------------------------------------------------------------------

    /// Handle RECV completion: copy data to read_buf, process protocol, resubmit.
    fn handle_recv_complete(&mut self, conn_id: u64, result: i32) {
        self.recv_inflight.remove(&conn_id);

        if result <= 0 {
            // 0 = EOF, negative = error. Remove the connection.
            if result == 0 {
                debug!(conn_id, "uring RECV: EOF");
            } else {
                let errno = -result;
                // EAGAIN/EWOULDBLOCK shouldn't happen with completion I/O, but
                // handle gracefully: just resubmit.
                if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                    self.submit_recv(conn_id);
                    return;
                }
                debug!(conn_id, errno, "uring RECV error");
            }
            self.remove_conn(conn_id);
            return;
        }

        let n = result as usize;

        // Copy data from the recv buffer into the connection's read_buf.
        if let Some(client) = self.core.conns.get_mut(&conn_id) {
            if let Some(recv_buf) = self.recv_bufs.get(&conn_id) {
                client.read_buf.reserve(n);
                let chunk = client.read_buf.chunk_mut();
                let dest = unsafe { std::slice::from_raw_parts_mut(chunk.as_mut_ptr(), n) };
                dest.copy_from_slice(&recv_buf[..n]);
                unsafe { client.read_buf.advance_mut(n) };
            }
            client.last_activity = Instant::now();
        }

        // Process the protocol data.
        self.core.process_read_buf(conn_id);
        self.core.flush_notifications();

        // Check if connection was removed during processing.
        if !self.core.conns.contains_key(&conn_id) {
            return;
        }

        // Resubmit RECV for the next batch of data.
        self.submit_recv(conn_id);

        // If protocol processing generated outbound data, submit a SEND.
        self.try_submit_send(conn_id);
    }

    /// Handle SEND completion: advance buffer, resubmit if partial write.
    fn handle_send_complete(&mut self, conn_id: u64, result: i32) {
        let data = match self.send_inflight.remove(&conn_id) {
            Some(d) => d,
            None => return,
        };

        if result < 0 {
            let errno = -result;
            if errno == libc::EAGAIN || errno == libc::EWOULDBLOCK {
                // Put data back and retry.
                if let Some(client) = self.core.conns.get_mut(&conn_id) {
                    // Prepend the unsent data before any new data.
                    let new_data = client.write_buf.split();
                    client.write_buf.extend_from_slice(&data);
                    client.write_buf.extend_from_slice(&new_data);
                }
                self.submit_send(conn_id);
                return;
            }
            debug!(conn_id, errno, "uring SEND error");
            self.remove_conn(conn_id);
            return;
        }

        let written = result as usize;
        if written < data.len() {
            // Partial write — put the remainder back and resubmit.
            if let Some(client) = self.core.conns.get_mut(&conn_id) {
                let remainder = &data[written..];
                let new_data = client.write_buf.split();
                client.write_buf.extend_from_slice(remainder);
                client.write_buf.extend_from_slice(&new_data);
            }
            self.submit_send(conn_id);
        } else {
            // Full write — check if more data accumulated during the write.
            self.try_submit_send(conn_id);
        }
    }

    /// Submit a SEND if the connection has pending outbound data.
    fn try_submit_send(&mut self, conn_id: u64) {
        if self.send_inflight.contains_key(&conn_id) {
            return;
        }
        let has_data = self
            .core
            .conns
            .get(&conn_id)
            .is_some_and(|c| !c.write_buf.is_empty() || c.has_pending.load(Ordering::Relaxed));
        if has_data {
            self.submit_send(conn_id);
        }
    }

    /// Remove a connection: cancel inflight I/O, clean up WorkerCore state.
    fn remove_conn(&mut self, conn_id: u64) {
        self.recv_inflight.remove(&conn_id);
        self.recv_bufs.remove(&conn_id);
        self.send_inflight.remove(&conn_id);
        // WorkerCore::remove_conn handles epoll_deregister (no-op with io_fd=-1),
        // subscription cleanup, writer deregistration, etc.
        self.core.remove_conn(conn_id);
    }

    // -----------------------------------------------------------------------
    // Event loop
    // -----------------------------------------------------------------------

    fn run(&mut self) {
        // Compute timeout for periodic checks (same as epoll worker).
        let ping_interval_ms = self.core.state.ping_interval_ms.load(Ordering::Relaxed);
        let auth_timeout_ms = self.core.state.auth_timeout_ms.load(Ordering::Relaxed);
        let timeout_ms = {
            let ping_half = if ping_interval_ms == 0 {
                u64::MAX
            } else {
                (ping_interval_ms / 2).min(30_000)
            };
            let auth_half = if auth_timeout_ms == 0 {
                u64::MAX
            } else {
                (auth_timeout_ms / 2).clamp(100, 30_000)
            };
            let min = ping_half.min(auth_half);
            if min == u64::MAX {
                0u64
            } else {
                min
            }
        };

        // Submit a linked timeout if we need periodic checks.
        let use_timeout = timeout_ms > 0;
        let mut last_periodic = Instant::now();

        loop {
            // Submit pending SQEs and wait for at least 1 CQE.
            match self.ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(e) => {
                    if e.raw_os_error() == Some(libc::EINTR) {
                        continue;
                    }
                    warn!(error = %e, "io_uring submit_and_wait failed");
                    break;
                }
            }

            // Drain completions into a local buffer to release the borrow on
            // self.ring before we process them.
            let mut cqes: Vec<(u64, i32)> = Vec::with_capacity(64);
            for cqe in self.ring.completion() {
                cqes.push((cqe.user_data(), cqe.result()));
            }

            for (user_data, result) in cqes {
                let op = user_data & OP_MASK;
                let token = user_data & TOKEN_MASK;

                match op {
                    OP_POLL => {
                        if token == EVENT_FD_KEY {
                            self.core.handle_eventfd();
                            // Multi-shot POLL_ADD: no need to resubmit.

                            // Process new connections: submit initial RECVs.
                            let new_conn_ids: Vec<u64> = self.core.conns.keys().copied().collect();
                            for cid in new_conn_ids {
                                if !self.recv_inflight.contains(&cid) {
                                    self.submit_recv(cid);
                                }
                                self.try_submit_send(cid);
                            }
                        }
                    }
                    OP_RECV => {
                        self.handle_recv_complete(token, result);
                    }
                    OP_SEND => {
                        self.handle_send_complete(token, result);
                    }
                    _ => {}
                }
            }

            // Scan for pending MsgWriter data (same-worker delivery without
            // eventfd round-trip).
            self.flush_pending_sends();

            self.core.flush_metrics();

            // Periodic checks.
            if use_timeout {
                let elapsed = last_periodic.elapsed();
                if elapsed.as_millis() as u64 >= timeout_ms {
                    last_periodic = Instant::now();
                    self.check_pings_uring();
                    self.check_auth_timeout_uring();
                }
            }

            if self.core.shutdown {
                break;
            }
        }

        // Cleanup: remove all connections.
        let conn_ids: Vec<u64> = self.core.conns.keys().copied().collect();
        for conn_id in conn_ids {
            self.remove_conn(conn_id);
        }
    }

    /// Scan all connections for pending MsgWriter data and submit SENDs.
    fn flush_pending_sends(&mut self) {
        let conn_ids: Vec<u64> = self.core.conns.keys().copied().collect();
        for conn_id in conn_ids {
            let has_pending = self
                .core
                .conns
                .get(&conn_id)
                .map(|c| c.has_pending.load(Ordering::Relaxed) || !c.write_buf.is_empty())
                .unwrap_or(false);
            if has_pending {
                self.try_submit_send(conn_id);
            }
        }
    }

    /// Keepalive check for io_uring worker — writes PING to write_buf and
    /// submits a SEND if needed. Mirrors `WorkerCore::check_pings` but
    /// without epoll_modify calls.
    fn check_pings_uring(&mut self) {
        let now = Instant::now();
        let interval_ms = self.core.state.ping_interval_ms.load(Ordering::Relaxed);
        let interval = std::time::Duration::from_millis(interval_ms);
        let max_pings = self
            .core
            .state
            .max_pings_outstanding
            .load(Ordering::Relaxed);
        let mut to_remove: Vec<u64> = Vec::new();

        for (conn_id, client) in &mut self.core.conns {
            if !client.is_active() {
                continue;
            }
            let elapsed = now.duration_since(client.last_activity);
            if elapsed < interval {
                continue;
            }
            if client.pings_outstanding >= max_pings {
                warn!(conn_id = *conn_id, "connection stale, closing");
                to_remove.push(*conn_id);
                continue;
            }
            client.write_buf.extend_from_slice(b"PING\r\n");
            client.pings_outstanding += 1;
            client.last_activity = now;
        }

        // Submit sends for connections that got PINGs.
        let conn_ids: Vec<u64> = self.core.conns.keys().copied().collect();
        for conn_id in conn_ids {
            self.try_submit_send(conn_id);
        }

        for conn_id in to_remove {
            self.remove_conn(conn_id);
        }
    }

    /// Auth timeout check for io_uring worker.
    fn check_auth_timeout_uring(&mut self) {
        let timeout_ms = self.core.state.auth_timeout_ms.load(Ordering::Relaxed);
        if timeout_ms == 0 {
            return;
        }
        let timeout = std::time::Duration::from_millis(timeout_ms);
        let now = Instant::now();
        let mut to_remove: Vec<u64> = Vec::new();

        for (conn_id, client) in &mut self.core.conns {
            if client.is_active() {
                continue;
            }
            if now.duration_since(client.accepted_at) > timeout {
                warn!(conn_id = *conn_id, "authentication timeout");
                client
                    .write_buf
                    .extend_from_slice(b"-ERR 'Authentication Timeout'\r\n");
                to_remove.push(*conn_id);
            }
        }

        // Try to flush error messages before removing.
        for &conn_id in &to_remove {
            self.try_submit_send(conn_id);
        }
        for conn_id in to_remove {
            self.remove_conn(conn_id);
        }
    }
}
