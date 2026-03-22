//! Raw UDP transport with GSO/GRO — service thread for UDP binary transport.
//!
//! Uses UDP Generic Segmentation Offload (GSO) on the send path to submit
//! multiple datagrams in a single `sendmsg()` call, and Generic Receive
//! Offload (GRO) on the receive path to read coalesced datagrams from a
//! single `recvmsg()`. This amortizes per-packet kernel overhead.
//!
//! Fire-and-forget (at-most-once) delivery.

use std::net::{SocketAddr, UdpSocket};
use std::os::fd::{AsRawFd, OwnedFd, RawFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;

use tracing::{debug, error, info, warn};

use crate::handler::{deliver_to_subs_upstream_inner, handle_expired_subs_upstream};
use crate::server::ServerState;
use crate::sub_list::{create_eventfd, DirectWriter};

use super::codec::{self, BatchEncoder};
use super::{UdpCmd, UdpSender};

/// A UDP data channel to a single route peer.
/// Spawns a service thread that sends/receives raw datagrams.
pub(crate) struct UdpTransport {
    cmd_tx: mpsc::Sender<UdpCmd>,
    eventfd: Arc<OwnedFd>,
    shutdown: Arc<AtomicBool>,
}

impl UdpTransport {
    /// Create a new UDP transport and spawn the service thread.
    ///
    /// `local_port` is the UDP port to bind on this host.
    /// `peer_addr` is the remote peer's UDP address (host:port).
    /// `state` is the shared server state for delivering received messages.
    /// `is_server` controls thread naming only (raw UDP is symmetric).
    pub fn new(
        local_port: u16,
        peer_addr: SocketAddr,
        state: Arc<ServerState>,
        is_server: bool,
    ) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], local_port)))?;
        socket.set_nonblocking(true)?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let eventfd = Arc::new(create_eventfd());
        let (cmd_tx, cmd_rx) = mpsc::channel();

        let thread_shutdown = Arc::clone(&shutdown);
        let thread_eventfd = Arc::clone(&eventfd);
        let thread_name = format!(
            "udp-transport-{}",
            if is_server { "server" } else { "client" }
        );

        std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                if let Err(e) = run_service(
                    socket,
                    peer_addr,
                    state,
                    cmd_rx,
                    thread_shutdown,
                    thread_eventfd,
                ) {
                    error!(error = %e, "UDP transport service thread exited with error");
                }
            })?;

        Ok(Self {
            cmd_tx,
            eventfd,
            shutdown,
        })
    }

    /// Get a `UdpSender` wrapping the channel + eventfd for this transport.
    pub fn sender(&self) -> UdpSender {
        UdpSender::new(self.cmd_tx.clone(), Arc::clone(&self.eventfd))
    }

    /// Signal shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        let _ = self.cmd_tx.send(UdpCmd::Shutdown);
    }
}

impl Drop for UdpTransport {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Constants
// ────────────────────────────────────────────────────────────────────────────

/// Default batch buffer capacity — room for ~10 messages with 128B payloads.
const DEFAULT_BATCH_CAP: usize = 2048;

/// Timeout for poll() in milliseconds when the service loop is idle.
const POLL_TIMEOUT_MS: i32 = 5;

/// Segment size for GSO: each UDP datagram is at most this many bytes.
/// Batches are flushed at this threshold. All GSO segments except the last
/// must be exactly this size.
const SEGMENT_SIZE: u16 = 8000;

/// Flush threshold in bytes — alias for readability.
const FLUSH_SIZE: usize = SEGMENT_SIZE as usize;

/// Maximum GSO segments per `sendmsg()` call. Kept conservative to avoid
/// EMSGSIZE on kernels with lower limits (total payload ~64 KB).
const MAX_GSO_SEGMENTS: usize = 8;

/// Receive buffer size — large enough for GRO-coalesced datagrams.
/// GRO can coalesce multiple packets, so we size for many segments.
const RECV_BUF_SIZE: usize = MAX_GSO_SEGMENTS * FLUSH_SIZE;

/// Linux socket option level for UDP.
const SOL_UDP: libc::c_int = 17;

/// GSO: per-sendmsg segment size (cmsg type).
const UDP_SEGMENT: libc::c_int = 103;

/// GRO: enable receive offload (setsockopt + cmsg type on recv).
const UDP_GRO: libc::c_int = 104;

// ────────────────────────────────────────────────────────────────────────────
// Low-level helpers
// ────────────────────────────────────────────────────────────────────────────

/// Consume the eventfd counter (clear its readable state).
fn consume_eventfd(efd: &OwnedFd) {
    let mut val: u64 = 0;
    unsafe {
        libc::read(
            efd.as_raw_fd(),
            &mut val as *mut u64 as *mut libc::c_void,
            8,
        );
    }
}

/// Convert a `SocketAddr` to a raw `sockaddr_in` (IPv4).
fn sockaddr_v4(addr: &SocketAddr) -> (libc::sockaddr_in, libc::socklen_t) {
    match addr {
        SocketAddr::V4(v4) => {
            let mut sa: libc::sockaddr_in = unsafe { std::mem::zeroed() };
            sa.sin_family = libc::AF_INET as libc::sa_family_t;
            sa.sin_port = v4.port().to_be();
            sa.sin_addr.s_addr = u32::from_ne_bytes(v4.ip().octets());
            (
                sa,
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
            )
        }
        SocketAddr::V6(_) => {
            panic!("IPv6 not supported for UDP transport");
        }
    }
}

/// Enlarge socket send/receive buffers for GSO/GRO bulk transfers.
fn set_socket_buffers(fd: RawFd) {
    let buf_size: libc::c_int = 512 * 1024; // 512 KB
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &buf_size as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &buf_size as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

/// Enable UDP GRO on a socket. Returns `true` if successful.
fn enable_gro(fd: RawFd) -> bool {
    let val: libc::c_int = 1;
    let rc = unsafe {
        libc::setsockopt(
            fd,
            SOL_UDP,
            UDP_GRO,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        warn!(
            error = %std::io::Error::last_os_error(),
            "failed to enable UDP_GRO — falling back to per-packet recv"
        );
        false
    } else {
        debug!("UDP_GRO enabled");
        true
    }
}

// ────────────────────────────────────────────────────────────────────────────
// GSO send path
// ────────────────────────────────────────────────────────────────────────────

/// GSO send buffer — accumulates fixed-size segments for bulk `sendmsg()`.
///
/// All segments except the last are padded to exactly `SEGMENT_SIZE` bytes
/// so the kernel can split them correctly. The batch codec tolerates trailing
/// padding because it reads exactly `msg_count` messages and stops.
struct GsoBuf {
    buf: Vec<u8>,
    segment_count: usize,
}

impl GsoBuf {
    fn new() -> Self {
        Self {
            buf: Vec::with_capacity(MAX_GSO_SEGMENTS * FLUSH_SIZE),
            segment_count: 0,
        }
    }

    fn clear(&mut self) {
        self.buf.clear();
        self.segment_count = 0;
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn is_empty(&self) -> bool {
        self.segment_count == 0
    }

    /// Append a batch datagram. Must be `<= SEGMENT_SIZE` bytes.
    /// Pads the *previous* segment to `SEGMENT_SIZE` before appending.
    fn push(&mut self, data: &[u8]) {
        debug_assert!(data.len() <= FLUSH_SIZE);
        // Pad the previous segment so all segments except the last are SEGMENT_SIZE.
        if self.segment_count > 0 {
            let expected = self.segment_count * FLUSH_SIZE;
            if self.buf.len() < expected {
                self.buf.resize(expected, 0);
            }
        }
        self.buf.extend_from_slice(data);
        self.segment_count += 1;
    }

    /// Send all accumulated segments.
    ///
    /// If `gso_ok` is true and there are multiple segments, uses a single
    /// `sendmsg()` with `UDP_SEGMENT` cmsg. Falls back to per-segment
    /// `sendto()` if GSO is unavailable or fails.
    fn flush(&self, fd: RawFd, sa: &libc::sockaddr_in, sa_len: libc::socklen_t, gso_ok: &mut bool) {
        if self.segment_count == 0 {
            return;
        }

        // Single segment — plain sendto (no GSO overhead).
        if self.segment_count == 1 {
            unsafe {
                libc::sendto(
                    fd,
                    self.buf.as_ptr() as *const libc::c_void,
                    self.buf.len(),
                    0,
                    sa as *const libc::sockaddr_in as *const libc::sockaddr,
                    sa_len,
                );
            }
            return;
        }

        // Multiple segments — try GSO.
        if *gso_ok {
            let rc = sendmsg_gso(fd, &self.buf, SEGMENT_SIZE, sa, sa_len);
            if rc >= 0 {
                return;
            }
            let err = std::io::Error::last_os_error();
            let ecode = err.raw_os_error().unwrap_or(0);
            match ecode {
                // Send buffer full — fire-and-forget, drop this batch.
                libc::EAGAIN => return,
                // Super-datagram too large — fall through to per-segment
                // sendto for this call, but keep GSO enabled for smaller
                // batches in future calls.
                libc::EMSGSIZE => {
                    debug!("sendmsg GSO: EMSGSIZE, falling back to per-segment sendto");
                }
                // Kernel doesn't support GSO — permanently disable.
                _ => {
                    warn!(error = %err, "sendmsg GSO not supported — disabling");
                    *gso_ok = false;
                }
            }
        }

        // Fallback: per-segment sendto.
        let mut offset = 0;
        for i in 0..self.segment_count {
            let end = if i + 1 < self.segment_count {
                offset + FLUSH_SIZE
            } else {
                self.buf.len()
            };
            unsafe {
                libc::sendto(
                    fd,
                    self.buf[offset..].as_ptr() as *const libc::c_void,
                    end - offset,
                    0,
                    sa as *const libc::sockaddr_in as *const libc::sockaddr,
                    sa_len,
                );
            }
            offset = end;
        }
    }
}

/// Send a GSO super-datagram via `sendmsg()` with `UDP_SEGMENT` cmsg.
/// Returns the sendmsg return value (>= 0 on success, < 0 on error).
fn sendmsg_gso(
    fd: RawFd,
    buf: &[u8],
    segment_size: u16,
    sa: &libc::sockaddr_in,
    sa_len: libc::socklen_t,
) -> isize {
    let mut iov = libc::iovec {
        iov_base: buf.as_ptr() as *mut libc::c_void,
        iov_len: buf.len(),
    };

    // Control message buffer for UDP_SEGMENT.
    let cmsg_space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<u16>() as u32) } as usize;
    let mut cmsg_buf = [0u8; 64]; // more than enough for CMSG_SPACE(2)
    debug_assert!(cmsg_space <= cmsg_buf.len());

    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_name = sa as *const libc::sockaddr_in as *const libc::c_void as *mut libc::c_void;
    msg.msg_namelen = sa_len;
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_space as _;

    unsafe {
        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_level = SOL_UDP;
        (*cmsg).cmsg_type = UDP_SEGMENT;
        (*cmsg).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<u16>() as u32) as _;
        *(libc::CMSG_DATA(cmsg) as *mut u16) = segment_size;

        libc::sendmsg(fd, &msg, 0)
    }
}

// ────────────────────────────────────────────────────────────────────────────
// GRO receive path
// ────────────────────────────────────────────────────────────────────────────

/// Receive a (possibly GRO-coalesced) datagram via `recvmsg()`.
///
/// Returns `(bytes_read, gro_segment_size)`. If `gro_segment_size` is `Some`,
/// the buffer contains multiple coalesced segments of that size (last may be
/// smaller). If `None`, it's a single datagram.
///
/// Returns `(0, None)` on `EAGAIN` / `EWOULDBLOCK`.
fn recv_gro(fd: RawFd, buf: &mut [u8], cmsg_buf: &mut [u8; 64]) -> (usize, Option<u16>) {
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr() as *mut libc::c_void,
        iov_len: buf.len(),
    };

    let cmsg_space = unsafe { libc::CMSG_SPACE(std::mem::size_of::<u16>() as u32) } as usize;
    debug_assert!(cmsg_space <= cmsg_buf.len());

    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg_buf.as_mut_ptr() as *mut libc::c_void;
    msg.msg_controllen = cmsg_space as _;

    let n = unsafe { libc::recvmsg(fd, &mut msg, libc::MSG_DONTWAIT) };
    if n <= 0 {
        return (0, None);
    }

    // Parse cmsg for UDP_GRO segment size.
    let mut seg_size: Option<u16> = None;
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == SOL_UDP && (*cmsg).cmsg_type == UDP_GRO {
                seg_size = Some(*(libc::CMSG_DATA(cmsg) as *const u16));
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }
    }

    (n as usize, seg_size)
}

/// Process a received buffer, splitting by GRO segment size if present.
fn process_recv(
    buf: &[u8],
    total: usize,
    seg_size: Option<u16>,
    state: &ServerState,
    dirty_writers: &mut Vec<DirectWriter>,
) {
    match seg_size {
        Some(seg) => {
            let seg = seg as usize;
            let mut offset = 0;
            while offset < total {
                let end = std::cmp::min(offset + seg, total);
                handle_incoming_batch(&buf[offset..end], state, dirty_writers);
                offset = end;
            }
        }
        None => {
            handle_incoming_batch(&buf[..total], state, dirty_writers);
        }
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Service loop (bidirectional transport)
// ────────────────────────────────────────────────────────────────────────────

/// Main service loop. Runs on a dedicated thread.
fn run_service(
    socket: UdpSocket,
    peer_addr: SocketAddr,
    state: Arc<ServerState>,
    cmd_rx: mpsc::Receiver<UdpCmd>,
    shutdown: Arc<AtomicBool>,
    eventfd: Arc<OwnedFd>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut encoder = BatchEncoder::with_capacity(DEFAULT_BATCH_CAP);
    let mut dirty_writers: Vec<DirectWriter> = Vec::new();
    let mut gso_buf = GsoBuf::new();
    let (peer_sa, peer_sa_len) = sockaddr_v4(&peer_addr);
    let mut gso_ok = true; // optimistic — disable on first failure

    let sock_fd = socket.as_raw_fd();
    let efd = eventfd.as_raw_fd();

    set_socket_buffers(sock_fd);
    let gro_enabled = enable_gro(sock_fd);

    let mut recv_buf = vec![0u8; RECV_BUF_SIZE];
    let mut cmsg_buf = [0u8; 64];

    debug!(
        peer = %peer_addr,
        gso = gso_ok,
        gro = gro_enabled,
        "UDP transport service started"
    );

    loop {
        if shutdown.load(Ordering::Relaxed) {
            debug!("UDP transport shutting down");
            return Ok(());
        }

        // Block until the socket has data OR the eventfd is signalled.
        let mut pollfds = [
            libc::pollfd {
                fd: sock_fd,
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: efd,
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        unsafe {
            libc::poll(pollfds.as_mut_ptr(), 2, POLL_TIMEOUT_MS);
        }

        // Consume eventfd if it fired (prevents busy-loop).
        if pollfds[1].revents & libc::POLLIN != 0 {
            consume_eventfd(&eventfd);
        }

        // Receive datagrams (possibly GRO-coalesced).
        loop {
            let (n, seg_size) = recv_gro(sock_fd, &mut recv_buf, &mut cmsg_buf);
            if n == 0 {
                break;
            }
            process_recv(&recv_buf, n, seg_size, &state, &mut dirty_writers);
        }

        // Notify all dirty writers from this batch of received messages.
        for w in dirty_writers.drain(..) {
            w.notify();
        }

        // Drain command channel — accumulate batches, then GSO send.
        drain_and_send(
            &cmd_rx,
            &mut encoder,
            &mut gso_buf,
            sock_fd,
            &peer_sa,
            peer_sa_len,
            &mut gso_ok,
        );
    }
}

/// Drain the command channel, accumulate batch segments, and send via GSO.
fn drain_and_send(
    cmd_rx: &mpsc::Receiver<UdpCmd>,
    encoder: &mut BatchEncoder,
    gso_buf: &mut GsoBuf,
    fd: RawFd,
    peer_sa: &libc::sockaddr_in,
    peer_sa_len: libc::socklen_t,
    gso_ok: &mut bool,
) {
    encoder.clear();
    gso_buf.clear();

    while let Ok(cmd) = cmd_rx.try_recv() {
        match cmd {
            UdpCmd::Send(msg) => {
                // Estimate wire size of this message to ensure the batch stays
                // within SEGMENT_SIZE (required for correct GSO splitting).
                let msg_wire = codec::MSG_HEADER_SIZE
                    + msg.subject().len()
                    + msg.reply().map_or(0, |r| r.len())
                    + msg.hdr_bytes().map_or(0, |h| h.len())
                    + msg.payload().len();

                // Flush BEFORE push if adding this message would exceed the
                // segment size, so every batch fits in one GSO segment.
                if !encoder.is_empty()
                    && (encoder.encoded_size() + msg_wire > FLUSH_SIZE
                        || encoder.len() == 255)
                {
                    gso_buf.push(encoder.finish());
                    encoder.clear();

                    if gso_buf.segment_count >= MAX_GSO_SEGMENTS {
                        gso_buf.flush(fd, peer_sa, peer_sa_len, gso_ok);
                        gso_buf.clear();
                    }
                }

                encoder.push(msg.subject(), msg.reply(), msg.hdr_bytes(), msg.payload());
            }
            UdpCmd::Shutdown => return,
        }
    }

    // Flush remaining encoder contents as a final (possibly smaller) segment.
    if !encoder.is_empty() {
        gso_buf.push(encoder.finish());
    }

    // Send all accumulated segments.
    gso_buf.flush(fd, peer_sa, peer_sa_len, gso_ok);
}

/// Decode an incoming binary batch and deliver messages to local subscribers.
fn handle_incoming_batch(data: &[u8], state: &ServerState, dirty_writers: &mut Vec<DirectWriter>) {
    let iter = match codec::decode_batch(data) {
        Ok(iter) => iter,
        Err(e) => {
            warn!(error = %e, "failed to decode UDP batch");
            return;
        }
    };

    for result in iter {
        let msg = match result {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "failed to decode UDP message entry");
                return;
            }
        };

        let subject_str = match std::str::from_utf8(msg.subject) {
            Ok(s) => s,
            Err(_) => {
                warn!("invalid UTF-8 in UDP message subject");
                continue;
            }
        };

        // Parse header bytes back into HeaderMap if present.
        let headers = msg.headers.and_then(|h| {
            crate::nats_proto::parse_headers(h)
                .map_err(|e| warn!(error = %e, "failed to parse headers from UDP message"))
                .ok()
        });

        // One-hop: skip route subs — messages from UDP transport are never
        // re-forwarded to other route peers.
        let (_delivered, expired) = deliver_to_subs_upstream_inner(
            state,
            msg.subject,
            subject_str,
            msg.reply,
            headers.as_ref(),
            msg.payload,
            dirty_writers,
            true, // skip_routes (one-hop rule)
            #[cfg(feature = "gateway")]
            false, // don't skip gateways
            #[cfg(feature = "accounts")]
            0, // default account
        );
        handle_expired_subs_upstream(
            &expired,
            state,
            #[cfg(feature = "accounts")]
            0,
        );
    }
}

// ────────────────────────────────────────────────────────────────────────────
// UdpListener — shared server-side socket receiving inbound datagrams
// ────────────────────────────────────────────────────────────────────────────

/// A shared server-side UDP listener that binds on `cluster_udp_port` and
/// receives incoming datagrams from outbound `UdpTransport` senders.
///
/// Runs a service thread that polls the socket for incoming datagrams, decodes
/// binary batches, and delivers messages to local subscribers. Does not send
/// messages (outbound transport handles that direction).
pub(crate) struct UdpListener {
    shutdown: Arc<AtomicBool>,
}

impl UdpListener {
    /// Bind on `port` and spawn the server-side UDP listener thread.
    pub fn new(port: u16, state: Arc<ServerState>) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], port)))?;
        socket.set_nonblocking(true)?;

        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_shutdown = Arc::clone(&shutdown);

        std::thread::Builder::new()
            .name("udp-listener".into())
            .spawn(move || {
                if let Err(e) = run_listener(socket, state, thread_shutdown) {
                    error!(error = %e, "UDP listener thread exited with error");
                }
            })?;

        info!(port, "UDP listener started");
        Ok(Self { shutdown })
    }

    /// Signal shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

impl Drop for UdpListener {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Server-side UDP listener loop. Receives datagrams and delivers messages.
fn run_listener(
    socket: UdpSocket,
    state: Arc<ServerState>,
    shutdown: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut dirty_writers: Vec<DirectWriter> = Vec::new();
    let sock_fd = socket.as_raw_fd();

    set_socket_buffers(sock_fd);
    let gro_enabled = enable_gro(sock_fd);

    let mut recv_buf = vec![0u8; RECV_BUF_SIZE];
    let mut cmsg_buf = [0u8; 64];

    debug!(gro = gro_enabled, "UDP listener started");

    loop {
        if shutdown.load(Ordering::Relaxed) {
            debug!("UDP listener shutting down");
            return Ok(());
        }

        // Block until the socket has data or timeout.
        let mut pollfds = [libc::pollfd {
            fd: sock_fd,
            events: libc::POLLIN,
            revents: 0,
        }];
        unsafe {
            libc::poll(pollfds.as_mut_ptr(), 1, POLL_TIMEOUT_MS);
        }

        // Receive datagrams (possibly GRO-coalesced).
        loop {
            let (n, seg_size) = recv_gro(sock_fd, &mut recv_buf, &mut cmsg_buf);
            if n == 0 {
                break;
            }
            process_recv(&recv_buf, n, seg_size, &state, &mut dirty_writers);
        }

        for w in dirty_writers.drain(..) {
            w.notify();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_encoder_flush_threshold() {
        let mut encoder = BatchEncoder::with_capacity(DEFAULT_BATCH_CAP);
        // Push messages until we'd exceed the flush threshold.
        let subject = b"test.subject.long.name";
        let payload = [0u8; 512];
        let mut count = 0;
        while encoder.encoded_size() < FLUSH_SIZE && encoder.len() < 255 {
            encoder.push(subject, None, None, &payload);
            count += 1;
        }
        // Should have accumulated multiple messages.
        assert!(count > 10);
        assert!(encoder.encoded_size() >= FLUSH_SIZE || encoder.len() == 255);
    }

    #[test]
    fn test_batch_size_calculation() {
        let encoder = BatchEncoder::with_capacity(256);
        // Empty batch = 3 bytes (header only).
        assert_eq!(encoder.encoded_size(), codec::BATCH_HEADER_SIZE);
    }

    #[test]
    fn test_gso_buf_padding() {
        let mut gso = GsoBuf::new();
        assert!(gso.is_empty());

        // Push a small segment (100 bytes).
        let seg1 = vec![0xAA; 100];
        gso.push(&seg1);
        assert_eq!(gso.segment_count, 1);
        assert_eq!(gso.buf.len(), 100); // no padding yet (only segment)

        // Push a second segment — first should get padded to SEGMENT_SIZE.
        let seg2 = vec![0xBB; 200];
        gso.push(&seg2);
        assert_eq!(gso.segment_count, 2);
        assert_eq!(gso.buf.len(), FLUSH_SIZE + 200);
        // First segment padded with zeros.
        assert_eq!(gso.buf[100], 0);
        assert_eq!(gso.buf[FLUSH_SIZE - 1], 0);
        // Second segment data starts at FLUSH_SIZE.
        assert_eq!(gso.buf[FLUSH_SIZE], 0xBB);

        gso.clear();
        assert!(gso.is_empty());
        assert_eq!(gso.buf.len(), 0);
    }

    #[test]
    fn test_sockaddr_v4_conversion() {
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let (sa, len) = sockaddr_v4(&addr);
        assert_eq!(sa.sin_family, libc::AF_INET as libc::sa_family_t);
        assert_eq!(u16::from_be(sa.sin_port), 8080);
        assert_eq!(len as usize, std::mem::size_of::<libc::sockaddr_in>());
    }

    #[test]
    fn test_process_recv_single_batch() {
        // Verify process_recv with seg_size=None passes whole buffer.
        // (Cannot test full delivery without ServerState, but can test the split logic.)
        let data = [0xCA, 0xFE, 0x00]; // valid empty batch
        let _dirty: Vec<DirectWriter> = Vec::new();

        // We can't call process_recv without a real ServerState, but we can
        // verify the GRO splitting logic separately.
        let seg_size: Option<u16> = None;
        let total = data.len();
        let segments: Vec<&[u8]> = match seg_size {
            Some(seg) => {
                let seg = seg as usize;
                let mut v = Vec::new();
                let mut off = 0;
                while off < total {
                    let end = std::cmp::min(off + seg, total);
                    v.push(&data[off..end]);
                    off = end;
                }
                v
            }
            None => vec![&data[..total]],
        };
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0], &data);
    }

    #[test]
    fn test_process_recv_gro_split() {
        // Simulate a GRO-coalesced buffer with segment_size=4.
        let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let seg_size: Option<u16> = Some(4);
        let total = data.len();

        let mut segments: Vec<&[u8]> = Vec::new();
        if let Some(seg) = seg_size {
            let seg = seg as usize;
            let mut off = 0;
            while off < total {
                let end = std::cmp::min(off + seg, total);
                segments.push(&data[off..end]);
                off = end;
            }
        }
        // Should split into [1,2,3,4], [5,6,7,8], [9,10]
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0], &[1, 2, 3, 4]);
        assert_eq!(segments[1], &[5, 6, 7, 8]);
        assert_eq!(segments[2], &[9, 10]);
    }
}
