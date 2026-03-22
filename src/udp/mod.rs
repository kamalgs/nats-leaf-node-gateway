//! UDP binary transport for inter-cluster communication.
//!
//! This module provides a binary wire protocol over UDP for high-performance
//! message forwarding between cluster peers. See `docs/adr/011-udp-binary-transport.md`.
//!
//! # Architecture
//!
//! Each `UdpTransport` manages a single enet `Host` on a dedicated service thread.
//! The service thread runs an enet poll loop that:
//! - Receives `UdpCmd::Send` messages via mpsc, batches them, and sends via enet
//! - On receive, decodes binary batches and delivers to local subscribers
//!
//! The TCP route connection remains for the control plane (RS+/RS-/PING/PONG).
//! The UDP transport is the data plane (message forwarding only).

pub mod codec;
pub(crate) mod transport;

use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::mpsc;
use std::sync::Arc;

use bytes::Bytes;

/// Configuration for the UDP transport layer.
#[derive(Debug, Clone)]
pub struct UdpTransportConfig {
    /// UDP port for enet host.
    pub udp_port: u16,
    /// Batch accumulation interval in microseconds (default: 200).
    pub batch_interval_us: u64,
}

impl Default for UdpTransportConfig {
    fn default() -> Self {
        Self {
            udp_port: 9222,
            batch_interval_us: 200,
        }
    }
}

/// A message packed into a single contiguous `Bytes` buffer with field offsets.
///
/// Layout: `[subject][reply][hdr_bytes][payload]`
/// - `subject_end` marks end of subject
/// - `reply_end` marks end of reply (== subject_end when no reply)
/// - `hdr_end` marks end of pre-serialized headers (== reply_end when no headers)
/// - payload is `data[hdr_end..]`
pub(crate) struct PackedUdpMsg {
    data: Bytes,
    subject_end: u16,
    reply_end: u16,
    hdr_end: u16,
}

impl PackedUdpMsg {
    /// Pack subject, optional reply, optional pre-serialized header bytes, and payload
    /// into a single contiguous buffer.
    pub fn new(
        subject: &[u8],
        reply: Option<&[u8]>,
        hdr_bytes: Option<&[u8]>,
        payload: &[u8],
    ) -> Self {
        let reply_bytes = reply.unwrap_or(&[]);
        let hdr = hdr_bytes.unwrap_or(&[]);
        let total = subject.len() + reply_bytes.len() + hdr.len() + payload.len();
        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(subject);
        let subject_end = buf.len() as u16;
        buf.extend_from_slice(reply_bytes);
        let reply_end = buf.len() as u16;
        buf.extend_from_slice(hdr);
        let hdr_end = buf.len() as u16;
        buf.extend_from_slice(payload);
        Self {
            data: Bytes::from(buf),
            subject_end,
            reply_end,
            hdr_end,
        }
    }

    /// The message subject.
    pub fn subject(&self) -> &[u8] {
        &self.data[..self.subject_end as usize]
    }

    /// The optional reply-to.
    pub fn reply(&self) -> Option<&[u8]> {
        if self.reply_end == self.subject_end {
            None
        } else {
            Some(&self.data[self.subject_end as usize..self.reply_end as usize])
        }
    }

    /// Pre-serialized header bytes, if any.
    pub fn hdr_bytes(&self) -> Option<&[u8]> {
        if self.hdr_end == self.reply_end {
            None
        } else {
            Some(&self.data[self.reply_end as usize..self.hdr_end as usize])
        }
    }

    /// The message payload.
    pub fn payload(&self) -> &[u8] {
        &self.data[self.hdr_end as usize..]
    }
}

/// Commands sent to the UDP transport service thread.
pub(crate) enum UdpCmd {
    /// Forward a message to this peer via UDP binary protocol.
    Send(PackedUdpMsg),
    /// Shut down the transport.
    Shutdown,
}

/// Wrapper around the mpsc sender + eventfd for waking the service thread.
///
/// After enqueuing a `UdpCmd`, calling `notify()` writes the eventfd so the
/// service thread wakes from `poll()` immediately instead of waiting for the
/// timeout.
pub(crate) struct UdpSender {
    pub tx: mpsc::Sender<UdpCmd>,
    eventfd: Arc<OwnedFd>,
}

impl UdpSender {
    /// Create a new sender with the given channel and eventfd.
    pub fn new(tx: mpsc::Sender<UdpCmd>, eventfd: Arc<OwnedFd>) -> Self {
        Self { tx, eventfd }
    }

    /// Wake the service thread after enqueuing a command.
    pub fn notify(&self) {
        let val: u64 = 1;
        unsafe {
            libc::write(
                self.eventfd.as_raw_fd(),
                &val as *const u64 as *const libc::c_void,
                8,
            );
        }
    }
}

impl Clone for UdpSender {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            eventfd: Arc::clone(&self.eventfd),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_udp_msg_no_reply_no_headers() {
        let msg = PackedUdpMsg::new(b"foo.bar", None, None, b"hello");
        assert_eq!(msg.subject(), b"foo.bar");
        assert!(msg.reply().is_none());
        assert!(msg.hdr_bytes().is_none());
        assert_eq!(msg.payload(), b"hello");
    }

    #[test]
    fn packed_udp_msg_with_reply_and_headers() {
        let msg = PackedUdpMsg::new(b"foo", Some(b"_INBOX.123"), Some(b"NATS/1.0\r\n\r\n"), b"x");
        assert_eq!(msg.subject(), b"foo");
        assert_eq!(msg.reply().unwrap(), b"_INBOX.123");
        assert_eq!(msg.hdr_bytes().unwrap(), b"NATS/1.0\r\n\r\n");
        assert_eq!(msg.payload(), b"x");
    }
}
