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

use bytes::Bytes;

use crate::types::HeaderMap;

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

/// Commands sent to the UDP transport service thread.
pub(crate) enum UdpCmd {
    /// Forward a message to this peer via UDP binary protocol.
    Send {
        subject: Bytes,
        reply: Option<Bytes>,
        headers: Option<HeaderMap>,
        payload: Bytes,
    },
    /// Shut down the transport.
    Shutdown,
}
