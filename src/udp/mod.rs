//! UDP binary transport for inter-cluster communication.
//!
//! This module provides a binary wire protocol over UDP for high-performance
//! message forwarding between cluster peers. See `docs/adr/011-udp-binary-transport.md`.

pub mod codec;
