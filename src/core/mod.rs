//! Core runtime: server accept loop and worker event loops.
pub mod server;
#[cfg(feature = "io-uring")]
pub(crate) mod uring_worker;
pub(crate) mod worker;
