//! DirectWriter — shared buffer + eventfd notification for zero-channel message delivery.
//!
//! Instead of sending structs through an mpsc channel, the upstream reader formats
//! MSG/HMSG wire bytes directly into this shared buffer. The worker thread is
//! notified via a shared eventfd to flush the buffer to TCP.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use bytes::BytesMut;

use crate::nats_proto::MsgBuilder;
use crate::types::HeaderMap;

/// Create a Linux eventfd for notification.
pub(crate) fn create_eventfd() -> OwnedFd {
    let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
    assert!(
        fd >= 0,
        "eventfd creation failed: {}",
        std::io::Error::last_os_error()
    );
    unsafe { OwnedFd::from_raw_fd(fd) }
}

/// A shared write buffer + eventfd pair for zero-channel message delivery.
///
/// Instead of sending `ClientMsg` structs through an mpsc channel (which costs
/// atomic ops + linked-list push + task wake per message), the upstream reader
/// formats MSG/HMSG wire bytes directly into this shared buffer. The worker
/// thread is notified via a shared eventfd to flush the buffer to TCP.
///
/// Multiple DirectWriters on the same worker share one eventfd, so fan-out
/// to N connections on one worker costs only 1 eventfd write.
#[derive(Clone)]
pub(crate) struct DirectWriter {
    buf: Arc<Mutex<BytesMut>>,
    event_fd: Arc<OwnedFd>,
    has_pending: Arc<AtomicBool>,
    /// Pre-built MsgBuilder for formatting — kept per-writer to avoid allocation.
    msg_builder: Arc<Mutex<MsgBuilder>>,
}

impl DirectWriter {
    /// Create a DirectWriter with an externally-owned eventfd (shared by worker).
    pub(crate) fn new(
        buf: Arc<Mutex<BytesMut>>,
        has_pending: Arc<AtomicBool>,
        event_fd: Arc<OwnedFd>,
    ) -> Self {
        Self {
            buf,
            has_pending,
            event_fd,
            msg_builder: Arc::new(Mutex::new(MsgBuilder::new())),
        }
    }

    /// Create a standalone DirectWriter with its own eventfd (for tests/benchmarks).
    pub(crate) fn new_dummy() -> Self {
        let buf = Arc::new(Mutex::new(BytesMut::with_capacity(65536)));
        let has_pending = Arc::new(AtomicBool::new(false));
        let event_fd = Arc::new(create_eventfd());
        Self {
            buf,
            has_pending,
            event_fd,
            msg_builder: Arc::new(Mutex::new(MsgBuilder::new())),
        }
    }

    /// Format and append a MSG/HMSG to the shared buffer. Fully synchronous.
    pub(crate) fn write_msg(
        &self,
        subject: &[u8],
        sid_bytes: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) {
        let mut builder = self.msg_builder.lock().unwrap();
        let data = builder.build_msg(subject, sid_bytes, reply, headers, payload);
        let mut buf = self.buf.lock().unwrap();
        buf.extend_from_slice(data);
        drop(buf);
        self.has_pending.store(true, Ordering::Release);
    }

    /// Format and append an LMSG to the shared buffer (for leaf node delivery).
    #[cfg(any(feature = "leaf", feature = "hub"))]
    pub(crate) fn write_lmsg(
        &self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) {
        let mut builder = self.msg_builder.lock().unwrap();
        let data = builder.build_lmsg(subject, reply, headers, payload);
        let mut buf = self.buf.lock().unwrap();
        buf.extend_from_slice(data);
        drop(buf);
        self.has_pending.store(true, Ordering::Release);
    }

    /// Format and append an RMSG to the shared buffer (for route/gateway delivery).
    #[cfg(any(feature = "cluster", feature = "gateway"))]
    pub(crate) fn write_rmsg(
        &self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
        #[cfg(feature = "accounts")] account: &[u8],
    ) {
        let mut builder = self.msg_builder.lock().unwrap();
        let data = builder.build_rmsg(
            subject,
            reply,
            headers,
            payload,
            #[cfg(feature = "accounts")]
            account,
        );
        let mut buf = self.buf.lock().unwrap();
        buf.extend_from_slice(data);
        drop(buf);
        self.has_pending.store(true, Ordering::Release);
    }

    /// Append raw protocol bytes to the shared buffer (e.g. LS+/LS-/RS+ lines).
    #[cfg(any(feature = "hub", feature = "cluster", feature = "gateway"))]
    pub(crate) fn write_raw(&self, data: &[u8]) {
        let mut buf = self.buf.lock().unwrap();
        buf.extend_from_slice(data);
        drop(buf);
        self.has_pending.store(true, Ordering::Release);
    }

    /// Notify the worker thread that there is data to flush.
    /// Writes 1 to the eventfd — wakes epoll_wait() on the worker thread.
    /// Multiple writers sharing one eventfd collapse into a single wake.
    pub(crate) fn notify(&self) {
        let val: u64 = 1;
        unsafe {
            libc::write(
                self.event_fd.as_raw_fd(),
                &val as *const u64 as *const libc::c_void,
                8,
            );
        }
    }

    /// Drain all buffered data. Returns `None` if buffer was empty.
    #[cfg(any(test, feature = "cluster", feature = "gateway"))]
    pub(crate) fn drain(&self) -> Option<BytesMut> {
        let mut buf = self.buf.lock().unwrap();
        if buf.is_empty() {
            None
        } else {
            Some(buf.split())
        }
    }

    /// Get the raw fd of the eventfd.
    pub(crate) fn event_raw_fd(&self) -> std::os::fd::RawFd {
        self.event_fd.as_raw_fd()
    }

    /// Read the eventfd to reset it after poll() returns POLLIN.
    #[cfg(test)]
    pub(crate) fn consume_notify(&self) {
        let mut val: u64 = 0;
        unsafe {
            libc::read(
                self.event_fd.as_raw_fd(),
                &mut val as *mut u64 as *mut libc::c_void,
                8,
            );
        }
    }
}

impl std::fmt::Debug for DirectWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DirectWriter").finish()
    }
}
