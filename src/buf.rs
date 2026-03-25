//! Adaptive read buffer and connection I/O primitives.
//!
//! `AdaptiveBuf` implements Go-style dynamic buffer sizing (512B → 64KB).
//! `BufConfig` controls buffer sizes and slow-consumer limits.
//! `ServerConn` is a test-only wrapper for unit testing protocol parsing.

use std::io::{self, Read};
#[cfg(test)]
use std::io::{BufWriter, Write};
#[cfg(test)]
use std::net::TcpStream;
use std::ops::{Deref, DerefMut};
use std::os::fd::RawFd;

use bytes::{BufMut, BytesMut};

#[cfg(test)]
use crate::nats_proto::{self, MsgBuilder};
#[cfg(test)]
use crate::types::HeaderMap;
#[cfg(test)]
use crate::types::ServerInfo;

// Re-export parsed op types so the rest of the crate uses nats_proto's types.
pub(crate) use crate::nats_proto::ClientOp;
#[cfg(any(feature = "leaf", feature = "hub"))]
pub(crate) use crate::nats_proto::LeafOp;
#[cfg(feature = "cluster")]
pub(crate) use crate::nats_proto::RouteOp;

// --- Adaptive read buffer (Go-style dynamic sizing) ---

const DEFAULT_START_BUF: usize = 512;
const DEFAULT_MIN_BUF: usize = 64;
const DEFAULT_MAX_BUF: usize = 65536;
const SHORTS_TO_SHRINK: u8 = 2;

/// Configuration for adaptive read buffer sizing.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BufConfig {
    pub max_read_buf: usize,
    pub write_buf: usize,
    /// Maximum pending write bytes per connection before disconnecting as slow consumer.
    /// 0 means unlimited.
    pub max_pending: usize,
}

impl Default for BufConfig {
    fn default() -> Self {
        Self {
            max_read_buf: DEFAULT_MAX_BUF,
            write_buf: DEFAULT_MAX_BUF,
            max_pending: 64 * 1024 * 1024,
        }
    }
}

/// A read buffer that starts small and grows/shrinks based on utilization,
/// matching Go's nats-server strategy: start at 512B, double on full reads,
/// halve after 2 consecutive short reads, floor at 64B, ceiling at max.
pub(crate) struct AdaptiveBuf {
    buf: BytesMut,
    target_cap: usize,
    max_cap: usize,
    shorts: u8,
}

impl AdaptiveBuf {
    pub(crate) fn new(max_cap: usize) -> Self {
        let start = DEFAULT_START_BUF.min(max_cap);
        Self {
            buf: BytesMut::with_capacity(start),
            target_cap: start,
            max_cap,
            shorts: 0,
        }
    }

    /// Called after each successful socket read with the number of bytes read.
    /// Adjusts the target capacity and reallocates if appropriate.
    pub(crate) fn after_read(&mut self, n: usize) {
        if n >= self.target_cap && self.target_cap < self.max_cap {
            // Buffer was fully utilized — grow
            self.target_cap = (self.target_cap * 2).min(self.max_cap);
            // Ensure we have enough capacity for the next read
            let additional = self
                .target_cap
                .saturating_sub(self.buf.capacity() - self.buf.len());
            if additional > 0 {
                self.buf.reserve(additional);
            }
            self.shorts = 0;
        } else if n < self.target_cap / 2 {
            // Short read
            self.shorts = self.shorts.saturating_add(1);
            if self.shorts > SHORTS_TO_SHRINK && self.target_cap > DEFAULT_MIN_BUF {
                self.target_cap = (self.target_cap / 2).max(DEFAULT_MIN_BUF);
                // Only reallocate when buffer is empty (all data consumed)
                if self.buf.is_empty() {
                    self.buf = BytesMut::with_capacity(self.target_cap);
                }
            }
        } else {
            self.shorts = 0;
        }
    }

    /// Try to shrink the buffer if it is empty and oversized.
    /// Call this after parsing has consumed all data.
    pub(crate) fn try_shrink(&mut self) {
        if self.buf.is_empty() && self.buf.capacity() > self.target_cap * 2 {
            self.buf = BytesMut::with_capacity(self.target_cap);
        }
    }

    /// Read from a raw fd into the buffer's spare capacity (non-blocking).
    /// Uses libc::read directly for non-blocking socket I/O.
    pub(crate) fn read_from_fd(&mut self, fd: RawFd) -> io::Result<usize> {
        if self.buf.remaining_mut() == 0 {
            self.buf.reserve(self.target_cap.max(DEFAULT_START_BUF));
        }
        let chunk = self.buf.chunk_mut();
        let n = unsafe { libc::read(fd, chunk.as_mut_ptr() as *mut libc::c_void, chunk.len()) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        let n = n as usize;
        unsafe { self.buf.advance_mut(n) };
        Ok(n)
    }

    /// Read from a reader into the buffer's spare capacity.
    /// Ensures spare capacity exists before reading.
    pub(crate) fn read_from(&mut self, reader: &mut impl Read) -> io::Result<usize> {
        if self.buf.remaining_mut() == 0 {
            self.buf.reserve(self.target_cap.max(DEFAULT_START_BUF));
        }
        let chunk = self.buf.chunk_mut();
        let n = unsafe {
            let raw = std::slice::from_raw_parts_mut(chunk.as_mut_ptr(), chunk.len());
            reader.read(raw)?
        };
        unsafe { self.buf.advance_mut(n) };
        Ok(n)
    }
}

impl Deref for AdaptiveBuf {
    type Target = BytesMut;
    fn deref(&self) -> &BytesMut {
        &self.buf
    }
}

impl DerefMut for AdaptiveBuf {
    fn deref_mut(&mut self) -> &mut BytesMut {
        &mut self.buf
    }
}

/// Server-side connection wrapper for unit tests.
#[cfg(test)]
pub(crate) struct ServerConn {
    reader: TcpStream,
    writer: BufWriter<TcpStream>,
    read_buf: AdaptiveBuf,
    msg_builder: MsgBuilder,
}

#[cfg(test)]
impl ServerConn {
    pub(crate) fn from_tcp(stream: TcpStream, buf_config: BufConfig) -> io::Result<Self> {
        let writer_stream = stream.try_clone()?;
        Ok(Self {
            reader: stream,
            writer: BufWriter::with_capacity(buf_config.write_buf, writer_stream),
            read_buf: AdaptiveBuf::new(buf_config.max_read_buf),
            msg_builder: MsgBuilder::new(),
        })
    }

    /// Get the raw fd of the reader socket for use with poll().
    #[cfg(unix)]
    pub(crate) fn reader_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.reader.as_raw_fd()
    }

    /// Send INFO to connected client.
    pub(crate) fn send_info(&mut self, info: &ServerInfo) -> io::Result<()> {
        let json = serde_json::to_string(info)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let line = format!("INFO {json}\r\n");
        self.write_flush(line.as_bytes())
    }

    /// Send MSG to connected client (write + flush).
    pub(crate) fn send_msg(
        &mut self,
        subject: &str,
        sid: u64,
        reply: Option<&str>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) -> io::Result<()> {
        self.write_msg(
            subject.as_bytes(),
            sid,
            reply.map(|r| r.as_bytes()),
            headers,
            payload,
        )?;
        self.flush()
    }

    /// Write a MSG to the client without flushing.
    /// Uses direct byte assembly — no `write!()` formatting.
    pub(crate) fn write_msg(
        &mut self,
        subject: &[u8],
        sid: u64,
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) -> io::Result<()> {
        let sid_bytes = nats_proto::sid_to_bytes(sid);
        let data = self
            .msg_builder
            .build_msg(subject, &sid_bytes, reply, headers, payload);
        self.writer.write_all(data)
    }

    /// Write pre-formatted raw bytes to the client (no flush).
    pub(crate) fn write_raw(&mut self, data: &[u8]) -> io::Result<()> {
        self.writer.write_all(data)
    }

    /// Flush buffered writes to the wire.
    pub(crate) fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    pub(crate) fn send_ping(&mut self) -> io::Result<()> {
        self.write_flush(b"PING\r\n")
    }

    pub(crate) fn send_pong(&mut self) -> io::Result<()> {
        self.write_flush(b"PONG\r\n")
    }

    pub(crate) fn send_ok(&mut self) -> io::Result<()> {
        self.write_flush(b"+OK\r\n")
    }

    pub(crate) fn send_err(&mut self, msg: &str) -> io::Result<()> {
        let line = format!("-ERR '{msg}'\r\n");
        self.write_flush(line.as_bytes())
    }

    /// Read the next client operation from the wire.
    pub(crate) fn read_client_op(&mut self) -> io::Result<Option<ClientOp>> {
        self.read_client_op_inner(false)
    }

    pub(crate) fn read_client_op_inner(&mut self, skip_pub: bool) -> io::Result<Option<ClientOp>> {
        loop {
            let parsed = if skip_pub {
                self.try_skip_or_parse_client_op()?
            } else {
                self.try_parse_client_op()?
            };
            if let Some(op) = parsed {
                return Ok(Some(op));
            }
            let n = self.read_buf.read_from(&mut self.reader)?;
            if n == 0 {
                if self.read_buf.is_empty() {
                    return Ok(None);
                }
                return Err(io::ErrorKind::ConnectionReset.into());
            }
            self.read_buf.after_read(n);
        }
    }

    /// Read the next non-PUB/HPUB client operation, skipping all publishes
    /// in a tight loop. Used when there are no subscribers and no upstream,
    /// avoiding poll/Notify overhead entirely.
    pub(crate) fn read_next_non_pub(&mut self) -> io::Result<Option<ClientOp>> {
        loop {
            // Skip all buffered PUBs, return on first non-PUB op
            loop {
                match nats_proto::try_skip_or_parse_client_op(&mut self.read_buf)? {
                    Some(ClientOp::Pong) => continue, // skipped PUB/HPUB
                    Some(op) => return Ok(Some(op)),
                    None => break, // need more data
                }
            }
            self.read_buf.try_shrink();
            // Read more data from socket
            let n = self.read_buf.read_from(&mut self.reader)?;
            if n == 0 {
                if self.read_buf.is_empty() {
                    return Ok(None);
                }
                return Err(io::ErrorKind::ConnectionReset.into());
            }
            self.read_buf.after_read(n);
        }
    }

    pub(crate) fn try_parse_client_op(&mut self) -> io::Result<Option<ClientOp>> {
        let result = nats_proto::try_parse_client_op(&mut self.read_buf);
        self.read_buf.try_shrink();
        result
    }

    /// Parse the next op, but skip PUB/HPUB without creating Bytes objects.
    /// Used when there are no subscribers and no upstream to save CPU.
    pub(crate) fn try_skip_or_parse_client_op(&mut self) -> io::Result<Option<ClientOp>> {
        let result = nats_proto::try_skip_or_parse_client_op(&mut self.read_buf);
        self.read_buf.try_shrink();
        result
    }

    fn write_flush(&mut self, data: &[u8]) -> io::Result<()> {
        self.writer.write_all(data)?;
        self.writer.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    /// Create a TCP loopback pair for testing.
    fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let (server, _) = listener.accept().unwrap();
        (server, client)
    }

    fn make_pair() -> (ServerConn, TcpStream) {
        let (server, client) = tcp_pair();
        let conn = ServerConn::from_tcp(server, BufConfig::default()).unwrap();
        (conn, client)
    }

    #[test]
    fn test_send_info() {
        let (mut conn, mut client) = make_pair();
        let info = ServerInfo {
            server_id: "test".to_string(),
            max_payload: 1024 * 1024,
            proto: 1,
            headers: true,
            ..Default::default()
        };
        conn.send_info(&info).unwrap();

        let mut buf = vec![0u8; 4096];
        let n = client.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(s.starts_with("INFO "));
        assert!(s.ends_with("\r\n"));
        assert!(s.contains("\"server_id\":\"test\""));
    }

    #[test]
    fn test_parse_ping_pong() {
        let (mut conn, mut client) = make_pair();
        client.write_all(b"PING\r\nPONG\r\n").unwrap();
        client.flush().unwrap();

        let op = conn.read_client_op().unwrap().unwrap();
        assert!(matches!(op, ClientOp::Ping));
        let op = conn.read_client_op().unwrap().unwrap();
        assert!(matches!(op, ClientOp::Pong));
    }

    #[test]
    fn test_parse_sub() {
        let (mut conn, mut client) = make_pair();
        client.write_all(b"SUB test.subject 1\r\n").unwrap();
        client.write_all(b"SUB test.queue myqueue 2\r\n").unwrap();
        client.flush().unwrap();

        let op = conn.read_client_op().unwrap().unwrap();
        match op {
            ClientOp::Subscribe {
                sid,
                subject,
                queue_group,
            } => {
                assert_eq!(sid, 1);
                assert_eq!(&subject[..], b"test.subject");
                assert!(queue_group.is_none());
            }
            _ => panic!("expected Subscribe"),
        }

        let op = conn.read_client_op().unwrap().unwrap();
        match op {
            ClientOp::Subscribe {
                sid,
                subject,
                queue_group,
            } => {
                assert_eq!(sid, 2);
                assert_eq!(&subject[..], b"test.queue");
                assert_eq!(&queue_group.unwrap()[..], b"myqueue");
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn test_parse_pub() {
        let (mut conn, mut client) = make_pair();
        client
            .write_all(b"PUB test.subject 5\r\nhello\r\n")
            .unwrap();
        client.flush().unwrap();

        let op = conn.read_client_op().unwrap().unwrap();
        match op {
            ClientOp::Publish {
                subject,
                payload,
                respond,
                headers,
                ..
            } => {
                assert_eq!(&subject[..], b"test.subject");
                assert_eq!(payload.as_ref(), b"hello");
                assert!(respond.is_none());
                assert!(headers.is_none());
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn test_parse_pub_with_reply() {
        let (mut conn, mut client) = make_pair();
        client
            .write_all(b"PUB test.subject reply.to 5\r\nhello\r\n")
            .unwrap();
        client.flush().unwrap();

        let op = conn.read_client_op().unwrap().unwrap();
        match op {
            ClientOp::Publish {
                subject,
                respond,
                payload,
                ..
            } => {
                assert_eq!(&subject[..], b"test.subject");
                assert_eq!(&respond.unwrap()[..], b"reply.to");
                assert_eq!(payload.as_ref(), b"hello");
            }
            _ => panic!("expected Publish"),
        }
    }

    #[test]
    fn test_parse_connect() {
        let (mut conn, mut client) = make_pair();
        client
            .write_all(
                b"CONNECT {\"verbose\":false,\"pedantic\":false,\"lang\":\"rust\",\"version\":\"0.1\",\"protocol\":1,\"echo\":true,\"headers\":true,\"no_responders\":true,\"tls_required\":false}\r\n",
            )
            .unwrap();
        client.flush().unwrap();

        let op = conn.read_client_op().unwrap().unwrap();
        match op {
            ClientOp::Connect(info) => {
                assert_eq!(info.lang, "rust");
                assert!(info.headers);
            }
            _ => panic!("expected Connect"),
        }
    }

    #[test]
    fn test_parse_unsub() {
        let (mut conn, mut client) = make_pair();
        client.write_all(b"UNSUB 1\r\n").unwrap();
        client.write_all(b"UNSUB 2 5\r\n").unwrap();
        client.flush().unwrap();

        let op = conn.read_client_op().unwrap().unwrap();
        assert!(matches!(op, ClientOp::Unsubscribe { sid: 1, max: None }));

        let op = conn.read_client_op().unwrap().unwrap();
        assert!(matches!(
            op,
            ClientOp::Unsubscribe {
                sid: 2,
                max: Some(5)
            }
        ));
    }

    #[test]
    fn test_send_msg() {
        let (mut conn, mut client) = make_pair();
        conn.send_msg("test.sub", 1, None, None, b"hello").unwrap();

        let mut buf = vec![0u8; 4096];
        let n = client.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert_eq!(s, "MSG test.sub 1 5\r\nhello\r\n");
    }

    #[test]
    fn test_send_msg_with_reply() {
        let (mut conn, mut client) = make_pair();
        conn.send_msg("test.sub", 1, Some("reply.to"), None, b"hi")
            .unwrap();

        let mut buf = vec![0u8; 4096];
        let n = client.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert_eq!(s, "MSG test.sub 1 reply.to 2\r\nhi\r\n");
    }

    #[test]
    fn test_eof_returns_none() {
        let (mut conn, client) = make_pair();
        drop(client);
        let result = conn.read_client_op().unwrap();
        assert!(result.is_none());
    }

    // --- LeafConn tests ---

    #[cfg(feature = "leaf")]
    fn make_leaf_pair() -> (crate::leaf_conn::LeafConn, TcpStream) {
        let (server, client) = tcp_pair();
        let conn = crate::leaf_conn::LeafConn::new(server, BufConfig::default());
        (conn, client)
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_parse_info() {
        let (mut conn, mut hub) = make_leaf_pair();
        hub.write_all(b"INFO {\"server_id\":\"hub1\",\"max_payload\":1048576}\r\n")
            .unwrap();
        hub.flush().unwrap();

        let op = conn.read_leaf_op().unwrap().unwrap();
        match op {
            LeafOp::Info(info) => {
                assert_eq!(info.server_id, "hub1");
                assert_eq!(info.max_payload, 1048576);
            }
            _ => panic!("expected Info"),
        }
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_parse_ping_pong_ok_err() {
        let (mut conn, mut hub) = make_leaf_pair();
        hub.write_all(b"PING\r\nPONG\r\n+OK\r\n-ERR 'test error'\r\n")
            .unwrap();
        hub.flush().unwrap();

        assert!(matches!(
            conn.read_leaf_op().unwrap().unwrap(),
            LeafOp::Ping
        ));
        assert!(matches!(
            conn.read_leaf_op().unwrap().unwrap(),
            LeafOp::Pong
        ));
        assert!(matches!(conn.read_leaf_op().unwrap().unwrap(), LeafOp::Ok));
        match conn.read_leaf_op().unwrap().unwrap() {
            LeafOp::Err(msg) => assert_eq!(msg, "test error"),
            _ => panic!("expected Err"),
        }
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_parse_ls_sub_unsub() {
        let (mut conn, mut hub) = make_leaf_pair();
        hub.write_all(b"LS+ foo.bar\r\nLS+ baz.* myqueue\r\nLS- foo.bar\r\n")
            .unwrap();
        hub.flush().unwrap();

        match conn.read_leaf_op().unwrap().unwrap() {
            LeafOp::LeafSub { subject, queue } => {
                assert_eq!(&subject[..], b"foo.bar");
                assert!(queue.is_none());
            }
            _ => panic!("expected LeafSub"),
        }
        match conn.read_leaf_op().unwrap().unwrap() {
            LeafOp::LeafSub { subject, queue } => {
                assert_eq!(&subject[..], b"baz.*");
                assert_eq!(&queue.unwrap()[..], b"myqueue");
            }
            _ => panic!("expected LeafSub"),
        }
        match conn.read_leaf_op().unwrap().unwrap() {
            LeafOp::LeafUnsub { subject, queue } => {
                assert_eq!(&subject[..], b"foo.bar");
                assert!(queue.is_none());
            }
            _ => panic!("expected LeafUnsub"),
        }
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_parse_lmsg_no_reply_no_headers() {
        let (mut conn, mut hub) = make_leaf_pair();
        hub.write_all(b"LMSG test.subject 5\r\nhello\r\n").unwrap();
        hub.flush().unwrap();

        match conn.read_leaf_op().unwrap().unwrap() {
            LeafOp::LeafMsg {
                subject,
                reply,
                headers,
                payload,
            } => {
                assert_eq!(&subject[..], b"test.subject");
                assert!(reply.is_none());
                assert!(headers.is_none());
                assert_eq!(payload.as_ref(), b"hello");
            }
            _ => panic!("expected LeafMsg"),
        }
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_parse_lmsg_with_reply() {
        let (mut conn, mut hub) = make_leaf_pair();
        hub.write_all(b"LMSG test.subject reply.to 5\r\nhello\r\n")
            .unwrap();
        hub.flush().unwrap();

        match conn.read_leaf_op().unwrap().unwrap() {
            LeafOp::LeafMsg {
                subject,
                reply,
                headers,
                payload,
            } => {
                assert_eq!(&subject[..], b"test.subject");
                assert_eq!(&reply.unwrap()[..], b"reply.to");
                assert!(headers.is_none());
                assert_eq!(payload.as_ref(), b"hello");
            }
            _ => panic!("expected LeafMsg"),
        }
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_parse_lmsg_with_headers() {
        let (mut conn, mut hub) = make_leaf_pair();
        let hdr = b"NATS/1.0\r\nX-Key: val\r\n\r\n";
        let payload = b"data";
        let hdr_len = hdr.len();
        let total_len = hdr_len + payload.len();
        let line = format!("LMSG test.subject {hdr_len} {total_len}\r\n");
        hub.write_all(line.as_bytes()).unwrap();
        hub.write_all(hdr).unwrap();
        hub.write_all(payload).unwrap();
        hub.write_all(b"\r\n").unwrap();
        hub.flush().unwrap();

        match conn.read_leaf_op().unwrap().unwrap() {
            LeafOp::LeafMsg {
                subject,
                reply,
                headers,
                payload,
            } => {
                assert_eq!(&subject[..], b"test.subject");
                assert!(reply.is_none());
                let hdrs = headers.unwrap();
                assert_eq!(
                    hdrs.get("X-Key").map(|v| v.to_string()),
                    Some("val".to_string())
                );
                assert_eq!(payload.as_ref(), b"data");
            }
            _ => panic!("expected LeafMsg"),
        }
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_parse_lmsg_with_reply_and_headers() {
        let (mut conn, mut hub) = make_leaf_pair();
        let hdr = b"NATS/1.0\r\nFoo: bar\r\n\r\n";
        let payload = b"body";
        let hdr_len = hdr.len();
        let total_len = hdr_len + payload.len();
        let line = format!("LMSG test.subject reply.inbox {hdr_len} {total_len}\r\n");
        hub.write_all(line.as_bytes()).unwrap();
        hub.write_all(hdr).unwrap();
        hub.write_all(payload).unwrap();
        hub.write_all(b"\r\n").unwrap();
        hub.flush().unwrap();

        match conn.read_leaf_op().unwrap().unwrap() {
            LeafOp::LeafMsg {
                subject,
                reply,
                headers,
                payload,
            } => {
                assert_eq!(&subject[..], b"test.subject");
                assert_eq!(&reply.unwrap()[..], b"reply.inbox");
                let hdrs = headers.unwrap();
                assert_eq!(
                    hdrs.get("Foo").map(|v| v.to_string()),
                    Some("bar".to_string())
                );
                assert_eq!(payload.as_ref(), b"body");
            }
            _ => panic!("expected LeafMsg"),
        }
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_send_leaf_sub_unsub() {
        let (conn, mut hub) = make_leaf_pair();
        let (_reader, mut writer) = conn.split().unwrap();
        writer.send_leaf_sub(b"foo.>").unwrap();
        writer.send_leaf_unsub(b"foo.>").unwrap();
        writer.flush().unwrap();

        let mut buf = vec![0u8; 4096];
        let n = hub.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert_eq!(s, "LS+ foo.>\r\nLS- foo.>\r\n");
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_send_lmsg_no_headers() {
        let (conn, mut hub) = make_leaf_pair();
        let (_reader, mut writer) = conn.split().unwrap();
        writer
            .send_leaf_msg(b"test.sub", None, None, b"hello")
            .unwrap();
        writer.flush().unwrap();

        let mut buf = vec![0u8; 4096];
        let n = hub.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert_eq!(s, "LMSG test.sub 5\r\nhello\r\n");
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_send_lmsg_with_reply() {
        let (conn, mut hub) = make_leaf_pair();
        let (_reader, mut writer) = conn.split().unwrap();
        writer
            .send_leaf_msg(b"test.sub", Some(b"reply.to"), None, b"hi")
            .unwrap();
        writer.flush().unwrap();

        let mut buf = vec![0u8; 4096];
        let n = hub.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert_eq!(s, "LMSG test.sub reply.to 2\r\nhi\r\n");
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_eof_returns_none() {
        let (mut conn, hub) = make_leaf_pair();
        drop(hub);
        let result = conn.read_leaf_op().unwrap();
        assert!(result.is_none());
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_connect_no_creds() {
        let (mut conn, mut hub) = make_leaf_pair();
        conn.send_leaf_connect("test-leaf", true, None).unwrap();

        let mut buf = vec![0u8; 4096];
        let n = hub.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(s.starts_with("CONNECT "));
        assert!(s.ends_with("\r\n"));
        // Should not contain auth fields
        assert!(!s.contains("\"user\""));
        assert!(!s.contains("\"auth_token\""));
        assert!(s.contains("\"name\":\"test-leaf\""));
    }

    #[test]
    #[cfg(feature = "leaf")]
    fn test_leaf_connect_with_creds() {
        use crate::leaf_conn::UpstreamConnectCreds;
        let (mut conn, mut hub) = make_leaf_pair();
        let creds = UpstreamConnectCreds {
            user: Some("admin".into()),
            pass: Some("secret".into()),
            token: Some("tok".into()),
            ..Default::default()
        };
        conn.send_leaf_connect("test-leaf", true, Some(&creds))
            .unwrap();

        let mut buf = vec![0u8; 4096];
        let n = hub.read(&mut buf).unwrap();
        let s = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(s.contains("\"user\":\"admin\""));
        assert!(s.contains("\"pass\":\"secret\""));
        assert!(s.contains("\"auth_token\":\"tok\""));
    }
}
