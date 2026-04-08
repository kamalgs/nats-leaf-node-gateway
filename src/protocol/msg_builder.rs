//! Outgoing NATS protocol message builder.
//!
//! Builds outgoing `MSG`/`HMSG`/`LMSG`/`RMSG` protocol lines using direct
//! `extend_from_slice` appends instead of `write!()` formatting, eliminating
//! the `std::fmt` machinery from the hot path.

use bytes::Bytes;

use crate::types::HeaderMap;

macro_rules! int_to_buf {
    ($name:ident, $ty:ty) => {
        #[inline]
        fn $name(n: $ty, buf: &mut [u8; 20]) -> &[u8] {
            if n == 0 {
                buf[0] = b'0';
                return &buf[..1];
            }
            let mut i = 20;
            let mut v = n;
            while v > 0 {
                i -= 1;
                buf[i] = b'0' + (v % 10) as u8;
                v /= 10;
            }
            &buf[i..]
        }
    };
}

int_to_buf!(usize_to_buf, usize);
int_to_buf!(u64_to_buf, u64);

/// Scratch buffer for building outgoing protocol lines. Reuse across calls
/// to avoid allocation.
pub struct MsgBuilder {
    buf: Vec<u8>,
}

impl Default for MsgBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MsgBuilder {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(1024),
        }
    }

    /// Build `MSG subject sid [reply] size\r\npayload\r\n` into the internal
    /// buffer and return the complete bytes ready for writing.
    pub fn build_msg(
        &mut self,
        subject: &[u8],
        sid_bytes: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) -> &[u8] {
        self.buf.clear();
        match headers {
            Some(hdrs) if !hdrs.is_empty() => {
                let hdr_bytes = hdrs.to_bytes();
                let hdr_len = hdr_bytes.len();
                let total_len = hdr_len + payload.len();
                let mut tmp = [0u8; 20];

                self.buf.extend_from_slice(b"HMSG ");
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                self.buf.extend_from_slice(sid_bytes);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                self.buf.extend_from_slice(usize_to_buf(hdr_len, &mut tmp));
                self.buf.push(b' ');
                self.buf
                    .extend_from_slice(usize_to_buf(total_len, &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
                self.buf.extend_from_slice(&hdr_bytes);
                self.buf.extend_from_slice(payload);
                self.buf.extend_from_slice(b"\r\n");
            }
            _ => {
                self.buf.extend_from_slice(b"MSG ");
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                self.buf.extend_from_slice(sid_bytes);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                let mut tmp = [0u8; 20];
                self.buf
                    .extend_from_slice(usize_to_buf(payload.len(), &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
                self.buf.extend_from_slice(payload);
                self.buf.extend_from_slice(b"\r\n");
            }
        }
        &self.buf
    }

    /// Build `LMSG subject [reply] [hdr_len] total_len\r\npayload\r\n`.
    pub fn build_lmsg(
        &mut self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
    ) -> &[u8] {
        self.buf.clear();
        let mut tmp = [0u8; 20];
        match headers {
            Some(hdrs) if !hdrs.is_empty() => {
                let hdr_bytes = hdrs.to_bytes();
                let hdr_len = hdr_bytes.len();
                let total_len = hdr_len + payload.len();

                self.buf.extend_from_slice(b"LMSG ");
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                self.buf.extend_from_slice(usize_to_buf(hdr_len, &mut tmp));
                self.buf.push(b' ');
                self.buf
                    .extend_from_slice(usize_to_buf(total_len, &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
                self.buf.extend_from_slice(&hdr_bytes);
                self.buf.extend_from_slice(payload);
                self.buf.extend_from_slice(b"\r\n");
            }
            _ => {
                self.buf.extend_from_slice(b"LMSG ");
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                self.buf
                    .extend_from_slice(usize_to_buf(payload.len(), &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
                self.buf.extend_from_slice(payload);
                self.buf.extend_from_slice(b"\r\n");
            }
        }
        &self.buf
    }

    /// Build `LMSG` header only (no payload copy).
    /// Returns the protocol header line ending with `\r\n`, plus any serialized
    /// headers. Caller writes payload + `\r\n` separately.
    pub fn build_lmsg_header(
        &mut self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload_len: usize,
    ) -> &[u8] {
        self.buf.clear();
        let mut tmp = [0u8; 20];
        match headers {
            Some(hdrs) if !hdrs.is_empty() => {
                let hdr_bytes = hdrs.to_bytes();
                let hdr_len = hdr_bytes.len();
                let total_len = hdr_len + payload_len;

                self.buf.extend_from_slice(b"LMSG ");
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                self.buf.extend_from_slice(usize_to_buf(hdr_len, &mut tmp));
                self.buf.push(b' ');
                self.buf
                    .extend_from_slice(usize_to_buf(total_len, &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
                self.buf.extend_from_slice(&hdr_bytes);
            }
            _ => {
                self.buf.extend_from_slice(b"LMSG ");
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                self.buf
                    .extend_from_slice(usize_to_buf(payload_len, &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
            }
        }
        &self.buf
    }

    /// Build `LS+ subject\r\n`.
    pub fn build_leaf_sub(&mut self, subject: &[u8]) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(b"LS+ ");
        self.buf.extend_from_slice(subject);
        self.buf.extend_from_slice(b"\r\n");
        &self.buf
    }

    /// Build `LS- subject\r\n`.
    pub fn build_leaf_unsub(&mut self, subject: &[u8]) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(b"LS- ");
        self.buf.extend_from_slice(subject);
        self.buf.extend_from_slice(b"\r\n");
        &self.buf
    }

    /// Build `LS+ subject queue\r\n` for queue group subscriptions.
    pub fn build_leaf_sub_queue(&mut self, subject: &[u8], queue: &[u8]) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(b"LS+ ");
        self.buf.extend_from_slice(subject);
        self.buf.extend_from_slice(b" ");
        self.buf.extend_from_slice(queue);
        self.buf.extend_from_slice(b"\r\n");
        &self.buf
    }

    /// Build `LS- subject queue\r\n` for queue group unsubscriptions.
    pub fn build_leaf_unsub_queue(&mut self, subject: &[u8], queue: &[u8]) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(b"LS- ");
        self.buf.extend_from_slice(subject);
        self.buf.extend_from_slice(b" ");
        self.buf.extend_from_slice(queue);
        self.buf.extend_from_slice(b"\r\n");
        &self.buf
    }

    /// Build `RMSG $G subject [reply] [hdr_len] total_len\r\npayload\r\n`.
    pub fn build_rmsg(
        &mut self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&HeaderMap>,
        payload: &[u8],
        #[cfg(feature = "accounts")] account: &[u8],
    ) -> &[u8] {
        self.buf.clear();
        let mut tmp = [0u8; 20];
        #[cfg(feature = "accounts")]
        let acct = account;
        #[cfg(not(feature = "accounts"))]
        let acct = b"$G".as_slice();
        match headers {
            Some(hdrs) if !hdrs.is_empty() => {
                let hdr_bytes = hdrs.to_bytes();
                let hdr_len = hdr_bytes.len();
                let total_len = hdr_len + payload.len();

                self.buf.extend_from_slice(b"RMSG ");
                self.buf.extend_from_slice(acct);
                self.buf.push(b' ');
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                self.buf.extend_from_slice(usize_to_buf(hdr_len, &mut tmp));
                self.buf.push(b' ');
                self.buf
                    .extend_from_slice(usize_to_buf(total_len, &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
                self.buf.extend_from_slice(&hdr_bytes);
                self.buf.extend_from_slice(payload);
                self.buf.extend_from_slice(b"\r\n");
            }
            _ => {
                self.buf.extend_from_slice(b"RMSG ");
                self.buf.extend_from_slice(acct);
                self.buf.push(b' ');
                self.buf.extend_from_slice(subject);
                self.buf.push(b' ');
                if let Some(r) = reply {
                    self.buf.extend_from_slice(r);
                    self.buf.push(b' ');
                }
                self.buf
                    .extend_from_slice(usize_to_buf(payload.len(), &mut tmp));
                self.buf.extend_from_slice(b"\r\n");
                self.buf.extend_from_slice(payload);
                self.buf.extend_from_slice(b"\r\n");
            }
        }
        &self.buf
    }

    /// Build `RS+ account subject\r\n`.
    pub fn build_route_sub(
        &mut self,
        subject: &[u8],
        #[cfg(feature = "accounts")] account: &[u8],
    ) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(b"RS+ ");
        #[cfg(feature = "accounts")]
        self.buf.extend_from_slice(account);
        #[cfg(not(feature = "accounts"))]
        self.buf.extend_from_slice(b"$G");
        self.buf.push(b' ');
        self.buf.extend_from_slice(subject);
        self.buf.extend_from_slice(b"\r\n");
        &self.buf
    }

    /// Build `RS- account subject\r\n`.
    pub fn build_route_unsub(
        &mut self,
        subject: &[u8],
        #[cfg(feature = "accounts")] account: &[u8],
    ) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(b"RS- ");
        #[cfg(feature = "accounts")]
        self.buf.extend_from_slice(account);
        #[cfg(not(feature = "accounts"))]
        self.buf.extend_from_slice(b"$G");
        self.buf.push(b' ');
        self.buf.extend_from_slice(subject);
        self.buf.extend_from_slice(b"\r\n");
        &self.buf
    }

    /// Build `RS+ account subject queue weight\r\n` for queue group route subscriptions.
    pub fn build_route_sub_queue(
        &mut self,
        subject: &[u8],
        queue: &[u8],
        #[cfg(feature = "accounts")] account: &[u8],
    ) -> &[u8] {
        self.buf.clear();
        self.buf.extend_from_slice(b"RS+ ");
        #[cfg(feature = "accounts")]
        self.buf.extend_from_slice(account);
        #[cfg(not(feature = "accounts"))]
        self.buf.extend_from_slice(b"$G");
        self.buf.push(b' ');
        self.buf.extend_from_slice(subject);
        self.buf.push(b' ');
        self.buf.extend_from_slice(queue);
        self.buf.extend_from_slice(b" 1\r\n");
        &self.buf
    }

    /// Build `RS- account subject\r\n` for queue group route unsubscriptions.
    pub fn build_route_unsub_queue(
        &mut self,
        subject: &[u8],
        queue: &[u8],
        #[cfg(feature = "accounts")] account: &[u8],
    ) -> &[u8] {
        // RS- doesn't use queue — it unsubscribes the subject entirely
        // (Go nats-server just uses RS- $G subject)
        let _ = queue;
        self.buf.clear();
        self.buf.extend_from_slice(b"RS- ");
        #[cfg(feature = "accounts")]
        self.buf.extend_from_slice(account);
        #[cfg(not(feature = "accounts"))]
        self.buf.extend_from_slice(b"$G");
        self.buf.push(b' ');
        self.buf.extend_from_slice(subject);
        self.buf.extend_from_slice(b"\r\n");
        &self.buf
    }
}

/// Pre-format a `u64` sid as decimal ASCII bytes for reuse in MSG lines.
pub fn sid_to_bytes(sid: u64) -> Bytes {
    let mut tmp = [0u8; 20];
    let s = u64_to_buf(sid, &mut tmp);
    Bytes::copy_from_slice(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- itoa -------------------------------------------------------------------

    #[test]
    fn test_usize_to_buf() {
        let mut tmp = [0u8; 20];
        assert_eq!(usize_to_buf(0, &mut tmp), b"0");
        assert_eq!(usize_to_buf(128, &mut tmp), b"128");
        assert_eq!(usize_to_buf(1000000, &mut tmp), b"1000000");
    }

    #[test]
    fn test_u64_to_buf() {
        let mut tmp = [0u8; 20];
        assert_eq!(u64_to_buf(0, &mut tmp), b"0");
        assert_eq!(u64_to_buf(42, &mut tmp), b"42");
    }

    #[test]
    fn test_sid_to_bytes() {
        assert_eq!(&sid_to_bytes(0)[..], b"0");
        assert_eq!(&sid_to_bytes(123)[..], b"123");
    }

    // -- MsgBuilder tests -------------------------------------------------------

    #[test]
    fn test_build_msg_no_reply() {
        let mut b = MsgBuilder::new();
        let result = b.build_msg(b"test.sub", b"1", None, None, b"hello");
        assert_eq!(result, b"MSG test.sub 1 5\r\nhello\r\n");
    }

    #[test]
    fn test_build_msg_with_reply() {
        let mut b = MsgBuilder::new();
        let result = b.build_msg(b"test.sub", b"1", Some(b"reply.to"), None, b"hi");
        assert_eq!(result, b"MSG test.sub 1 reply.to 2\r\nhi\r\n");
    }

    #[test]
    fn test_build_msg_empty_payload() {
        let mut b = MsgBuilder::new();
        let result = b.build_msg(b"foo", b"42", None, None, b"");
        assert_eq!(result, b"MSG foo 42 0\r\n\r\n");
    }

    #[test]
    fn test_build_lmsg_no_reply() {
        let mut b = MsgBuilder::new();
        let result = b.build_lmsg(b"test.sub", None, None, b"hello");
        assert_eq!(result, b"LMSG test.sub 5\r\nhello\r\n");
    }

    #[test]
    fn test_build_lmsg_with_reply() {
        let mut b = MsgBuilder::new();
        let result = b.build_lmsg(b"test.sub", Some(b"reply.to"), None, b"hi");
        assert_eq!(result, b"LMSG test.sub reply.to 2\r\nhi\r\n");
    }

    #[test]
    fn test_build_leaf_sub() {
        let mut b = MsgBuilder::new();
        assert_eq!(b.build_leaf_sub(b"foo.>"), b"LS+ foo.>\r\n");
    }

    #[test]
    fn test_build_leaf_unsub() {
        let mut b = MsgBuilder::new();
        assert_eq!(b.build_leaf_unsub(b"foo.>"), b"LS- foo.>\r\n");
    }

    #[test]
    fn test_build_leaf_sub_queue() {
        let mut b = MsgBuilder::new();
        assert_eq!(
            b.build_leaf_sub_queue(b"foo.bar", b"myqueue"),
            b"LS+ foo.bar myqueue\r\n"
        );
    }

    #[test]
    fn test_build_leaf_unsub_queue() {
        let mut b = MsgBuilder::new();
        assert_eq!(
            b.build_leaf_unsub_queue(b"foo.bar", b"myqueue"),
            b"LS- foo.bar myqueue\r\n"
        );
    }

    #[test]
    fn test_build_rmsg() {
        let mut b = MsgBuilder::new();
        let data = b.build_rmsg(
            b"test.sub",
            None,
            None,
            b"hello",
            #[cfg(feature = "accounts")]
            b"$G",
        );
        assert_eq!(data, b"RMSG $G test.sub 5\r\nhello\r\n");
    }

    #[test]
    fn test_build_rmsg_with_reply() {
        let mut b = MsgBuilder::new();
        let data = b.build_rmsg(
            b"test.sub",
            Some(b"reply.to"),
            None,
            b"hi",
            #[cfg(feature = "accounts")]
            b"$G",
        );
        assert_eq!(data, b"RMSG $G test.sub reply.to 2\r\nhi\r\n");
    }

    #[test]
    fn test_build_route_sub() {
        let mut b = MsgBuilder::new();
        let data = b.build_route_sub(
            b"test.subject",
            #[cfg(feature = "accounts")]
            b"$G",
        );
        assert_eq!(data, b"RS+ $G test.subject\r\n");
    }

    #[test]
    fn test_build_route_unsub() {
        let mut b = MsgBuilder::new();
        let data = b.build_route_unsub(
            b"test.subject",
            #[cfg(feature = "accounts")]
            b"$G",
        );
        assert_eq!(data, b"RS- $G test.subject\r\n");
    }

    #[test]
    fn test_build_route_sub_queue() {
        let mut b = MsgBuilder::new();
        let data = b.build_route_sub_queue(
            b"test.subject",
            b"q1",
            #[cfg(feature = "accounts")]
            b"$G",
        );
        assert_eq!(data, b"RS+ $G test.subject q1 1\r\n");
    }
}
