//! Binary encode/decode for the UDP transport wire protocol.
//!
//! ## Batch format
//!
//! ```text
//! Batch header (3 bytes):
//!   magic: 0xCA 0xFE  (2 bytes)
//!   msg_count: u8      (1 byte)
//!
//! Followed by msg_count message entries.
//! ```
//!
//! ## Message entry format
//!
//! ```text
//! Fixed header (11 bytes):
//!   type:        u8   (1 byte)
//!   subject_len: u16  (2 bytes, big-endian)
//!   payload_len: u32  (4 bytes, big-endian)
//!   reply_len:   u16  (2 bytes, big-endian)
//!   hdr_len:     u16  (2 bytes, big-endian)
//!
//! Variable data:
//!   subject bytes  (subject_len)
//!   reply bytes    (reply_len, omitted if 0)
//!   header bytes   (hdr_len, omitted if 0)
//!   payload bytes  (payload_len)
//! ```

use std::io;

/// Batch header magic bytes.
const MAGIC: [u8; 2] = [0xCA, 0xFE];

/// Batch header size: 2 (magic) + 1 (msg_count).
pub const BATCH_HEADER_SIZE: usize = 3;

/// Per-message fixed header size.
pub const MSG_HEADER_SIZE: usize = 11;

/// Message type: subject + reply + payload.
const MSG_TYPE_MSG: u8 = 0x01;
/// Message type: subject + payload, no reply.
const MSG_TYPE_MSG_NOREPLY: u8 = 0x02;
/// Message type: subject + reply + headers + payload.
const MSG_TYPE_MSG_HEADERS: u8 = 0x03;

/// A decoded message from a binary batch.
#[derive(Debug, Clone)]
pub struct BinaryMsg<'a> {
    pub subject: &'a [u8],
    pub reply: Option<&'a [u8]>,
    pub headers: Option<&'a [u8]>,
    pub payload: &'a [u8],
}

/// Scratch buffer for encoding binary batches. Reuse across calls to avoid allocation.
#[derive(Debug)]
pub struct BatchEncoder {
    buf: Vec<u8>,
    count: u8,
}

impl BatchEncoder {
    /// Create a new encoder with the given capacity.
    pub fn with_capacity(cap: usize) -> Self {
        let mut buf = Vec::with_capacity(cap);
        // Write batch header placeholder.
        buf.extend_from_slice(&MAGIC);
        buf.push(0); // msg_count — patched on finish()
        Self { buf, count: 0 }
    }

    /// Reset the encoder for a new batch.
    pub fn clear(&mut self) {
        self.buf.clear();
        self.buf.extend_from_slice(&MAGIC);
        self.buf.push(0);
        self.count = 0;
    }

    /// Append a message to the batch.
    pub fn push(
        &mut self,
        subject: &[u8],
        reply: Option<&[u8]>,
        headers: Option<&[u8]>,
        payload: &[u8],
    ) {
        let reply_bytes = reply.unwrap_or(&[]);
        let hdr_bytes = headers.unwrap_or(&[]);

        let msg_type = if !hdr_bytes.is_empty() {
            MSG_TYPE_MSG_HEADERS
        } else if reply_bytes.is_empty() {
            MSG_TYPE_MSG_NOREPLY
        } else {
            MSG_TYPE_MSG
        };

        // Fixed header (11 bytes).
        self.buf.push(msg_type);
        self.buf
            .extend_from_slice(&(subject.len() as u16).to_be_bytes());
        self.buf
            .extend_from_slice(&(payload.len() as u32).to_be_bytes());
        self.buf
            .extend_from_slice(&(reply_bytes.len() as u16).to_be_bytes());
        self.buf
            .extend_from_slice(&(hdr_bytes.len() as u16).to_be_bytes());

        // Variable data.
        self.buf.extend_from_slice(subject);
        if !reply_bytes.is_empty() {
            self.buf.extend_from_slice(reply_bytes);
        }
        if !hdr_bytes.is_empty() {
            self.buf.extend_from_slice(hdr_bytes);
        }
        self.buf.extend_from_slice(payload);

        self.count = self.count.saturating_add(1);
    }

    /// Finalize the batch and return the encoded bytes.
    pub fn finish(&mut self) -> &[u8] {
        self.buf[2] = self.count;
        &self.buf
    }

    /// Number of messages in the current batch.
    pub fn len(&self) -> u8 {
        self.count
    }

    /// Current encoded size in bytes.
    pub fn encoded_size(&self) -> usize {
        self.buf.len()
    }

    /// Whether the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

/// Decode a binary batch from a datagram. Returns an iterator over messages.
pub fn decode_batch(data: &[u8]) -> io::Result<BatchIter<'_>> {
    if data.len() < BATCH_HEADER_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "batch too short",
        ));
    }
    if data[0] != MAGIC[0] || data[1] != MAGIC[1] {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad batch magic",
        ));
    }
    let msg_count = data[2];
    Ok(BatchIter {
        data,
        pos: BATCH_HEADER_SIZE,
        remaining: msg_count,
    })
}

/// Iterator over messages in a decoded batch.
pub struct BatchIter<'a> {
    data: &'a [u8],
    pos: usize,
    remaining: u8,
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = io::Result<BinaryMsg<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;

        let d = self.data;
        let p = self.pos;

        // Need at least the fixed header.
        if p + MSG_HEADER_SIZE > d.len() {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated message header",
            )));
        }

        let _msg_type = d[p];
        let subject_len = u16::from_be_bytes([d[p + 1], d[p + 2]]) as usize;
        let payload_len = u32::from_be_bytes([d[p + 3], d[p + 4], d[p + 5], d[p + 6]]) as usize;
        let reply_len = u16::from_be_bytes([d[p + 7], d[p + 8]]) as usize;
        let hdr_len = u16::from_be_bytes([d[p + 9], d[p + 10]]) as usize;

        let var_start = p + MSG_HEADER_SIZE;
        let total_var = subject_len + reply_len + hdr_len + payload_len;
        if var_start + total_var > d.len() {
            return Some(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated message data",
            )));
        }

        let mut off = var_start;
        let subject = &d[off..off + subject_len];
        off += subject_len;

        let reply = if reply_len > 0 {
            let r = &d[off..off + reply_len];
            off += reply_len;
            Some(r)
        } else {
            None
        };

        let headers = if hdr_len > 0 {
            let h = &d[off..off + hdr_len];
            off += hdr_len;
            Some(h)
        } else {
            None
        };

        let payload = &d[off..off + payload_len];
        off += payload_len;

        self.pos = off;

        Some(Ok(BinaryMsg {
            subject,
            reply,
            headers,
            payload,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_single_msg() {
        let mut enc = BatchEncoder::with_capacity(256);
        enc.push(b"foo.bar", None, None, b"hello");
        let data = enc.finish();

        let mut iter = decode_batch(data).unwrap();
        let msg = iter.next().unwrap().unwrap();
        assert_eq!(msg.subject, b"foo.bar");
        assert!(msg.reply.is_none());
        assert!(msg.headers.is_none());
        assert_eq!(msg.payload, b"hello");
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_encode_decode_with_reply() {
        let mut enc = BatchEncoder::with_capacity(256);
        enc.push(b"svc.orders", Some(b"_INBOX.abc"), None, b"data");
        let data = enc.finish();

        let mut iter = decode_batch(data).unwrap();
        let msg = iter.next().unwrap().unwrap();
        assert_eq!(msg.subject, b"svc.orders");
        assert_eq!(msg.reply.unwrap(), b"_INBOX.abc");
        assert!(msg.headers.is_none());
        assert_eq!(msg.payload, b"data");
    }

    #[test]
    fn test_encode_decode_with_headers() {
        let mut enc = BatchEncoder::with_capacity(256);
        let hdrs = b"NATS/1.0\r\nX-Key: val\r\n\r\n";
        enc.push(b"test", Some(b"reply"), Some(hdrs.as_slice()), b"body");
        let data = enc.finish();

        let mut iter = decode_batch(data).unwrap();
        let msg = iter.next().unwrap().unwrap();
        assert_eq!(msg.subject, b"test");
        assert_eq!(msg.reply.unwrap(), b"reply");
        assert_eq!(msg.headers.unwrap(), hdrs.as_slice());
        assert_eq!(msg.payload, b"body");
    }

    #[test]
    fn test_encode_decode_batch_multiple() {
        let mut enc = BatchEncoder::with_capacity(512);
        enc.push(b"a.b", None, None, b"p1");
        enc.push(b"c.d", Some(b"r"), None, b"p2");
        enc.push(b"e.f", None, None, b"p3");
        assert_eq!(enc.len(), 3);
        let data = enc.finish();
        let mut iter = decode_batch(data).unwrap();

        let m1 = iter.next().unwrap().unwrap();
        assert_eq!(m1.subject, b"a.b");
        assert_eq!(m1.payload, b"p1");

        let m2 = iter.next().unwrap().unwrap();
        assert_eq!(m2.subject, b"c.d");
        assert_eq!(m2.reply.unwrap(), b"r");
        assert_eq!(m2.payload, b"p2");

        let m3 = iter.next().unwrap().unwrap();
        assert_eq!(m3.subject, b"e.f");
        assert_eq!(m3.payload, b"p3");

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_clear_resets() {
        let mut enc = BatchEncoder::with_capacity(256);
        enc.push(b"a", None, None, b"b");
        assert_eq!(enc.len(), 1);

        enc.clear();
        assert_eq!(enc.len(), 0);
        assert!(enc.is_empty());
        assert_eq!(enc.encoded_size(), BATCH_HEADER_SIZE);

        enc.push(b"x", None, None, b"y");
        let data = enc.finish();
        let mut iter = decode_batch(data).unwrap();
        let msg = iter.next().unwrap().unwrap();
        assert_eq!(msg.subject, b"x");
        assert_eq!(msg.payload, b"y");
    }

    #[test]
    fn test_bad_magic() {
        let data = [0x00, 0x00, 0x00];
        assert!(decode_batch(&data).is_err());
    }

    #[test]
    fn test_truncated_batch() {
        let data = [0xCA, 0xFE];
        assert!(decode_batch(&data).is_err());
    }

    #[test]
    fn test_truncated_message() {
        // Valid batch header claiming 1 message, but no message data.
        let data = [0xCA, 0xFE, 0x01];
        let mut iter = decode_batch(&data).unwrap();
        assert!(iter.next().unwrap().is_err());
    }

    #[test]
    fn test_empty_payload() {
        let mut enc = BatchEncoder::with_capacity(64);
        enc.push(b"test", None, None, b"");
        let data = enc.finish();

        let mut iter = decode_batch(data).unwrap();
        let msg = iter.next().unwrap().unwrap();
        assert_eq!(msg.subject, b"test");
        assert_eq!(msg.payload, b"");
    }

    #[test]
    fn test_encoded_size() {
        let mut enc = BatchEncoder::with_capacity(256);
        assert_eq!(enc.encoded_size(), BATCH_HEADER_SIZE);

        // 11 (fixed hdr) + 3 (subject "foo") + 5 (payload "hello") = 19
        enc.push(b"foo", None, None, b"hello");
        assert_eq!(
            enc.encoded_size(),
            BATCH_HEADER_SIZE + MSG_HEADER_SIZE + 3 + 5
        );
    }
}
