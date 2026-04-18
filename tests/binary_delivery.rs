//! Targeted tests for the binary-protocol client delivery path.
//!
//! Hypothesis: at realistic tick rates, binary-protocol subscribers see
//! each published message TWICE, which would explain both the ~1:1 dup
//! counter in trading-sim and the non-linear latency blow-up at higher
//! load. These tests publish exactly one message via the NATS text port
//! and assert that the binary sub receives exactly one Msg frame.
//!
//! Structured so the number of workers / whether ShardedServer wraps
//! the base `Server` can be swept independently, narrowing which layer
//! (if any) is responsible for the double delivery.

use std::io::{Read, Write};
use std::net::{TcpListener as StdTcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use open_wire::core::sharded::ShardedServer;
use open_wire::{Server, ServerConfig};

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn init_tracing() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_test_writer()
            .try_init();
    });
}

fn free_port() -> u16 {
    let l = StdTcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn wait_for_port(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if TcpStream::connect_timeout(
            &format!("127.0.0.1:{}", port).parse().unwrap(),
            Duration::from_millis(200),
        )
        .is_ok()
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("port {} did not accept within 5s", port);
}

struct Running {
    shutdown: Arc<AtomicBool>,
    port: u16,
    binary_port: u16,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

/// Start a single unsharded Server with the given worker count. Returns
/// once both the NATS text port and the binary port are accepting.
fn spawn_server(workers: usize) -> Running {
    let port = free_port();
    let binary_port = free_port();
    let shutdown = Arc::new(AtomicBool::new(false));
    let reload = Arc::new(AtomicBool::new(false));

    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        binary_port: Some(binary_port),
        workers,
        server_name: format!("test-bin-{}", port),
        ..Default::default()
    };

    let server = Server::new(config);
    let shutdown_c = Arc::clone(&shutdown);
    std::thread::spawn(move || {
        if let Err(e) = server.run_until_shutdown(shutdown_c, reload, None) {
            eprintln!("server error: {}", e);
        }
    });
    wait_for_port(port);
    wait_for_port(binary_port);
    Running { shutdown, port, binary_port }
}

// ─── Binary-protocol frame helpers (matches `src/protocol/bin_proto.rs`) ─────

fn encode_header(op: u8, subj_len: u16, repl_len: u16, pay_len: u32) -> [u8; 9] {
    let mut h = [0u8; 9];
    h[0] = op;
    h[1..3].copy_from_slice(&subj_len.to_le_bytes());
    h[3..5].copy_from_slice(&repl_len.to_le_bytes());
    h[5..9].copy_from_slice(&pay_len.to_le_bytes());
    h
}

/// Binary SUB frame. payload = SID as u32 LE.
fn sub_frame(subject: &[u8], sid: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(9 + subject.len() + 4);
    out.extend_from_slice(&encode_header(0x05, subject.len() as u16, 0, 4));
    out.extend_from_slice(subject);
    out.extend_from_slice(&sid.to_le_bytes());
    out
}

/// Read Msg/HMsg frames from `stream` until `timeout` elapses with no
/// new frame arriving. Returns the count of 0x03/0x04 frames seen.
fn count_msg_frames(stream: &mut TcpStream, quiet_timeout: Duration) -> usize {
    stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    let mut count = 0;
    let mut last_rx = Instant::now();
    let mut hdr = [0u8; 9];
    let mut scratch = vec![0u8; 4096];
    loop {
        match stream.read_exact(&mut hdr) {
            Ok(()) => {
                last_rx = Instant::now();
                let subj_len = u16::from_le_bytes([hdr[1], hdr[2]]) as usize;
                let repl_len = u16::from_le_bytes([hdr[3], hdr[4]]) as usize;
                let pay_len =
                    u32::from_le_bytes([hdr[5], hdr[6], hdr[7], hdr[8]]) as usize;
                let body_len = subj_len + repl_len + pay_len;
                if body_len > scratch.len() {
                    scratch.resize(body_len + 256, 0);
                }
                if body_len > 0 {
                    stream.read_exact(&mut scratch[..body_len]).unwrap();
                }
                if hdr[0] == 0x03 || hdr[0] == 0x04 {
                    count += 1;
                }
                // Respond to Ping with Pong so the server doesn't disconnect us.
                if hdr[0] == 0x01 {
                    let pong = encode_header(0x02, 0, 0, 0);
                    stream.write_all(&pong).unwrap();
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if last_rx.elapsed() > quiet_timeout {
                    return count;
                }
                // else: keep waiting for more frames
            }
            Err(_) => return count,
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// One NATS pub + one binary sub on a single-worker server. Publisher
/// sends exactly one message. Sub must receive exactly one Msg frame.
#[tokio::test]
async fn binary_client_single_worker_no_dup() {
    let rt = spawn_server(1);

    let mut sub = TcpStream::connect(("127.0.0.1", rt.binary_port)).unwrap();
    sub.write_all(&sub_frame(b"test.sub", 1)).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let nats = async_nats::connect(format!("127.0.0.1:{}", rt.port))
        .await
        .unwrap();
    nats.publish("test.sub", "hello".into()).await.unwrap();
    nats.flush().await.unwrap();

    let count = count_msg_frames(&mut sub, Duration::from_millis(500));
    assert_eq!(
        count, 1,
        "binary sub received {} msg frames, expected exactly 1",
        count
    );
}

/// Multi-worker single Server (no ShardedServer). Verifies the
/// non-sharded multi-worker path doesn't double-deliver.
#[tokio::test]
async fn binary_client_multi_worker_no_dup() {
    let rt = spawn_server(2);

    let mut sub = TcpStream::connect(("127.0.0.1", rt.binary_port)).unwrap();
    sub.write_all(&sub_frame(b"test.sub", 1)).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let nats = async_nats::connect(format!("127.0.0.1:{}", rt.port))
        .await
        .unwrap();
    nats.publish("test.sub", "hello".into()).await.unwrap();
    nats.flush().await.unwrap();

    let count = count_msg_frames(&mut sub, Duration::from_millis(500));
    assert_eq!(
        count, 1,
        "binary sub received {} msg frames, expected exactly 1",
        count
    );
}

/// Start a ShardedServer with `n` shards. `ShardedServer::run()` blocks
/// forever — the test just leaks the thread and relies on the process
/// exiting cleanly. Each test picks unique ports so there's no overlap.
fn spawn_sharded(n_shards: usize) -> (u16, u16) {
    let port = free_port();
    let binary_port = free_port();
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        binary_port: Some(binary_port),
        workers: 1,
        server_name: format!("test-sharded-{}", port),
        ..Default::default()
    };
    std::thread::spawn(move || {
        let sharded = ShardedServer::new(config, n_shards);
        let _ = sharded.run();
    });
    wait_for_port(port);
    wait_for_port(binary_port);
    (port, binary_port)
}

/// ShardedServer with 2 shards. This matches the bench's `OW_SHARDS=2`
/// configuration. If double-delivery is a shard-dispatch bug, this
/// test should fail (count == 2).
#[tokio::test]
async fn binary_client_sharded_2_no_dup() {
    let (port, binary_port) = spawn_sharded(2);

    let mut sub = TcpStream::connect(("127.0.0.1", binary_port)).unwrap();
    sub.write_all(&sub_frame(b"test.sub", 1)).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let nats = async_nats::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    nats.publish("test.sub", "hello".into()).await.unwrap();
    nats.flush().await.unwrap();

    let count = count_msg_frames(&mut sub, Duration::from_millis(500));
    assert_eq!(
        count, 1,
        "sharded(2) binary sub received {} msg frames, expected exactly 1",
        count
    );
}

/// Control: NATS sub + NATS pub on sharded 2. If this works but
/// the binary variant doesn't, the bug is specific to binary-client
/// sub registration in sharded mode.
#[tokio::test]
async fn nats_on_multi_worker_no_shards() {
    // Control: plain Server with 2 workers (no ShardedServer). Proves
    // the test harness can drive multi-worker NATS pub/sub.
    init_tracing();
    let rt = spawn_server(2);
    tokio::time::sleep(Duration::from_millis(500)).await;

    let sub_client = async_nats::connect(format!("127.0.0.1:{}", rt.port))
        .await
        .unwrap();
    let mut sub = sub_client.subscribe("test.sub").await.unwrap();
    sub_client.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let pub_client = async_nats::connect(format!("127.0.0.1:{}", rt.port))
        .await
        .unwrap();
    pub_client.publish("test.sub", "hello".into()).await.unwrap();
    pub_client.flush().await.unwrap();

    let got = tokio::time::timeout(Duration::from_secs(2), futures_util::StreamExt::next(&mut sub))
        .await
        .expect("timed out waiting for message");
    assert!(got.is_some(), "multi-worker NATS control: no message received");
}

/// Same-connection sub+pub on sharded 2. Tests the local-shard
/// delivery path (no cross-shard dispatch needed since both pub
/// and sub are on the same connection).
#[tokio::test]
async fn nats_same_conn_sharded_2() {
    init_tracing();
    let (port, _) = spawn_sharded(2);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let client = async_nats::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let mut sub = client.subscribe("test.sub").await.unwrap();
    client.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    client.publish("test.sub", "hello".into()).await.unwrap();
    client.flush().await.unwrap();

    let got = tokio::time::timeout(Duration::from_secs(2), futures_util::StreamExt::next(&mut sub))
        .await
        .expect("timed out");
    assert!(got.is_some(), "sharded(2) same-conn: no message");
}

#[tokio::test]
async fn nats_client_sharded_2_control() {
    init_tracing();
    let (port, _) = spawn_sharded(2);

    // Extra long warmup: ShardedServer goes through 3-phase barrier
    // before shard_dispatch is wired; wait for it to settle.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let sub_client = async_nats::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    let mut sub = sub_client.subscribe("test.sub").await.unwrap();
    sub_client.flush().await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;

    let pub_client = async_nats::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();
    pub_client.publish("test.sub", "hello".into()).await.unwrap();
    pub_client.flush().await.unwrap();

    let got = tokio::time::timeout(Duration::from_secs(3), futures_util::StreamExt::next(&mut sub))
        .await
        .expect("timed out waiting for message");
    assert!(got.is_some(), "sharded(2) NATS control: no message received");
}

/// ShardedServer with 2 shards, burst publish.
#[tokio::test]
async fn binary_client_sharded_2_burst() {
    let (port, binary_port) = spawn_sharded(2);

    let mut sub = TcpStream::connect(("127.0.0.1", binary_port)).unwrap();
    sub.write_all(&sub_frame(b"test.sub", 1)).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let nats = async_nats::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    const N: usize = 100;
    for _ in 0..N {
        nats.publish("test.sub", "x".into()).await.unwrap();
    }
    nats.flush().await.unwrap();

    let count = count_msg_frames(&mut sub, Duration::from_millis(750));
    assert_eq!(
        count, N,
        "sharded(2) burst: binary sub received {} msg frames, expected exactly {}",
        count, N
    );
}

/// Like `binary_client_multi_worker_no_dup` but publishes many messages
/// back to back. Any double-delivery shows up as ratio close to 2×.
#[tokio::test]
async fn binary_client_multi_worker_no_dup_burst() {
    let rt = spawn_server(2);

    let mut sub = TcpStream::connect(("127.0.0.1", rt.binary_port)).unwrap();
    sub.write_all(&sub_frame(b"test.sub", 1)).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let nats = async_nats::connect(format!("127.0.0.1:{}", rt.port))
        .await
        .unwrap();

    const N: usize = 100;
    for _ in 0..N {
        nats.publish("test.sub", "x".into()).await.unwrap();
    }
    nats.flush().await.unwrap();

    let count = count_msg_frames(&mut sub, Duration::from_millis(750));
    assert_eq!(
        count, N,
        "binary sub received {} msg frames, expected exactly {}",
        count, N
    );
}
