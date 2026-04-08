//! End-to-end integration tests for open-wire.
//!
//! These tests require the `nats-server` binary in PATH (or at the Go install
//! location). Install via: `go install github.com/nats-io/nats-server/v2@main`

use std::net::TcpListener as StdTcpListener;
use std::process::{Child, Command};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;

use open_wire::ClusterConfig;

use open_wire::GatewayRemote;

use open_wire::HubConfig;

use open_wire::InboundLeafConfig;
use open_wire::{Server, ServerConfig};
use tokio::time::timeout;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// A running open-wire server for testing. Shuts down automatically on drop.
struct TestServer {
    shutdown: Arc<AtomicBool>,
    port: u16,
}

impl TestServer {
    /// Connect an async-nats client to this server.
    async fn connect(&self) -> async_nats::Client {
        async_nats::connect(format!("127.0.0.1:{}", self.port))
            .await
            .unwrap_or_else(|e| panic!("connect to port {} failed: {e}", self.port))
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

/// Poll until a server accepts a TCP connection on `port` (up to 5 s).
fn wait_for_server(port: u16) {
    let addr = format!("127.0.0.1:{}", port);
    for _ in 0..50 {
        if std::net::TcpStream::connect(&addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("server did not become ready on port {}", port);
}

/// Connect an async-nats client to the given port.
async fn connect_to(port: u16) -> async_nats::Client {
    async_nats::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap_or_else(|e| panic!("connect to port {port} failed: {e}"))
}

/// Publish a message and wait for it to arrive on a subscriber.
async fn publish_and_expect(
    pub_client: &async_nats::Client,
    sub: &mut async_nats::Subscriber,
    subject: &str,
    payload: &[u8],
) -> async_nats::Message {
    pub_client
        .publish(subject.to_owned(), bytes::Bytes::copy_from_slice(payload))
        .await
        .expect("publish failed");
    pub_client.flush().await.expect("flush failed");
    timeout(Duration::from_secs(5), sub.next())
        .await
        .expect("timed out waiting for message")
        .expect("subscription ended")
}

/// Drain a subscriber, counting messages until a timeout gap with no messages.
async fn collect_msgs(sub: &mut async_nats::Subscriber, gap_ms: u64) -> u32 {
    let mut count = 0u32;
    while let Ok(Some(_)) = timeout(Duration::from_millis(gap_ms), sub.next()).await {
        count += 1;
    }
    count
}

/// Find a free TCP port by binding to :0.
fn free_port() -> u16 {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Find the nats-server binary, checking common locations.
fn nats_server_bin() -> String {
    // Check PATH first
    if let Ok(output) = Command::new("which").arg("nats-server").output() {
        if output.status.success() {
            return String::from_utf8_lossy(&output.stdout).trim().to_string();
        }
    }

    // Check Go install path
    if let Ok(output) = Command::new("go").arg("env").arg("GOPATH").output() {
        if output.status.success() {
            let gopath = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let bin = format!("{}/bin/nats-server", gopath);
            if std::path::Path::new(&bin).exists() {
                return bin;
            }
        }
    }

    panic!("nats-server binary not found. Install with: go install github.com/nats-io/nats-server/v2@main");
}

/// A running nats-server process for testing.
struct NatsServer {
    child: Child,
    port: u16,
}

impl NatsServer {
    /// Start a nats-server on the given port and wait until it accepts connections.
    fn start(port: u16) -> Self {
        let bin = nats_server_bin();
        let child = Command::new(&bin)
            .args(["-p", &port.to_string(), "-a", "127.0.0.1"])
            .spawn()
            .unwrap_or_else(|e| panic!("failed to start nats-server at {}: {}", bin, e));

        let server = NatsServer { child, port };
        server.wait_ready();
        server
    }

    /// Start a nats-server with both a client port and a leafnode listener port.
    fn start_with_leafnode(client_port: u16, leafnode_port: u16) -> Self {
        let bin = nats_server_bin();
        let config_path = format!("/tmp/nats_hub_test_{}.conf", client_port);
        std::fs::write(
            &config_path,
            format!(
                "listen: 127.0.0.1:{client_port}\n\
                 leafnodes {{\n  listen: 127.0.0.1:{leafnode_port}\n}}\n"
            ),
        )
        .unwrap();

        let child = Command::new(&bin)
            .args(["-c", &config_path])
            .spawn()
            .unwrap_or_else(|e| panic!("failed to start nats-server at {}: {}", bin, e));

        let server = NatsServer {
            child,
            port: client_port,
        };
        server.wait_ready();
        server
    }

    /// Poll until the server accepts a TCP connection (up to 5s).
    fn wait_ready(&self) {
        let addr = format!("127.0.0.1:{}", self.port);
        for _ in 0..50 {
            if std::net::TcpStream::connect(&addr).is_ok() {
                return;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        panic!("nats-server did not become ready on port {}", self.port);
    }
}

impl Drop for NatsServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Start a Server on the given port with optional hub_url. Waits until the
/// server is accepting connections before returning.
fn spawn_leaf(port: u16, hub_url: Option<String>) -> TestServer {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);
    let reload = Arc::new(AtomicBool::new(false));

    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port,
        server_name: format!("test-leaf-{}", port),
        hub: HubConfig {
            url: hub_url,
            ..Default::default()
        },
        ..Default::default()
    };
    let server = Server::new(config);

    std::thread::spawn(move || {
        if let Err(e) = server.run_until_shutdown(shutdown_clone, reload, None) {
            eprintln!("leaf server error: {}", e);
        }
    });

    wait_for_server(port);
    TestServer { shutdown, port }
}

#[tokio::test]

async fn local_pub_sub() {
    let _server = spawn_leaf(free_port(), None);
    let client = _server.connect().await;

    let mut sub = client
        .subscribe("test.subject")
        .await
        .expect("subscribe failed");

    // Small delay to let subscription propagate
    tokio::time::sleep(Duration::from_millis(100)).await;

    let msg = publish_and_expect(&client, &mut sub, "test.subject", b"hello").await;

    assert_eq!(msg.subject.as_str(), "test.subject");
    assert_eq!(&msg.payload[..], b"hello");
}

#[tokio::test]

async fn upstream_forward() {
    // Start upstream nats-server with a leafnode listener
    let upstream_client_port = free_port();
    let upstream_leaf_port = free_port();
    let _upstream = NatsServer::start_with_leafnode(upstream_client_port, upstream_leaf_port);

    // Start leaf pointing at upstream's leafnode port
    let _leaf = spawn_leaf(
        free_port(),
        Some(format!("nats://127.0.0.1:{}", upstream_leaf_port)),
    );

    // Connect clients
    let leaf_client = _leaf.connect().await;
    let upstream_client = connect_to(upstream_client_port).await;

    // Leaf subscribes to wildcard
    let mut leaf_sub = leaf_client
        .subscribe("events.>")
        .await
        .expect("leaf subscribe failed");

    // Let subscription propagate to upstream via the leaf's hub connection
    tokio::time::sleep(Duration::from_millis(500)).await;

    let msg = publish_and_expect(
        &upstream_client,
        &mut leaf_sub,
        "events.hello",
        b"from-upstream",
    )
    .await;

    assert_eq!(msg.subject.as_str(), "events.hello");
    assert_eq!(&msg.payload[..], b"from-upstream");
}

#[tokio::test]

async fn leaf_to_upstream() {
    // Start upstream nats-server with a leafnode listener
    let upstream_client_port = free_port();
    let upstream_leaf_port = free_port();
    let _upstream = NatsServer::start_with_leafnode(upstream_client_port, upstream_leaf_port);

    // Start leaf pointing at upstream's leafnode port
    let _leaf = spawn_leaf(
        free_port(),
        Some(format!("nats://127.0.0.1:{}", upstream_leaf_port)),
    );

    // Connect clients
    let leaf_client = _leaf.connect().await;
    let upstream_client = connect_to(upstream_client_port).await;

    // Upstream subscribes
    let mut upstream_sub = upstream_client
        .subscribe("data.test")
        .await
        .expect("upstream subscribe failed");

    // Let subscription settle
    tokio::time::sleep(Duration::from_millis(200)).await;

    let msg = publish_and_expect(&leaf_client, &mut upstream_sub, "data.test", b"from-leaf").await;

    assert_eq!(msg.subject.as_str(), "data.test");
    assert_eq!(&msg.payload[..], b"from-leaf");
}

// --- Wire protocol tests (no feature gate beyond "leaf") ---

#[tokio::test]

async fn queue_sub_distribution() {
    let _server = spawn_leaf(free_port(), None);
    let client = _server.connect().await;

    let mut sub1 = client
        .queue_subscribe("work.tasks", "workers".into())
        .await
        .expect("queue sub 1 failed");
    let mut sub2 = client
        .queue_subscribe("work.tasks", "workers".into())
        .await
        .expect("queue sub 2 failed");
    let mut sub3 = client
        .queue_subscribe("work.tasks", "workers".into())
        .await
        .expect("queue sub 3 failed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..90u32 {
        client
            .publish("work.tasks", format!("msg-{}", i).into())
            .await
            .expect("publish failed");
    }
    client.flush().await.expect("flush failed");

    let (c1, c2, c3) = tokio::join!(
        collect_msgs(&mut sub1, 500),
        collect_msgs(&mut sub2, 500),
        collect_msgs(&mut sub3, 500)
    );

    assert_eq!(c1 + c2 + c3, 90, "total should be 90, got {c1}+{c2}+{c3}");
    assert!(c1 >= 1, "sub1 should get at least 1, got {c1}");
    assert!(c2 >= 1, "sub2 should get at least 1, got {c2}");
    assert!(c3 >= 1, "sub3 should get at least 1, got {c3}");
}

#[tokio::test]

async fn multiple_queue_groups() {
    let _server = spawn_leaf(free_port(), None);
    let client = _server.connect().await;

    let mut ga1 = client
        .queue_subscribe("events.tick", "group-a".into())
        .await
        .expect("ga1 failed");
    let mut ga2 = client
        .queue_subscribe("events.tick", "group-a".into())
        .await
        .expect("ga2 failed");
    let mut gb1 = client
        .queue_subscribe("events.tick", "group-b".into())
        .await
        .expect("gb1 failed");
    let mut gb2 = client
        .queue_subscribe("events.tick", "group-b".into())
        .await
        .expect("gb2 failed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..20u32 {
        client
            .publish("events.tick", format!("t-{}", i).into())
            .await
            .expect("publish failed");
    }
    client.flush().await.expect("flush failed");

    let (a1, a2, b1, b2) = tokio::join!(
        collect_msgs(&mut ga1, 500),
        collect_msgs(&mut ga2, 500),
        collect_msgs(&mut gb1, 500),
        collect_msgs(&mut gb2, 500)
    );

    assert_eq!(a1 + a2, 20, "group-a total should be 20, got {a1}+{a2}");
    assert_eq!(b1 + b2, 20, "group-b total should be 20, got {b1}+{b2}");
}

#[tokio::test]

async fn unsub_max_auto_unsubscribe() {
    let _server = spawn_leaf(free_port(), None);
    let client = _server.connect().await;

    let mut sub = client
        .subscribe("unsub.test")
        .await
        .expect("subscribe failed");

    sub.unsubscribe_after(5)
        .await
        .expect("unsubscribe_after failed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    for i in 0..10u32 {
        client
            .publish("unsub.test", format!("m-{}", i).into())
            .await
            .expect("publish failed");
    }
    client.flush().await.expect("flush failed");

    let mut count = 0u32;
    while let Ok(Some(_)) = timeout(Duration::from_millis(500), sub.next()).await {
        count += 1;
    }

    assert_eq!(count, 5, "should receive exactly 5, got {count}");
}

#[tokio::test]

async fn wildcard_subscriptions() {
    let _server = spawn_leaf(free_port(), None);
    let client = _server.connect().await;

    // Subscribe to wildcards and exact
    let mut star_sub = client.subscribe("events.*").await.expect("star sub failed");
    let mut gt_sub = client.subscribe("events.>").await.expect("gt sub failed");
    let mut exact_sub = client
        .subscribe("events.login")
        .await
        .expect("exact sub failed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // events.login -> matched by *, >, exact
    client
        .publish("events.login", "a".into())
        .await
        .expect("pub failed");
    // events.logout -> matched by *, >
    client
        .publish("events.logout", "b".into())
        .await
        .expect("pub failed");
    // events.login.us -> matched by > only (two tokens deep)
    client
        .publish("events.login.us", "c".into())
        .await
        .expect("pub failed");
    client.flush().await.expect("flush failed");

    let (star, gt, exact) = tokio::join!(
        collect_msgs(&mut star_sub, 500),
        collect_msgs(&mut gt_sub, 500),
        collect_msgs(&mut exact_sub, 500)
    );

    assert_eq!(star, 2, "events.* should match 2, got {star}");
    assert_eq!(gt, 3, "events.> should match 3, got {gt}");
    assert_eq!(exact, 1, "events.login exact should match 1, got {exact}");
}

#[tokio::test]

async fn no_echo_publish() {
    let _server = spawn_leaf(free_port(), None);

    // client_a with no_echo
    let client_a = async_nats::connect_with_options(
        format!("127.0.0.1:{}", _server.port),
        async_nats::ConnectOptions::new().no_echo(),
    )
    .await
    .expect("connect a failed");

    // client_b normal
    let client_b = _server.connect().await;

    let mut sub_a = client_a.subscribe("echo.test").await.expect("sub a failed");
    let mut sub_b = client_b.subscribe("echo.test").await.expect("sub b failed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    // client_a publishes
    client_a
        .publish("echo.test", "hello".into())
        .await
        .expect("pub failed");
    client_a.flush().await.expect("flush failed");

    // client_b should receive
    let msg_b = timeout(Duration::from_secs(5), sub_b.next())
        .await
        .expect("timed out waiting for msg on b")
        .expect("sub b ended");
    assert_eq!(&msg_b.payload[..], b"hello");

    // client_a should NOT receive (no_echo)
    let result_a = timeout(Duration::from_millis(500), sub_a.next()).await;
    assert!(
        result_a.is_err(),
        "client_a should not receive its own echo"
    );
}

// --- Hub mode helpers ---

/// Start a Server in hub mode (with leafnode_port). Waits until the server is
/// accepting connections before returning.
fn spawn_hub(client_port: u16, leafnode_port: u16) -> TestServer {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);
    let reload = Arc::new(AtomicBool::new(false));

    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: client_port,
        server_name: format!("test-hub-{}", client_port),
        leafnodes: InboundLeafConfig {
            port: Some(leafnode_port),
            ..Default::default()
        },
        ..Default::default()
    };
    let server = Server::new(config);

    std::thread::spawn(move || {
        if let Err(e) = server.run_until_shutdown(shutdown_clone, reload, None) {
            eprintln!("hub server error: {}", e);
        }
    });

    wait_for_server(client_port);
    TestServer {
        shutdown,
        port: client_port,
    }
}

impl NatsServer {
    /// Start a Go nats-server configured as a leaf connecting to the given hub leafnode port.
    fn start_as_leaf(client_port: u16, hub_leafnode_port: u16) -> Self {
        let bin = nats_server_bin();

        // Write a temporary config file for leaf mode
        let config_path = format!("/tmp/nats_leaf_test_{}.conf", client_port);
        std::fs::write(
            &config_path,
            format!(
                "listen: 127.0.0.1:{client_port}\n\
                 leafnodes {{\n  remotes [{{\n    url: \"nats://127.0.0.1:{hub_leafnode_port}\"\n  }}]\n}}\n"
            ),
        )
        .unwrap();

        let child = Command::new(&bin)
            .args(["-c", &config_path])
            .spawn()
            .unwrap_or_else(|e| panic!("failed to start nats-server leaf at {}: {}", bin, e));

        let server = NatsServer {
            child,
            port: client_port,
        };
        server.wait_ready();

        // Give the leaf connection time to establish
        std::thread::sleep(Duration::from_millis(500));

        server
    }
}

// --- Hub mode tests ---

#[tokio::test]

async fn hub_mode_local_pub_sub() {
    let _hub = spawn_hub(free_port(), free_port());
    let client = _hub.connect().await;

    let mut sub = client
        .subscribe("hub.test")
        .await
        .expect("subscribe failed");

    tokio::time::sleep(Duration::from_millis(100)).await;

    let msg = publish_and_expect(&client, &mut sub, "hub.test", b"hello-hub").await;

    assert_eq!(msg.subject.as_str(), "hub.test");
    assert_eq!(&msg.payload[..], b"hello-hub");
}

#[tokio::test]

async fn hub_mode_leaf_to_hub() {
    // Start Rust hub
    let hub_leaf_port = free_port();
    let _hub = spawn_hub(free_port(), hub_leaf_port);

    // Start Go nats-server as leaf connecting to Rust hub
    let leaf_client_port = free_port();
    let _leaf_server = NatsServer::start_as_leaf(leaf_client_port, hub_leaf_port);

    // Subscribe on hub
    let hub_client = _hub.connect().await;
    let mut hub_sub = hub_client
        .subscribe("cross.test")
        .await
        .expect("hub subscribe failed");

    // Wait for LS+ to propagate from hub to leaf
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on leaf
    let leaf_client = connect_to(leaf_client_port).await;
    let msg = publish_and_expect(&leaf_client, &mut hub_sub, "cross.test", b"from-leaf").await;

    assert_eq!(msg.subject.as_str(), "cross.test");
    assert_eq!(&msg.payload[..], b"from-leaf");
}

#[tokio::test]

async fn hub_mode_hub_to_leaf() {
    // Start Rust hub
    let hub_leaf_port = free_port();
    let _hub = spawn_hub(free_port(), hub_leaf_port);

    // Start Go nats-server as leaf connecting to Rust hub
    let leaf_client_port = free_port();
    let _leaf_server = NatsServer::start_as_leaf(leaf_client_port, hub_leaf_port);

    // Subscribe on leaf
    let leaf_client = connect_to(leaf_client_port).await;
    let mut leaf_sub = leaf_client
        .subscribe("reverse.test")
        .await
        .expect("leaf subscribe failed");

    // Wait for LS+ from Go leaf to propagate to Rust hub
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on hub
    let hub_client = _hub.connect().await;
    let msg = publish_and_expect(&hub_client, &mut leaf_sub, "reverse.test", b"from-hub").await;

    assert_eq!(msg.subject.as_str(), "reverse.test");
    assert_eq!(&msg.payload[..], b"from-hub");
}

#[tokio::test]

async fn leaf_subscription_propagation() {
    // Rust hub + Go leaf: subscribe on leaf, publish from hub -> received
    let hub_leaf_port = free_port();
    let _hub = spawn_hub(free_port(), hub_leaf_port);

    let leaf_client_port = free_port();
    let _leaf_server = NatsServer::start_as_leaf(leaf_client_port, hub_leaf_port);

    // Subscribe on leaf
    let leaf_client = connect_to(leaf_client_port).await;
    let mut leaf_sub = leaf_client
        .subscribe("leaf.prop.test")
        .await
        .expect("leaf subscribe failed");

    // Wait for LS+ to propagate from Go leaf through hub
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish from hub
    let hub_client = _hub.connect().await;
    let msg = publish_and_expect(&hub_client, &mut leaf_sub, "leaf.prop.test", b"from-hub").await;

    assert_eq!(msg.subject.as_str(), "leaf.prop.test");
    assert_eq!(&msg.payload[..], b"from-hub");
}

#[tokio::test]

async fn leaf_no_echo() {
    // Rust hub + Go leaf: sub + pub on same leaf -> publisher should NOT get echo
    let hub_leaf_port = free_port();
    let _hub = spawn_hub(free_port(), hub_leaf_port);

    let leaf_client_port = free_port();
    let _leaf_server = NatsServer::start_as_leaf(leaf_client_port, hub_leaf_port);

    let leaf_client = connect_to(leaf_client_port).await;
    let mut leaf_sub = leaf_client
        .subscribe("leaf.echo.test")
        .await
        .expect("subscribe failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish from same leaf
    leaf_client
        .publish("leaf.echo.test", "echo-check".into())
        .await
        .expect("pub failed");
    leaf_client.flush().await.expect("flush failed");

    // The subscriber on the same leaf should get it directly from Go nats-server
    // but it should NOT come back through the hub as a duplicate
    let msg = timeout(Duration::from_secs(5), leaf_sub.next())
        .await
        .expect("timed out")
        .expect("sub ended");
    assert_eq!(&msg.payload[..], b"echo-check");

    // Should NOT get a second (echoed) copy from the hub
    let dup = timeout(Duration::from_millis(500), leaf_sub.next()).await;
    assert!(dup.is_err(), "should not get echo back through hub");
}

#[tokio::test]

async fn leaf_queue_distribution() {
    // Rust hub + 2 Go leaves: queue sub on each + hub, publish 30 from hub
    let hub_leaf_port = free_port();
    let _hub = spawn_hub(free_port(), hub_leaf_port);

    let leaf1_port = free_port();
    let _leaf1 = NatsServer::start_as_leaf(leaf1_port, hub_leaf_port);

    let leaf2_port = free_port();
    let _leaf2 = NatsServer::start_as_leaf(leaf2_port, hub_leaf_port);

    let hub_client = _hub.connect().await;
    let leaf1_client = connect_to(leaf1_port).await;
    let leaf2_client = connect_to(leaf2_port).await;

    // Queue subs
    let mut hub_sub = hub_client
        .queue_subscribe("leaf.q.test", "qg".into())
        .await
        .expect("hub queue sub failed");
    let mut leaf1_sub = leaf1_client
        .queue_subscribe("leaf.q.test", "qg".into())
        .await
        .expect("leaf1 queue sub failed");
    let mut leaf2_sub = leaf2_client
        .queue_subscribe("leaf.q.test", "qg".into())
        .await
        .expect("leaf2 queue sub failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish 30 from hub
    for i in 0..30u32 {
        hub_client
            .publish("leaf.q.test", format!("q-{}", i).into())
            .await
            .expect("pub failed");
    }
    hub_client.flush().await.expect("flush failed");

    let (h, l1, l2) = tokio::join!(
        collect_msgs(&mut hub_sub, 1000),
        collect_msgs(&mut leaf1_sub, 1000),
        collect_msgs(&mut leaf2_sub, 1000)
    );

    let total = h + l1 + l2;
    assert_eq!(total, 30, "total should be 30, got {h}+{l1}+{l2}");
    assert!(h >= 1 || l1 >= 1 || l2 >= 1, "at least one should get msgs");
}

// --- Cluster mode helpers ---

/// Start a Server in cluster mode. Waits until the server is accepting
/// connections before returning.
fn spawn_cluster_node(
    client_port: u16,
    cluster_port: u16,
    seeds: Vec<String>,
    name: &str,
) -> TestServer {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);
    let reload = Arc::new(AtomicBool::new(false));

    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: client_port,
        server_name: name.to_string(),
        cluster: ClusterConfig {
            port: Some(cluster_port),
            seeds,
            name: Some("test-cluster".to_string()),
        },
        ..Default::default()
    };
    let server = Server::new(config);
    std::thread::Builder::new()
        .name(format!("cluster-{}", name))
        .spawn(move || {
            let _ = server.run_until_shutdown(shutdown_clone, reload, None);
        })
        .expect("failed to spawn cluster node thread");

    wait_for_server(client_port);
    TestServer {
        shutdown,
        port: client_port,
    }
}

// --- Cluster mode tests ---

#[tokio::test]

async fn cluster_two_node_pub_sub() {
    // Node A: cluster port, no seeds
    let cluster_port_a = free_port();
    let _node_a = spawn_cluster_node(free_port(), cluster_port_a, vec![], "node-a");

    // Node B: connects to Node A as seed
    let _node_b = spawn_cluster_node(
        free_port(),
        free_port(),
        vec![format!("nats-route://127.0.0.1:{}", cluster_port_a)],
        "node-b",
    );

    // Let route connection establish
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client on Node A subscribes
    let client_a = _node_a.connect().await;
    let mut sub_a = client_a
        .subscribe("cluster.test")
        .await
        .expect("subscribe on A failed");

    // Let subscription propagate via RS+ to Node B
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Client on Node B publishes
    let client_b = _node_b.connect().await;
    let msg = publish_and_expect(&client_b, &mut sub_a, "cluster.test", b"hello-cluster").await;

    assert_eq!(msg.subject.as_str(), "cluster.test");
    assert_eq!(&msg.payload[..], b"hello-cluster");
}

#[tokio::test]

async fn cluster_reverse_direction() {
    // Test message flow: publish on A, subscribe on B
    let cluster_port_a = free_port();
    let _node_a = spawn_cluster_node(free_port(), cluster_port_a, vec![], "node-a-rev");

    let _node_b = spawn_cluster_node(
        free_port(),
        free_port(),
        vec![format!("nats-route://127.0.0.1:{}", cluster_port_a)],
        "node-b-rev",
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe on Node B
    let client_b = _node_b.connect().await;
    let mut sub_b = client_b
        .subscribe("reverse.>")
        .await
        .expect("subscribe on B failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on Node A
    let client_a = _node_a.connect().await;
    let msg = publish_and_expect(&client_a, &mut sub_b, "reverse.hello", b"from-a").await;

    assert_eq!(msg.subject.as_str(), "reverse.hello");
    assert_eq!(&msg.payload[..], b"from-a");
}

#[tokio::test]

async fn cluster_three_node() {
    // Three-node cluster: A <- B (seed A), A <- C (seed A)
    let cport_a = free_port();
    let _node_a = spawn_cluster_node(free_port(), cport_a, vec![], "tri-a");

    let _node_b = spawn_cluster_node(
        free_port(),
        free_port(),
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "tri-b",
    );

    let _node_c = spawn_cluster_node(
        free_port(),
        free_port(),
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "tri-c",
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe on B and C
    let client_b = _node_b.connect().await;
    let mut sub_b = client_b
        .subscribe("tri.test")
        .await
        .expect("subscribe on B failed");

    let client_c = _node_c.connect().await;
    let mut sub_c = client_c
        .subscribe("tri.test")
        .await
        .expect("subscribe on C failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on A
    let client_a = _node_a.connect().await;
    client_a
        .publish("tri.test", "from-a".into())
        .await
        .expect("publish on A failed");
    client_a.flush().await.expect("flush failed");

    // Both B and C should receive
    let msg_b = timeout(Duration::from_secs(5), sub_b.next())
        .await
        .expect("timed out waiting for message on B")
        .expect("sub B ended");
    assert_eq!(msg_b.subject.as_str(), "tri.test");
    assert_eq!(&msg_b.payload[..], b"from-a");

    let msg_c = timeout(Duration::from_secs(5), sub_c.next())
        .await
        .expect("timed out waiting for message on C")
        .expect("sub C ended");
    assert_eq!(msg_c.subject.as_str(), "tri.test");
    assert_eq!(&msg_c.payload[..], b"from-a");
}

#[tokio::test]

async fn cluster_gossip_discovery() {
    // Three-node cluster where B and C only seed A.
    // B and C should discover each other via A's gossip and form a direct route.
    let cport_a = free_port();
    let node_a = spawn_cluster_node(free_port(), cport_a, vec![], "gossip-a");

    let _node_b = spawn_cluster_node(
        free_port(),
        free_port(),
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "gossip-b",
    );

    let _node_c = spawn_cluster_node(
        free_port(),
        free_port(),
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "gossip-c",
    );

    // Allow time for gossip discovery and route formation between B<->C.
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Now shut down node A to prove B<->C have a direct route.
    node_a.shutdown.store(true, Ordering::Release);
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe on C
    let client_c = _node_c.connect().await;
    let mut sub_c = client_c
        .subscribe("gossip.test")
        .await
        .expect("subscribe on C failed");

    // Let subscription propagate via RS+ over B<->C route
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on B
    let client_b = _node_b.connect().await;
    let msg = publish_and_expect(&client_b, &mut sub_c, "gossip.test", b"from-b-via-gossip").await;

    assert_eq!(msg.subject.as_str(), "gossip.test");
    assert_eq!(&msg.payload[..], b"from-b-via-gossip");
}

#[tokio::test]

async fn cluster_partial_seed() {
    // Chain topology: A seeds B, B seeds C.
    // C should discover A via transitive gossip and form a full mesh.
    let cport_a = free_port();
    let _node_a = spawn_cluster_node(free_port(), cport_a, vec![], "chain-a");

    let cport_b = free_port();
    let _node_b = spawn_cluster_node(
        free_port(),
        cport_b,
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "chain-b",
    );

    // C only seeds B -- should discover A via gossip.
    let _node_c = spawn_cluster_node(
        free_port(),
        free_port(),
        vec![format!("nats-route://127.0.0.1:{}", cport_b)],
        "chain-c",
    );

    // Allow time for gossip discovery and full mesh formation.
    tokio::time::sleep(Duration::from_millis(2000)).await;

    // Subscribe on C
    let client_c = _node_c.connect().await;
    let mut sub_c = client_c
        .subscribe("chain.test")
        .await
        .expect("subscribe on C failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on A -- should reach C via full mesh
    let client_a = _node_a.connect().await;
    let msg = publish_and_expect(&client_a, &mut sub_c, "chain.test", b"from-a-chain").await;

    assert_eq!(msg.subject.as_str(), "chain.test");
    assert_eq!(&msg.payload[..], b"from-a-chain");
}

#[tokio::test]

async fn cluster_queue_semantics() {
    let cport_a = free_port();
    let _node_a = spawn_cluster_node(free_port(), cport_a, vec![], "queue-a");

    let _node_b = spawn_cluster_node(
        free_port(),
        free_port(),
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "queue-b",
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    let client_a = _node_a.connect().await;
    let client_b = _node_b.connect().await;

    // 2 queue subs on each node
    let mut qa1 = client_a
        .queue_subscribe("work.>", "workers".into())
        .await
        .expect("qa1 failed");
    let mut qa2 = client_a
        .queue_subscribe("work.>", "workers".into())
        .await
        .expect("qa2 failed");
    let mut qb1 = client_b
        .queue_subscribe("work.>", "workers".into())
        .await
        .expect("qb1 failed");
    let mut qb2 = client_b
        .queue_subscribe("work.>", "workers".into())
        .await
        .expect("qb2 failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish 100 from node-A
    for i in 0..100u32 {
        client_a
            .publish("work.item", format!("job-{}", i).into())
            .await
            .expect("publish failed");
    }
    client_a.flush().await.expect("flush failed");

    let (a1, a2, b1, b2) = tokio::join!(
        collect_msgs(&mut qa1, 1000),
        collect_msgs(&mut qa2, 1000),
        collect_msgs(&mut qb1, 1000),
        collect_msgs(&mut qb2, 1000)
    );

    let total = a1 + a2 + b1 + b2;
    assert_eq!(total, 100, "total should be 100, got {a1}+{a2}+{b1}+{b2}");
    assert!(b1 + b2 >= 1, "node-B should get at least 1, got {b1}+{b2}");
}

#[tokio::test]

async fn cluster_one_hop_enforcement() {
    // 3-node cluster: A, B (seed A), C (seed A)
    let cport_a = free_port();
    let _node_a = spawn_cluster_node(free_port(), cport_a, vec![], "hop-a");

    let _node_b = spawn_cluster_node(
        free_port(),
        free_port(),
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "hop-b",
    );

    let _node_c = spawn_cluster_node(
        free_port(),
        free_port(),
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "hop-c",
    );
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Subscribe on C
    let client_c = _node_c.connect().await;
    let mut sub_c = client_c.subscribe("hop.test").await.expect("sub C failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on A
    let client_a = _node_a.connect().await;
    client_a
        .publish("hop.test", "once".into())
        .await
        .expect("pub failed");
    client_a.flush().await.expect("flush failed");

    // Should get exactly 1 copy (no duplication from re-forwarding)
    let msg = timeout(Duration::from_secs(5), sub_c.next())
        .await
        .expect("timed out")
        .expect("sub ended");
    assert_eq!(&msg.payload[..], b"once");

    // No second copy
    let dup = timeout(Duration::from_millis(500), sub_c.next()).await;
    assert!(
        dup.is_err(),
        "should not receive duplicate from re-forwarding"
    );
}

#[tokio::test]

async fn cluster_sub_unsub_propagation() {
    let cport_a = free_port();
    let _node_a = spawn_cluster_node(free_port(), cport_a, vec![], "prop-a");

    let _node_b = spawn_cluster_node(
        free_port(),
        free_port(),
        vec![format!("nats-route://127.0.0.1:{}", cport_a)],
        "prop-b",
    );
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Subscribe on A
    let client_a = _node_a.connect().await;
    let mut sub_a = client_a.subscribe("prop.test").await.expect("sub A failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish from B -> should be received on A
    let client_b = _node_b.connect().await;
    let msg = publish_and_expect(&client_b, &mut sub_a, "prop.test", b"before").await;
    assert_eq!(&msg.payload[..], b"before");

    // Unsubscribe on A
    sub_a.unsubscribe().await.expect("unsub failed");
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish again from B -> should NOT be received
    client_b
        .publish("prop.test", "after".into())
        .await
        .expect("pub failed");
    client_b.flush().await.expect("flush failed");

    let result = timeout(Duration::from_millis(500), sub_a.next()).await;
    assert!(
        result.is_err() || result.unwrap().is_none(),
        "should not receive after unsub"
    );
}

// --- Gateway mode helpers ---

/// Start a Server in gateway mode. Waits until the server is accepting
/// connections before returning.
fn spawn_gateway_node(
    client_port: u16,
    cluster_port: u16,
    gateway_port: u16,
    gateway_name: &str,
    remotes: Vec<(String, String)>,
    cluster_seeds: Vec<String>,
    server_name: &str,
) -> TestServer {
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);
    let reload = Arc::new(AtomicBool::new(false));

    let gateway_remotes = remotes
        .into_iter()
        .map(|(name, url)| GatewayRemote {
            name,
            urls: vec![url],
        })
        .collect();

    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: client_port,
        server_name: server_name.to_string(),

        cluster: open_wire::ClusterConfig {
            port: Some(cluster_port),
            seeds: cluster_seeds,
            name: Some(gateway_name.to_string()),
        },
        gateway: open_wire::GatewayConfig {
            port: Some(gateway_port),
            name: Some(gateway_name.to_string()),
            remotes: gateway_remotes,
        },
        ..Default::default()
    };
    let server = Server::new(config);
    std::thread::Builder::new()
        .name(format!("gw-{}", server_name))
        .spawn(move || {
            let _ = server.run_until_shutdown(shutdown_clone, reload, None);
        })
        .expect("failed to spawn gateway node thread");

    wait_for_server(client_port);
    TestServer {
        shutdown,
        port: client_port,
    }
}

// --- Gateway mode tests ---

#[tokio::test]

async fn gateway_basic_pub_sub() {
    let gport_a = free_port();
    let gport_b = free_port();

    let _gw_a = spawn_gateway_node(
        free_port(),
        free_port(),
        gport_a,
        "cluster-a",
        vec![(
            "cluster-b".to_string(),
            format!("nats://127.0.0.1:{}", gport_b),
        )],
        vec![],
        "gw-node-a",
    );

    let _gw_b = spawn_gateway_node(
        free_port(),
        free_port(),
        gport_b,
        "cluster-b",
        vec![(
            "cluster-a".to_string(),
            format!("nats://127.0.0.1:{}", gport_a),
        )],
        vec![],
        "gw-node-b",
    );

    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Subscribe on B
    let client_b = _gw_b.connect().await;
    let mut sub_b = client_b
        .subscribe("gw.basic.test")
        .await
        .expect("sub B failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on A
    let client_a = _gw_a.connect().await;
    let msg = publish_and_expect(&client_a, &mut sub_b, "gw.basic.test", b"hello-gw").await;

    assert_eq!(msg.subject.as_str(), "gw.basic.test");
    assert_eq!(&msg.payload[..], b"hello-gw");
}

#[tokio::test]

async fn gateway_reverse_direction() {
    let gport_a = free_port();
    let gport_b = free_port();

    let _gw_a = spawn_gateway_node(
        free_port(),
        free_port(),
        gport_a,
        "cluster-a",
        vec![(
            "cluster-b".to_string(),
            format!("nats://127.0.0.1:{}", gport_b),
        )],
        vec![],
        "gw-rev-a",
    );

    let _gw_b = spawn_gateway_node(
        free_port(),
        free_port(),
        gport_b,
        "cluster-b",
        vec![(
            "cluster-a".to_string(),
            format!("nats://127.0.0.1:{}", gport_a),
        )],
        vec![],
        "gw-rev-b",
    );
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Subscribe on A
    let client_a = _gw_a.connect().await;
    let mut sub_a = client_a
        .subscribe("gw.rev.test")
        .await
        .expect("sub A failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish on B
    let client_b = _gw_b.connect().await;
    let msg = publish_and_expect(&client_b, &mut sub_a, "gw.rev.test", b"from-b").await;

    assert_eq!(msg.subject.as_str(), "gw.rev.test");
    assert_eq!(&msg.payload[..], b"from-b");
}

#[tokio::test]

async fn gateway_request_reply() {
    let gport_a = free_port();
    let gport_b = free_port();

    let _gw_a = spawn_gateway_node(
        free_port(),
        free_port(),
        gport_a,
        "cluster-a",
        vec![(
            "cluster-b".to_string(),
            format!("nats://127.0.0.1:{}", gport_b),
        )],
        vec![],
        "gw-rr-a",
    );

    let _gw_b = spawn_gateway_node(
        free_port(),
        free_port(),
        gport_b,
        "cluster-b",
        vec![(
            "cluster-a".to_string(),
            format!("nats://127.0.0.1:{}", gport_a),
        )],
        vec![],
        "gw-rr-b",
    );
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Service on B: subscribe and reply
    let client_b = _gw_b.connect().await;
    let mut service_sub = client_b
        .subscribe("service.echo")
        .await
        .expect("service sub failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Spawn responder task
    let responder_client = client_b.clone();
    let responder = tokio::spawn(async move {
        if let Some(msg) = timeout(Duration::from_secs(5), service_sub.next())
            .await
            .ok()
            .flatten()
        {
            if let Some(reply) = msg.reply {
                responder_client
                    .publish(reply, "echo-reply".into())
                    .await
                    .expect("reply failed");
                responder_client.flush().await.expect("flush failed");
            }
        }
    });

    // Client on A: request
    let client_a = _gw_a.connect().await;

    let reply = timeout(
        Duration::from_secs(10),
        client_a.request("service.echo", "ping".into()),
    )
    .await
    .expect("request timed out")
    .expect("request failed");

    assert_eq!(&reply.payload[..], b"echo-reply");

    responder.await.expect("responder task failed");
}

#[tokio::test]

async fn gateway_queue_groups() {
    let gport_a = free_port();
    let gport_b = free_port();

    let _gw_a = spawn_gateway_node(
        free_port(),
        free_port(),
        gport_a,
        "cluster-a",
        vec![(
            "cluster-b".to_string(),
            format!("nats://127.0.0.1:{}", gport_b),
        )],
        vec![],
        "gw-qq-a",
    );

    let _gw_b = spawn_gateway_node(
        free_port(),
        free_port(),
        gport_b,
        "cluster-b",
        vec![(
            "cluster-a".to_string(),
            format!("nats://127.0.0.1:{}", gport_a),
        )],
        vec![],
        "gw-qq-b",
    );
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // 2 queue subs on B
    let client_b = _gw_b.connect().await;
    let mut qb1 = client_b
        .queue_subscribe("gw.queue.test", "qworkers".into())
        .await
        .expect("qb1 failed");
    let mut qb2 = client_b
        .queue_subscribe("gw.queue.test", "qworkers".into())
        .await
        .expect("qb2 failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish 20 from A
    let client_a = _gw_a.connect().await;
    for i in 0..20u32 {
        client_a
            .publish("gw.queue.test", format!("gq-{}", i).into())
            .await
            .expect("pub failed");
    }
    client_a.flush().await.expect("flush failed");

    let (b1, b2) = tokio::join!(collect_msgs(&mut qb1, 1000), collect_msgs(&mut qb2, 1000));

    assert_eq!(b1 + b2, 20, "total should be 20, got {b1}+{b2}");
    assert!(b1 >= 1, "qb1 should get at least 1, got {b1}");
    assert!(b2 >= 1, "qb2 should get at least 1, got {b2}");
}

#[tokio::test]
#[ignore] // Gateway interest mode transition is timing-sensitive; tracked separately.
async fn gateway_interest_mode_transition() {
    let gport_a = free_port();
    let gport_b = free_port();

    let _gw_a = spawn_gateway_node(
        free_port(),
        free_port(),
        gport_a,
        "cluster-a",
        vec![(
            "cluster-b".to_string(),
            format!("nats://127.0.0.1:{}", gport_b),
        )],
        vec![],
        "gw-im-a",
    );

    let _gw_b = spawn_gateway_node(
        free_port(),
        free_port(),
        gport_b,
        "cluster-b",
        vec![(
            "cluster-a".to_string(),
            format!("nats://127.0.0.1:{}", gport_a),
        )],
        vec![],
        "gw-im-b",
    );
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Subscribe to a known subject on A
    let client_a = _gw_a.connect().await;
    let mut sub_a = client_a
        .subscribe("interest.final")
        .await
        .expect("sub A failed");

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish 1100 unique no-interest subjects from B to trigger Optimistic -> InterestOnly
    let client_b = _gw_b.connect().await;
    for i in 0..1100u32 {
        client_b
            .publish(format!("nointerest.unique.{}", i), "x".into())
            .await
            .expect("pub failed");
    }
    client_b.flush().await.expect("flush failed");

    // Wait for interest mode transition (1100 no-interest messages trigger switch)
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Publish the subject A is subscribed to
    client_b
        .publish("interest.final", "after-transition".into())
        .await
        .expect("pub failed");
    client_b.flush().await.expect("flush failed");

    let msg = timeout(Duration::from_secs(10), sub_a.next())
        .await
        .expect("timed out waiting for interest mode message")
        .expect("sub ended");

    assert_eq!(msg.subject.as_str(), "interest.final");
    assert_eq!(&msg.payload[..], b"after-transition");
}

#[tokio::test]

async fn gateway_fan_out() {
    let gport_a = free_port();
    let gport_b = free_port();

    let _gw_a = spawn_gateway_node(
        free_port(),
        free_port(),
        gport_a,
        "cluster-a",
        vec![(
            "cluster-b".to_string(),
            format!("nats://127.0.0.1:{}", gport_b),
        )],
        vec![],
        "gw-fan-a",
    );

    let _gw_b = spawn_gateway_node(
        free_port(),
        free_port(),
        gport_b,
        "cluster-b",
        vec![(
            "cluster-a".to_string(),
            format!("nats://127.0.0.1:{}", gport_a),
        )],
        vec![],
        "gw-fan-b",
    );
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Subscribe to 5 distinct subjects on B
    let client_b = _gw_b.connect().await;

    let subjects = [
        "fan.alpha",
        "fan.beta",
        "fan.gamma",
        "fan.delta",
        "fan.epsilon",
    ];
    let mut subs = Vec::new();
    for &subj in &subjects {
        let sub = client_b.subscribe(subj).await.expect("sub failed");
        subs.push(sub);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Publish 1 to each from A
    let client_a = _gw_a.connect().await;
    for &subj in &subjects {
        client_a
            .publish(subj, format!("payload-{}", subj).into())
            .await
            .expect("pub failed");
    }
    client_a.flush().await.expect("flush failed");

    // All 5 should be received on correct subjects
    for (i, sub) in subs.iter_mut().enumerate() {
        let msg = timeout(Duration::from_secs(5), sub.next())
            .await
            .unwrap_or_else(|_| panic!("timed out on subject {}", subjects[i]))
            .unwrap_or_else(|| panic!("sub ended for {}", subjects[i]));

        assert_eq!(
            msg.subject.as_str(),
            subjects[i],
            "wrong subject for index {i}"
        );
        assert_eq!(
            &msg.payload[..],
            format!("payload-{}", subjects[i]).as_bytes()
        );
    }
}
