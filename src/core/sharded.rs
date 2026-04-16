//! Sharded server: N single-worker instances connected via in-process
//! Unix socket pairs, reproducing the multi-process spike from ADR-012.
//!
//! Each shard is a full `Server` with `workers = 1`. Cross-shard message
//! routing uses the existing route protocol (RS+/RS-/RMSG) — zero changes
//! to the delivery path. The Unix socket pairs replace loopback TCP,
//! keeping data in kernel buffers without touching the network stack.
//!
//! Usage:
//! ```ignore
//! let sharded = ShardedServer::new(base_config, 4);
//! sharded.run()?;
//! ```

use std::net::{SocketAddr, TcpStream};
use std::os::unix::io::{FromRawFd, IntoRawFd};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use tracing::info;

use super::server::{Server, ServerConfig};
use super::server::ServerState;

/// Run N single-worker `Server` instances in one process, connected by
/// Unix socket pairs that carry the existing route protocol.
pub struct ShardedServer {
    shards: Vec<Server>,
    metrics_port: Option<u16>,
}

impl ShardedServer {
    /// Create `n` shards from `base`. Each shard gets:
    ///   - `workers = 1`
    ///   - Client NATS port = `base.port + i * 10`
    ///   - Binary port = `base.binary_port.map(|p| p + i as u16 * 10)`
    ///   - Cluster port = NONE (we inject routes ourselves, not via TCP seeds)
    ///   - Metrics port = `base.metrics_port.map(|p| p + i as u16)`
    ///   - No cluster seeds (mesh is wired via Unix sockets, not TCP)
    pub fn new(base: ServerConfig, n: usize) -> Self {
        let metrics_port = base.metrics_port;
        let mut shards = Vec::with_capacity(n);
        for i in 0..n {
            let mut cfg = base.clone();
            cfg.workers = 1;
            cfg.port = base.port + (i as u16) * 10;
            cfg.binary_port = base.binary_port.map(|p| p + (i as u16) * 10);
            // Metrics recorder is a process-global singleton — install
            // once in run(), not per-shard.
            cfg.metrics_port = None;
            cfg.monitoring_port = None;
            // No TCP cluster seeds — the hook below injects Unix pairs.
            cfg.cluster.seeds.clear();
            cfg.cluster.port = None;
            cfg.server_name = format!("{}-shard-{}", base.server_name, i);

            shards.push(Server::new(cfg));
        }
        Self { shards, metrics_port }
    }

    /// Start all shards. Blocks until all threads exit (i.e., forever).
    ///
    /// The sequence is:
    ///   1. Each shard calls `run_with_hook()` in its own thread.
    ///   2. The hook fires after workers are spawned but before the
    ///      accept loop. Each shard registers its worker handles in a
    ///      shared vec, then waits at a barrier.
    ///   3. Once all shards have registered, the main thread creates
    ///      `UnixStream::pair()` for each (i, j) pair and injects one
    ///      end into shard i's worker, the other into shard j's worker.
    ///   4. A second barrier releases all shards into their accept loops.
    pub fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let n = self.shards.len();
        if n < 2 {
            return self.shards[0].run();
        }

        // Install the metrics exporter once for the whole process.
        // Individual shards have metrics_port=None so they don't try
        // to set the global recorder again.
        if let Some(port) = self.metrics_port {
            super::server::install_metrics_exporter(port)?;
            info!(port, "prometheus metrics endpoint listening (sharded)");
        }

        info!(shards = n, "starting sharded server");

        // Phase 1: start all shards; each registers its handles + waits.
        let barrier = Arc::new(Barrier::new(n + 1)); // +1 for main
        // Registry stores (worker senders, state) — we only need the
        // Sender to inject route connections, not the full WorkerHandle.
        type Sender = std::sync::mpsc::Sender<super::worker::WorkerCmd>;
        type WorkerReg = Vec<(Vec<Sender>, Arc<ServerState>)>;
        let registry: Arc<std::sync::Mutex<WorkerReg>> =
            Arc::new(std::sync::Mutex::new(Vec::with_capacity(n)));

        // Use scoped threads so we can borrow &self.shards without 'static.
        thread::scope(|s| {
            for i in 0..n {
                let barrier = Arc::clone(&barrier);
                let registry = Arc::clone(&registry);
                let server = &self.shards[i];
                s.spawn(move || {
                    server.run_with_hook(|workers, state| {
                        {
                            let mut reg = registry.lock().expect("registry lock");
                            let senders: Vec<Sender> =
                                workers.iter().map(|w| w.tx.clone()).collect();
                            reg.push((senders, Arc::clone(state)));
                        }
                        barrier.wait(); // signal: "my workers are ready"
                        barrier.wait(); // wait: "main injected all streams"
                    })
                    .expect("shard run failed");
                });
            }

            // Phase 2: wait for all shards, then inject Unix stream pairs.
            barrier.wait(); // all shards registered

            let reg = registry.lock().expect("registry lock");
            info!(
                shards = n,
                pairs = n * (n - 1) / 2,
                "injecting in-process route connections"
            );

            for i in 0..n {
                for j in (i + 1)..n {
                    let (us_a, us_b) = UnixStream::pair().expect("UnixStream::pair");
                    // Convert to TcpStream via raw fd. The worker's epoll
                    // loop treats all STREAM fds uniformly (read/write/poll).
                    // TCP-specific setsockopts (TCP_NODELAY) fail harmlessly
                    // — the code already uses .ok() for those.
                    let tcp_a =
                        unsafe { TcpStream::from_raw_fd(us_a.into_raw_fd()) };
                    let tcp_b =
                        unsafe { TcpStream::from_raw_fd(us_b.into_raw_fd()) };

                    // Side A → inject into shard i's worker as INBOUND
                    // (worker runs the server-side handshake: send INFO,
                    // wait for CONNECT).
                    inject_route_stream(&reg[i], tcp_a);

                    // Side B → spawn a thread that runs the outbound
                    // handshake (send CONNECT, wait for INFO) + the
                    // route reader/writer loops. This thread lives as
                    // long as the connection.
                    let state_j = Arc::clone(&reg[j].1);
                    let shutdown_j = Arc::new(AtomicBool::new(false));
                    thread::spawn(move || {
                        if let Err(e) = crate::connector::mesh::conn::run_outbound_route(
                            tcp_b,
                            &state_j,
                            &shutdown_j,
                        ) {
                            tracing::warn!(error = %e, "in-process route connection closed");
                        }
                    });
                }
            }
            drop(reg);

            info!("all route streams injected, releasing shards");
            barrier.wait(); // release all shards to accept loops

            // Scoped threads are joined automatically when this block exits.
        });
        Ok(())
    }
}

/// Push a pre-connected stream into a shard as an inbound route connection.
fn inject_route_stream(
    (senders, state): &(Vec<std::sync::mpsc::Sender<super::worker::WorkerCmd>>, Arc<ServerState>),
    stream: TcpStream,
) {
    let cid = state.next_client_id();
    state
        .active_connections
        .fetch_add(1, Ordering::Relaxed);
    let idx = cid as usize % senders.len(); // single-worker shard → always 0
    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let _ = senders[idx].send(super::worker::WorkerCmd::NewRouteConn {
        id: cid,
        stream,
        addr,
    });
}
