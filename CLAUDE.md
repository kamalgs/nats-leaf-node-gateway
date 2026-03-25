# CLAUDE.md — AI Agent Instructions for open-wire

## Project Overview

**open-wire** is a high-performance NATS-compatible message relay written in Rust.
It speaks the standard NATS client, leaf node, and route protocols, routes messages between
local clients, optionally bridges traffic to an upstream NATS hub server, and can form
full-mesh clusters with peer nodes.

Built with raw epoll, zero-copy parsing, and no async runtime.

## Repository Structure

```
src/
├── main.rs              # CLI binary (--port, --hub, --ws-port, --workers, --cluster-*)
├── lib.rs               # Public API: LeafServer, LeafServerConfig
├── config.rs            # Go nats-server .conf file parser
├── server.rs            # Accept loop, worker spawning, shutdown
├── worker.rs            # Per-thread epoll event loop, connection state machine
├── nats_proto.rs        # Zero-copy protocol parser + MsgBuilder
├── sub_list.rs          # SubList (exact + wildcard subscription matching)
├── direct_writer.rs     # DirectWriter: shared buffer + eventfd fan-out
├── handler.rs           # Shared handler types, ConnExt, deliver_to_subs
├── propagation.rs       # Interest propagation (LS+/LS-, RS+/RS-) + gateway reply rewriting
├── client_handler.rs    # Client protocol dispatch (PUB/SUB/UNSUB/PING/PONG)
├── leaf_handler.rs      # Inbound leaf protocol dispatch (LS+/LS-/LMSG) [hub]
├── leaf_conn.rs         # LeafConn, LeafReader, LeafWriter, HubStream [leaf]
├── upstream.rs          # Hub connection via leaf node protocol [leaf]
├── interest.rs          # InterestPipeline: subject mapping + interest collapse [leaf]
├── route_handler.rs     # Route protocol dispatch (RS+/RS-/RMSG) [cluster]
├── route_conn.rs        # Outbound route connection manager [cluster]
├── gateway_handler.rs   # Gateway protocol dispatch (RS+/RS-/RMSG) [gateway]
├── gateway_conn.rs      # Outbound gateway connection manager [gateway]
├── buf.rs               # AdaptiveBuf, BufConfig, ServerConn (test-only)
├── websocket.rs         # HTTP upgrade handshake, WS frame codec
└── types.rs             # ServerInfo, ConnectInfo, HeaderMap
examples/
└── chat/            # Sample chat app (HTML + README)
tests/
├── e2e.rs           # Integration tests (requires nats-server + async-nats)
├── throughput.rs    # Criterion benchmarks
├── throughput.sh    # Main Rust vs Go leaf benchmark
├── smoke_test.sh    # Quick functional smoke test
├── profile.sh       # Perf profiling (pub-only, pubsub, fanout)
├── profile_run.sh   # Ad-hoc perf: pub-only + pubsub
├── profile_pubsub.sh   # Ad-hoc perf: pubsub with frame pointers
├── profile_pubonly.sh   # Ad-hoc perf: pub-only with frame pointers
├── profile_hubleaf.sh   # Ad-hoc perf: hub→leaf with frame pointers
├── memory.sh        # Idle-connection memory comparison (Go vs Rust)
├── clients/         # Go helper binary for memory bench
└── configs/         # nats-server configs for benchmarks
docs/
├── architecture.md  # Detailed message flow diagrams
├── goals.md         # Project goals
├── backlog.md       # Feature backlog
├── portability.md   # Portability notes
└── adr/             # Architecture decision records
Cargo.toml               # Package manifest
Cargo.lock
BENCHMARKS.md            # Full benchmark results log
Dockerfile  .dockerignore
.gitignore  .rustfmt.toml
CLAUDE.md  LICENSE  NOTICE  README.md
.cargo/  .claude/  .github/
```

## Build & Test Commands

```bash
# Check
cargo check

# Check with cluster feature
cargo check --features cluster

# Test (unit — no external deps)
cargo test --lib

# Test with cluster feature
cargo test --lib --features cluster

# Test (all — requires nats-server in PATH)
cargo test

# Test cluster integration tests
cargo test --test e2e --features cluster -- cluster

# Format (required: nightly toolchain)
cargo +nightly fmt

# Lint
cargo clippy --all-targets -- --deny clippy::all
cargo clippy --all-targets --features cluster -- --deny clippy::all

# Build release
cargo build --release

# Build release with cluster
cargo build --release --features cluster

# Build release with frame pointers (for perf profiling)
RUSTFLAGS="-C force-frame-pointers=yes" cargo build --release

# Benchmarks (quick — 5 core scenarios)
cd tests && ./throughput.sh

# Benchmarks (full — all 16 scenarios including cluster)
cd tests && ./throughput.sh --full

cd tests && ./smoke_test.sh
```

**nats-server**: Integration tests and benchmarks require the `nats-server` binary:
```bash
go install github.com/nats-io/nats-server/v2@main
```

## Formatting Rules

Defined in `.rustfmt.toml`:
- `max_width = 100`
- `reorder_imports = true`
- `format_code_in_doc_comments = true`
- Edition: 2018 (rustfmt setting — the crate itself is edition 2021)

Always run `cargo +nightly fmt` before committing.

## Architecture

```
                        ┌────────────────────────┐
                        │     Upstream Hub        │
                        │  (standard nats-server) │
                        └───────────┬────────────┘
                                    │ leaf node protocol
                        ┌───────────┴────────────┐
                        │    Upstream Module      │
                        │  reader + writer thread │
                        └───────────┬────────────┘
                                    │
    ┌───────────────────────────────┼───────────────────────────────┐
    │  ┌──────────┐  round  ┌──────┴─────┐  round  ┌──────────┐   │
    │  │ Worker 0 │◄─robin─►│  Acceptor  │◄─robin─►│ Worker N │   │
    │  └────┬─────┘         └────────────┘         └────┬─────┘   │
    │       │ epoll                                      │ epoll   │
    │  ┌────┴──────────┐                          ┌─────┴───────┐ │
    │  │ C0  C1  C2 .. │                          │ Cm .. Cn    │ │
    │  └───────────────┘                          └─────────────┘ │
    └─────────────────────────────────────────────────────────────┘
```

### Key Design Points

- **N-worker epoll model**: N worker threads, each with one epoll instance multiplexing many connections.
- **DirectWriter**: Cross-worker message delivery via shared buffers + eventfd notifications.
  Fan-out to N conns on same worker = 1 eventfd write. Batched notifications reduce syscalls.
- **Zero-copy parsing**: Protocol parsed directly from read buffers via `nats_proto.rs`.
- **No async runtime**: Pure `std::thread` + `epoll` + `std::sync::mpsc`.
- **Connection state machine**: `SendInfo → WaitConnect → Active` phases in worker.
- **AdaptiveBuf**: Go-style dynamic buffer sizing (512B → 64KB).
- **Full-mesh clustering** (`cluster` feature): Route connections between peers using RS+/RS-/RMSG
  protocol. One-hop message forwarding — messages from routes are never re-forwarded to other routes.

### Key Types

| Type | Location | Purpose |
|------|----------|---------|
| `LeafServer` | `lib.rs` | Public API entry point |
| `LeafServerConfig` | `lib.rs` | Server configuration |
| `load_config` | `config.rs` | Go nats-server `.conf` file parser |
| `Worker` | `worker.rs` | Per-thread epoll event loop |
| `NatsProto` / `MsgBuilder` | `nats_proto.rs` | Protocol parser + message builder |
| `SubList` | `sub_list.rs` | Subscription storage + wildcard matching |
| `DirectWriter` | `direct_writer.rs` | Shared buffer + eventfd fan-out delivery |
| `ServerConn` | `buf.rs` | Connection I/O wrapper (test-only) |
| `LeafConn` | `leaf_conn.rs` | Leaf connection I/O wrapper |
| `AdaptiveBuf` | `buf.rs` | Dynamic read buffer |
| `Upstream` | `upstream.rs` | Hub connection management |
| `InterestPipeline` | `interest.rs` | Subject mapping + interest collapse |
| `RouteHandler` | `route_handler.rs` | Route protocol dispatch (`cluster`) |
| `RouteConnManager` | `route_conn.rs` | Outbound route connections (`cluster`) |
| `GatewayHandler` | `gateway_handler.rs` | Gateway protocol dispatch (`gateway`) |
| `GatewayConnManager` | `gateway_conn.rs` | Outbound gateway connections (`gateway`) |

## Feature Flags

| Feature | Default | Purpose |
|---------|---------|---------|
| `leaf` | yes | Upstream hub connection support |
| `hub` | yes | Inbound leaf node connection support |
| `interest-collapse` | yes | N:1 wildcard aggregation for upstream subs |
| `subject-mapping` | yes | Stateless subject rewriting before upstream |
| `cluster` | no | Full mesh route clustering (RS+/RS-/RMSG) |
| `gateway` | no | Gateway inter-cluster traffic |
| `accounts` | no | Multi-tenant per-account subject isolation |
| `worker-affinity` | no | Subject-based worker affinity tracking |

## Dependencies

```toml
[dependencies]
bytes = "1.4.0"
libc = "0.2"
memchr = "2.4"
rand = "0.8"
serde = { version = "1", features = ["derive"] }
serde_json = "1.0.104"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
metrics = "0.24"
metrics-exporter-prometheus = "0.16"
nkeys = { version = "0.4", default-features = false }
data-encoding = "2"
rustls = "0.23"
rustls-pemfile = "2"
rustls-pki-types = "1"
webpki-roots = "0.26"
```

Minimal dependency footprint. No async runtime.

New dependencies should use permissive licenses: MIT, Apache-2.0, ISC, BSD-2-Clause, BSD-3-Clause.

## Build Environment

- Using zig as C compiler/linker (no system gcc). Config in `.cargo/config.toml`.
- `[profile.release] debug = 1, strip = false` for perf symbol resolution.

## Code Conventions

- **No `unwrap()` or `expect()`** in library code.
- **Imports**: Group std → external crates → crate-internal.
- All public items should have doc comments.
- Tests go in `#[cfg(test)] mod tests` within each source file (295 unit tests with `--all-features`).
- Integration tests in `tests/e2e.rs` use `async-nats` client against a real `nats-server`.

## Commit & PR Standards

- Linear history — rebase, no merge commits.
- Atomic, reasonably-sized commits.
- Well-formed commit messages.

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE) for attribution.
This project is a fork of [nats-io/nats.rs](https://github.com/nats-io/nats.rs).
