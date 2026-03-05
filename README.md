# NATS Leaf Node Gateway

A lightweight NATS leaf node gateway server written in Rust. It accepts local client connections, routes messages between them, and optionally forwards traffic to an upstream NATS hub server.

The server lives in the `nats-server` crate and depends on [async-nats](https://github.com/nats-io/nats.rs) (Apache 2.0) for protocol types and the upstream hub connection.

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

## Features

- **Local pub/sub** -- Clients connect directly and exchange messages through the gateway
- **Upstream hub forwarding** -- Optionally connects to a NATS server and bridges traffic bidirectionally
- **NATS wildcard matching** -- Full support for `*` (single token) and `>` (tail match) wildcards
- **Standard NATS protocol** -- Works with any NATS client (`nats` CLI, async-nats, nats.go, etc.)
- **Headers support** -- HMSG/HPUB protocol for NATS headers
- **WebSocket support** -- Accept browser and WS-capable NATS clients (requires `websockets` feature)

## Quick Start

### Build from source

```bash
cargo build --release -p nats-server --example leaf_server
```

### Run as a standalone server

```bash
# Listen on port 4222
cargo run -p nats-server --example leaf_server -- --port 4222

# With upstream hub
cargo run -p nats-server --example leaf_server -- --port 4222 --hub nats://hub-server:4111
```

### Docker

```bash
docker build -t nats-leaf-gateway .
docker run -p 4222:4222 nats-leaf-gateway

# With upstream hub
docker run -p 4222:4222 nats-leaf-gateway --hub nats://hub-server:4111
```

## Usage

### CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `--port`, `-p` | `4222` | Port to listen on |
| `--host` | `0.0.0.0` | Address to bind to |
| `--hub` | *(none)* | Upstream NATS server URL (e.g., `nats://hub:4222`) |
| `--name` | `leaf-node` | Server name |
| `--ws-port` | *(none)* | WebSocket listener port (requires `websockets` feature) |

### Local pub/sub (no upstream)

```bash
# Terminal 1: Start the gateway
cargo run -p nats-server --example leaf_server -- --port 4222

# Terminal 2: Subscribe
nats sub test.subject -s nats://localhost:4222

# Terminal 3: Publish
nats pub test.subject "hello" -s nats://localhost:4222
```

### Leaf node with upstream hub

```bash
# Terminal 1: Start an upstream NATS server
nats-server -p 4111

# Terminal 2: Start the leaf gateway pointing at the hub
cargo run -p nats-server --example leaf_server -- --port 4222 --hub nats://localhost:4111

# Terminal 3: Subscribe via the leaf gateway
nats sub "test.>" -s nats://localhost:4222

# Terminal 4: Publish to the hub -- message arrives at the leaf subscriber
nats pub test.hello "from hub" -s nats://localhost:4111
```

### WebSocket clients

```bash
# Start with both TCP and WebSocket listeners
cargo run -p nats-server --features websockets \
  --example leaf_server -- --port 4222 --ws-port 4223

# Connect a NATS client via WebSocket
nats sub test.subject -s ws://localhost:4223
```

### As a library

```rust
use nats_server::{LeafServer, LeafServerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = LeafServerConfig {
        host: "0.0.0.0".into(),
        port: 4222,
        #[cfg(feature = "websockets")]
        ws_port: Some(4223),
        hub_url: Some("nats://hub:4111".into()),
        server_name: "my-leaf".into(),
    };

    let server = LeafServer::new(config);
    server.run().await
}
```

## Architecture

```
nats-server/src/
  lib.rs             Public API (LeafServer, LeafServerConfig)
  protocol.rs        Server-side protocol read/write (ServerConn)
  sub_list.rs        Subscription list with NATS wildcard matching
  client_conn.rs     Per-client connection handler
  upstream.rs        Hub connection via async-nats Client
  server.rs          TCP accept loop and shared server state
```

The gateway reuses the following from the async-nats crate:

- `ClientOp` -- protocol message enum
- `ServerInfo` / `ConnectInfo` -- handshake types
- `Client` -- used for the upstream hub connection
- `Subject`, `HeaderMap`, `Message` -- message types

## Tests

```bash
cargo test -p nats-server --lib
```

18 unit tests covering protocol parsing (CONNECT, PUB, HPUB, SUB, UNSUB, PING, PONG, MSG serialization) and subscription wildcard matching.

## License

Apache License 2.0 -- see [LICENSE](LICENSE).

This project is a fork of [nats-io/nats.rs](https://github.com/nats-io/nats.rs). See [NOTICE](NOTICE) for attribution details.
