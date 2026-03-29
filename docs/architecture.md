# Architecture

A high-performance NATS-compatible message relay written in Rust. It accepts
client connections over TCP, routes messages between local subscribers, and
optionally forwards traffic to an upstream NATS hub via the leaf node protocol.
It can also accept inbound leaf connections from other servers (hub mode) and
form full-mesh clusters with peer nodes via the route protocol (cluster mode).

Upstream (`leaf`), inbound leaf (`hub`), and cluster (`cluster`) capabilities
are Cargo feature-gated. `leaf` and `hub` are default-enabled; `cluster` is
opt-in. See [ADR-009](adr/009-leaf-hub-feature-flags.md) and
[ADR-010](adr/010-full-mesh-clustering.md).

## System Diagram

```
                          ┌─────────────────────────────┐
                          │      Upstream Hub            │
                          │   (standard nats-server)     │
                          └──────────┬──────────────────┘
                                     │ leaf node protocol
                                     │ (LS+/LS-/LMSG)
                          ┌──────────┴──────────────────┐
                          │    Upstream Module [leaf]    │
                          │  reader thread  writer thread│
                          └──────────┬──────────────────┘
                                     │ Arc<ServerState>
    ┌────────────────────────────────┼────────────────────────────────┐
    │                                │                                │
    │  ┌──────────┐  round   ┌──────┴─────┐  round   ┌──────────┐   │
    │  │ Worker 0 │◄─robin──►│  Acceptor  │◄─robin──►│ Worker N │   │
    │  └────┬─────┘          └────────────┘          └────┬─────┘   │
    │       │ epoll                                        │ epoll   │
    │  ┌────┴────────────┐                         ┌──────┴──────┐  │
    │  │ C0  C1  L0  ... │                         │ Cm  Ln  Cn  │  │
    │  │ (clients + leafs)│                         │             │  │
    │  └─────────────────┘                         └─────────────┘  │
    └──────────────────────────────┬──────────────────────────────────┘
                                   │ leafnode listener [hub]
                          ┌────────┴───────────────────┐
                          │   Inbound Leaf Connections  │
                          │  (other leaf node servers)  │
                          └────────────────────────────┘

                    ┌──────────┐
          RS+/RS-   │  Node B  │   RS+/RS-
        ┌──────────►│          │◄──────────┐
        │    RMSG   │          │   RMSG    │
        │           └──────────┘           │
   ┌────┴─────┐                      ┌────┴─────┐
   │  Node A  │       RS+/RS-        │  Node C  │
   │          │◄─────────────────────►│          │
   │          │       RMSG           │          │
   └──────────┘                      └──────────┘
        Full-mesh cluster [cluster]
```

Features marked `[leaf]` require the `leaf` Cargo feature; `[hub]` requires
`hub`; `[cluster]` requires `cluster`. `leaf` and `hub` are default-enabled.

## Module Map

| File | Purpose | Key Types | Feature |
|------|---------|-----------|---------|
| `lib.rs` | Public API re-exports | `LeafServer`, `LeafServerConfig` | — |
| `config.rs` | Go nats-server `.conf` file parser | `load_config` | — |
| `server.rs` | Accept loop, worker spawning, shutdown | `LeafServer`, `ServerState` | — |
| `worker.rs` | Per-thread epoll event loop | `Worker`, `ClientState`, `ConnPhase` | — |
| `nats_proto.rs` | Zero-copy protocol parser and message builder | `ClientOp`, `LeafOp`, `MsgBuilder` | `leaf\|hub`* |
| `sub_list.rs` | Subscription storage, wildcard matching | `SubscriptionManager`, `Subscription` | — |
| `msg_writer.rs` | Shared buffer + eventfd cross-worker delivery | `MsgWriter` | — |
| `handler.rs` | Shared handler types and delivery | `ConnCtx`, `MessageDeliveryHub`, `ConnExt` | — |
| `propagation.rs` | Interest propagation (LS+/LS-, RS+/RS-) | propagate functions | — |
| `client_handler.rs` | Client protocol dispatch | `ClientHandler` | — |
| `leaf_handler.rs` | Inbound leaf protocol dispatch | `LeafHandler` | `hub` |
| `leaf_conn.rs` | Leaf connection I/O wrappers | `LeafConn`, `LeafReader`, `LeafWriter` | `leaf` |
| `buf.rs` | Adaptive buffers, connection I/O | `AdaptiveBuf`, `BufConfig`, `ServerConn` | — |
| `upstream.rs` | Hub connection (reader + writer threads) | `Upstream`, `UpstreamCmd` | `leaf` |
| `interest.rs` | Interest collapse + subject mapping pipeline | `InterestPipeline` | `leaf` |
| `route_handler.rs` | Route protocol dispatch (RS+/RS-/RMSG) | `RouteHandler` | `cluster` |
| `route_conn.rs` | Outbound route connection manager | `RouteConnManager` | `cluster` |
| `gateway_handler.rs` | Gateway protocol dispatch | `GatewayHandler` | `gateway` |
| `gateway_conn.rs` | Outbound gateway connection manager | `GatewayConnManager` | `gateway` |
| `websocket.rs` | HTTP upgrade handshake, WS frame codec | — | — |
| `types.rs` | ServerInfo, ConnectInfo, HeaderMap | — | — |

\* These modules are always compiled but individual types/functions within
them are gated behind the indicated features.

## Connection Lifecycle

```
  accept()
     │
     ▼
  SendInfo ──write INFO──► client
     │
     ▼
  WaitConnect ◄──CONNECT── client
     │
     ▼
  Active ◄──── PUB / SUB / UNSUB / PING / PONG
     │
     ▼
  close (EOF / error / shutdown)
```

States are tracked per-connection in `ConnPhase`. The worker adds each new
socket to its epoll instance and manages state transitions inline.

## Message Flow: Local Pub/Sub (Same Worker)

```
  Publisher conn           SubList              Subscriber conn
       │                     │                       │
  PUB "foo" ──EPOLLIN──►    │                       │
       │            for_each_match("foo")            │
       │                     │                       │
       │              MsgWriter::write_msg()      │
       │                     │──► shared buf ────────┤
       │                     │                       │
       │         flush_pending() (no eventfd)        │
       │                     │         socket write ◄┘
```

When publisher and subscriber are on the same worker, `flush_pending()` drains
the subscriber's `MsgWriter` buffer directly — no eventfd wake is needed.

## Message Flow: Cross-Worker Pub/Sub

```
  Worker A                                    Worker B
  ────────                                    ────────
  PUB "foo"                                       │
       │                                          │
  for_each_match()                                │
       │                                          │
  MsgWriter::write_msg()                       │
  (into subscriber's shared buf)                  │
       │                                          │
  accumulate eventfd in pending_notify[]          │
       │                                          │
  flush_notifications() ── eventfd write(1) ──►   │
                                            epoll wakes
                                            scan has_pending
                                            drain buf → socket
```

Eventfd notifications are batched: all PUBs in a single read buffer are
processed first, then one deduplicated eventfd write per remote worker.
See [ADR-005](adr/005-batched-notifications.md).

## Message Flow: Leaf ↔ Hub

```
  Local client        Upstream module           Hub server
       │                    │                       │
  SUB "foo" ──────► add_interest("foo")             │
       │              refcount++ ──► LS+ foo ──────►│
       │                    │                       │
       │                    │◄── LMSG foo payload ──│
       │                    │                       │
       │   for_each_match("foo")                    │
       │◄── MsgWriter ──┘                        │
       │                                            │
  PUB "bar" ────────────────► LMSG bar payload ────►│
```

The upstream module reference-counts local subscriptions. The first SUB sends
`LS+` to the hub; the last UNSUB sends `LS-`. The reader and writer each run
on their own OS thread with blocking I/O.

## Message Flow: Cluster Route (RS+/RS-/RMSG)

```
  Node A                  Route TCP              Node B
  ──────                  ─────────              ──────
  Client SUB "foo"             │                    │
       │                       │                    │
  propagate_route_sub()        │                    │
       │──── RS+ $G foo ──────►│                    │
       │                       │──► insert route    │
       │                       │    Subscription    │
       │                       │    (is_route=true) │
       │                       │                    │
  Client PUB "foo"             │                    │
       │                       │                    │
  deliver_to_subs()            │                    │
       │──► local subs (MSG)   │                    │
       │──► route subs (RMSG)  │                    │
       │              RMSG $G foo payload           │
       │                       │──────────────────►│
       │                       │         deliver_to_subs(skip_routes=true)
       │                       │              local subs only (MSG)
       │                       │         (one-hop: never re-forward to routes)
```

Each node propagates RS+/RS- for local subscription changes to all route peers.
Messages received from a route are delivered only to local client and leaf
subscribers — never re-forwarded to other routes (one-hop rule).

Route connections are managed by `RouteConnManager` (outbound, dedicated threads)
and the worker epoll loop (inbound, multiplexed like client connections).

## Subscription Model

`SubscriptionManager` splits subscriptions into two collections:

- **Exact subjects** — `HashMap<String, Vec<Subscription>>`, O(1) lookup.
- **Wildcard patterns** (`*`, `>`) — `Vec<Subscription>`, linear scan.

`for_each_match(subject, callback)` avoids allocation by invoking a closure
on each match rather than collecting into a `Vec`.

Each `Subscription` holds a `MsgWriter` (defined in `msg_writer.rs`)
that points to the subscriber's shared buffer. Fan-out is lock → memcpy →
unlock per subscriber, with a single eventfd notification per remote worker.

## See Also

- [ADR-001: epoll over Tokio](adr/001-epoll-over-tokio.md)
- [ADR-002: MsgWriter](adr/002-direct-writer.md)
- [ADR-003: Zero-copy parsing](adr/003-zero-copy-parsing.md)
- [ADR-004: Adaptive buffers](adr/004-adaptive-buffers.md)
- [ADR-005: Batched notifications](adr/005-batched-notifications.md)
- [ADR-010: Full-mesh clustering](adr/010-full-mesh-clustering.md)
