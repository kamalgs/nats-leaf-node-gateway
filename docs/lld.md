# Low-Level Design — open-wire

Interactive diagrams using Mermaid. Each diagram links types and methods
to source locations (`file:line`) so you can jump straight to the code.

---

## Table of Contents

1. [Type Relationships](#1-type-relationships)
2. [Connection Lifecycle State Machine](#2-connection-lifecycle-state-machine)
3. [Client PUB → Delivery → Socket](#3-client-pub--delivery--socket)
4. [Route RMSG → Local Delivery](#4-route-rmsg--local-delivery)
5. [Client SUB → Interest Propagation](#5-client-sub--interest-propagation)
6. [Upstream Leaf Connection Flow](#6-upstream-leaf-connection-flow)
7. [Worker Event Loop](#7-worker-event-loop)
8. [DirectWriter Cross-Worker Delivery](#8-directwriter-cross-worker-delivery)
9. [Inbound Leaf Handshake](#9-inbound-leaf-handshake)
10. [Inbound Route Handshake](#10-inbound-route-handshake)
11. [Gateway Interest Modes](#11-gateway-interest-modes)

---

## 1. Type Relationships

Core structs and their ownership/reference relationships.

```mermaid
classDiagram
    class LeafServer {
        +config: LeafServerConfig
        +run()
        %%  server.rs:1432
    }

    class ServerState {
        +sub_list: SubList
        +route_writers: RwLock~HashMap~
        +gateway_writers: RwLock~HashMap~
        +upstream_txs: Mutex~Vec~
        %%  server.rs:1094
    }

    class Worker {
        +state: Arc~ServerState~
        +clients: HashMap~u64, ClientState~
        +reactor: R
        +run()
        %%  worker.rs:275
    }

    class ClientState {
        +conn_id: u64
        +phase: ConnPhase
        +ext: ConnExt
        +write_buf: BytesMut
        +direct_writer: DirectWriter
        +subs: HashMap~u64, Subscription~
        %%  worker.rs:218
    }

    class ConnExt {
        <<enum>>
        Client
        Leaf(leaf_sid_counter, leaf_sids)
        Route(route_sid_counter, route_sids)
        Gateway(gateway_sid_counter, gateway_sids)
        +kind_tag() ConnKind
        +is_leaf() bool
        +is_route() bool
        +is_gateway() bool
        %%  handler/conn.rs:67
    }

    class ConnCtx {
        +conn_id: u64
        +write_buf: &mut BytesMut
        +direct_writer: &DirectWriter
        +echo: bool
        +no_responders: bool
        +permissions: &Option~Permissions~
        +ext: &mut ConnExt
        +draining: bool
        %%  handler/conn.rs:46
    }

    class WorkerCtx {
        +state: &Arc~ServerState~
        +dirty_eventfds: &mut Vec~RawFd~
        +publish()
        +record_delivery()
        +queue_notify()
        %%  handler/delivery.rs:31
    }

    class SubList {
        +exact: HashMap~Bytes, Vec~Subscription~~
        +wildcards: WildTrie
        +insert()
        +remove()
        +for_each_match()
        %%  sub_list.rs:375
    }

    class Subscription {
        +conn_id: u64
        +sid: u64
        +subject: Bytes
        +queue: Option~Bytes~
        +writer: DirectWriter
        +is_leaf: bool
        +is_route: bool
        +is_gateway: bool
        %%  sub_list.rs:10
    }

    class DirectWriter {
        +buf: Arc~Mutex~BytesMut~~
        +has_pending: Arc~AtomicBool~
        +event_fd: Arc~OwnedFd~
        +write_msg()
        +write_lmsg()
        +write_rmsg()
        +drain()
        %%  direct_writer.rs:37
    }

    class Msg {
        +subject: &Bytes
        +reply: Option~&Bytes~
        +header: Option~&[u8]~
        +payload: &[u8]
        %%  handler/delivery.rs:75
    }

    class DeliveryScope {
        +skip_routes: bool
        +skip_gateways: bool
        +skip_leafs: bool
        +skip_echo: bool
        +local()
        +from_route()
        +from_gateway()
        %%  handler/delivery.rs:104
    }

    LeafServer --> ServerState : creates
    ServerState --> SubList : owns
    Worker --> ServerState : Arc ref
    Worker --> ClientState : owns many
    ClientState --> ConnExt : owns
    ClientState --> DirectWriter : owns
    ConnCtx --> ConnExt : borrows
    ConnCtx --> DirectWriter : borrows
    WorkerCtx --> ServerState : borrows
    Subscription --> DirectWriter : clones
    SubList --> Subscription : stores
```

---

## 2. Connection Lifecycle State Machine

Every inbound connection (client, leaf, route, gateway) follows this
state machine in the worker event loop.

> [`ConnPhase`](../src/worker.rs#L178) — `worker.rs:178`

```mermaid
stateDiagram-v2
    [*] --> SendInfo : accept() → register fd
    SendInfo --> WaitConnect : INFO written to write_buf
    WaitConnect --> Active : valid CONNECT received

    state Active {
        [*] --> Processing
        Processing --> Processing : parse_op → handle_op
        Processing --> Flushing : HandleResult::Flush
        Flushing --> Processing : write_buf flushed
    }

    Active --> Draining : server shutdown / drain cmd
    Draining --> [*] : write_buf empty, close fd

    WaitConnect --> [*] : invalid CONNECT / timeout
    Active --> [*] : protocol error (HandleResult::Disconnect)
    Active --> [*] : I/O error / EOF

    note right of SendInfo
        worker.rs — handle_writable()
        Writes INFO JSON to socket
    end note

    note right of WaitConnect
        worker.rs — process_wait_connect()
        Parses CONNECT, validates auth
        Sets echo, no_responders, etc.
    end note

    note right of Active
        worker.rs — process_active()
        Calls H::parse_op() + H::handle_op()
        via ConnectionHandler trait
        handler/conn.rs:25
    end note
```

---

## 3. Client PUB → Delivery → Socket

The most common flow: a client publishes a message that reaches subscribers.

```mermaid
sequenceDiagram
    participant C as Client Socket
    participant W as Worker<br/>worker.rs:275
    participant CH as ClientHandler<br/>client_handler.rs:20
    participant WCtx as WorkerCtx<br/>handler/delivery.rs:31
    participant SL as SubList<br/>sub_list.rs:375
    participant DW as DirectWriter<br/>direct_writer.rs:37
    participant S as Subscriber Socket

    C->>W: PUB subject reply payload
    W->>W: process_active<ClientHandler>()
    W->>CH: parse_op(buf) → ClientOp::Pub
    Note over CH: nats_proto.rs:153<br/>try_parse_client_op()

    W->>CH: handle_op(conn, wctx, op)
    CH->>CH: handle_pub()<br/>client_handler.rs:281
    CH->>CH: check permissions, payload size
    CH->>CH: Msg::new(subject, reply, hdr, payload)<br/>handler/delivery.rs:75

    CH->>WCtx: publish(&msg, scope, conn_id, ...)<br/>handler/delivery.rs:162
    WCtx->>WCtx: deliver_to_subs(wctx, &msg, ...)<br/>handler/delivery.rs:331

    WCtx->>SL: for_each_match(subject, callback)<br/>sub_list.rs:495
    SL-->>WCtx: matching Subscription[]

    loop Each matching sub
        WCtx->>WCtx: deliver_to_sub_inner(sub, &msg)<br/>handler/delivery.rs:210
        WCtx->>DW: sub.writer.write_msg(sid, subject, reply, hdr, payload)<br/>direct_writer.rs:74
        DW->>DW: lock buf, append MSG bytes, set has_pending=true
        WCtx->>WCtx: record_delivery(len)<br/>queue_notify(event_fd)
    end

    Note over WCtx: Also calls:<br/>forward_to_optimistic_gateways()<br/>deliver_cross_account()<br/>handler/delivery.rs:590, 642

    CH->>CH: forward_to_upstream(&msg)<br/>handler/delivery.rs:448
    Note over CH: mpsc::Sender → upstream thread

    W->>W: batch: deduplicate dirty_eventfds
    W->>DW: eventfd write(1) per unique worker
    DW-->>S: Worker wakes → drain() → socket write
```

---

## 4. Route RMSG → Local Delivery

A message arriving from a route peer is delivered to local clients only
(one-hop enforcement).

```mermaid
sequenceDiagram
    participant R as Route Peer Socket
    participant W as Worker<br/>worker.rs:275
    participant RH as RouteHandler<br/>route_handler.rs:21
    participant WCtx as WorkerCtx<br/>handler/delivery.rs:31
    participant SL as SubList<br/>sub_list.rs:375
    participant DW as DirectWriter<br/>direct_writer.rs:37

    R->>W: RMSG $G subject reply payload
    W->>W: process_active<RouteHandler>()
    W->>RH: parse_op(buf) → RouteOp::Rmsg
    Note over RH: nats_proto.rs:838

    W->>RH: handle_op(conn, wctx, op)
    RH->>RH: handle_rmsg()<br/>route_handler.rs

    RH->>RH: Msg::new(subject, reply, hdr, payload)
    RH->>WCtx: publish(&msg, DeliveryScope::from_route(), ...)<br/>handler/delivery.rs:162

    Note over WCtx: DeliveryScope::from_route()<br/>skip_routes=true, skip_gateways=false<br/>handler/delivery.rs:104

    WCtx->>SL: for_each_match(subject, callback)

    loop Each matching sub
        alt sub.is_route == true
            WCtx->>WCtx: SKIP (one-hop rule)
        else sub.is_route == false
            WCtx->>DW: write_msg() or write_lmsg()
        end
    end

    Note over WCtx: forward_to_optimistic_gateways()<br/>Forwards to gateway peers
```

---

## 5. Client SUB → Interest Propagation

When a client subscribes, interest is propagated to upstream, routes,
leafs, and gateways.

```mermaid
sequenceDiagram
    participant C as Client Socket
    participant CH as ClientHandler<br/>client_handler.rs:20
    participant SL as SubList<br/>sub_list.rs:375
    participant US as Upstream<br/>upstream.rs:76
    participant P as Propagation<br/>propagation.rs
    participant LW as Leaf Writers
    participant RW as Route Writers
    participant GW as Gateway Writers

    C->>CH: SUB subject [queue] sid
    CH->>CH: handle_sub()<br/>client_handler.rs:72
    CH->>CH: create Subscription<br/>sub_list.rs:10
    CH->>SL: insert(subscription)<br/>sub_list.rs:389

    CH->>US: UpstreamCmd::AddInterest(subject, queue)<br/>upstream.rs:25
    Note over US: InterestPipeline applies<br/>subject-mapping + collapse<br/>interest.rs:131

    CH->>P: propagate_all_interest(subject, queue, is_sub=true)<br/>propagation.rs:279

    par Leaf propagation
        P->>LW: propagate_leaf_interest()<br/>propagation.rs:20
        Note over LW: LS+ subject [queue]\r\n
    and Route propagation
        P->>RW: propagate_route_interest()<br/>propagation.rs:107
        Note over RW: RS+ $G subject [queue]\r\n
    and Gateway propagation
        P->>GW: propagate_gateway_interest()
        Note over GW: RS+ $G subject [queue]\r\n
    end
```

---

## 6. Upstream Leaf Connection Flow

The upstream module connects to a hub server using the leaf node protocol.

> [`Upstream`](../src/upstream.rs#L76) — `upstream.rs:76`
> [`LeafConn`](../src/leaf_conn.rs#L98) — `leaf_conn.rs:98`

```mermaid
sequenceDiagram
    participant W as Workers
    participant U as Upstream<br/>upstream.rs:76
    participant LC as LeafConn<br/>leaf_conn.rs:98
    participant LR as LeafReader<br/>leaf_conn.rs:261
    participant LW as LeafWriter<br/>leaf_conn.rs:294
    participant Hub as Hub Server

    U->>LC: LeafConn::connect(addr, tls)<br/>leaf_conn.rs:98
    LC->>Hub: TCP connect + optional TLS
    Hub-->>LC: INFO {...}
    LC->>Hub: CONNECT {...}
    LC->>Hub: PING
    Hub-->>LC: PONG
    Note over LC: Connection established

    U->>LR: spawn reader thread
    U->>LW: spawn writer thread

    par Reader loop
        Hub->>LR: LMSG / PING / -ERR
        LR->>LR: parse leaf ops<br/>nats_proto.rs:546 (LeafOp)
        LR->>W: deliver_to_subs_upstream()<br/>handler/delivery.rs
        LR->>LW: PONG (via mpsc)
    and Writer loop
        W->>U: UpstreamCmd via mpsc<br/>upstream.rs:25
        U->>LW: SUB/UNSUB/PUB commands
        LW->>Hub: LS+ / LS- / LMSG
    end

    Note over LR: On disconnect:<br/>UpstreamCmd::Shutdown<br/>Upstream reconnects
```

---

## 7. Worker Event Loop

Each worker thread runs a tight epoll loop processing socket events
and cross-worker notifications.

> [`Worker::run()`](../src/worker.rs#L275) — `worker.rs:275`

```mermaid
stateDiagram-v2
    [*] --> EpollWait

    EpollWait --> HandleEvent : event ready

    state HandleEvent {
        [*] --> CheckFd
        CheckFd --> EventFd : fd == eventfd
        CheckFd --> ListenerFd : fd == listener
        CheckFd --> ClientFd : fd == client socket

        state EventFd {
            [*] --> ReadEventFd
            ReadEventFd --> ScanPending
            ScanPending --> DrainDirectBuf : has_pending == true
            DrainDirectBuf --> WriteSocket
        }
        note right of EventFd
            handle_eventfd()
            worker.rs:476
            Scans ALL conns for pending
            direct_writer data
        end note

        state ClientFd {
            [*] --> CheckPhase
            CheckPhase --> ProcessSendInfo : SendInfo
            CheckPhase --> ProcessWaitConnect : WaitConnect
            CheckPhase --> ProcessActive : Active

            state ProcessActive {
                [*] --> ReadLoop
                ReadLoop --> ParseOp : data available
                ParseOp --> HandleOp : H::parse_op()
                HandleOp --> ReadLoop : HandleResult::Ok
                HandleOp --> FlushAndRead : HandleResult::Flush
                HandleOp --> Disconnect : HandleResult::Disconnect
                ReadLoop --> [*] : WouldBlock
            }
        }
        note right of ClientFd
            process_active<H>()
            worker.rs:2152
            Generic over ConnectionHandler
            handler/conn.rs:25
        end note
    }

    HandleEvent --> FlushPending : all events processed
    note right of FlushPending
        flush_pending()
        worker.rs:809
        Same-worker delivery bypass:
        skip eventfd round-trip
    end note
    FlushPending --> EpollWait
```

---

## 8. DirectWriter Cross-Worker Delivery

How messages cross worker boundaries using shared buffers and eventfd.

> [`DirectWriter`](../src/direct_writer.rs#L37) — `direct_writer.rs:37`

```mermaid
sequenceDiagram
    participant PW as Publisher Worker
    participant DW as DirectWriter<br/>direct_writer.rs:37
    participant EFD as eventfd (kernel)
    participant SW as Subscriber Worker
    participant SS as Subscriber Socket

    Note over PW: Processing PUB from client

    PW->>DW: write_msg(sid, subject, reply, hdr, payload)<br/>direct_writer.rs:74
    DW->>DW: lock buf (Arc<Mutex<BytesMut>>)
    DW->>DW: append MSG wire bytes
    DW->>DW: set has_pending = true (AtomicBool)
    DW->>DW: unlock buf

    PW->>PW: queue_notify(event_fd)<br/>handler/delivery.rs — WorkerCtx
    Note over PW: Dedup: collect unique eventfds<br/>across all deliveries in batch

    PW->>EFD: write(1) — one per unique worker
    Note over EFD: Only 1 syscall even if<br/>100 subs on same worker

    EFD-->>SW: epoll_wait returns eventfd ready
    SW->>SW: handle_eventfd()<br/>worker.rs:476

    loop Each conn on this worker
        SW->>DW: check has_pending (AtomicBool)
        alt has_pending == true
            SW->>DW: drain()<br/>direct_writer.rs:157
            DW->>DW: lock buf, split_to(len)
            DW-->>SW: Bytes chunk
            SW->>SS: write(chunk) to socket
        end
    end
```

---

## 9. Inbound Leaf Handshake

Hub accepting a leaf node connection.

```mermaid
sequenceDiagram
    participant Leaf as Leaf Node
    participant A as Acceptor<br/>server.rs:1699
    participant W as Worker<br/>worker.rs:275

    Leaf->>A: TCP connect to leafnode_port
    A->>W: WorkerCmd::NewLeafConn(fd)
    W->>W: register fd, phase = SendInfo

    W->>Leaf: INFO {"server_id": "...", "leafnode": true}
    W->>W: phase = WaitConnect

    Leaf->>W: CONNECT {"verbose": false, ...}
    W->>W: validate auth, create ConnExt::Leaf
    Note over W: ConnExt::Leaf {<br/>  leaf_sid_counter: 0,<br/>  leaf_sids: HashMap::new()<br/>}<br/>handler/conn.rs:72

    W->>Leaf: PING
    Leaf->>W: PONG
    W->>W: phase = Active

    Note over W: send_existing_subs()<br/>propagation.rs<br/>Sends LS+ for all local subs

    loop Active
        Leaf->>W: LS+ subject / LS- subject / LMSG / PING
        W->>W: process_active<LeafHandler>()
        Note over W: LeafHandler<br/>leaf_handler.rs:22
    end
```

---

## 10. Inbound Route Handshake

Cluster peer connecting via route protocol.

```mermaid
sequenceDiagram
    participant Peer as Route Peer
    participant A as Acceptor<br/>server.rs:1699
    participant W as Worker<br/>worker.rs:275

    Peer->>A: TCP connect to cluster_port
    A->>W: WorkerCmd::NewRouteConn(fd)
    W->>W: register fd, phase = SendInfo

    W->>Peer: INFO {"server_id": "...", "cluster": "..."}
    W->>W: phase = WaitConnect

    Peer->>W: INFO {"server_id": "peer-id", ...}
    Note over W: Skip peer INFO (already have it)
    Peer->>W: CONNECT {"verbose": false, ...}
    W->>W: validate, create ConnExt::Route
    Note over W: ConnExt::Route {<br/>  route_sid_counter: 0,<br/>  route_sids: HashMap::new(),<br/>  peer_server_id: Some("peer-id")<br/>}<br/>handler/conn.rs:78

    W->>Peer: PONG
    W->>W: phase = Active

    Note over W: send_existing_route_subs()<br/>propagation.rs<br/>Sends RS+ for all local subs

    loop Active
        Peer->>W: RS+ $G subject / RS- $G subject / RMSG / PING
        W->>W: process_active<RouteHandler>()
        Note over W: RouteHandler<br/>route_handler.rs:21
    end
```

---

## 11. Gateway Interest Modes

Gateways use two interest modes to optimize cross-cluster traffic.

```mermaid
stateDiagram-v2
    [*] --> Optimistic : initial state

    state Optimistic {
        [*] --> ForwardAll
        ForwardAll : Forward all messages to peer gateway
        ForwardAll : No per-subject filtering
        ForwardAll --> TrackNoInterest : peer sends RS- (no interest)
        TrackNoInterest : Build deny list of subjects
        TrackNoInterest : Skip subjects in deny list
    }

    Optimistic --> InterestOnly : too many RS- received
    note right of Optimistic
        Gateway starts optimistic:
        sends all messages, learns
        what peer doesn't want.
        gateway_conn.rs / gateway_handler.rs
    end note

    state InterestOnly {
        [*] --> WaitForInterest
        WaitForInterest : Only forward subjects with RS+
        WaitForInterest : Peer sends RS+ for wanted subjects
    }

    note right of InterestOnly
        After threshold, switch to
        interest-only: only forward
        messages peer explicitly wants.
    end note
```

---

## Source File Index

Quick reference linking diagram elements to source code.

| Type / Function | File | Line |
|---|---|---|
| `LeafServer` | `server.rs` | 1432 |
| `ServerState` | `server.rs` | 1094 |
| `Worker` | `worker.rs` | 275 |
| `ClientState` | `worker.rs` | 218 |
| `ConnPhase` | `worker.rs` | 178 |
| `process_active()` | `worker.rs` | 2152 |
| `flush_pending()` | `worker.rs` | 809 |
| `handle_eventfd()` | `worker.rs` | 476 |
| `ConnectionHandler` | `handler/conn.rs` | 25 |
| `ConnCtx` | `handler/conn.rs` | 46 |
| `ConnExt` | `handler/conn.rs` | 67 |
| `ConnKind` | `handler/conn.rs` | 97 |
| `HandleResult` | `handler/conn.rs` | 179 |
| `WorkerCtx` | `handler/delivery.rs` | 31 |
| `Msg` | `handler/delivery.rs` | 75 |
| `DeliveryScope` | `handler/delivery.rs` | 104 |
| `WorkerCtx::publish()` | `handler/delivery.rs` | 162 |
| `deliver_to_sub_inner()` | `handler/delivery.rs` | 210 |
| `deliver_to_subs_core()` | `handler/delivery.rs` | 258 |
| `deliver_to_subs()` | `handler/delivery.rs` | 331 |
| `forward_to_upstream()` | `handler/delivery.rs` | 448 |
| `handle_expired_subs()` | `handler/delivery.rs` | 503 |
| `forward_to_optimistic_gateways()` | `handler/delivery.rs` | 590 |
| `deliver_cross_account()` | `handler/delivery.rs` | 642 |
| `SubList` | `sub_list.rs` | 375 |
| `Subscription` | `sub_list.rs` | 10 |
| `SubList::insert()` | `sub_list.rs` | 389 |
| `SubList::remove()` | `sub_list.rs` | 397 |
| `SubList::for_each_match()` | `sub_list.rs` | 495 |
| `DirectWriter` | `direct_writer.rs` | 37 |
| `DirectWriter::write_msg()` | `direct_writer.rs` | 74 |
| `DirectWriter::write_lmsg()` | `direct_writer.rs` | 92 |
| `DirectWriter::write_rmsg()` | `direct_writer.rs` | 109 |
| `DirectWriter::drain()` | `direct_writer.rs` | 157 |
| `ClientOp` | `nats_proto.rs` | 153 |
| `LeafOp` | `nats_proto.rs` | 546 |
| `RouteOp` | `nats_proto.rs` | 838 |
| `MsgBuilder` | `nats_proto.rs` | 1194 |
| `try_parse_client_op()` | `nats_proto.rs` | 182 |
| `ClientHandler` | `client_handler.rs` | 20 |
| `handle_sub()` | `client_handler.rs` | 72 |
| `handle_pub()` | `client_handler.rs` | 281 |
| `LeafHandler` | `leaf_handler.rs` | 22 |
| `RouteHandler` | `route_handler.rs` | 21 |
| `GatewayHandler` | `gateway_handler.rs` | 23 |
| `Upstream` | `upstream.rs` | 76 |
| `UpstreamCmd` | `upstream.rs` | 25 |
| `LeafConn` | `leaf_conn.rs` | 98 |
| `LeafReader` | `leaf_conn.rs` | 261 |
| `LeafWriter` | `leaf_conn.rs` | 294 |
| `InterestPipeline` | `interest.rs` | 131 |
| `propagate_all_interest()` | `propagation.rs` | 279 |
| `propagate_leaf_interest()` | `propagation.rs` | 20 |
| `propagate_route_interest()` | `propagation.rs` | 107 |
