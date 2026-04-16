# ADR-012: Sharded Workers with In-Process Routing

**Status:** Proposed
**Date:** 2026-04-16
**Branch:** `feat/sharded-workers` (to be cut)

## Context

The current N-worker design pins every connection to one worker thread. When
a PUB arrives on worker A, `deliver_to_subs` iterates every matching sub in
the shared `SubList` and writes each one's `direct_buf` on worker A's
thread. Subs owned by other workers get their buffers written cross-thread;
the owning worker later wakes on an eventfd and flushes its buffers to the
sockets.

This cleanly separates **dispatch** (publishing thread) from **drain**
(owning thread), but the *dispatch* cost scales with total fan-out while
*drain* cost scales with local sub count. On any non-trivial workload the
dispatch cost is the dominant one:

- Parse the PUB frame
- Walk the shared `SubList` trie to find matching subs
- For each match: RMSG/MSG encode into the sub's `direct_buf`, set
  `has_pending`, batch an eventfd notify — most of this touches cache lines
  owned by the *peer* worker, causing cache line bouncing

At high fan-out, this puts all the work on the publishing worker's single
core. Other workers sit near-idle waiting to drain. We diagnosed and
measured this exact pattern:

- AWS mini sweep (2026-04-14): at 2000 users / 500 symbols, hub-0 was at
  22.5% CPU and hub-1 at 56.0% — 2.5× skew across two identically-sized
  nodes running identical workloads. The NLB distributed connections
  roughly evenly; the skew came from *which* connection happened to be
  the publisher, because all fan-out ran on that one worker.
- Local 4-shard vs 1-instance-`--workers 4` spike (2026-04-15): baseline
  stalled at 1.28 cores despite 4 workers on 6 available cores; spike
  (4 single-worker instances in loopback mesh) used 2.72 cores and
  delivered 2× more messages at the same bench-client load.
- AWS mini 2-shard-per-hub spike (2026-04-15): identical throughput to
  the baseline (347 vs 349 K rx/s) but **p99 dropped from 162 ms to
  24 ms** — a 6.7× improvement. Per-shard CPU balanced within 5%
  across all 4 shards.

ADR-011 built an in-process `ShardBus` and rejected it. The two reasons it
was rejected:

1. **Silent drops** — the bounded channel discarded messages when a peer
   couldn't drain fast enough, with no backpressure to the publisher.
2. **"The use case is already covered"** — the claim at the time was that
   the N-worker model already used multiple cores. It does for draining;
   it does *not* for dispatch at high fan-out, which is what the recent
   measurements exposed.

This ADR supersedes ADR-011 for reason #2 (the use case is *not* covered)
and proposes a concrete fix for reason #1 (backpressure, not drops).

## Decision

Each worker becomes an independent shard with its own `SubList`. Cross-worker
message delivery uses an **in-process bus** with proper backpressure wired
into the existing `read_budget` congestion path.

### Core shape

```
  Worker 0            Worker 1            Worker 2            Worker 3
  ┌───────────┐       ┌───────────┐       ┌───────────┐       ┌───────────┐
  │ SubList 0 │       │ SubList 1 │       │ SubList 2 │       │ SubList 3 │
  │ conns 0   │       │ conns 1   │       │ conns 2   │       │ conns 3   │
  │ inbox ────┼─◄─────┤           │       │           │       │           │
  │           │       │ inbox ────┼─◄─────┤           │       │           │
  │           │       │           │       │ inbox ────┼─◄─────┤           │
  │           │       │           │       │           │       │ inbox ◄───┤
  └───────────┘       └───────────┘       └───────────┘       └───────────┘
       ▲                   ▲                   ▲                   ▲
       └───────────────────┴───── WorkerInterest ────────────────────┘
                       subject → {worker bitmask}
```

- **Per-worker `SubList`** owns only the subs whose connections live on
  that worker. No more global `RwLock<SubList>` read on every PUB.
- **Per-worker inbox** — bounded MPSC queue carrying `Msg` descriptors
  from other workers. One producer per remote worker, one consumer
  (the owning worker's epoll loop).
- **`WorkerInterest`** — shared read-mostly structure mapping
  `subject → bitmask_of_workers_with_interest`. Updated on SUB/UNSUB by
  the owning worker, read on every PUB by all workers. Same shape as the
  existing `cluster.local_sub_counts` refcount.
- **PUB hot path** on worker P:
  1. Parse the PUB frame.
  2. Look up subject in `WorkerInterest` → get `mask: u64` of interested
     workers.
  3. For each bit set in `mask`, push the `Msg` (refcounted `Bytes`, no
     copy) into that worker's inbox. Skip self-write (still uses
     `direct_buf` on the publishing worker).
  4. One batched eventfd notify per affected worker.
  Cost is **O(popcount(mask))** — constant per PUB regardless of how
  many subs match.
- **Receive path** on worker W:
  1. Drain inbox → for each `Msg`, walk its local `SubList` → write to
     local `direct_buf` → writev to socket.
  2. Cache-hot throughout; every write hits memory owned by this worker.

### Backpressure — the ADR-011 fix

The bounded inbox provides a natural congestion signal. When worker W's
inbox fills:

1. Worker W sets a per-producer `congestion` atomic (`AtomicU8`, 0/1/2)
   indexed by producer worker. Same shape as the existing route
   `MsgWriter::congestion` field we already use for route backpressure.
2. The producing worker (say P) reads this atomic on its next PUB. If
   any receiving worker is at level 1 (soft) or 2 (hard), P sets
   `wctx.route_congested = true`.
3. Worker P's read loop sees `route_congested` on its `ClientState`
   and shrinks the publishing connection's `read_budget` (the existing
   `max(256, budget / 2)` path from `src/core/worker.rs`).
4. Reduced read budget → kernel TCP recv buffer fills → publisher's
   TCP window closes → publisher slows down naturally. **No
   application-layer backpressure messages, no silent drops.**

This is the same chain that handles route backpressure today (commit
`16b8ca4`, ADR from the remote-interest-tracking work). We just add a
new congestion source: peer-worker inbox pressure, instead of only peer-
node route buffer pressure.

### Interest propagation

Every existing `propagate_route_interest` call already refcounts through
`state.cluster.local_sub_counts`. We add a parallel
`state.worker_interest` that serves the same role for intra-process
sharding:

```rust
pub struct WorkerInterest {
    // Exact subjects: subject -> (mask: AtomicU64, refcount: AtomicU32)
    exact: RwLock<FxHashMap<Bytes, WorkerInterestEntry>>,
    // Wildcard subjects: kept in a second per-subject map, checked on the
    // slow path in deliver_to_subs_core. Tree of patterns by token count.
    wildcards: RwLock<WildWorkerInterest>,
}

struct WorkerInterestEntry {
    // Bit i set iff worker i has at least one sub matching this subject.
    mask: AtomicU64,
    // Refcount per worker; only toggles `mask` bit at 0→1 / 1→0 transitions.
    per_worker_count: [AtomicU32; MAX_WORKERS],
}
```

On SUB on worker W:

1. W inserts the Sub into its own private `SubList`.
2. W does `per_worker_count[W] += 1`. If 0 → 1, W sets bit `W` in `mask`.
3. No broadcast message — `mask` is a shared atomic.

On UNSUB on W: reverse the above.

On PUB on P: one `mask.load(Relaxed)` per subject lookup, plus a wildcard
walk for non-exact matches. The mask fast-path is O(1).

### Fast paths we preserve

- **Same-worker PUB → same-worker sub**: stays an in-line
  `direct_buf` write. Inbox is only used for cross-worker dispatch.
- **Local delivery batching** (the existing `flush_pending` that writes
  pending `direct_buf`s to sockets after each epoll wake) is unchanged.
- **Eventfd notification batching** (`queue_notify` / `flush_notifications`)
  is unchanged — we just extend it to also cover the new inbox-push path.

### Selection

Build flag: default on. A `--disable-worker-sharding` runtime flag reverts
to the legacy "shared `SubList` + cross-worker `direct_buf` writes" path
for A/B comparison and rollback. Both paths live in the tree until the
new path is validated on mini and full envs.

## What Changes

### `src/core/server.rs`

Add `worker_interest: WorkerInterest` to `ServerState`. Thread it through
`ClusterState` / `GatewayState`-style (shared, read-mostly).

Replace the single `state.subs: RwLock<SubList>` with
`state.subs: [RwLock<SubList>; MAX_WORKERS]`, indexed by worker id, plus
a convenience accessor `state.subs_for_worker(w)`. Access pattern shifts
from "everyone reads the global list" to "each worker reads its own, and
`for_each_worker` iterates over all lists during bulk operations like
metrics or shutdown".

(Feature-gating: the `accounts` feature today has its own
`account_subs: Vec<RwLock<SubList>>`. The per-worker split layers cleanly
— we end up with `Vec<[RwLock<SubList>; MAX_WORKERS]>` indexed by account
then worker. Not beautiful, but mechanical.)

### `src/pubsub/sub_list.rs`

No changes to the SubList implementation itself. SubList already holds
exact + wildcard subs and exposes `for_each_match`. We're just going to
have N of them.

Add `WorkerInterest` as a new type next to `SubList` (or in a new
`worker_interest.rs`), with `insert(subject, worker_id)`,
`remove(subject, worker_id)`, and `matching_workers(subject) -> u64`.

### `src/core/worker.rs`

**Biggest touch.** Worker gets:

1. `inbox: mpsc::Receiver<InboxMsg>` (or `crossbeam::channel::Receiver` —
   we want a fixed-size bounded MPSC). Drained in the main epoll loop
   alongside the eventfd wake.
2. `inbox_producers: [mpsc::Sender<InboxMsg>; MAX_WORKERS]` — one per
   peer worker, shared via `Arc`. Cross-worker dispatch pushes here.
3. `inbox_congestion: Arc<AtomicU8>` (per this worker, read by
   producers).
4. Updated `handle_eventfd` / `process_read_buf`:
   - Replace `state.subs.read()` access with
     `state.subs_for_worker(self.worker_index)`.
   - For cross-worker delivery, call a new
     `dispatch_cross_worker(&msg, target_mask)` helper that pushes to
     the inbox and applies backpressure.

The new dispatch helper is the minimal new code:

```rust
fn dispatch_cross_worker(
    producers: &[Sender<InboxMsg>],
    inbox_congestion: &[Arc<AtomicU8>; N],
    route_congested: &mut bool,
    msg: &Msg<'_>,
    mask: u64,
) -> usize {
    let mut delivered = 0;
    let mut m = mask;
    while m != 0 {
        let i = m.trailing_zeros() as usize;
        m &= m - 1;
        match producers[i].try_send(InboxMsg::from(msg)) {
            Ok(_) => delivered += 1,
            Err(TrySendError::Full(_)) => {
                inbox_congestion[i].store(2, Ordering::Relaxed); // hard
                *route_congested = true;
            }
            Err(TrySendError::Disconnected(_)) => {}
        }
        // soft congestion hint based on channel fill ratio — checked on
        // the producer side, set from the consumer's drain loop.
        if inbox_congestion[i].load(Ordering::Relaxed) >= 1 {
            *route_congested = true;
        }
    }
    delivered
}
```

Drain loop on the consumer side updates `inbox_congestion[own_idx]`
based on queue fill ratio after each batch drain (25%/75% thresholds —
same logic as the route `MsgWriter::congestion` set by the drainer).

### `src/handler/delivery.rs`

`deliver_to_subs_core` grows a worker-aware code path:

```rust
// Current logic: iterate SubList, write to each matching sub.
// New logic:
// 1. mask = state.worker_interest.matching_workers(msg.subject_str())
// 2. local_bit = 1 << current_worker_idx
// 3. If local_bit & mask != 0:
//      iterate own SubList, deliver to local subs via direct_buf (as today).
// 4. For each other worker bit in mask:
//      dispatch_cross_worker(&msg, bit)
// 5. Return delivered count.
```

`deliver_to_subs_upstream_inner` (called from route/leaf readers, which
aren't tied to a specific worker thread) stays as a "broadcast to all
workers" fallback — it doesn't know which worker it's "on", so it uses
the mask directly and dispatches via inbox to every interested worker.

`DeliveryScope::from_route` keeps its one-hop semantics. The flag `skip_routes` now also implicitly means "skip re-forwarding across the worker bus" — route inbound messages should be cross-worker-dispatched but not re-routed outbound. This is trivially handled by the existing `skip_routes` bit.

### `src/handler/propagation.rs`

Add `WorkerInterest` maintenance to the SUB/UNSUB path. Today's
`propagate_route_interest` does the refcount + RS+/RS- writes to route
peers. We add a parallel block that updates `state.worker_interest`
before (or after) route propagation. Both use the same refcount pattern.

The client-handler `handle_sub` / `handle_unsub` changes are:

```rust
// On SUB, BEFORE inserting to SubList:
state.worker_interest.insert(&subject, worker_idx);

// On UNSUB, AFTER removing from SubList:
state.worker_interest.remove(&subject, worker_idx);
```

No new call sites — every existing call to `propagate_all_interest`
already hits the right path. We just add one more thing that propagation
propagates to.

### `src/io/msg_writer.rs`

No changes. `MsgWriter` is still the per-connection cross-thread writer
for drains. Cross-worker fan-out no longer goes through a peer
connection's `MsgWriter`; it goes through the inbox.

### `src/connector/mesh/handler.rs`

No changes. Route messages still arrive via `deliver_to_subs_upstream_inner`,
which fans out via the new worker-interest mask.

### `src/bin/bench.rs`, `tests/`

No functional changes. The benchmark tool and e2e tests hit the client
protocol the same way. We expect throughput and latency to improve;
test assertions don't change.

### Feature flag

Add a runtime flag: `ServerConfig::enable_worker_sharding: bool`
(default `true` once the new path is stable; `false` during the
transition). The `Server::run()` method picks between the legacy shared
`SubList` path and the new per-worker sharded path based on this flag.

## Implementation Order

1. **Add `WorkerInterest` type** next to `SubList`. Unit tests for
   `insert` / `remove` / `matching_workers` + bitmask math.
2. **Thread it through `ServerState`** — no behavior change yet, just
   the new field and constructor wiring. `cargo check`, tests pass.
3. **Split `state.subs` into `[SubList; MAX_WORKERS]`**, gated behind
   the new feature flag. Behind the flag, client SUB/UNSUB / route
   interest propagation write to the appropriate per-worker list. PUB
   delivery still uses the legacy broadcast-to-everyone path — so the
   only difference at this step is where subs are stored. Tests must
   still pass.
4. **Add the inbox MPSC per worker** and the
   `dispatch_cross_worker` helper. Still unused.
5. **Switch `deliver_to_subs_core`** (behind the flag) to:
   - Read the worker interest mask.
   - Deliver to self via in-line `direct_buf` writes (as today).
   - Dispatch to other workers via the new inbox path.
6. **Drain the inbox** in the worker epoll loop and deliver to local
   subs. E2E test: single-publisher, subs scattered across workers,
   verify everyone receives.
7. **Wire backpressure**: inbox fill ratio → `inbox_congestion` atomic
   → producer-side `route_congested` flag → `read_budget` shrink. Unit
   test: force-fill the inbox, verify the publisher's read budget
   shrinks.
8. **Bench**: run the throughput suite (`tests/throughput.sh --full`).
   Compare vs `--disable-worker-sharding`. Commit if improvement ≥20%
   on the fan-out scenarios, no regression on pub-only scenarios.
9. **Run on AWS mini** with the full `trading-sim` sweep at
   200/500/1000/2000/5000 users. Confirm the p99 improvement we saw in
   the SSM spike (162 ms → 24 ms at 2000u) holds.
10. **Flip the flag to default-on**, deprecate the legacy path behind
    `--disable-worker-sharding`, and remove the legacy path after one
    more benchmark cycle.

## Why This Beats ADR-011

| | ADR-011 (rejected) | ADR-012 (this) |
|---|---|---|
| Shard unit | Whole `Server` instance | Single `Worker` thread |
| Client pool split | Per-shard | Shared — any worker can accept any conn |
| Routing wire | Crossbeam channel, app-level | Crossbeam channel, app-level |
| Backpressure | None (silent drops) | Inbox fill → `read_budget` shrink → TCP window |
| Interest propagation | Initial sync of SubList on connect | `WorkerInterest` shared atomic, zero-copy lookup |
| Invariant at memory pressure | Drop | Throttle publisher via TCP |
| Rollout path | Opt-in feature flag | Opt-in feature flag |

The key difference is **scope**. ADR-011 built a process-level sharding
mechanism without a strong reason to use it; the N-worker model already
used multiple cores *for draining*. This ADR builds the same mechanism
but at the worker level, where the measured bottleneck actually lives
(dispatch fan-out), and wires backpressure through the path that already
handles route backpressure today.

## What Would Invalidate This

- If the throughput benchmark (step 8) shows regression on single-pub /
  single-sub workloads — meaning the inbox overhead is larger than the
  direct-write savings at low fan-out. In that case, keep the legacy
  path for low-fan-out scenarios and only enable worker sharding at high
  fan-out (detected at runtime via rolling average of matching sub
  count).
- If `WorkerInterest` contention becomes its own hot spot. Mitigation:
  shard the interest table by subject hash across multiple locks, or
  use a per-worker copy with batched gossip updates (further away from
  ADR-011 but viable).

## Related

- [ADR-002: Direct Writer](002-direct-writer.md) — the `MsgWriter + eventfd`
  design for cross-worker delivery that we're partially replacing.
- [ADR-010: Full-Mesh Clustering](010-full-mesh-clustering.md) — the
  cross-node route protocol; unchanged by this ADR.
- [ADR-011: In-Process ShardBus Spike](011-in-process-shard-bus-spike.md)
  — the previous attempt, which this ADR supersedes.
