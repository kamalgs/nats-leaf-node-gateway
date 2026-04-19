# ADR-005: Batched Eventfd Notifications

**Status:** Accepted

## Context

In the cross-worker fan-out path, each PUB that matched N subscribers on M
remote workers triggered M eventfd writes. With fan-out ×5 (5 subscribers on
different workers), a burst of 1000 PUBs caused 5000 eventfd writes. Each
eventfd write is a syscall. The fan-out ×5 benchmark achieved only 39% of
Go throughput, with eventfd writes dominating the profile.

The Go server batches notifications: it processes an entire read buffer of
client operations before flushing signals to other goroutines.

## Decision

Batch eventfd notifications per read buffer in `worker.rs`:

1. Each worker has a `pending_notify: [RawFd; 16]` array and a count.
2. During `process_read_buf()`, when a PUB fans out to a remote worker, the
   worker's eventfd is appended to `pending_notify` (deduplicated — each fd
   appears at most once).
3. After the entire read buffer is processed, `flush_notifications()` writes
   to each unique eventfd once.

This converts N_pubs × M_workers eventfd writes into at most M_workers writes
per read batch.

Additionally, same-worker delivery skips eventfd entirely. After each epoll
event batch, `flush_pending()` iterates all local connections with
`has_pending` set and drains their buffers directly.

## Consequences

- **Positive:** Fan-out ×5 throughput went from 39% to 191% of Go. This was
  the single largest improvement in the project.
- **Positive:** Syscall count dropped dramatically. Under fan-out, eventfd
  writes went from O(pubs × workers) to O(workers) per batch.
- **Positive:** The fixed-size array (`[RawFd; 16]`) avoids allocation and
  keeps the hot path branch-free.
- **Negative:** The array has a hard limit of 16 remote workers. Exceeding
  this would require a fallback to a `Vec` or an increase in the array size.
- **Negative:** Batching adds latency — a message is not signalled until the
  entire read buffer is processed. In practice, epoll returns quickly and
  the added latency is sub-microsecond.

## Addendum: Broadcast FD experiment (2026-04-19, abandoned)

A follow-on experiment replaced the per-worker eventfd array with a single
shared `broadcast_fd` on `ServerState`, registered in every worker's epoll.
One write wakes all workers simultaneously, reducing `flush_notifications()`
to a single unconditional syscall regardless of fan-out width.

**Result:** throughput was unchanged (585K msg/s) but binary p99 degraded 5×
(146ms → ~725ms avg across two reps) at 5K users / 3 hubs on c5.xlarge.

**Root cause:** with 4 workers per hub, every cross-worker delivery wakes all
4 workers instead of just the one with pending work. The three spurious wakes
scan their connection lists, find nothing, and return — but the extra cache
pressure and scheduling jitter accumulates into p99 latency. At the fan-out
ratios observed (~190×), the thundering-herd cost dominates the syscall savings.

**Conclusion:** the deduplicated per-fd array (O(unique workers) per batch,
max 16 entries) is the correct design. Branch `feat/broadcast-fd` was
abandoned; the dedup-only commit (`9e01584`) on `main` is the accepted state.
