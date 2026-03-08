# Goals and Non-Goals

## Goals

- **Match or beat Go nats-server throughput** on leaf node workloads (pub/sub,
  fan-out, leaf↔hub forwarding). The Go server is the reference benchmark.
- **Minimal idle memory** — a server with 10K mostly-idle connections should not
  consume hundreds of megabytes of buffer space.
- **Minimal dependencies** — prefer std and libc over large frameworks. Keep the
  dependency tree small and auditable.
- **Single binary** — the server compiles to one static binary with no runtime
  dependencies beyond libc.
- **Learning project** — explore what raw-syscall, zero-copy Rust can achieve
  against a mature Go implementation. Document decisions and tradeoffs.

## Non-Goals

- **Full NATS server** — no JetStream, clustering, accounts, authorization,
  or multi-tenancy. This is a leaf node only.
- **TLS / authentication** — not implemented. The server trusts its network.
- **Production use** — this is an experiment, not a hardened production system.
  Error handling, observability, and operational tooling are minimal.
- **Cross-platform** — the server uses Linux-specific APIs (epoll, eventfd).
  macOS and Windows are not supported.
- **Protocol completeness** — only the subset of NATS client and leaf protocols
  needed for pub/sub benchmarking is implemented (no queue groups, no request
  headers, no UNSUB max-messages).
