# ADR-009: Leaf and Hub Feature Flags

**Status:** Accepted

## Context

open-wire evolved from a leaf-only relay (connecting upstream to a hub) to also
supporting inbound leaf connections (hub mode). Both modes pull in substantial
code: upstream connection management, interest tracking, and TLS for the `leaf`
path; inbound leaf protocol handling, LS+/LS- propagation, and `ConnExt::Leaf`
for the `hub` path.

Not every deployment needs both. A pure local message relay needs neither. An
edge node connecting to a central hub doesn't need to accept inbound leaf
connections. A hub aggregating leaf nodes may not itself connect upstream.

Compiling everything unconditionally increases binary size and exposes code
paths that a given deployment will never exercise.

## Decision

Gate hub and leaf functionality behind two independent Cargo features:

```toml
[features]
default = ["interest-collapse", "subject-mapping", "leaf", "hub"]
leaf = []   # Upstream hub connection support
hub = []    # Inbound leaf node connection support
```

Both are default-enabled so the out-of-the-box build has full functionality.
Users can build stripped-down variants:

| Features | Behavior |
|----------|----------|
| Neither | Pure local message relay |
| `leaf` only | Connects to upstream hub |
| `hub` only | Accepts inbound leaf connections |
| Both (default) | Full functionality |

### What each feature gates

**`leaf`** — `upstream.rs`, `interest.rs` modules; `HubCredentials`; `hub_url` /
`hub_credentials` config fields; `upstream` / `upstream_tx` on `ServerState`;
`forward_to_upstream()`; upstream interest management in SUB/UNSUB/PUB;
`connect_upstream()` in server; `LeafConn` / `LeafReader` / `LeafWriter` /
`HubStream` / `UpstreamConnectCreds` in protocol.

**`hub`** — `leaf_handler.rs` module; `leafnode_port` config; `leaf_writers` /
`leafnode_port` on `ServerState`; `ConnExt::Leaf` variant; `NewLeafConn`;
`add_leaf_conn()`; `propagate_leaf_sub/unsub()`; `send_existing_subs()`;
`client_interests()`; `write_raw()` on DirectWriter; leaf protocol dispatch in
Active phase; `leafnode_urls` on `ServerInfo`.

**`any(leaf, hub)`** — `LeafOp` enum; `try_parse_leaf_op()`; `parse_lmsg()`;
`build_lmsg*`; `build_leaf_sub/unsub*`; `write_lmsg()` on DirectWriter.

### Key design choices

- **`has_subs: AtomicBool` stays ungated.** It is used in the PUB fast-path skip
  optimization. When `leaf` is disabled, the skip condition simplifies from
  `upstream_tx.is_none() && !has_subs` to just `!has_subs`. Keeping the 1-byte
  atomic always present avoids conditional compilation on the hot path.

- **`is_leaf: bool` on Subscription stays ungated.** Always `false` when `hub` is
  disabled. Avoids conditional compilation in `SubList` matching.

- **`interest-collapse` and `subject-mapping` implicitly require `leaf`.**
  These features transform upstream interest and only make sense with an
  upstream connection. Config parsing and the `InterestPipeline` are gated
  behind `all(feature = "leaf", feature = "interest-collapse")` etc.

## Consequences

- **Positive:** Binary size reduction for deployments that don't need both modes.
  With neither feature, leaf/hub protocol parsing, upstream connection, and
  inbound leaf handling are completely compiled out.
- **Positive:** Clearer code boundaries. The `#[cfg]` gates document which code
  belongs to which mode.
- **Positive:** Compile-time guarantees that unused code paths cannot be reached.
- **Negative:** More conditional compilation attributes throughout the codebase.
  Every file touching leaf or hub functionality has `#[cfg]` annotations.
- **Negative:** CI must test four feature combinations (default, none, leaf-only,
  hub-only) to ensure all gates are correct.
