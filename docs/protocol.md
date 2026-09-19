# Protocol

## RESP2 subset

The prototype's RESP parser/encoder (`src/protocol/mod.rs`) is reused as-is
— it already correctly handles simple strings, errors, integers, bulk
strings, arrays, null bulk strings, and incremental/pipelined parsing
(returns `Ok(None)` on incomplete input rather than assuming read()
boundaries are message boundaries; verified by existing tests). No
protocol-level rewrite is needed; see [[architecture]]'s reuse decision.

## Required commands (v1)

```
PING
GET SET DEL
LPUSH RPUSH LPOP
HSET HGET
SADD SMEMBERS
EXPIRE TTL
INFO
```

Plus cluster-routing responses (not client-initiated commands, but
protocol-level replies): `-MOVED`, `-ASK`, and the `ASKING` command a
client must send before retrying an ASK-redirected command (see
[[resharding]]).

`CLUSTER INFO` / `CLUSTER NODES` / `CLUSTER SLOTS` and `RAFT INFO` are
admin/introspection commands, specified in [[observability]].

## Routing responses

- **`-MOVED <slot> <host>:<port>`**: this node is not (and, as far as its
  view of the consensus-backed slot table goes, was never recently) the
  owner of `<slot>`. The client should update its local slot-to-node
  cache and retry against `<host>:<port>`. Not returned during an active
  migration's `PREPARING`/`TRANSFERRING`/`CATCHING_UP` states (source is
  still fully authoritative then) — see [[resharding]] for exactly when
  each redirect applies.
- **`-ASK <slot> <host>:<port>`**: this slot is in the `CUTOVER` state of
  an active migration; the correct data now lives (or is about to
  definitively live) at `<host>:<port>`, but the client must not update
  its long-term routing cache from this — it's a one-shot redirect. The
  client sends `ASKING` on the connection to `<host>:<port>`, then retries
  the *exact same* command. The target only honors the command as
  ASK-redirected traffic if `ASKING` immediately preceded it on that
  connection (matches Redis Cluster's rationale: distinguishes "sent here
  deliberately via ASK" from "arrived here due to a stale/wrong client
  cache," which would otherwise let a client bypass normal ownership
  checks by guessing).
- No transparent, invisible server-side proxying (see decision
  `docs/decisions/0003-moved-ask-not-proxy.md`) — the server always tells
  the client where the data actually is rather than silently fetching it
  on the client's behalf.

## Limits (Prompt.md section 17)

Configurable, enforced before allocating memory for a request's payload:

```
max_key_size
max_value_size
max_command_size
max_request_size
max_pipeline_depth
max_connections
max_memory
```

The parser must reject an oversized incoming frame based on its declared
length prefix *before* attempting to buffer/allocate that much memory —
i.e., check the bulk-string/array length header against the configured
max before reading the body, not after. A request that fails a limit
check gets a RESP error reply and, for a sufficiently severe violation
(e.g., a length header that's absurd/adversarial rather than just over a
soft cap), the connection may be closed rather than kept open expecting a
well-formed follow-up.

## Non-goals for the protocol layer (v1)

RESP3, Pub/Sub push messages, MULTI/EXEC transactions, and Lua (`EVAL`)
are not implemented, per `Prompt.md` section 49.
