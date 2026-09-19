# 0003 — Client-facing MOVED/ASK redirects, not silent server-side proxying

## Decision

When a request reaches a node that does not own the relevant slot, the
node returns `-MOVED`/`-ASK` (per [[protocol]], [[resharding]]) rather
than transparently forwarding the request to the correct owner and
relaying the response.

## Context

The prototype's `commands/mod.rs` + `cluster::proxy_to` silently forwards
misrouted commands to the correct shard and returns the result as if it
had been handled locally. `Prompt.md` section 15 explicitly specifies
Redis-inspired `-MOVED`/`-ASK` client responses and says "a client must be
able to discover the current slot owner" — proxying defeats this by
design: a client using this server never learns the real topology and
never builds a routing cache, so every single request pays an extra
network hop through whichever node it happened to contact first.

## Options considered

1. **Keep transparent proxying.** Simpler client (no redirect handling
   needed), matches the prototype. Rejected: doubles latency for any
   misrouted request indefinitely (no incentive/mechanism for the client
   to learn correct routing), hides topology from operators debugging
   client-observed latency, and contradicts `Prompt.md` section 15's
   explicit requirement.
2. **Client-facing MOVED/ASK, server never proxies.** Matches Redis
   Cluster's well-understood model; a reasonably simple client-side
   routing cache (built once, updated on MOVED) gets full performance
   after a brief warm-up. Chosen.
3. **Hybrid: proxy by default, but also return topology via `CLUSTER
   SLOTS` for clients that want to route themselves.** Rejected as
   unnecessary complexity — once MOVED/ASK exist and are correct, a
   well-behaved client converges to direct routing on its own; a
   dedicated proxy mode isn't needed for v1 and can be added later
   without changing this decision if there's a concrete need (e.g., a
   dumb client that can't cache routing).

## Chosen approach

Server always replies with `-MOVED`/`-ASK` when it isn't the right
target; it never fetches-and-relays on the client's behalf. The minimal
cluster-aware `client`/`test-utils` crate (Prompt.md section 15) implements
the client-side half: cache slot ownership, retry on MOVED with cache
update, retry on ASK with `ASKING` prefix and no cache update (see
[[resharding]]).

## Trade-offs

- Clients that don't implement MOVED/ASK handling (e.g., raw `redis-cli`
  in non-cluster mode) will see errors instead of transparently-working
  requests when they hit the wrong node. Accepted: this matches real
  Redis Cluster's documented behavior and any cluster-aware client
  (including the one this project ships) handles it.

## Consequences

- `cluster::proxy_to` and its call sites are removed, not kept as a
  fallback path.
- The command-dispatch path in `commands/` gains a routing check ahead of
  execution (consult the local `ClusterState` cache from the metadata
  group, per [[sharding]]) that short-circuits to a MOVED/ASK reply
  instead of ever calling into a shard this node doesn't own.
