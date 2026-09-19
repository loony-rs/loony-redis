# Membership

## Node identity

- A node's identity (`NodeId`) is a ULID generated once, on first startup
  with an empty data directory, and persisted at
  `<data_dir>/identity` alongside its role-specific Raft/WAL state.
- Identity is independent of network address. `host:port` is a mutable
  attribute of a node, stored in the metadata group's `ClusterState`
  alongside the `NodeId`, and can change (e.g., after a restart with a new
  IP) as long as the node presents the same persisted `NodeId` when
  rejoining. This directly replaces the prototype's
  `node_id = hash(address)` scheme, which broke identity on address change
  (see gap analysis in [[architecture]]).
- Losing the identity file means losing the node's identity — it must
  rejoin as a new node with a new `NodeId` and an empty data directory. A
  node must never fabricate a plausible-looking identity for disk contents
  it did not itself write, since that would let a corrupted/foreign disk
  masquerade as a known member.

## Membership is consensus-backed, not gossip

The prototype's `ClusterConfig`/`HealthTable` were plain in-memory structs
mutated by best-effort heartbeats and a 3-missed-heartbeats `auto_heal`
that unilaterally rewrote routing — this can violate quorum safety (a node
that's alive but slow could be treated as gone by some nodes and not
others, simultaneously). This is replaced by a **metadata Raft group**:

- A small Raft group (recommended: one replica per node up to a
  reasonable cap, or a fixed subset for large clusters — configurable,
  default all nodes) whose committed log is the single source of truth for:
  - the node list (`NodeId -> address, role, health-as-last-observed`);
  - the slot ownership table (`slot -> ShardId`, see [[sharding]]);
  - per-shard replica rosters;
  - resharding/migration state ([[resharding]]).
- All of the above only change via a committed metadata-group log entry.
  Gossip/heartbeats still exist, but only as *inputs* that trigger a
  proposal to the metadata group (e.g., "propose marking node X
  unreachable" or "propose starting failover for shard 3") — they never
  directly mutate `ClusterState`. This is the mechanism described
  abstractly in [[failure-model]]'s "failure detection is not authority"
  section.

## JOIN / LEAVE / REJOIN

**JOIN** (new node, or a permanently-lost node's replacement):
1. New node starts with an empty data directory, generates a `NodeId`.
2. It is given the address of at least one existing cluster member out of
   band (config file / CLI arg — this is the trust boundary: an operator
   decides which cluster a new node is allowed to attempt joining, per
   `Prompt.md` section 27, "do not allow arbitrary untrusted nodes to
   silently join").
3. It contacts that member and requests to join. The contacted node
   proposes a `AddNode` command to the metadata group.
4. Once committed, the new `NodeId`/address pair is visible in
   `ClusterState` cluster-wide; the node can now be assigned shard replica
   roles by the control plane's placement logic ([[sharding]]).

**LEAVE** (graceful): operator issues a leave request for a `NodeId`. The
control plane first reassigns/drains any shard replica or leader roles the
node holds (transferring leadership if it's a leader, removing it as a
Raft member from each shard group it belongs to via that shard's own
membership-change process — see [[raft]]), then proposes `RemoveNode` to
the metadata group once the node holds no more shard roles.

**REJOIN** (a node that crashed/restarted with disk intact): the node
starts, loads its persisted `NodeId` and role state from disk, and
reconnects to its known peers. It does *not* re-run JOIN — it resumes
participation in whatever shard/metadata Raft groups its persisted state
says it belongs to, catching up via normal Raft catch-up ([[replication]]).
If its address changed, it proposes (through any reachable existing
member) an address update for its existing `NodeId` — this is a metadata
update, not a new identity.

## Permanent node loss

Per [[failure-model]], a node is never automatically declared permanently
gone purely from heartbeat failure. An operator (or an ops tool built on
the admin commands in `Prompt.md` section 33) explicitly issues a
"remove and replace" operation for a `NodeId` believed to be permanently
dead, which: proposes `RemoveNode` to the metadata group, and triggers
placement of replacement replicas for every shard role that node held,
each joining as a fresh learner per [[sharding]]/[[replication]].

## Untrusted nodes

A node can only be added via a committed `AddNode` metadata-group entry
proposed by an already-trusted member on behalf of an operator-initiated
join request. There is no path by which a node can add itself to
`ClusterState` unilaterally. TLS/mutual-auth for the join handshake itself
is an extension point (see `Prompt.md` section 34), not implemented in v1,
but the *protocol* already requires going through an existing trusted
member rather than broadcasting an unauthenticated "I exist" gossip
message that others must accept.
