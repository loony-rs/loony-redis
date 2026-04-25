# Command Reference

All commands follow the Redis RESP wire protocol. You can use any Redis client, `redis-cli`, or raw TCP.

---

## Connection

### PING [message]
Returns `PONG` or echoes `message`.
```
> PING
PONG
> PING hello
"hello"
```

### ECHO message
```
> ECHO "hello world"
"hello world"
```

### QUIT
Closes the connection gracefully.

### SELECT index
Accepted but ignored — loony-redis uses a single keyspace.

---

## Keyspace

### DEL key [key ...]
Deletes one or more keys. Returns the number of keys deleted.
```
> SET a 1
OK
> DEL a b c
(integer) 1
```

In cluster mode, all keys must hash to the same slot or the command returns `CROSSSLOT`.

### EXISTS key [key ...]
Returns the number of provided keys that exist.
```
> EXISTS a b
(integer) 1
```

### TYPE key
Returns `string`, `list`, `hash`, `set`, `zset`, or `none`.

### EXPIRE key seconds
Set a timeout (seconds) on a key. Returns `1` if set, `0` if key does not exist.

### PEXPIRE key milliseconds
Same as `EXPIRE` but in milliseconds.

### TTL key
Remaining time to live in seconds. Returns `-1` if no expiry, `-2` if key does not exist.

### PTTL key
Same as `TTL` but in milliseconds.

### PERSIST key
Remove the expiry from a key. Returns `1` if removed, `0` if key has no expiry.

### KEYS pattern
Returns all keys matching `pattern`. Only `*` (match all) is supported.
```
> KEYS *
1) "hello"
2) "counter"
```

### DBSIZE
Returns the total number of keys in the store.

### FLUSHDB / FLUSHALL
Removes all keys.

---

## Strings

### GET key
```
> SET name alice
OK
> GET name
"alice"
> GET missing
(nil)
```

### SET key value [EX seconds] [PX milliseconds] [NX] [XX]
- `EX` / `PX` — set expiry.
- `NX` — only set if key does not exist.
- `XX` — only set if key already exists.
```
> SET counter 0 EX 60
OK
> SET lock token NX
OK
> SET lock other NX
(nil)
```

### MGET key [key ...]
```
> MSET k1 a k2 b
OK
> MGET k1 k2 k3
1) "a"
2) "b"
3) (nil)
```

### MSET key value [key value ...]
Sets multiple key-value pairs atomically.

### STRLEN key
Returns the byte length of the string value.

### APPEND key value
Appends `value` to the existing string. Returns the new length.

### INCR key
Increments integer value by 1. Creates key with value `1` if it does not exist.

### DECR key
Decrements integer value by 1.

### INCRBY key increment
### DECRBY key decrement

### GETSET key value
Sets `key` to `value` and returns the old value.

### SETNX key value
Sets `key` only if it does not exist. Returns `1` on success, `0` if key existed.

---

## Lists

### LPUSH key value [value ...]
Prepends values to a list. Returns the new list length.

### RPUSH key value [value ...]
Appends values to a list.

### LPOP key [count]
Removes and returns the first element. With `count`, removes and returns up to `count` elements as an array.
```
> RPUSH items a b c d
(integer) 4
> LPOP items
"a"
> LPOP items 2
1) "b"
2) "c"
```

### RPOP key [count]
Removes and returns the last element. With `count`, removes and returns up to `count` elements as an array (from tail toward head).

### LLEN key
Returns the list length.

### LRANGE key start stop
Returns elements from `start` to `stop` (inclusive). Negative indices count from the end (`-1` = last element).
```
> RPUSH items a b c
(integer) 3
> LRANGE items 0 -1
1) "a"
2) "b"
3) "c"
```

### LINDEX key index
Returns the element at `index`. Returns `(nil)` if out of range.

### LSET key index value
Sets the element at `index` to `value`. Returns `OK`. Errors if out of range.
```
> RPUSH items a b c
(integer) 3
> LSET items 1 B
OK
> LRANGE items 0 -1
1) "a"
2) "B"
3) "c"
```

### LINSERT key BEFORE|AFTER pivot value
Inserts `value` immediately before or after the first occurrence of `pivot`. Returns the new list length, or `-1` if `pivot` is not found.
```
> RPUSH items a c
(integer) 2
> LINSERT items BEFORE c b
(integer) 3
> LRANGE items 0 -1
1) "a"
2) "b"
3) "c"
```

### LREM key count value
Removes occurrences of `value` from the list:
- `count > 0` — remove up to `count` from the head.
- `count < 0` — remove up to `|count|` from the tail.
- `count = 0` — remove all occurrences.

Returns the number of elements removed.

### LTRIM key start stop
Trims the list to the range `[start, stop]`, discarding all elements outside it. Returns `OK`.
```
> RPUSH items a b c d e
(integer) 5
> LTRIM items 1 3
OK
> LRANGE items 0 -1
1) "b"
2) "c"
3) "d"
```

### LMOVE source destination LEFT|RIGHT LEFT|RIGHT
Atomically pops one element from `source` (from `LEFT` or `RIGHT`) and pushes it to `destination` (to `LEFT` or `RIGHT`). Returns the moved element.
```
> RPUSH src a b c
(integer) 3
> LMOVE src dst LEFT RIGHT
"a"
> LRANGE dst 0 -1
1) "a"
```

---

## Hashes

### HSET key field value [field value ...]
Sets one or more fields. Returns the number of new fields added.
```
> HSET user name alice age 30
(integer) 2
```

### HMSET key field value [field value ...]
Alias for `HSET`.

### HGET key field
```
> HGET user name
"alice"
```

### HMGET key field [field ...]
Returns multiple field values (nil for missing fields).

### HGETALL key
Returns all field-value pairs as a flat array.

### HDEL key field [field ...]
Deletes fields. Returns the number of fields removed.

### HLEN key
Returns the number of fields.

### HEXISTS key field
Returns `1` if the field exists, `0` otherwise.

### HKEYS key
Returns all field names.

### HVALS key
Returns all field values.

### HSETNX key field value
Sets `field` only if it does not already exist. Returns `1` if set, `0` if field already existed.

### HINCRBY key field increment
Increments the integer value of `field` by `increment`. Creates the field with value `0` before incrementing if it does not exist.
```
> HSET stats views 10
(integer) 1
> HINCRBY stats views 5
(integer) 15
```

### HINCRBYFLOAT key field increment
Increments the float value of `field` by `increment`. Accepts decimal and scientific notation.
```
> HINCRBYFLOAT stats ratio 1.5
"1.5"
> HINCRBYFLOAT stats ratio -0.3
"1.2"
```

---

## Sets

### SADD key member [member ...]
Adds members to a set. Returns the number of new members added.

### SMEMBERS key
Returns all members (order is not guaranteed).

### SISMEMBER key member
Returns `1` if `member` is in the set, `0` otherwise.

### SREM key member [member ...]
Removes members. Returns the number removed.

### SCARD key
Returns the number of members.

### SMOVE source destination member
Atomically moves `member` from `source` to `destination`. Returns `1` on success, `0` if `member` is not in `source`.

### SRANDMEMBER key [count]
Returns a random member without removing it.
- No `count` — returns one element (or `nil` if the set is empty).
- `count >= 0` — returns up to `count` distinct elements.
- `count < 0` — returns exactly `|count|` elements, possibly with repeats.

### SPOP key [count]
Removes and returns one or `count` random members.

### SUNION key [key ...]
Returns the union of all given sets.
```
> SADD s1 a b c
> SADD s2 b c d
> SUNION s1 s2
1) "a"
2) "b"
3) "c"
4) "d"
```

### SINTER key [key ...]
Returns the intersection of all given sets.
```
> SINTER s1 s2
1) "b"
2) "c"
```

### SDIFF key [key ...]
Returns the difference: members of the first key that are not in any subsequent key.
```
> SDIFF s1 s2
1) "a"
```

### SUNIONSTORE destination key [key ...]
Stores the union result in `destination`. Returns the number of members stored.

### SINTERSTORE destination key [key ...]
Stores the intersection result in `destination`.

### SDIFFSTORE destination key [key ...]
Stores the difference result in `destination`.

---

## Sorted Sets

Sorted sets store unique members each associated with a floating-point score. Members are ordered by score; ties are broken lexicographically by member bytes.

**Score bounds** (for range commands): use `-inf` and `+inf` for unbounded ranges; prefix with `(` for an exclusive bound (e.g. `(1.0` means score > 1.0).

### ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
Adds or updates members with the given scores. Returns the number of new members added (or, with `CH`, the number of members added or changed).

Flags:
- `NX` — only add new members, never update existing ones.
- `XX` — only update existing members, never add new ones.
- `GT` — only update if the new score is greater than the current score.
- `LT` — only update if the new score is less than the current score.
- `CH` — change return value to count of added + changed members.

```
> ZADD leaderboard 100 alice 200 bob 150 carol
(integer) 3
> ZADD leaderboard GT 250 alice
(integer) 0
> ZSCORE leaderboard alice
"250"
```

### ZSCORE key member
Returns the score of `member` as a bulk string, or `(nil)` if the member does not exist.

### ZRANK key member
Returns the 0-based rank of `member` (lowest score = rank 0), or `(nil)` if not found.

### ZREVRANK key member
Returns the rank with highest score = rank 0.

### ZCARD key
Returns the number of members.

### ZCOUNT key min max
Returns the number of members with scores in `[min, max]`. Supports `(` prefix and `±inf`.
```
> ZCOUNT leaderboard 100 200
(integer) 2
> ZCOUNT leaderboard (100 +inf
(integer) 2
```

### ZINCRBY key increment member
Increments the score of `member` by `increment`. Returns the new score as a bulk string.

### ZREM key member [member ...]
Removes members. Returns the number of members removed.

### ZRANGE key start stop [WITHSCORES]
Returns members with ranks in `[start, stop]` (lowest score first). Negative indices are allowed (`-1` = highest rank).
```
> ZRANGE leaderboard 0 -1 WITHSCORES
1) "carol"
2) "150"
3) "bob"
4) "200"
5) "alice"
6) "250"
```

### ZREVRANGE key start stop [WITHSCORES]
Same as `ZRANGE` but in descending order (highest score first).

### ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
Returns members whose scores fall within `[min, max]`, in ascending score order.
```
> ZRANGEBYSCORE leaderboard 100 200 WITHSCORES LIMIT 0 2
1) "carol"
2) "150"
3) "bob"
4) "200"
```

### ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
Same but in descending order. Note: `max` comes before `min`.
```
> ZREVRANGEBYSCORE leaderboard +inf 150
1) "alice"
2) "bob"
3) "carol"
```

### ZPOPMIN key [count]
Removes and returns up to `count` members with the lowest scores (default 1). Returns alternating member/score pairs.

### ZPOPMAX key [count]
Removes and returns up to `count` members with the highest scores.

### ZREMRANGEBYRANK key start stop
Removes all members with ranks in `[start, stop]`. Returns the number removed.

### ZREMRANGEBYSCORE key min max
Removes all members with scores in `[min, max]`. Returns the number removed.
```
> ZREMRANGEBYSCORE leaderboard -inf 149
(integer) 0
> ZREMRANGEBYSCORE leaderboard 100 200
(integer) 2
```

---

## Cluster

### CLUSTER INFO
Returns cluster status as a bulk string (compatible with `redis-cli --cluster check`).

### CLUSTER NODES
Returns one line per node: `<id> <addr>@0 <flags> - 0 0 <index> connected <slot-ranges>`.

### CLUSTER SLOTS
Returns a nested array of slot ranges with their owning node's host, port, and id.

### CLUSTER SHARDS
Redis 7+ format. Returns shard objects with slot ranges and node details.

### CLUSTER MYID
Returns this node's deterministic 40-character hex node ID.

### CLUSTER KEYSLOT key
Returns the hash slot for `key`, honouring hash tags.
```
> CLUSTER KEYSLOT foo
(integer) 12182
> CLUSTER KEYSLOT {user}.name
(integer) 5474
> CLUSTER KEYSLOT {user}.email
(integer) 5474
```

### CLUSTER MEET host port
Adds a new node to the cluster and triggers automatic slot rebalancing with live data migration.
```
> CLUSTER MEET 127.0.0.1 6382
OK
```

### CLUSTER FORGET node-id
Gracefully removes a node. All data is migrated to surviving nodes before removal.
```
> CLUSTER MYID          # on the node to remove
"000000000000000000000000000000000000cafe"
> CLUSTER FORGET 000000000000000000000000000000000000cafe
OK
```

### CLUSTER HEALTH
Returns a sorted list of peers and their health states (`alive`, `suspected`, `failed`).
```
> CLUSTER HEALTH
"127.0.0.1:6480 alive
127.0.0.1:6481 alive"
```

### CLUSTER RESET
Accepted and returns OK (no-op in this implementation).

---

## MIGRATE

### MIGRATE host port key db timeout [COPY] [REPLACE]
Moves a key to another node. If `COPY` is specified, the key is left on the source node. Used internally by the cluster migration layer.
```
> MIGRATE 127.0.0.1 6480 mykey 0 5000
OK
```

Returns `NOKEY` if the key does not exist on this node.

---

## Internal Commands (cluster-to-cluster)

These are sent between cluster nodes and are not intended for client use.

| Command | Purpose |
|---|---|
| `CLUSTER SYNC <node-list>` | Peer says membership changed; recipient recomputes routing and migrates outbound slots |
| `CLUSTER DRAIN <node-list>` | Recipient pushes all its current slots to the new owners and acknowledges |
| `CLUSTER GOSSIP <csv>` | Piggybacked health state update (`addr=state,...`) |
