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
Returns `string`, `list`, `hash`, `set`, or `none`.

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

### LPOP key
Removes and returns the first element.

### RPOP key
Removes and returns the last element.

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
Returns the element at `index`.

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
