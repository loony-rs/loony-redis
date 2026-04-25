#!/bin/bash

# ################################################################################################################################

./target/release/loony-redis --port 7379 &
SERVER_PID=$!
sleep 0.3

echo "=== PING ===" && redis-cli -p 7379 PING
echo "=== SET/GET ===" && redis-cli -p 7379 SET hello world && redis-cli -p 7379 GET hello
echo "=== DEL ===" && redis-cli -p 7379 DEL hello && redis-cli -p 7379 GET hello
echo "=== INCR ===" && redis-cli -p 7379 INCR counter && redis-cli -p 7379 INCRBY counter 5
echo "=== LIST ===" && redis-cli -p 7379 RPUSH mylist a b c && redis-cli -p 7379 LRANGE mylist 0 -1
echo "=== HASH ===" && redis-cli -p 7379 HSET user name Alice age 30 && redis-cli -p 7379 HGETALL user
echo "=== SET ===" && redis-cli -p 7379 SADD myset x y z && redis-cli -p 7379 SMEMBERS myset
echo "=== TTL ===" && redis-cli -p 7379 SET temp val EX 100 && redis-cli -p 7379 TTL temp
echo "=== MGET/MSET ===" && redis-cli -p 7379 MSET k1 v1 k2 v2 && redis-cli -p 7379 MGET k1 k2 k3
echo "=== DBSIZE ===" && redis-cli -p 7379 DBSIZE

kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null

# ################################################################################################################################

rm -f /tmp/test.aof
./target/release/loony-redis --port 7380 --aof /tmp/test.aof &
SERVER_PID=$!
sleep 0.3

redis-cli -p 7380 SET name "loony-redis"
redis-cli -p 7380 RPUSH items a b c
redis-cli -p 7380 HSET config version 1

echo "--- before restart ---"
redis-cli -p 7380 GET name
redis-cli -p 7380 LRANGE items 0 -1
redis-cli -p 7380 HGET config version

kill $SERVER_PID
sleep 0.3

# Restart with same AOF
./target/release/loony-redis --port 7380 --aof /tmp/test.aof &
SERVER_PID=$!
sleep 0.3

echo "--- after restart (AOF replay) ---"
redis-cli -p 7380 GET name
redis-cli -p 7380 LRANGE items 0 -1
redis-cli -p 7380 HGET config version

kill $SERVER_PID
wait $SERVER_PID 2>/dev/null
rm -f /tmp/test.aof


# ################################################################################################################################


BINARY=./target/release/loony-redis

# Start leader
$BINARY --port 7400 &
LEADER_PID=$!
sleep 0.3

# Pre-populate some data on the leader
redis-cli -p 7400 SET preexisting "hello"
redis-cli -p 7400 RPUSH mylist a b c
redis-cli -p 7400 HSET user name Alice

sleep 0.1

# Start two replicas
$BINARY --port 7401 --replicaof 127.0.0.1:7400 &
REPLICA1_PID=$!
$BINARY --port 7402 --replicaof 127.0.0.1:7400 &
REPLICA2_PID=$!

sleep 0.5

echo "=== Replicas received pre-existing snapshot ==="
echo -n "replica1 GET preexisting: " && redis-cli -p 7401 GET preexisting
echo -n "replica2 GET preexisting: " && redis-cli -p 7402 GET preexisting
echo -n "replica1 LRANGE mylist: "  && redis-cli -p 7401 LRANGE mylist 0 -1 | tr '\n' ' ' && echo
echo -n "replica1 HGET user name: " && redis-cli -p 7401 HGET user name

echo ""
echo "=== Live write propagation ==="
redis-cli -p 7400 SET live "replicated"
redis-cli -p 7400 SADD colors red blue green
sleep 0.1

echo -n "replica1 GET live: "     && redis-cli -p 7401 GET live
echo -n "replica2 GET live: "     && redis-cli -p 7402 GET live
echo -n "replica1 SMEMBERS colors: " && redis-cli -p 7401 SMEMBERS colors | sort | tr '\n' ' ' && echo

echo ""
echo "=== Replica rejects writes (READONLY) ==="
redis-cli -p 7401 SET shouldfail value

echo ""
echo "=== Replica serves reads ==="
redis-cli -p 7401 GET preexisting
redis-cli -p 7401 DBSIZE

kill $LEADER_PID $REPLICA1_PID $REPLICA2_PID 2>/dev/null
wait 2>/dev/null


# ################################################################################################################################

BIN=./target/release/loony-redis

# 3-node cluster: client ports 7500/7501/7502, Raft ports 8500/8501/8502
$BIN --port 7500 --raft-addr 127.0.0.1:8500 --peers 127.0.0.1:8501,127.0.0.1:8502 &
P0=$!
$BIN --port 7501 --raft-addr 127.0.0.1:8501 --peers 127.0.0.1:8500,127.0.0.1:8502 &
P1=$!
$BIN --port 7502 --raft-addr 127.0.0.1:8502 --peers 127.0.0.1:8500,127.0.0.1:8501 &
P2=$!

echo "Waiting for leader election..."
sleep 1.5

# Find which node is the leader by trying writes
LEADER_PORT=""
for port in 7500 7501 7502; do
  result=$(redis-cli -p $port SET probe ok 2>&1)
  if [[ "$result" == "OK" ]]; then
    LEADER_PORT=$port
    break
  fi
done
echo "Leader is on port $LEADER_PORT"

redis-cli -p $LEADER_PORT SET key1 "hello"
redis-cli -p $LEADER_PORT INCR counter
redis-cli -p $LEADER_PORT RPUSH mylist a b c

sleep 0.2
echo "Data on leader: key1=$(redis-cli -p $LEADER_PORT GET key1), counter=$(redis-cli -p $LEADER_PORT GET counter)"

# Kill the leader
case $LEADER_PORT in
  7500) kill $P0 2>/dev/null; echo "Killed node 7500" ;;
  7501) kill $P1 2>/dev/null; echo "Killed node 7501" ;;
  7502) kill $P2 2>/dev/null; echo "Killed node 7502" ;;
esac

echo "Waiting for new leader election (~300ms)..."
sleep 0.8

# Find new leader
NEW_LEADER=""
for port in 7500 7501 7502; do
  [[ "$port" == "$LEADER_PORT" ]] && continue
  result=$(redis-cli -p $port SET after_failover "works" 2>&1)
  if [[ "$result" == "OK" ]]; then
    NEW_LEADER=$port
    break
  fi
done

echo "New leader elected on port: $NEW_LEADER"
echo "after_failover = $(redis-cli -p $NEW_LEADER GET after_failover)"

# Verify old data is still there
echo "key1 = $(redis-cli -p $NEW_LEADER GET key1)"
echo "counter = $(redis-cli -p $NEW_LEADER GET counter)"
echo "mylist = $(redis-cli -p $NEW_LEADER LRANGE mylist 0 -1 | tr '\n' ' ')"

kill $P0 $P1 $P2 2>/dev/null
wait 2>/dev/null

# ============================================================================================================================================

NODES="127.0.0.1:6479,127.0.0.1:6480,127.0.0.1:6481"
nohup ./target/release/loony-redis --host 127.0.0.1 --port 6479 --cluster-nodes "$NODES" --cluster-self 127.0.0.1:6479 > /tmp/node0.log 2>&1 &
nohup ./target/release/loony-redis --host 127.0.0.1 --port 6480 --cluster-nodes "$NODES" --cluster-self 127.0.0.1:6480 > /tmp/node1.log 2>&1 &
nohup ./target/release/loony-redis --host 127.0.0.1 --port 6481 --cluster-nodes "$NODES" --cluster-self 127.0.0.1:6481 > /tmp/node2.log 2>&1 &
sleep 1
echo "=== Node logs ==="
cat /tmp/node0.log
cat /tmp/node1.log
cat /tmp/node2.log

# ============================================================================================================================================

# Test hash tags - {user} should land on same slot
redis-cli -p 6479 CLUSTER KEYSLOT "{user}.name"
redis-cli -p 6479 CLUSTER KEYSLOT "{user}.email"
echo "--- SET both via node 0, should proxy to same node ---"
redis-cli -p 6479 SET "{user}.name" "alice"
redis-cli -p 6479 SET "{user}.email" "alice@example.com"
echo "--- GET from whichever node owns that slot ---"
redis-cli -p 6479 GET "{user}.name"
redis-cli -p 6479 GET "{user}.email"

echo "=== CROSSSLOT test ==="
# DEL with keys from different slots should fail
redis-cli -p 6479 DEL foo bar

# ============================================================================================================================================

pkill -f loony-redis 2>/dev/null; sleep 0.5
NODES="127.0.0.1:6479,127.0.0.1:6480,127.0.0.1:6481"
./target/release/loony-redis --host 127.0.0.1 --port 6479 --cluster-nodes "$NODES" --cluster-self 127.0.0.1:6479 > /tmp/node0.log 2>&1 &
PID0=$!
./target/release/loony-redis --host 127.0.0.1 --port 6480 --cluster-nodes "$NODES" --cluster-self 127.0.0.1:6480 > /tmp/node1.log 2>&1 &
PID1=$!
./target/release/loony-redis --host 127.0.0.1 --port 6481 --cluster-nodes "$NODES" --cluster-self 127.0.0.1:6481 > /tmp/node2.log 2>&1 &
PID2=$!
sleep 1
echo "PIDs: $PID0 $PID1 $PID2"
echo "=== Node 0 log ==="
cat /tmp/node0.log