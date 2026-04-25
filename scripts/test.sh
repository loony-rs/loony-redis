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