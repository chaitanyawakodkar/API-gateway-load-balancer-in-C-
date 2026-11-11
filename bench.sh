#!/bin/bash
# bench.sh - reproduce every measurement quoted in the README on YOUR machine.
# Usage: make all && ./bench.sh        (takes ~2 minutes)
# Numbers depend on your CPU: loadgen, gateway and the backends all share it.
# Numbers depend on your hardware; regenerate them on your own machine.

cd "$(dirname "$0")" || exit 1
LOADGEN=./loadgen

stop_gw()  { for p in $(pgrep -x gateway); do kill "$p" 2>/dev/null; done; sleep 0.5; }
stop_all() { stop_gw; for p in $(pgrep -x dummy_backend); do kill "$p" 2>/dev/null; done; sleep 0.5; }
start_backends() { # args: optional "delay" for :9003
    for p in 9001 9002; do nohup ./dummy_backend $p >/dev/null 2>&1 & done
    nohup ./dummy_backend 9003 ${1:-0} >/dev/null 2>&1 &
    sleep 1
}
start_gw() { nohup ./gateway "$@" --quiet >/dev/null 2>&1 & sleep 1; }
pid_of_port() { pgrep -a -x dummy_backend | awk -v p="$1" '$3==p{print $1}'; }
trap stop_all EXIT

echo "cores: $(nproc)  (loadgen + gateway + 3 backends share them)"
stop_all

echo; echo "##### 1. THROUGHPUT / LATENCY (round robin, limiter off, 8 s each) #####"
start_backends; start_gw --strategy=rr --limiter=off
for c in 50 200 500; do echo "--- $c concurrent connections ---"; $LOADGEN --conns=$c --seconds=8 | sed -n 2,4p; done
stop_all

echo; echo "##### 2. RATE LIMITER (token bucket: burst 20, refill 5/s; 6 s) #####"
start_backends
start_gw --limiter=token
echo "--- ONE client IP: expect ~ 20 + 5*6 = ~50 allowed ---"
$LOADGEN --conns=10 --seconds=6 | sed -n 3p
stop_gw; start_gw --limiter=token
echo "--- TEN client IPs: expect ~10x (independent buckets) ---"
$LOADGEN --conns=10 --seconds=6 --src-ips=10 | sed -n 3p
stop_gw; start_gw --limiter=window --rl-window-max=30
echo "--- sliding window, max 30 per 60 s, ONE client: expect exactly 30 ---"
$LOADGEN --conns=10 --seconds=6 | sed -n 3p
stop_all

echo; echo "##### 3. LOAD BALANCING with one SLOW backend (:9003 = 200 ms), 30 conns, 6 s #####"
start_backends 200
for s in rr lc; do
    start_gw --strategy=$s --limiter=off
    echo "--- strategy=$s ---"; $LOADGEN --conns=30 --seconds=6 | sed -n 2p
    stop_gw
done
stop_all

echo; echo "##### 4. CONSISTENT HASH: 12 client IPs, each must stick to ONE backend #####"
start_backends; start_gw --strategy=ch --limiter=off
$LOADGEN --conns=24 --seconds=4 --src-ips=12 | tail -1
stop_all

echo; echo "##### 5. BACKEND CRASH UNDER LOAD: kill :9002 at t=4 s of a 14 s run #####"
start_backends; start_gw --strategy=rr --limiter=off --cb-cooldown=4
( sleep 4; kill "$(pid_of_port 9002)" ) &
$LOADGEN --conns=20 --seconds=14 | sed -n 2,3p
curl -s localhost:8080/__gateway/stats | grep "^backend"
stop_all

echo; echo "##### 6. CIRCUIT BREAKER: :9002 answers 500 to everything (port stays open) #####"
for p in 9001 9003; do nohup ./dummy_backend $p >/dev/null 2>&1 & done
nohup ./dummy_backend 9002 0 100 >/dev/null 2>&1 & sleep 1
start_gw --strategy=rr --limiter=off --cb-threshold=5 --cb-cooldown=4
$LOADGEN --conns=20 --seconds=12 | sed -n 2,3p
curl -s localhost:8080/__gateway/stats | grep "^backend"
echo "(breaker transitions are in gateway.log)"; grep CircuitBreaker gateway.log | tail -5
