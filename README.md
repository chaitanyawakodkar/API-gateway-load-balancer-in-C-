# API Gateway / Load Balancer (C++17)

A reverse proxy built on POSIX sockets. Clients talk to the gateway; it forwards each
HTTP request to one of several backends and relays the response.

```
client -> [rate limiter] -> [load balancer] -> [circuit breaker] -> backend
              429               picks healthy        fail fast           (1 retry on another
                                backend              if tripped           backend for GET/HEAD)
```

## Features
| Component | What it does |
|---|---|
| Rate limiter | Per-client-IP **token bucket** (bursts) and **sliding-window log** (strict quota); either or both. Over limit -> `429`. Idle clients are evicted after 10 min. |
| Load balancer | **Round robin**, **least connections**, **consistent hashing** (150 virtual nodes/backend, clockwise failover). Skips unhealthy backends. |
| Circuit breaker | One per backend: `CLOSED -> OPEN -> HALF_OPEN`. After N consecutive failures it fails fast; after a cooldown exactly one probe request decides whether to close again. |
| Health checker | Background TCP probe of every backend (default every 5 s). |
| Failover | A failed idempotent request (GET/HEAD/OPTIONS) is retried once on a *different* backend. |
| Metrics | Lock-free latency histogram (10 us buckets) -> p50/p95/p99; counters; live at `GET /__gateway/stats`. |

## Build & run
```bash
make all
./dummy_backend 9001 & ./dummy_backend 9002 & ./dummy_backend 9003 &
./gateway --strategy=rr          # see ./gateway --help for all options
curl localhost:8080/             # proxied
curl localhost:8080/__gateway/stats
```
`dummy_backend <port> [delay_ms] [fail_percent]` can simulate slow or failing backends.
`loadgen` is a closed-loop load generator (`--conns --seconds --src-ips`).
`./bench.sh` reproduces every number below on your machine (~2.5 min).

## Measured results
Environment: 2-core sandbox; load generator, gateway and 3 backends all share those 2 cores.
Each request is a new TCP connection (no keep-alive). Latency is measured client-side and includes
the TCP handshake. Run-to-run variation was roughly +/-10%.

| Test | Result |
|---|---|
| Throughput, 50 / 200 / 500 concurrent connections | ~8,200-9,100 / ~8,000-8,500 / ~7,100-8,200 req/s, 0 errors |
| Latency @ 50 connections | p50 ~5.5 ms, p99 ~11-13 ms |
| Latency @ 500 connections | p50 ~60-68 ms, p99 ~100-138 ms (queueing: 500 clients on 2 shared cores) |
| Token bucket (burst 20, 5/s), one client, 6 s | 49 admitted (expected ~50) |
| Same, ten client IPs | 490 admitted (independent buckets) |
| Sliding window, max 30 | exactly 30 admitted |
| One backend slow (200 ms), 30 clients | round robin 445 req/s vs **least connections ~8,500-9,500 req/s** |
| Consistent hash, 12 client IPs | every IP stuck to 1 backend; killing one backend moved only the 2 clients that were on it; all returned after recovery |
| Backend killed mid-load (14 s run) | ~125-138k requests, **0 client-visible failures** |
| Backend returning 500 on everything | clients saw 0 errors; the bad backend received only ~8-10 requests in 12-14 s (5 to trip, then one probe per cooldown) |

## Known limitations (be upfront about these)
- Thread per connection, new connection per request: no keep-alive, no backend connection pooling. An event loop (epoll) is the next step for higher concurrency.
- HTTP/1.1 only, no TLS; a request larger than 8 KB is truncated; headers are forwarded unmodified (no `X-Forwarded-For`).
- Health checks only verify that the port accepts connections, not that the app is healthy.
- Rate limiting is keyed by source IP, so clients behind one NAT share a limit.
- Gateway-side latency covers handler entry -> response ready; use `loadgen` numbers for end-to-end.
- Single process, in-memory state: not distributed.
