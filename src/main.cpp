// =============================================================================
// main.cpp - API Gateway / Load Balancer
// =============================================================================
//
// WHAT IT IS
//   A reverse proxy. Clients talk to the gateway (default :8080); the gateway
//   forwards each HTTP request to one of several backend servers and relays the
//   response. socket_core.cpp does the raw socket work (accept, parse, forward);
//   everything interesting lives in THIS file.
//
// REQUEST PIPELINE (runs on one thread per client connection)
//
//   client --> [1 Rate limiter] --> [2 Load balancer] --> [3 Circuit breaker]
//                  429 if over limit    picks a healthy       fail fast if the
//                                       backend                backend is tripped
//          --> [4 Forward + retry once on failure (idempotent methods only)]
//          --> [5 Metrics] --> response to client
//
// BACKGROUND THREADS
//   * health checker : every few seconds, TCP-connects to each backend
//   * stats printer  : every 10 seconds, logs throughput and latency percentiles
//
// THE FIVE COMPONENTS (all in this file, in this order)
//   1. RateLimiter    - per-client token bucket + sliding-window log
//   2. LoadBalancer   - round robin / least connections / consistent hashing
//   3. CircuitBreaker - CLOSED -> OPEN -> HALF_OPEN state machine, one per backend
//   4. Health checker - active TCP probes
//   5. Metrics        - lock-free latency histogram (p50/p95/p99) and counters
//
// RUN IT
//   ./dummy_backend 9001 & ./dummy_backend 9002 & ./dummy_backend 9003 &
//   ./gateway --strategy=rr
//   curl localhost:8080/            # proxied request
//   curl localhost:8080/__gateway/stats   # live stats from the gateway itself
//   ./gateway --help                # all options
// =============================================================================

#include "socket_core.h"
#include "logger.h"

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <climits>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

using Clock = std::chrono::steady_clock;

// =============================================================================
// SECTION 0: CONFIGURATION (all values can be overridden from the command line)
// =============================================================================

enum class LBStrategy { ROUND_ROBIN, LEAST_CONNECTIONS, CONSISTENT_HASH };
enum class LimiterMode { OFF, TOKEN, WINDOW, BOTH };

struct Config {
    int         port              = 8080;
    LBStrategy  strategy          = LBStrategy::ROUND_ROBIN;

    LimiterMode limiter           = LimiterMode::BOTH;
    double      rlCapacity        = 20;     // token bucket: max burst size
    double      rlRefillPerSec    = 5;      // token bucket: sustained rate
    int         rlWindowMax       = 100;    // sliding window: max requests ...
    int         rlWindowMs        = 60000;  // ... per this many milliseconds

    int         cbThreshold       = 5;      // consecutive failures before OPEN
    int         cbCooldownSec     = 30;     // seconds to stay OPEN before probing

    int         healthIntervalSec = 5;
    bool        quiet             = false;  // disable the periodic stats log line

    std::vector<std::pair<std::string, int>> backends = {
        {"127.0.0.1", 9001}, {"127.0.0.1", 9002}, {"127.0.0.1", 9003}};
};
static Config CFG;

static void usage() {
    std::cout <<
        "Usage: ./gateway [options]\n"
        "  --port=8080\n"
        "  --strategy=rr|lc|ch            round robin | least connections | consistent hash\n"
        "  --limiter=both|token|window|off\n"
        "  --rl-capacity=20 --rl-refill=5 token bucket burst size / tokens per second\n"
        "  --rl-window-max=100 --rl-window-ms=60000   sliding window limit\n"
        "  --cb-threshold=5 --cb-cooldown=30          circuit breaker\n"
        "  --health-interval=5            seconds between health probes\n"
        "  --backends=ip:port,ip:port     default 127.0.0.1:9001,9002,9003\n"
        "  --quiet                        no periodic stats log line\n";
}

static std::string valueOf(const std::string& arg) {
    auto eq = arg.find('=');
    return eq == std::string::npos ? "" : arg.substr(eq + 1);
}
static bool is(const std::string& arg, const char* prefix) { return arg.rfind(prefix, 0) == 0; }

static void parseArgs(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--help")                 { usage(); std::exit(0); }
        else if (a == "--quiet")           CFG.quiet = true;
        else if (is(a, "--port="))         CFG.port = std::stoi(valueOf(a));
        else if (is(a, "--strategy=")) {
            std::string v = valueOf(a);
            if      (v == "rr") CFG.strategy = LBStrategy::ROUND_ROBIN;
            else if (v == "lc") CFG.strategy = LBStrategy::LEAST_CONNECTIONS;
            else if (v == "ch") CFG.strategy = LBStrategy::CONSISTENT_HASH;
            else throw std::runtime_error("unknown strategy: " + v);
        }
        else if (is(a, "--limiter=")) {
            std::string v = valueOf(a);
            if      (v == "both")   CFG.limiter = LimiterMode::BOTH;
            else if (v == "token")  CFG.limiter = LimiterMode::TOKEN;
            else if (v == "window") CFG.limiter = LimiterMode::WINDOW;
            else if (v == "off")    CFG.limiter = LimiterMode::OFF;
            else throw std::runtime_error("unknown limiter: " + v);
        }
        else if (is(a, "--rl-capacity="))   CFG.rlCapacity = std::stod(valueOf(a));
        else if (is(a, "--rl-refill="))     CFG.rlRefillPerSec = std::stod(valueOf(a));
        else if (is(a, "--rl-window-max=")) CFG.rlWindowMax = std::stoi(valueOf(a));
        else if (is(a, "--rl-window-ms="))  CFG.rlWindowMs = std::stoi(valueOf(a));
        else if (is(a, "--cb-threshold="))  CFG.cbThreshold = std::stoi(valueOf(a));
        else if (is(a, "--cb-cooldown="))   CFG.cbCooldownSec = std::stoi(valueOf(a));
        else if (is(a, "--health-interval=")) CFG.healthIntervalSec = std::stoi(valueOf(a));
        else if (is(a, "--backends=")) {
            CFG.backends.clear();
            std::stringstream ss(valueOf(a));
            std::string item;
            while (std::getline(ss, item, ',')) {
                auto colon = item.rfind(':');
                if (colon == std::string::npos) throw std::runtime_error("bad backend: " + item);
                CFG.backends.emplace_back(item.substr(0, colon), std::stoi(item.substr(colon + 1)));
            }
        }
        else throw std::runtime_error("unknown option: " + a);
    }
    if (CFG.backends.empty()) throw std::runtime_error("need at least one backend");
}

// A backend server. Atomics because the health thread and request threads touch
// these concurrently. Atomics are not copyable, so we keep Backends in a
// std::deque and construct them in place (see main()).
struct Backend {
    std::string           ip;
    int                   port = 0;
    std::atomic<bool>     healthy{true};     // written by health checker
    std::atomic<int>      activeConns{0};    // in-flight requests (for least-conn)
    std::atomic<uint64_t> served{0};         // responses relayed OK
    std::atomic<uint64_t> failed{0};         // attempts that failed (5xx / no reply)
    Backend() = default;
    Backend(const Backend&) = delete;
    Backend& operator=(const Backend&) = delete;
    std::string name() const { return ip + ":" + std::to_string(port); }
};
static std::deque<Backend> BACKENDS;

// =============================================================================
// SECTION 1: RATE LIMITER  (per client IP)
// =============================================================================
// GOAL: stop one client from flooding the backends. Over the limit -> HTTP 429.
//
// Two independent algorithms, each tracked per client IP:
//
// TOKEN BUCKET
//   A bucket holds up to `capacity` tokens and refills at `refillPerSec`.
//   Every request takes 1 token; empty bucket = reject.
//   Property: allows short BURSTS (up to capacity) but caps the long-run rate.
//   We refill lazily: on each request, add (elapsed * rate) tokens. No timer
//   thread needed, and it is exact.
//
// SLIDING WINDOW LOG
//   Remember the timestamp of every accepted request in the last `windowMs`.
//   Reject if there are already `max` of them.
//   Property: strict - never more than `max` requests in ANY window, no bursts
//   past the quota. Cost: O(max) memory per client (the "log").
//
// With --limiter=both (default) a request must pass both. We CHECK both first
// and only then CONSUME from both, so a request rejected by one limiter does
// not burn a token from the other.
//
// Memory: idle clients are swept out of the map after 10 minutes, otherwise a
// flood of distinct IPs would grow the map forever.

struct TokenBucket {
    double tokens, capacity, refillPerSec;
    Clock::time_point last;
    TokenBucket(double cap, double rate, Clock::time_point now)
        : tokens(cap), capacity(cap), refillPerSec(rate), last(now) {}
    void refill(Clock::time_point now) {
        double elapsed = std::chrono::duration<double>(now - last).count();
        tokens = std::min(capacity, tokens + elapsed * refillPerSec);
        last = now;
    }
    bool canTake() const { return tokens >= 1.0; }
    void take() { tokens -= 1.0; }
};

struct SlidingWindowLog {
    std::deque<Clock::time_point> stamps;   // oldest at the front
    int maxRequests;
    std::chrono::milliseconds window;
    SlidingWindowLog(int max, int windowMs) : maxRequests(max), window(windowMs) {}
    void prune(Clock::time_point now) {
        while (!stamps.empty() && now - stamps.front() > window) stamps.pop_front();
    }
    bool canTake() const { return (int)stamps.size() < maxRequests; }
    void take(Clock::time_point now) { stamps.push_back(now); }
};

class RateLimiter {
public:
    bool allow(const std::string& clientIp) {
        if (CFG.limiter == LimiterMode::OFF) return true;
        const bool useToken  = CFG.limiter == LimiterMode::TOKEN  || CFG.limiter == LimiterMode::BOTH;
        const bool useWindow = CFG.limiter == LimiterMode::WINDOW || CFG.limiter == LimiterMode::BOTH;

        auto now = Clock::now();
        std::lock_guard<std::mutex> lock(mu_);   // many request threads call allow()
        sweepIfNeeded(now);

        auto it = clients_.find(clientIp);
        if (it == clients_.end())
            it = clients_.emplace(clientIp, ClientState(now)).first;
        ClientState& c = it->second;
        c.lastSeen = now;

        if (useToken)  c.bucket.refill(now);
        if (useWindow) c.window.prune(now);

        bool tokenOk  = !useToken  || c.bucket.canTake();
        bool windowOk = !useWindow || c.window.canTake();

        if (tokenOk && windowOk) {                // commit to both
            if (useToken)  c.bucket.take();
            if (useWindow) c.window.take(now);
            allowed_++;
            return true;
        }
        if (!tokenOk) deniedToken_++; else deniedWindow_++;
        return false;
    }

    uint64_t allowed()      const { return allowed_.load(); }
    uint64_t deniedToken()  const { return deniedToken_.load(); }
    uint64_t deniedWindow() const { return deniedWindow_.load(); }
    size_t   trackedClients() { std::lock_guard<std::mutex> l(mu_); return clients_.size(); }

private:
    struct ClientState {
        TokenBucket      bucket;
        SlidingWindowLog window;
        Clock::time_point lastSeen;
        explicit ClientState(Clock::time_point now)
            : bucket(CFG.rlCapacity, CFG.rlRefillPerSec, now),
              window(CFG.rlWindowMax, CFG.rlWindowMs), lastSeen(now) {}
    };

    void sweepIfNeeded(Clock::time_point now) {
        if (now - lastSweep_ < std::chrono::seconds(60)) return;
        lastSweep_ = now;
        for (auto it = clients_.begin(); it != clients_.end();) {
            if (now - it->second.lastSeen > std::chrono::minutes(10)) it = clients_.erase(it);
            else ++it;
        }
    }

    std::mutex mu_;                                        // guards clients_
    std::unordered_map<std::string, ClientState> clients_; // one state per client IP
    Clock::time_point lastSweep_ = Clock::now();
    std::atomic<uint64_t> allowed_{0}, deniedToken_{0}, deniedWindow_{0};
};

// =============================================================================
// SECTION 2: LOAD BALANCER
// =============================================================================
// GOAL: choose which backend serves a request. `usable[i]` tells us which
// backends are currently allowed (healthy and not already tried), so every
// strategy automatically skips dead ones. Returns -1 if none are usable.
//
// ROUND ROBIN        next backend in rotation. Simple and fair when requests
//                    cost about the same.
// LEAST CONNECTIONS  backend with the fewest in-flight requests. Adapts when
//                    some requests/backends are slower. Ties are broken by a
//                    rotating start offset so an idle system still spreads load.
// CONSISTENT HASH    hash(client IP) -> point on a ring; walk clockwise to the
//                    first backend. The same client sticks to the same backend,
//                    and adding/removing a backend only remaps ~1/N of clients.
//                    Each backend is placed on the ring VIRTUAL_NODES times so
//                    the ring is evenly divided. If the chosen backend is down
//                    we keep walking clockwise (natural failover).

class LoadBalancer {
public:
    LoadBalancer() {
        for (size_t i = 0; i < CFG.backends.size(); ++i)
            for (int v = 0; v < VIRTUAL_NODES; ++v) {
                std::string key = CFG.backends[i].first + ":" +
                                  std::to_string(CFG.backends[i].second) + "#" + std::to_string(v);
                ring_[std::hash<std::string>{}(key)] = (int)i;
            }
    }

    int pick(const std::string& clientKey, const std::vector<char>& usable) {
        const int n = (int)usable.size();
        switch (CFG.strategy) {

        case LBStrategy::ROUND_ROBIN: {
            size_t start = rr_++;                       // atomic: safe across threads
            for (int i = 0; i < n; ++i) {
                int c = (int)((start + i) % n);
                if (usable[c]) return c;
            }
            return -1;
        }

        case LBStrategy::LEAST_CONNECTIONS: {
            size_t start = rr_++;                       // rotating tie-break
            int best = -1, bestConns = INT_MAX;
            for (int i = 0; i < n; ++i) {
                int c = (int)((start + i) % n);
                if (!usable[c]) continue;
                int conns = BACKENDS[c].activeConns.load();
                if (conns < bestConns) { bestConns = conns; best = c; }
            }
            return best;
        }

        case LBStrategy::CONSISTENT_HASH: {
            auto it = ring_.lower_bound(std::hash<std::string>{}(clientKey)); // first point >= hash
            for (size_t step = 0; step < ring_.size(); ++step) {
                if (it == ring_.end()) it = ring_.begin();                    // wrap around the ring
                if (usable[it->second]) return it->second;
                ++it;
            }
            return -1;
        }
        }
        return -1;
    }

private:
    static constexpr int VIRTUAL_NODES = 150;
    std::atomic<size_t>   rr_{0};
    std::map<size_t, int> ring_;      // ring position -> backend index (read-only after ctor)
};

// =============================================================================
// SECTION 3: CIRCUIT BREAKER  (one per backend)
// =============================================================================
// PROBLEM: a backend is failing. Without protection every request still goes
// to it, waits for a timeout, and ties up a thread - the whole gateway slows
// down, not just that backend's share.
//
// SOLUTION: a small state machine.
//
//   CLOSED ----(threshold consecutive failures)----> OPEN
//     ^                                                |
//     |                                          (cooldown elapsed)
//     |                                                v
//     +------------(probe succeeds)------------- HALF_OPEN
//                                                      |
//                  OPEN <------(probe fails)-----------+
//
//   CLOSED    normal; requests pass; count consecutive failures.
//   OPEN      fail fast: allowRequest() = false, the backend is not touched.
//   HALF_OPEN after the cooldown exactly ONE request is let through as a probe;
//             everyone else is still refused until it reports back.
//
// Late results: a request started while CLOSED may finish after the breaker has
// opened. onSuccess/onFailure for such stragglers are ignored in OPEN so they
// cannot flip the state incorrectly.

enum class CBState { CLOSED, OPEN, HALF_OPEN };

class CircuitBreaker {
public:
    explicit CircuitBreaker(std::string label) : label_(std::move(label)) {}

    // Ask BEFORE sending. true = go ahead, false = fail fast.
    bool allowRequest() {
        std::lock_guard<std::mutex> lock(mu_);
        auto now = Clock::now();
        switch (state_) {
        case CBState::CLOSED:
            return true;
        case CBState::OPEN:
            if (now - openedAt_ >= std::chrono::seconds(CFG.cbCooldownSec)) {
                state_ = CBState::HALF_OPEN;
                probeStartedAt_ = now;
                writelog(LogComponent::CircuitBreaker, label_ + " OPEN -> HALF_OPEN (sending probe)");
                return true;                       // this caller IS the probe
            }
            return false;
        case CBState::HALF_OPEN:
            // A probe is already out. If it never reported back (e.g. its thread
            // hung), allow a new probe after another cooldown so we can't get stuck.
            if (now - probeStartedAt_ >= std::chrono::seconds(CFG.cbCooldownSec)) {
                probeStartedAt_ = now;
                return true;
            }
            return false;
        }
        return false;
    }

    // Report AFTER the request.
    void onSuccess() {
        std::lock_guard<std::mutex> lock(mu_);
        if (state_ == CBState::CLOSED)    failures_ = 0;
        else if (state_ == CBState::HALF_OPEN) {
            state_ = CBState::CLOSED;
            failures_ = 0;
            writelog(LogComponent::CircuitBreaker, label_ + " HALF_OPEN -> CLOSED (backend recovered)");
        }                                          // OPEN: stale success, ignore
    }

    void onFailure() {
        std::lock_guard<std::mutex> lock(mu_);
        auto now = Clock::now();
        if (state_ == CBState::HALF_OPEN) {
            state_ = CBState::OPEN;
            openedAt_ = now;
            writelog(LogComponent::CircuitBreaker, label_ + " HALF_OPEN -> OPEN (probe failed)");
        } else if (state_ == CBState::CLOSED && ++failures_ >= CFG.cbThreshold) {
            state_ = CBState::OPEN;
            openedAt_ = now;
            writelog(LogComponent::CircuitBreaker,
                     label_ + " CLOSED -> OPEN (" + std::to_string(failures_) + " consecutive failures)");
        }                                          // OPEN: stale failure, ignore
    }

    std::string stateName() {
        std::lock_guard<std::mutex> lock(mu_);
        return state_ == CBState::CLOSED ? "CLOSED" : state_ == CBState::OPEN ? "OPEN" : "HALF_OPEN";
    }

private:
    std::string       label_;
    std::mutex        mu_;
    CBState           state_ = CBState::CLOSED;
    int               failures_ = 0;
    Clock::time_point openedAt_{}, probeStartedAt_{};
};
static std::deque<CircuitBreaker> BREAKERS;

// =============================================================================
// SECTION 4: HEALTH CHECKER
// =============================================================================
// Active probing: every N seconds, try a TCP connect() to every backend and
// record whether it worked. The load balancer then skips unhealthy backends.
//
// Health check vs circuit breaker - they cover different failures:
//   health check    proactive; notices "process is dead / port closed" even
//                   when no traffic is flowing.
//   circuit breaker reactive; notices "port is open but requests keep failing"
//                   (errors, overload) from real traffic.
//
// Limitation: a TCP connect only proves something is listening, not that the
// app answers correctly. A real deployment would probe an HTTP /health route.

static bool pingBackend(const std::string& ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;
    timeval tv{2, 0};                                   // don't hang on a black-holed host
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));   // also bounds connect()
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);
    bool alive = connect(fd, (sockaddr*)&addr, sizeof(addr)) == 0;
    close(fd);
    return alive;
}

static void healthCheckLoop() {
    while (true) {
        for (auto& b : BACKENDS) {
            bool was = b.healthy.load();
            bool now = pingBackend(b.ip, b.port);
            b.healthy = now;
            if (was && !now)      writelog(LogComponent::HealthCheck, b.name() + " is DOWN");
            else if (!was && now) writelog(LogComponent::HealthCheck, b.name() + " is UP");
        }
        std::this_thread::sleep_for(std::chrono::seconds(CFG.healthIntervalSec));
    }
}

// =============================================================================
// SECTION 5: METRICS
// =============================================================================
// WHY PERCENTILES: if 99 requests take 1 ms and one takes 1000 ms, the average
// is ~11 ms and looks fine, while p99 = 1000 ms exposes the slow tail.
//
// HOW: instead of storing every sample (unbounded memory, needs a lock and a
// sort), latencies go into a fixed histogram of 10-microsecond buckets covering
// 0..500 ms (+1 overflow bucket). Recording is one atomic increment - no lock,
// no allocation, O(1) - so metrics don't distort what they measure. A percentile
// is found by walking the buckets until the cumulative count reaches p% of the
// total. Resolution: reported values are accurate to within 10 us.
//
// Scope: the gateway measures from "handler entered" to "response ready". That
// includes rate limiting, backend selection and the backend round trip, but NOT
// accept(), HTTP parsing before the handler, or sending the reply to the client.
// For true end-to-end latency use the client-side numbers from ./loadgen.

class Metrics {
public:
    Metrics() : hist_(new std::atomic<uint64_t>[NB + 1]) {
        for (int i = 0; i <= NB; ++i) hist_[i].store(0);
        started_ = Clock::now();
    }

    // A request that went to a backend (statusCode = what we returned).
    void recordProxied(double ms, int statusCode) {
        proxied_++;
        if (statusCode >= 200 && statusCode < 300) ok2xx_++;
        else if (statusCode >= 500)                err5xx_++;
        else                                       other_++;
        uint64_t us  = (uint64_t)(ms * 1000.0);
        size_t   idx = std::min<uint64_t>(us / BUCKET_US, NB);
        hist_[idx]++;
        uint64_t prev = maxUs_.load();
        while (us > prev && !maxUs_.compare_exchange_weak(prev, us)) {}
    }
    // Rejected by the gateway itself, never reached a backend.
    void recordRejected(int statusCode) {
        if (statusCode == 429) rejected429_++; else rejected503_++;
    }

    double percentileMs(double p) const {            // p in (0,1]
        uint64_t total = proxied_.load();
        if (total == 0) return 0;
        uint64_t target = (uint64_t)std::ceil(p * (double)total), cum = 0;
        for (int i = 0; i <= NB; ++i) {
            cum += hist_[i].load();
            if (cum >= target) return (i + 1) * BUCKET_US / 1000.0;   // bucket upper edge
        }
        return NB * BUCKET_US / 1000.0;
    }

    uint64_t proxied()  const { return proxied_.load(); }
    uint64_t ok2xx()    const { return ok2xx_.load(); }
    uint64_t err5xx()   const { return err5xx_.load(); }
    uint64_t other()    const { return other_.load(); }
    uint64_t rej429()   const { return rejected429_.load(); }
    uint64_t rej503()   const { return rejected503_.load(); }
    double   maxMs()    const { return maxUs_.load() / 1000.0; }
    double   uptimeSec() const { return std::chrono::duration<double>(Clock::now() - started_).count(); }

private:
    static constexpr uint64_t BUCKET_US = 10;
    static constexpr int      NB        = 50000;      // 50000 * 10us = 500 ms
    std::unique_ptr<std::atomic<uint64_t>[]> hist_;
    std::atomic<uint64_t> proxied_{0}, ok2xx_{0}, err5xx_{0}, other_{0};
    std::atomic<uint64_t> rejected429_{0}, rejected503_{0}, maxUs_{0};
    Clock::time_point started_;
};

static std::string fmt(double v, int digits = 2) {
    std::ostringstream o; o.setf(std::ios::fixed); o.precision(digits); o << v; return o.str();
}

static std::string buildReport(Metrics& m, RateLimiter& rl) {
    std::ostringstream o;
    double up = m.uptimeSec();
    uint64_t all = m.proxied() + m.rej429() + m.rej503();
    o << "uptime_s=" << fmt(up, 1) << "\n"
      << "requests_total=" << all << " avg_rps=" << fmt(all / std::max(up, 0.001), 1) << "\n"
      << "proxied=" << m.proxied() << " (2xx=" << m.ok2xx() << " 5xx=" << m.err5xx()
      << " other=" << m.other() << ")  rejected_429=" << m.rej429()
      << " rejected_503=" << m.rej503() << "\n"
      << "gateway_latency_ms p50=" << fmt(m.percentileMs(0.50)) << " p95=" << fmt(m.percentileMs(0.95))
      << " p99=" << fmt(m.percentileMs(0.99)) << " max=" << fmt(m.maxMs()) << "\n"
      << "rate_limiter allowed=" << rl.allowed() << " denied_by_token_bucket=" << rl.deniedToken()
      << " denied_by_window=" << rl.deniedWindow() << " tracked_clients=" << rl.trackedClients() << "\n";
    for (size_t i = 0; i < BACKENDS.size(); ++i)
        o << "backend " << BACKENDS[i].name() << " healthy=" << BACKENDS[i].healthy.load()
          << " breaker=" << BREAKERS[i].stateName() << " active=" << BACKENDS[i].activeConns.load()
          << " served=" << BACKENDS[i].served.load() << " failed=" << BACKENDS[i].failed.load() << "\n";
    return o.str();
}

// =============================================================================
// SECTION 6: HELPERS
// =============================================================================

// Pull the numeric status code out of "HTTP/1.1 200 OK...". 0 = unparseable/empty.
static int parseStatus(const std::string& resp) {
    if (resp.size() < 12 || resp.compare(0, 5, "HTTP/") != 0) return 0;
    auto sp = resp.find(' ');
    return sp == std::string::npos ? 0 : std::atoi(resp.c_str() + sp + 1);
}

// Only idempotent requests are safe to send twice, so only those get a retry.
static bool isIdempotent(const std::string& method) {
    return method == "GET" || method == "HEAD" || method == "OPTIONS";
}

// =============================================================================
// SECTION 7: MAIN - wire everything together
// =============================================================================

int main(int argc, char** argv) {
    try { parseArgs(argc, argv); }
    catch (const std::exception& e) { std::cerr << "error: " << e.what() << "\n"; usage(); return 1; }

    // Writing to a socket whose peer already closed would otherwise kill the
    // whole process with SIGPIPE. We want send() to just return an error.
    signal(SIGPIPE, SIG_IGN);

    initLogger();

    for (auto& [ip, port] : CFG.backends) {          // build in place (atomics can't be copied)
        BACKENDS.emplace_back();
        BACKENDS.back().ip = ip;
        BACKENDS.back().port = port;
        BREAKERS.emplace_back(ip + ":" + std::to_string(port));
    }

    RateLimiter  rateLimiter;
    LoadBalancer lb;
    Metrics      metrics;

    std::thread(healthCheckLoop).detach();

    if (!CFG.quiet) {
        std::thread([&]() {
            uint64_t lastTotal = 0;
            while (true) {
                std::this_thread::sleep_for(std::chrono::seconds(10));
                uint64_t total = metrics.proxied() + metrics.rej429() + metrics.rej503();
                writelog(LogComponent::Metrics,
                    "last10s_rps=" + fmt((total - lastTotal) / 10.0, 1) +
                    " total=" + std::to_string(total) +
                    " p50=" + fmt(metrics.percentileMs(0.50)) + "ms" +
                    " p95=" + fmt(metrics.percentileMs(0.95)) + "ms" +
                    " p99=" + fmt(metrics.percentileMs(0.99)) + "ms");
                lastTotal = total;
            }
        }).detach();
    }

    Server server(CFG.port);

    // Called once per request, on that connection's own thread.
    server.onRequest([&](const ParsedRequest& req) -> std::string {

        // Built-in introspection endpoint (not proxied, not rate limited).
        if (req.path == "/__gateway/stats")
            return buildHTTPResponse(200, "OK", buildReport(metrics, rateLimiter));

        auto t0 = Clock::now();

        // ---- 1. RATE LIMIT ---------------------------------------------------
        if (!rateLimiter.allow(req.clientIp)) {
            metrics.recordRejected(429);
            return buildHTTPResponse(429, "Too Many Requests", "rate limit exceeded\n");
        }

        // ---- 2/3/4. PICK, GUARD, FORWARD (at most 2 attempts) ----------------
        // `usable` = healthy backends we haven't already tried for this request.
        std::vector<char> usable(BACKENDS.size());
        for (size_t i = 0; i < BACKENDS.size(); ++i) usable[i] = BACKENDS[i].healthy.load();

        std::string resp;
        int status = 0;
        bool reachedBackend = false;

        for (int attempt = 0; attempt < 2; ++attempt) {
            // Ask the load balancer; if the chosen backend's breaker is OPEN,
            // cross it off and ask again.
            int idx = -1;
            while (true) {
                int c = lb.pick(req.clientIp, usable);
                if (c < 0) break;
                if (BREAKERS[c].allowRequest()) { idx = c; break; }
                usable[c] = 0;
            }
            if (idx < 0) break;                        // nobody left to try

            reachedBackend = true;
            BACKENDS[idx].activeConns++;
            resp = forwardToBackend(BACKENDS[idx].ip, BACKENDS[idx].port, req.raw);
            BACKENDS[idx].activeConns--;

            status = parseStatus(resp);
            bool failed = (status == 0 || status >= 500);   // empty reply, 5xx, gateway-made 502
            if (failed) { BACKENDS[idx].failed++; BREAKERS[idx].onFailure(); }
            else        { BACKENDS[idx].served++; BREAKERS[idx].onSuccess(); }

            if (!failed) break;                        // success: done
            usable[idx] = 0;                           // never retry on the same backend
            if (!isIdempotent(req.method)) break;      // POST etc.: don't risk a double-send
        }

        // ---- 5. RESPOND + METRICS -------------------------------------------
        if (!reachedBackend) {                         // every backend down or breaker-open
            metrics.recordRejected(503);
            return buildHTTPResponse(503, "Service Unavailable", "no available backend\n");
        }
        if (status == 0) resp = buildHTTPResponse(502, "Bad Gateway", "backend sent no valid response\n"), status = 502;

        double ms = std::chrono::duration<double, std::milli>(Clock::now() - t0).count();
        metrics.recordProxied(ms, status);
        return resp;
    });

    writelog(LogComponent::Server, "gateway listening on :" + std::to_string(CFG.port) + " with " +
             std::to_string(BACKENDS.size()) + " backends");
    server.start();                                    // blocks: the accept() loop
    return 0;
}
