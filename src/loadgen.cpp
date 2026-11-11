// loadgen.cpp - closed-loop HTTP load generator (so we don't depend on wrk/ab/hey).
//
// Usage: ./loadgen [--host=127.0.0.1] [--port=8080] [--path=/] [--conns=50]
//                  [--seconds=10] [--src-ips=1]
//
// Model: `conns` worker threads. Each thread loops: connect -> send GET ->
// read the full response -> record latency -> repeat, until time is up. So
// exactly `conns` requests are in flight at any moment (a real concurrency of
// `conns`, unlike a sequential loop). The gateway closes every connection after
// one response, so each request pays a fresh TCP handshake; numbers are for that
// connection-per-request model.
//
// --src-ips=K makes worker i bind to source address 127.0.0.(1 + i % K). The
// whole 127.0.0.0/8 block is loopback on Linux, so this simulates K different
// clients (needed to test per-client rate limiting and consistent hashing from
// one machine).
//
// Latency is measured client-side from just before connect() to the end of the
// response: true end-to-end time including the TCP handshake.

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <netinet/in.h>
#include <set>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using Clock = std::chrono::steady_clock;

struct Result {
    std::vector<double> latMs;                 // latencies of completed requests (any status)
    uint64_t ok2xx = 0, r429 = 0, s5xx = 0, otherStatus = 0, errors = 0;
    std::map<int, uint64_t> perBackend;        // backend port -> responses (2xx only)
    std::set<int> backendsSeen;                // distinct backends this worker saw
};

static std::string arg(int argc, char** argv, const std::string& key, const std::string& def) {
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a.rfind("--" + key + "=", 0) == 0) return a.substr(key.size() + 3);
    }
    return def;
}

int main(int argc, char** argv) {
    std::string host   = arg(argc, argv, "host", "127.0.0.1");
    int port           = std::stoi(arg(argc, argv, "port", "8080"));
    std::string path   = arg(argc, argv, "path", "/");
    int conns          = std::stoi(arg(argc, argv, "conns", "50"));
    int seconds        = std::stoi(arg(argc, argv, "seconds", "10"));
    int srcIps         = std::stoi(arg(argc, argv, "src-ips", "1"));

    std::string request = "GET " + path + " HTTP/1.1\r\nHost: " + host +
                          "\r\nConnection: close\r\nUser-Agent: loadgen\r\n\r\n";

    std::vector<Result> results(conns);
    std::vector<std::thread> threads;
    auto start = Clock::now();
    auto deadline = start + std::chrono::seconds(seconds);

    for (int t = 0; t < conns; ++t) {
        threads.emplace_back([&, t]() {
            Result& r = results[t];
            std::string srcIp = "127.0.0." + std::to_string(1 + (t % std::max(1, srcIps)));
            char buf[8192];
            while (Clock::now() < deadline) {
                auto t0 = Clock::now();
                int fd = socket(AF_INET, SOCK_STREAM, 0);
                if (fd < 0) { r.errors++; continue; }

                timeval tv{10, 0};
                setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

                if (srcIps > 1) {                          // choose the simulated client address
                    sockaddr_in src{};
                    src.sin_family = AF_INET;
                    inet_pton(AF_INET, srcIp.c_str(), &src.sin_addr);
                    bind(fd, (sockaddr*)&src, sizeof(src));
                }

                sockaddr_in dst{};
                dst.sin_family = AF_INET;
                dst.sin_port = htons(port);
                inet_pton(AF_INET, host.c_str(), &dst.sin_addr);

                if (connect(fd, (sockaddr*)&dst, sizeof(dst)) < 0 ||
                    send(fd, request.c_str(), request.size(), 0) < 0) {
                    r.errors++; close(fd); continue;
                }

                std::string resp;
                ssize_t n;
                while ((n = recv(fd, buf, sizeof(buf), 0)) > 0) resp.append(buf, n);
                close(fd);

                if (resp.size() < 12) { r.errors++; continue; }     // empty/timeout
                r.latMs.push_back(std::chrono::duration<double, std::milli>(Clock::now() - t0).count());

                int status = std::atoi(resp.c_str() + resp.find(' ') + 1);
                if (status >= 200 && status < 300) {
                    r.ok2xx++;
                    auto p = resp.find("backend :");
                    if (p != std::string::npos) {
                        int bp = std::atoi(resp.c_str() + p + 9);
                        r.perBackend[bp]++;
                        r.backendsSeen.insert(bp);
                    }
                } else if (status == 429) r.r429++;
                else if (status >= 500)   r.s5xx++;
                else                      r.otherStatus++;
            }
        });
    }
    for (auto& th : threads) th.join();
    double elapsed = std::chrono::duration<double>(Clock::now() - start).count();

    // ---- merge ----
    std::vector<double> lat;
    uint64_t ok = 0, r429 = 0, s5 = 0, oth = 0, err = 0;
    std::map<int, uint64_t> perBackend;
    for (auto& r : results) {
        lat.insert(lat.end(), r.latMs.begin(), r.latMs.end());
        ok += r.ok2xx; r429 += r.r429; s5 += r.s5xx; oth += r.otherStatus; err += r.errors;
        for (auto& [p, c] : r.perBackend) perBackend[p] += c;
    }
    std::sort(lat.begin(), lat.end());
    auto pct = [&](double p) { return lat.empty() ? 0.0 : lat[std::min(lat.size() - 1, (size_t)(p * lat.size()))]; };

    uint64_t completed = ok + r429 + s5 + oth;
    std::cout.setf(std::ios::fixed); std::cout.precision(2);
    std::cout << "conns=" << conns << " seconds=" << elapsed << " src_ips=" << srcIps << "\n"
              << "completed=" << completed << " throughput=" << completed / elapsed << " req/s\n"
              << "  2xx=" << ok << "  429=" << r429 << "  5xx=" << s5 << "  other=" << oth
              << "  connection_errors=" << err << "\n"
              << "latency_ms(all completed) p50=" << pct(0.50) << " p95=" << pct(0.95)
              << " p99=" << pct(0.99) << " max=" << (lat.empty() ? 0.0 : lat.back()) << "\n"
              << "2xx throughput=" << ok / elapsed << " req/s\n";
    if (!perBackend.empty()) {
        std::cout << "2xx per backend:";
        for (auto& [p, c] : perBackend) std::cout << "  :" << p << "=" << c;
        std::cout << "\n";
    }
    if (srcIps > 1) {                                       // stickiness check for consistent hashing
        std::map<std::string, std::set<int>> perSrc;
        for (int t = 0; t < conns; ++t) {
            std::string ip = "127.0.0." + std::to_string(1 + (t % srcIps));
            perSrc[ip].insert(results[t].backendsSeen.begin(), results[t].backendsSeen.end());
        }
        std::cout << "distinct backends seen per source IP:";
        for (auto& [ip, s] : perSrc) std::cout << "  " << ip.substr(8) << "->" << s.size();
        std::cout << "\n";
    }
    return 0;
}
