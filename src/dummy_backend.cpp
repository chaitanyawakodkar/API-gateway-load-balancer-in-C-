// dummy_backend.cpp - a tiny HTTP server used to test the gateway.
//
// Usage: ./dummy_backend <port> [delay_ms] [fail_percent]
//   delay_ms      artificial processing time per request (simulates a slow backend)
//   fail_percent  0-100: percentage of requests answered with HTTP 500
//
// Examples:
//   ./dummy_backend 9001                 normal, instant
//   ./dummy_backend 9003 200             slow backend (200 ms per request)
//   ./dummy_backend 9002 0 100           backend that always returns 500
//
// Every response body contains "backend :<port>" so tests can see who answered.

#include <arpa/inet.h>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

int main(int argc, char* argv[]) {
    int port        = argc > 1 ? std::atoi(argv[1]) : 9001;
    int delayMs     = argc > 2 ? std::atoi(argv[2]) : 0;
    int failPercent = argc > 3 ? std::atoi(argv[3]) : 0;

    signal(SIGPIPE, SIG_IGN);   // the gateway's health probe connects and hangs up; don't die on it

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    if (bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    listen(fd, 1024);

    std::cout << "[backend] :" << port << " delay=" << delayMs << "ms fail=" << failPercent << "%\n";

    while (true) {
        int client = accept(fd, nullptr, nullptr);
        if (client < 0) continue;
        std::thread([client, port, delayMs, failPercent]() {
            char buf[4096];
            ssize_t n = recv(client, buf, sizeof(buf), 0);
            if (n > 0) {                                   // ignore bare connect/close probes
                if (delayMs > 0) std::this_thread::sleep_for(std::chrono::milliseconds(delayMs));
                bool fail = failPercent > 0 && (rand() % 100) < failPercent;
                std::string body = std::string(fail ? "error from" : "Hello from") +
                                   " backend :" + std::to_string(port) + "\n";
                std::string resp = std::string("HTTP/1.1 ") + (fail ? "500 Internal Server Error" : "200 OK") +
                                   "\r\nContent-Length: " + std::to_string(body.size()) +
                                   "\r\nConnection: close\r\n\r\n" + body;
                send(client, resp.c_str(), resp.size(), 0);
            }
            close(client);
        }).detach();
    }
}
