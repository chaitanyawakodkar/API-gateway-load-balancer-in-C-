// socket_core.cpp
// Networking layer: the TCP accept loop, HTTP request parsing, and backend
// forwarding. Exposes a Server class with two callbacks:
//   onRequest(request)      -> routing + load balancing (see main.cpp)
//   onHealthCheck(ip, port) -> backend health probing

#include "socket_core.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <stdexcept>
#include <iostream>
#include <sstream>

// ─── parse a raw HTTP request off the wire ──────────────────────────────────
ParsedRequest parseHTTP(const std::string& raw) {
    ParsedRequest req;
    std::istringstream ss(raw);
    std::string line;

    // request line: METHOD /path HTTP/1.1
    std::getline(ss, line);
    if (!line.empty() && line.back() == '\r') line.pop_back();
    std::istringstream rl(line);
    rl >> req.method >> req.path;

    // headers
    while (std::getline(ss, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty()) break; // blank line = end of headers
        auto colon = line.find(':');
        if (colon != std::string::npos)
            req.headers[line.substr(0, colon)] = line.substr(colon + 2);
    }

    // body (everything after blank line)
    std::ostringstream body;
    body << ss.rdbuf();
    req.body = body.str();
    return req;
}

// ─── forward a request to a backend server, return its full response ─────────
std::string forwardToBackend(const std::string& backendIp, int backendPort,
                              const std::string& rawRequest) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return buildHTTPResponse(502, "Bad Gateway", "Cannot create socket");

    struct timeval tv{5, 0}; // 5-second timeout
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(backendPort);
    inet_pton(AF_INET, backendIp.c_str(), &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return buildHTTPResponse(502, "Bad Gateway", "Backend unreachable");
    }

    send(fd, rawRequest.c_str(), rawRequest.size(), 0);

    std::string response;
    char buf[4096];
    ssize_t n;
    while ((n = recv(fd, buf, sizeof(buf), 0)) > 0)
        response.append(buf, n);

    close(fd);
    return response;
}

// ─── build a simple HTTP response string ─────────────────────────────────────
std::string buildHTTPResponse(int status, const std::string& statusText,
                               const std::string& body) {
    return "HTTP/1.1 " + std::to_string(status) + " " + statusText + "\r\n"
           "Content-Length: " + std::to_string(body.size()) + "\r\n"
           "Content-Type: text/plain\r\n"
           "Connection: close\r\n\r\n" + body;
}

// ─── Server: binds, accepts connections, spawns a thread per connection ───────
Server::Server(int port) : port_(port), running_(false) {}

void Server::onRequest(RequestHandler handler)     { requestHandler_  = handler; }
void Server::onHealthCheck(HealthCheckHandler h)   { healthHandler_   = h;       }

void Server::start() {
    listenFd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listenFd_ < 0) throw std::runtime_error("socket() failed");

    int opt = 1;
    setsockopt(listenFd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port_);

    if (bind(listenFd_, (sockaddr*)&addr, sizeof(addr)) < 0)
        throw std::runtime_error("bind() failed on port " + std::to_string(port_));
    if (listen(listenFd_, SOMAXCONN) < 0)
        throw std::runtime_error("listen() failed");

    running_ = true;
    std::cout << "[gateway] listening on :" << port_ << "\n";

    while (running_) {
        sockaddr_in clientAddr{};
        socklen_t   clientLen = sizeof(clientAddr);
        int clientFd = accept(listenFd_, (sockaddr*)&clientAddr, &clientLen);
        if (clientFd < 0) continue;

        std::string clientIp = inet_ntoa(clientAddr.sin_addr);
        std::thread([this, clientFd, clientIp]() {
            handleClient(clientFd, clientIp);
        }).detach();
    }
}

void Server::stop() { running_ = false; close(listenFd_); }

void Server::handleClient(int clientFd, const std::string& clientIp) {
    // read the raw HTTP request
    std::string raw;
    char buf[8192];
    ssize_t n = recv(clientFd, buf, sizeof(buf) - 1, 0);
    if (n > 0) { buf[n] = '\0'; raw = buf; }

    if (raw.empty()) { close(clientFd); return; }

    ParsedRequest req = parseHTTP(raw);
    req.clientIp      = clientIp;
    req.raw           = raw;

    std::string response;
    if (requestHandler_) response = requestHandler_(req);
    else                 response = buildHTTPResponse(200, "OK", "gateway running");

    send(clientFd, response.c_str(), response.size(), 0);
    close(clientFd);
}
