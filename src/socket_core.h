#pragma once
#include <string>
#include <unordered_map>
#include <functional>
#include <thread>
#include <atomic>

// ─── what you get from every incoming request ────────────────────────────────
struct ParsedRequest {
    std::string method;   // GET / POST / etc.
    std::string path;     // /api/users
    std::string body;
    std::string clientIp;
    std::string raw;      // full raw request (to forward unchanged)
    std::unordered_map<std::string, std::string> headers;
};

// ─── your callbacks ──────────────────────────────────────────────────────────
// Return a full HTTP response string from requestHandler.
// Return true/false from healthHandler (is backend alive?).
using RequestHandler     = std::function<std::string(const ParsedRequest&)>;
using HealthCheckHandler = std::function<bool(const std::string& ip, int port)>;

// ─── helpers used by the request pipeline ─────────────────────────────────────
ParsedRequest parseHTTP(const std::string& raw);
std::string   forwardToBackend(const std::string& ip, int port, const std::string& rawRequest);
std::string   buildHTTPResponse(int status, const std::string& statusText, const std::string& body);

// ─── the server ──────────────────────────────────────────────────────────────
class Server {
public:
    explicit Server(int port);
    void onRequest(RequestHandler handler);
    void onHealthCheck(HealthCheckHandler handler);
    void start();   // blocks; runs accept loop
    void stop();
private:
    void handleClient(int clientFd, const std::string& clientIp);
    int  port_, listenFd_;
    std::atomic<bool> running_;
    RequestHandler     requestHandler_;
    HealthCheckHandler healthHandler_;
};
