#pragma once
#include <string>
#include <fstream>
#include <iostream>
#include <mutex>
#include <chrono>

enum class LogComponent {
    RateLimiter,
    LoadBalancer,
    CircuitBreaker,
    HealthCheck,
    Metrics,
    Server
};

inline std::string componentName(LogComponent c) {
    switch(c) {
        case LogComponent::RateLimiter: return "RateLimiter";
        case LogComponent::LoadBalancer: return "LoadBalancer";
        case LogComponent::CircuitBreaker: return "CircuitBreaker";
        case LogComponent::HealthCheck: return "HealthCheck";
        case LogComponent::Metrics: return "Metrics";
        case LogComponent::Server: return "Server";
    }
    return "Unknown";
}

inline std::string timestamp() {
    auto now = std::chrono::system_clock::now();
    auto time = std::chrono::system_clock::to_time_t(now);
    char buf[20];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", localtime(&time));
    return std::string(buf);
}

static std::mutex logMutex;
static std::ofstream logFile;

inline void initLogger() {
    logFile.open("gateway.log", std::ios::app);
}

inline void writelog(LogComponent comp, const std::string& msg) {
    std::lock_guard<std::mutex> lock(logMutex);
    std::string logLine = "[" + timestamp() + "] [" + componentName(comp) + "] " + msg;
    
    // write to console
    std::cout << logLine << "\n";
    
    // write to file
    if (logFile.is_open()) {
        logFile << logLine << "\n";
        logFile.flush();
    }
}

inline void writeLog(LogComponent comp, const std::string& msg) {
    writelog(comp, msg);
}
