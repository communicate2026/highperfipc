/**
 * @file server_example.cpp
 * @brief Example IPC Server usage
 */

#include "ipc/IPC.h"
#include <iostream>
#include <csignal>
#include <atomic>

std::atomic<bool> g_running{true};

void signalHandler(int signum) {
    g_running = false;
}

int main() {
    // Setup signal handler
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    
    const std::string socket_path = "/tmp/ipc_server.sock";
    
    std::cout << "Starting IPC Server on: " << socket_path << std::endl;
    
    ipc::IPCServer server;
    
    // Set message callback
    server.setCallback([](ipc::ClientId client_id, std::span<const uint8_t> data) {
        std::cout << "[Server] Received from client " << client_id 
                  << ": " << std::string(data.begin(), data.end()) 
                  << " (" << data.size() << " bytes)" << std::endl;
    });
    
    // Start server
    if (server.start(socket_path) != ipc::Result::Success) {
        std::cerr << "Failed to start server" << std::endl;
        return 1;
    }
    
    std::cout << "Server started. Press Ctrl+C to stop." << std::endl;
    std::cout << "Waiting for messages..." << std::endl;
    
    while (g_running && server.isRunning()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        // Print stats every second
        static auto last_print = std::chrono::steady_clock::now();
        auto now = std::chrono::steady_clock::now();
        
        if (now - last_print >= std::chrono::seconds(1)) {
            std::cout << "[Stats] Clients: " << server.clientCount() 
                      << ", Queue size: " << server.queueSize() << std::endl;
            last_print = now;
        }
    }
    
    std::cout << "Stopping server..." << std::endl;
    server.stop();
    std::cout << "Server stopped." << std::endl;
    
    return 0;
}
