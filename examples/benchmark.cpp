/**
 * @file benchmark.cpp
 * @brief Performance benchmark for IPC library
 */

#include "ipc/IPC.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <random>

constexpr int NUM_CLIENTS = 50;
constexpr int MESSAGES_PER_CLIENT = 10000;
constexpr size_t MESSAGE_SIZE = 256;

std::atomic<int> g_messages_received{0};
std::atomic<bool> g_running{true};

int main() {
    const std::string socket_path = "/tmp/ipc_benchmark.sock";
    
    std::cout << "=== IPC Library Benchmark ===" << std::endl;
    std::cout << "Clients: " << NUM_CLIENTS << std::endl;
    std::cout << "Messages per client: " << MESSAGES_PER_CLIENT << std::endl;
    std::cout << "Message size: " << MESSAGE_SIZE << " bytes" << std::endl;
    std::cout << "Total messages: " << NUM_CLIENTS * MESSAGES_PER_CLIENT << std::endl;
    std::cout << std::endl;
    
    // Start server
    ipc::IPCServer server;
    
    server.setCallback([](ipc::ClientId client_id, std::span<const uint8_t> data) {
        g_messages_received.fetch_add(1, std::memory_order_relaxed);
    });
    
    if (server.start(socket_path) != ipc::Result::Success) {
        std::cerr << "Failed to start server" << std::endl;
        return 1;
    }
    
    std::cout << "Server started, waiting for clients..." << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Create clients
    std::vector<std::thread> client_threads;
    
    auto client_func = [&](int id) {
        ipc::IPCClient client;
        
        if (client.connect(socket_path, 5000) != ipc::Result::Success) {
            std::cerr << "Client " << id << " failed to connect" << std::endl;
            return;
        }
        
        // Generate random message data
        std::vector<uint8_t> data(MESSAGE_SIZE);
        std::random_device rd;
        std::mt19937 gen(rd() + id);  // Use id to seed differently per thread
        std::uniform_int_distribution<> dis(0, 255);
        
        for (int i = 0; i < MESSAGES_PER_CLIENT; ++i) {
            for (auto& byte : data) {
                byte = static_cast<uint8_t>(dis(gen));
            }
            
            if (client.send(data.data(), data.size()) != ipc::Result::Success) {
                std::cerr << "Client " << id << " failed to send message " << i << std::endl;
                break;
            }
        }
        
        client.disconnect();
    };
    
    // Start timing
    auto start_time = std::chrono::steady_clock::now();
    
    // Launch client threads
    std::cout << "Launching " << NUM_CLIENTS << " client threads..." << std::endl;
    for (int i = 0; i < NUM_CLIENTS; ++i) {
        client_threads.emplace_back(client_func, i);
    }
    
    // Wait for all clients to finish
    for (auto& t : client_threads) {
        t.join();
    }
    
    auto end_time = std::chrono::steady_clock::now();
    
    // Wait for server to process remaining messages
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Calculate results
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    int total_messages = g_messages_received.load();
    double seconds = duration / 1000.0;
    double messages_per_second = total_messages / seconds;
    double bytes_per_second = (total_messages * MESSAGE_SIZE) / seconds / (1024 * 1024);
    
    std::cout << "\n=== Results ===" << std::endl;
    std::cout << "Duration: " << seconds << " seconds" << std::endl;
    std::cout << "Messages received: " << total_messages << std::endl;
    std::cout << "Throughput: " << messages_per_second << " msg/sec" << std::endl;
    std::cout << "Bandwidth: " << bytes_per_second << " MB/sec" << std::endl;
    std::cout << "Final queue size: " << server.queueSize() << std::endl;
    
    // Cleanup
    server.stop();
    unlink(socket_path.c_str());
    
    return 0;
}
