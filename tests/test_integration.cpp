/**
 * @file test_integration.cpp
 * @brief Integration tests for IPC library
 */

#include "ipc/IPC.h"
#include <iostream>
#include <cassert>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>

void test_multiple_clients() {
    std::cout << "Test: Multiple concurrent clients... ";
    
    const int NUM_CLIENTS = 10;
    const std::string socket_path = "/tmp/test_integration_1.sock";
    
    std::atomic<int> message_count{0};
    
    ipc::IPCServer server;
    server.setCallback([&](ipc::ClientId client_id, std::span<const uint8_t> data) {
        message_count.fetch_add(1, std::memory_order_relaxed);
    });
    
    assert(server.start(socket_path) == ipc::Result::Success);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Create and connect multiple clients
    std::vector<std::unique_ptr<ipc::IPCClient>> clients;
    for (int i = 0; i < NUM_CLIENTS; ++i) {
        auto client = std::make_unique<ipc::IPCClient>();
        assert(client->connect(socket_path, 1000) == ipc::Result::Success);
        clients.push_back(std::move(client));
    }
    
    assert(server.clientCount() == NUM_CLIENTS);
    
    // Each client sends messages
    for (auto& client : clients) {
        for (int i = 0; i < 10; ++i) {
            std::string msg = "client message";
            assert(client->send(std::as_bytes(std::span{msg})) == ipc::Result::Success);
        }
    }
    
    // Wait for processing
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    assert(message_count.load() == NUM_CLIENTS * 10);
    
    // Cleanup
    for (auto& client : clients) {
        client->disconnect();
    }
    server.stop();
    
    std::cout << "PASSED" << std::endl;
}

void test_message_boundaries() {
    std::cout << "Test: Message boundary preservation... ";
    
    const std::string socket_path = "/tmp/test_integration_2.sock";
    std::atomic<int> received_count{0};
    std::vector<size_t> received_sizes;
    std::mutex sizes_mutex;
    
    ipc::IPCServer server;
    server.setCallback([&](ipc::ClientId client_id, std::span<const uint8_t> data) {
        received_count.fetch_add(1, std::memory_order_relaxed);
        {
            std::lock_guard<std::mutex> lock(sizes_mutex);
            received_sizes.push_back(data.size());
        }
    });
    
    assert(server.start(socket_path) == ipc::Result::Success);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    ipc::IPCClient client;
    assert(client.connect(socket_path, 1000) == ipc::Result::Success);
    
    // Send messages of different sizes
    std::vector<size_t> sent_sizes = {10, 100, 500, 1000, 2000};
    
    for (size_t size : sent_sizes) {
        std::vector<uint8_t> data(size, 0x42);
        assert(client.send(data) == ipc::Result::Success);
    }
    
    // Wait for processing
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    assert(received_count.load() == 5);
    
    {
        std::lock_guard<std::mutex> lock(sizes_mutex);
        assert(received_sizes.size() == 5);
        for (size_t i = 0; i < sent_sizes.size(); ++i) {
            assert(received_sizes[i] == sent_sizes[i]);
        }
    }
    
    client.disconnect();
    server.stop();
    
    std::cout << "PASSED" << std::endl;
}

void test_high_throughput() {
    std::cout << "Test: High throughput... ";
    
    const int NUM_MESSAGES = 10000;
    const std::string socket_path = "/tmp/test_integration_3.sock";
    
    std::atomic<int> received{0};
    
    ipc::IPCServer server;
    server.setCallback([&](ipc::ClientId, std::span<const uint8_t>) {
        received.fetch_add(1, std::memory_order_relaxed);
    });
    
    assert(server.start(socket_path) == ipc::Result::Success);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    ipc::IPCClient client;
    assert(client.connect(socket_path, 1000) == ipc::Result::Success);
    
    auto start = std::chrono::steady_clock::now();
    
    std::string msg = "throughput test";
    for (int i = 0; i < NUM_MESSAGES; ++i) {
        if (client.send(reinterpret_cast<const uint8_t*>(msg.data()), msg.size()) != ipc::Result::Success) {
            break;
        }
    }
    
    // Wait for all messages to be processed
    int timeout = 5000;
    while (received.load() < NUM_MESSAGES && timeout > 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        timeout -= 10;
    }
    
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    
    double msgs_per_sec = (received.load() * 1000.0) / duration;
    
    std::cout << "(received " << received.load() << "/" << NUM_MESSAGES 
              << " in " << duration << "ms, " << msgs_per_sec << " msg/sec) ";
    
    assert(received.load() >= NUM_MESSAGES * 0.95);  // Allow 5% loss
    
    client.disconnect();
    server.stop();
    
    std::cout << "PASSED" << std::endl;
}

int main() {
    std::cout << "=== IPC Integration Tests ===" << std::endl << std::endl;
    
    test_multiple_clients();
    test_message_boundaries();
    test_high_throughput();
    
    std::cout << "\nAll integration tests passed!" << std::endl;
    return 0;
}
