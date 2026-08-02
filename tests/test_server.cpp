/**
 * @file test_server.cpp
 * @brief Unit tests for IPC Server
 */

#include "ipc/IPC.h"
#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>

void test_server_start_stop() {
    std::cout << "Test: Server start/stop... ";
    
    ipc::IPCServer server;
    assert(!server.isRunning());
    
    const std::string socket_path = "/tmp/test_server_1.sock";
    assert(server.start(socket_path) == ipc::Result::Success);
    assert(server.isRunning());
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    assert(server.stop() == ipc::Result::Success);
    assert(!server.isRunning());
    
    std::cout << "PASSED" << std::endl;
}

void test_server_double_start() {
    std::cout << "Test: Server double start prevention... ";
    
    ipc::IPCServer server;
    const std::string socket_path = "/tmp/test_server_2.sock";
    
    assert(server.start(socket_path) == ipc::Result::Success);
    assert(server.start(socket_path) == ipc::Result::InvalidState);
    
    server.stop();
    std::cout << "PASSED" << std::endl;
}

void test_server_callback() {
    std::cout << "Test: Server callback... ";
    
    ipc::IPCServer server;
    const std::string socket_path = "/tmp/test_server_3.sock";
    
    std::atomic<int> message_count{0};
    
    server.setCallback([&](ipc::ClientId client_id, std::span<const uint8_t> data) {
        message_count.fetch_add(1, std::memory_order_relaxed);
    });
    
    assert(server.start(socket_path) == ipc::Result::Success);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Create a client and send a message
    ipc::IPCClient client;
    assert(client.connect(socket_path, 1000) == ipc::Result::Success);
    
    std::string msg = "test message";
    assert(client.send(std::as_bytes(std::span{msg})) == ipc::Result::Success);
    
    // Wait for message to be processed
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    assert(message_count.load() >= 1);
    
    client.disconnect();
    server.stop();
    
    std::cout << "PASSED" << std::endl;
}

void test_server_client_count() {
    std::cout << "Test: Server client count... ";
    
    ipc::IPCServer server;
    const std::string socket_path = "/tmp/test_server_4.sock";
    
    assert(server.start(socket_path) == ipc::Result::Success);
    assert(server.clientCount() == 0);
    
    // Connect multiple clients
    std::vector<ipc::IPCClient> clients(5);
    for (auto& c : clients) {
        assert(c.connect(socket_path, 1000) == ipc::Result::Success);
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    assert(server.clientCount() == 5);
    
    // Disconnect some
    clients[0].disconnect();
    clients[1].disconnect();
    
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    assert(server.clientCount() == 3);
    
    for (auto& c : clients) {
        if (c.isConnected()) c.disconnect();
    }
    server.stop();
    
    std::cout << "PASSED" << std::endl;
}

int main() {
    std::cout << "=== IPC Server Tests ===" << std::endl << std::endl;
    
    test_server_start_stop();
    test_server_double_start();
    test_server_callback();
    test_server_client_count();
    
    std::cout << "\nAll tests passed!" << std::endl;
    return 0;
}
