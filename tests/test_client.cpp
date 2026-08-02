/**
 * @file test_client.cpp
 * @brief Unit tests for IPC Client
 */

#include "ipc/IPC.h"
#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>

void test_client_connect_disconnect() {
    std::cout << "Test: Client connect/disconnect... ";
    
    ipc::IPCClient client;
    assert(!client.isConnected());
    
    // Try to connect to non-existent server (should fail)
    auto result = client.connect("/tmp/nonexistent.sock", 100);
    assert(result != ipc::Result::Success);
    assert(!client.isConnected());
    
    std::cout << "PASSED" << std::endl;
}

void test_client_with_server() {
    std::cout << "Test: Client with server... ";
    
    // Start a server first
    ipc::IPCServer server;
    const std::string socket_path = "/tmp/test_client_1.sock";
    assert(server.start(socket_path) == ipc::Result::Success);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Connect client
    ipc::IPCClient client;
    assert(client.connect(socket_path, 1000) == ipc::Result::Success);
    assert(client.isConnected());
    
    // Send message
    std::string msg = "hello";
    assert(client.send(reinterpret_cast<const uint8_t*>(msg.data()), msg.size()) == ipc::Result::Success);
    
    // Disconnect
    assert(client.disconnect() == ipc::Result::Success);
    assert(!client.isConnected());
    
    server.stop();
    std::cout << "PASSED" << std::endl;
}

void test_client_send_timeout() {
    std::cout << "Test: Client send timeout... ";
    
    ipc::IPCServer server;
    const std::string socket_path = "/tmp/test_client_2.sock";
    assert(server.start(socket_path) == ipc::Result::Success);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    ipc::IPCClient client;
    assert(client.connect(socket_path, 1000) == ipc::Result::Success);
    
    // Set very short timeout
    client.setSendTimeout(1);
    
    // Disconnect server to cause timeout
    server.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    std::string msg = "test";
    auto result = client.send(reinterpret_cast<const uint8_t*>(msg.data()), msg.size());
    assert(result == ipc::Result::Disconnected || result == ipc::Result::Error);
    
    std::cout << "PASSED" << std::endl;
}

void test_client_reconnect() {
    std::cout << "Test: Client reconnect... ";
    
    // Start server
    ipc::IPCServer server;
    const std::string socket_path = "/tmp/test_client_3.sock";
    assert(server.start(socket_path) == ipc::Result::Success);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Connect client
    ipc::IPCClient client;
    assert(client.connect(socket_path, 1000) == ipc::Result::Success);
    
    // Stop server
    server.stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    assert(!client.isConnected());
    
    // Restart server
    assert(server.start(socket_path) == ipc::Result::Success);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Reconnect
    assert(client.reconnect() == ipc::Result::Success);
    assert(client.isConnected());
    
    client.disconnect();
    server.stop();
    
    std::cout << "PASSED" << std::endl;
}

int main() {
    std::cout << "=== IPC Client Tests ===" << std::endl << std::endl;
    
    test_client_connect_disconnect();
    test_client_with_server();
    test_client_send_timeout();
    test_client_reconnect();
    
    std::cout << "\nAll tests passed!" << std::endl;
    return 0;
}
