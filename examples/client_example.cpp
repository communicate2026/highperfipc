/**
 * @file client_example.cpp
 * @brief Example IPC Client usage
 */

#include "ipc/IPC.h"
#include <iostream>
#include <thread>
#include <chrono>

int main() {
    const std::string socket_path = "/tmp/ipc_server.sock";
    
    std::cout << "Starting IPC Client" << std::endl;
    
    ipc::IPCClient client;
    
    // Connect to server (blocking mode with timeout)
    std::cout << "Connecting to: " << socket_path << std::endl;
    
    if (client.connect(socket_path, 5000) != ipc::Result::Success) {
        std::cerr << "Failed to connect: " << client.getLastError().message() << std::endl;
        return 1;
    }
    
    std::cout << "Connected!" << std::endl;
    
    // Send some messages
    for (int i = 0; i < 10; ++i) {
        std::string message = "Hello from client! Message #" + std::to_string(i);
        
        if (client.send(reinterpret_cast<const uint8_t*>(message.data()), message.size()) != ipc::Result::Success) {
            std::cerr << "Failed to send message " << i << std::endl;
            break;
        }
        
        std::cout << "Sent: " << message << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    // Test auto-reconnect
    std::cout << "\nEnabling auto-reconnect..." << std::endl;
    client.enableAutoReconnect(true);
    
    // Simulate sending more messages
    for (int i = 10; i < 20; ++i) {
        std::string message = "Message #" + std::to_string(i);
        
        if (!client.isConnected()) {
            std::cout << "Disconnected, waiting for reconnect..." << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }
        
        if (client.send(reinterpret_cast<const uint8_t*>(message.data()), message.size()) == ipc::Result::Success) {
            std::cout << "Sent: " << message << std::endl;
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    // Disconnect
    std::cout << "\nDisconnecting..." << std::endl;
    client.disconnect();
    std::cout << "Disconnected." << std::endl;
    
    return 0;
}
