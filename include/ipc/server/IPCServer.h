/**
 * @file IPCServer.h
 * @brief High-performance IPC Server using Unix Domain Sockets
 * 
 * Features:
 * - AF_UNIX with SOCK_SEQPACKET for message boundary preservation
 * - One thread per client for simple, blocking I/O
 * - Blocking sockets for simplicity
 * - Lock-free concurrent queue for message processing
 * - Support for 200+ concurrent clients
 * - Packet sizes from 10 bytes to 8 KB
 */

#ifndef IPC_SERVER_IPCSERVER_H
#define IPC_SERVER_IPCSERVER_H

#include "ipc/common/Message.h"
#include "ipc/common/Types.h"
#include "ipc/common/Constants.h"
#include <string>
#include <atomic>
#include <memory>
#include <thread>
#include <unordered_map>
#include <mutex>

// Include concurrent queue before forward declarations
#include "concurrentqueue.h"

namespace ipc {

/**
 * @brief Client connection information
 */
struct ClientInfo {
    int fd = INVALID_FD;
    ConnectionId connection_id = INVALID_CONNECTION_ID;
};

/**
 * @brief High-performance IPC Server
 * 
 * Architecture:
 * - Acceptor Thread: Handles accept() and spawns client threads
 * - Client Threads (one per client): Handle blocking recv()
 * - Processor Thread: Dequeues messages and invokes callback
 * 
 * Message Flow:
 * 1. Client connects via Unix Domain Socket
 * 2. Server accepts and assigns unique client_id
 * 3. Dedicated client thread receives packets via blocking recv()
 * 4. Each packet creates a Message object
 * 5. Message pushed to concurrent queue
 * 6. Processor thread dequeues and calls user callback
 */
class IPCServer {
public:
    /**
     * @brief Construct IPC Server
     */
    IPCServer();
    
    /**
     * @brief Destructor
     */
    ~IPCServer();
    
    // Non-copyable, movable
    IPCServer(const IPCServer&) = delete;
    IPCServer& operator=(const IPCServer&) = delete;
    IPCServer(IPCServer&&) noexcept;
    IPCServer& operator=(IPCServer&&) noexcept;
    
    /**
     * @brief Start the server
     * @param socket_path Path for Unix domain socket
     * @return Result code
     */
    Result start(std::string_view socket_path);
    
    /**
     * @brief Stop the server gracefully
     */
    void stop();
    
    /**
     * @brief Set message callback function
     * @param callback Function called for each received message
     */
    void set_callback(MessageCallback callback);
    
    /**
     * @brief Get number of connected clients
     * @return Client count
     */
    size_t client_count() const;
    
    /**
     * @brief Get current queue size
     * @return Number of messages in queue
     */
    size_t queue_size() const;
    
    /**
     * @brief Check if server is running
     * @return true if running
     */
    bool is_running() const;
    
private:
    /**
     * @brief Acceptor loop - accepts connections and spawns client threads
     */
    void acceptorLoop();
    
    /**
     * @brief Client handler loop - handles one client connection
     * @param client_id Client ID
     * @param conn_id Connection ID
     * @param client_fd Client file descriptor
     */
    void clientHandler(ClientId client_id, ConnectionId conn_id, int client_fd);
    
    /**
     * @brief Message processor loop
     */
    void processorLoop();
    
private:
    // Configuration
    std::string socket_path_;
    size_t queue_capacity_;
    size_t max_clients_;
    
    // State
    std::atomic<bool> running_{false};
    
    // Socket
    int server_fd_ = INVALID_FD;
    
    // Threads
    std::thread acceptor_thread_;
    std::thread processor_thread_;
    
    // Concurrent queue for messages
    moodycamel::ConcurrentQueue<Message::Ptr> queue_;
    
    // Client management
    std::unordered_map<ClientId, ClientInfo> clients_;
    mutable std::mutex clients_mutex_;
    
    std::unordered_map<ClientId, std::thread> client_threads_;
    mutable std::mutex threads_mutex_;
    
    // Callback
    MessageCallback callback_;
};

} // namespace ipc

#endif // IPC_SERVER_IPCSERVER_H
