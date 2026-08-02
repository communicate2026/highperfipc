/**
 * @file IPCServer.h
 * @brief High-performance IPC Server using Unix Domain Sockets
 * 
 * Features:
 * - AF_UNIX with SOCK_SEQPACKET for message boundary preservation
 * - epoll (edge-triggered) for scalable I/O multiplexing
 * - Non-blocking sockets
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
    ClientId id = INVALID_CLIENT_ID;
    ConnectionId connection_id = INVALID_CONNECTION_ID;
    bool active = false;
};

/**
 * @brief High-performance IPC Server
 * 
 * Architecture:
 * - Network Thread: Handles accept(), epoll_wait(), recv()
 * - Processor Thread: Dequeues messages and invokes callback
 * 
 * Message Flow:
 * 1. Client connects via Unix Domain Socket
 * 2. Server accepts and assigns unique client_id
 * 3. Network thread receives packets via epoll
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
     * @return Result code
     */
    Result stop();
    
    /**
     * @brief Set message callback function
     * @param callback Function called for each received message
     */
    void setCallback(MessageCallback callback);
    
    /**
     * @brief Get number of connected clients
     * @return Client count
     */
    size_t clientCount() const;
    
    /**
     * @brief Get current queue size
     * @return Number of messages in queue
     */
    size_t queueSize() const;
    
    /**
     * @brief Check if server is running
     * @return true if running
     */
    bool isRunning() const;
    
private:
    /**
     * @brief Main network loop
     */
    void networkLoop();
    
    /**
     * @brief Message processor loop
     */
    void processorLoop();
    
    /**
     * @brief Handle new client connection
     * @param fd Client file descriptor
     * @return Client ID or INVALID_CLIENT_ID
     */
    ClientId handleNewClient(int fd);
    
    /**
     * @brief Handle client disconnection
     * @param client_id Client ID
     */
    void handleClientDisconnect(ClientId client_id);
    
    /**
     * @brief Read data from client
     * @param client_id Client ID
     * @param fd File descriptor
     * @return Result code
     */
    Result readFromClient(ClientId client_id, int fd);
    
    /**
     * @brief Add client to epoll
     * @param fd File descriptor
     * @return Result code
     */
    Result addToEpoll(int fd);
    
    /**
     * @brief Remove client from epoll
     * @param fd File descriptor
     * @return Result code
     */
    Result removeFromEpoll(int fd);
    
    /**
     * @brief Cleanup resources
     */
    void cleanup();
    
private:
    // Configuration
    std::string socket_path_;
    size_t queue_capacity_;
    
    // State
    std::atomic<bool> running_{false};
    std::atomic<bool> stopped_{false};
    
    // Socket
    int server_fd_ = INVALID_FD;
    int epoll_fd_ = INVALID_FD;
    
    // Threads
    std::thread network_thread_;
    std::thread processor_thread_;
    
    // Concurrent queue for messages
    std::unique_ptr<moodycamel::ConcurrentQueue<Message>> message_queue_;
    
    // Client management
    std::unordered_map<ClientId, ClientInfo> clients_;
    mutable std::mutex clients_mutex_;
    std::atomic<ClientId> next_client_id_{1};
    std::atomic<ConnectionId> next_connection_id_{1};
    
    // Callback
    MessageCallback callback_;
    std::mutex callback_mutex_;
    
    // Statistics
    std::atomic<size_t> total_messages_{0};
    std::atomic<size_t> total_clients_{0};
};

} // namespace ipc

#endif // IPC_SERVER_IPCSERVER_H
