/**
 * @file IPCClient.h
 * @brief Thread-safe IPC Client using Unix Domain Sockets
 * 
 * Features:
 * - AF_UNIX with SOCK_SEQPACKET for message boundary preservation
 * - Non-blocking sockets with configurable timeouts
 * - Automatic reconnection support
 * - Thread-safe send operations
 * - Blocking and non-blocking connect modes
 */

#ifndef IPC_CLIENT_IPCCLIENT_H
#define IPC_CLIENT_IPCCLIENT_H

#include "ipc/common/Types.h"
#include "ipc/common/Constants.h"
#include <string>
#include <atomic>
#include <mutex>
#include <chrono>
#include <condition_variable>

namespace ipc {

/**
 * @brief Thread-safe IPC Client
 * 
 * Guarantees:
 * - Every send() produces exactly one packet delivered to server
 * - Messages are not mixed between clients
 * - Packet boundaries preserved via SOCK_SEQPACKET
 * 
 * Connection Modes:
 * - Blocking: Waits until connection established or timeout
 * - Non-blocking: Returns immediately, connection happens asynchronously
 * 
 * Auto-reconnect:
 * - Automatically reconnects after server restart
 * - Configurable reconnect interval
 */
class IPCClient {
public:
    /**
     * @brief Construct IPC Client
     */
    IPCClient();
    
    /**
     * @brief Destructor
     */
    ~IPCClient();
    
    // Non-copyable, movable
    IPCClient(const IPCClient&) = delete;
    IPCClient& operator=(const IPCClient&) = delete;
    IPCClient(IPCClient&&) noexcept;
    IPCClient& operator=(IPCClient&&) noexcept;
    
    /**
     * @brief Connect to server (blocking mode)
     * @param socket_path Path to Unix domain socket
     * @return Result code
     */
    Result connect(std::string_view socket_path);
    
    /**
     * @brief Connect to server with timeout
     * @param socket_path Path to Unix domain socket
     * @param timeout_ms Connection timeout in milliseconds
     * @return Result code
     */
    Result connect(std::string_view socket_path, int timeout_ms);
    
    /**
     * @brief Connect to server (non-blocking mode)
     * @param socket_path Path to Unix domain socket
     * @param blocking If false, returns immediately
     * @return Result code
     */
    Result connect(std::string_view socket_path, bool blocking);
    
    /**
     * @brief Disconnect from server
     * @return Result code
     */
    Result disconnect();
    
    /**
     * @brief Reconnect to server
     * @return Result code
     */
    Result reconnect();
    
    /**
     * @brief Send data to server
     * 
     * Thread-safe. Each call sends exactly one packet.
     * Message boundaries are preserved.
     * 
     * @param data Data to send
     * @param size Size of data
     * @return Result code
     */
    Result send(const uint8_t* data, size_t size);
    
    /**
     * @brief Send data to server (span overload)
     * @param data Data span
     * @return Result code
     */
    Result send(std::span<const uint8_t> data);
    
    /**
     * @brief Check if connected
     * @return true if connected
     */
    bool isConnected() const;
    
    /**
     * @brief Set send timeout
     * @param milliseconds Timeout in milliseconds
     */
    void setSendTimeout(int milliseconds);
    
    /**
     * @brief Set receive timeout
     * @param milliseconds Timeout in milliseconds
     */
    void setReceiveTimeout(int milliseconds);
    
    /**
     * @brief Enable/disable automatic reconnection
     * @param enable true to enable auto-reconnect
     */
    void enableAutoReconnect(bool enable);
    
    /**
     * @brief Get current connection state
     * @return Connection state
     */
    ConnectionState getState() const;
    
    /**
     * @brief Get last error code
     * @return Error code
     */
    std::error_code getLastError() const;
    
private:
    /**
     * @brief Internal connect implementation
     * @param socket_path Path to socket
     * @param timeout_ms Timeout
     * @param blocking Whether to block
     * @return Result code
     */
    Result connectInternal(std::string_view socket_path, 
                          int timeout_ms, bool blocking);
    
    /**
     * @brief Background reconnection thread
     */
    void reconnectLoop();
    
    /**
     * @brief Close current socket
     */
    void closeSocket();
    
    /**
     * @brief Wait until connected
     * @param timeout_ms Timeout
     * @return Result code
     */
    Result waitForConnection(int timeout_ms);
    
private:
    // Configuration
    std::string socket_path_;
    std::atomic<long> send_timeout_{DEFAULT_SEND_TIMEOUT};
    std::atomic<long> recv_timeout_{DEFAULT_RECV_TIMEOUT};
    std::atomic<long> connect_timeout_{DEFAULT_CONNECT_TIMEOUT};
    std::atomic<bool> auto_reconnect_{false};
    
    // State
    std::atomic<ConnectionState> state_{ConnectionState::Disconnected};
    std::atomic<bool> stop_reconnect_{true};
    
    // Socket
    int socket_fd_ = INVALID_FD;
    
    // Synchronization
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::error_code last_error_;
    
    // Reconnection thread
    std::thread reconnect_thread_;
    
    // Identifiers
    std::atomic<ClientId> client_id_{INVALID_CLIENT_ID};
    std::atomic<ConnectionId> connection_id_{INVALID_CONNECTION_ID};
};

} // namespace ipc

#endif // IPC_CLIENT_IPCCLIENT_H
