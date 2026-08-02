/**
 * @file IPCClient.cpp
 * @brief Implementation of thread-safe IPC Client
 */

#include "ipc/client/IPCClient.h"
#include "ipc/common/Utils.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <chrono>
#include <thread>

namespace ipc {

IPCClient::IPCClient() = default;

IPCClient::~IPCClient() {
    disconnect();
}

IPCClient::IPCClient(IPCClient&& other) noexcept
    : socket_path_(std::move(other.socket_path_))
    , send_timeout_(other.send_timeout_.load())
    , recv_timeout_(other.recv_timeout_.load())
    , connect_timeout_(other.connect_timeout_.load())
    , auto_reconnect_(other.auto_reconnect_.load())
    , state_(other.state_.load())
    , stop_reconnect_(other.stop_reconnect_.load())
    , socket_fd_(other.socket_fd_)
    , last_error_(other.last_error_)
    , reconnect_thread_(std::move(other.reconnect_thread_))
    , client_id_(other.client_id_.load())
    , connection_id_(other.connection_id_.load()) {
    
    other.socket_fd_ = INVALID_FD;
    other.stop_reconnect_.store(true, std::memory_order_release);
}

IPCClient& IPCClient::operator=(IPCClient&& other) noexcept {
    if (this != &other) {
        disconnect();
        
        socket_path_ = std::move(other.socket_path_);
        send_timeout_.store(other.send_timeout_.load(), std::memory_order_relaxed);
        recv_timeout_.store(other.recv_timeout_.load(), std::memory_order_relaxed);
        connect_timeout_.store(other.connect_timeout_.load(), std::memory_order_relaxed);
        auto_reconnect_.store(other.auto_reconnect_.load(), std::memory_order_relaxed);
        state_.store(other.state_.load(), std::memory_order_relaxed);
        stop_reconnect_.store(other.stop_reconnect_.load(), std::memory_order_relaxed);
        socket_fd_ = other.socket_fd_;
        last_error_ = other.last_error_;
        reconnect_thread_ = std::move(other.reconnect_thread_);
        client_id_.store(other.client_id_.load(), std::memory_order_relaxed);
        connection_id_.store(other.connection_id_.load(), std::memory_order_relaxed);
        
        other.socket_fd_ = INVALID_FD;
        other.stop_reconnect_.store(true, std::memory_order_release);
    }
    return *this;
}

Result IPCClient::connect(std::string_view socket_path) {
    return connect(socket_path, static_cast<int>(connect_timeout_.load()));
}

Result IPCClient::connect(std::string_view socket_path, int timeout_ms) {
    return connectInternal(socket_path, timeout_ms, true);
}

Result IPCClient::connect(std::string_view socket_path, bool blocking) {
    return connectInternal(socket_path, static_cast<int>(connect_timeout_.load()), blocking);
}

Result IPCClient::connectInternal(std::string_view socket_path, 
                                   int timeout_ms, bool blocking) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (state_.load(std::memory_order_acquire) == ConnectionState::Connected) {
        return Result::Success;  // Already connected
    }
    
    socket_path_ = std::string(socket_path);
    
    // Close existing socket
    closeSocket();
    
    // Create socket
    socket_fd_ = utils::createSocket();
    if (socket_fd_ == INVALID_FD) {
        last_error_ = utils::getLastError();
        state_.store(ConnectionState::Disconnected, std::memory_order_release);
        return Result::Error;
    }
    
    // Connect
    Result result = utils::connectSocket(socket_fd_, socket_path_, timeout_ms, blocking);
    
    if (result != Result::Success) {
        last_error_ = utils::getLastError();
        closeSocket();
        state_.store(ConnectionState::Disconnected, std::memory_order_release);
        return result;
    }
    
    // Update state
    client_id_.store(utils::generateClientId(), std::memory_order_relaxed);
    connection_id_.fetch_add(1, std::memory_order_relaxed);
    state_.store(ConnectionState::Connected, std::memory_order_release);
    
    cv_.notify_all();
    return Result::Success;
}

Result IPCClient::disconnect() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (state_.load(std::memory_order_acquire) == ConnectionState::Disconnected) {
            return Result::Success;
        }
        
        // Stop reconnect thread
        stop_reconnect_.store(true, std::memory_order_release);
        auto_reconnect_ = false;
        
        if (reconnect_thread_.joinable()) {
            reconnect_thread_.join();
        }
        
        closeSocket();
        state_.store(ConnectionState::Disconnected, std::memory_order_release);
    }
    
    cv_.notify_all();
    return Result::Success;
}

Result IPCClient::reconnect() {
    disconnect();
    return connect(socket_path_);
}

Result IPCClient::send(const uint8_t* data, size_t size) {
    if (data == nullptr || size == 0) {
        return Result::Error;
    }
    
    return send(std::span<const uint8_t>(data, size));
}

Result IPCClient::send(std::span<const uint8_t> data) {
    if (data.empty()) {
        return Result::Error;
    }
    
    ConnectionState current_state = state_.load(std::memory_order_acquire);
    if (current_state != ConnectionState::Connected) {
        return Result::Disconnected;
    }
    
    int fd;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        fd = socket_fd_;
    }
    
    if (fd == INVALID_FD) {
        return Result::Disconnected;
    }
    
    // Set send timeout
    struct timeval tv {};
    auto timeout_ms = send_timeout_.load(std::memory_order_relaxed);
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    
    // Send with MSG_EOR to mark end of record (important for SOCK_SEQPACKET)
    ssize_t n = ::send(fd, data.data(), data.size(), MSG_EOR);
    
    if (n == -1) {
        last_error_ = utils::getLastError();
        
        if (errno == EPIPE || errno == ECONNRESET) {
            state_.store(ConnectionState::Disconnected, std::memory_order_release);
            return Result::Disconnected;
        }
        
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return Result::Timeout;
        }
        
        return Result::Error;
    }
    
    if (static_cast<size_t>(n) != data.size()) {
        return Result::Error;
    }
    
    return Result::Success;
}

bool IPCClient::isConnected() const {
    return state_.load(std::memory_order_acquire) == ConnectionState::Connected;
}

void IPCClient::setSendTimeout(int milliseconds) {
    send_timeout_.store(milliseconds, std::memory_order_relaxed);
}

void IPCClient::setReceiveTimeout(int milliseconds) {
    recv_timeout_.store(milliseconds, std::memory_order_relaxed);
}

void IPCClient::enableAutoReconnect(bool enable) {
    auto_reconnect_.store(enable, std::memory_order_relaxed);
    
    if (enable && !isConnected()) {
        stop_reconnect_.store(false, std::memory_order_release);
        reconnect_thread_ = std::thread(&IPCClient::reconnectLoop, this);
    } else if (!enable) {
        stop_reconnect_.store(true, std::memory_order_release);
        if (reconnect_thread_.joinable()) {
            reconnect_thread_.join();
        }
    }
}

ConnectionState IPCClient::getState() const {
    return state_.load(std::memory_order_acquire);
}

std::error_code IPCClient::getLastError() const {
    return last_error_;
}

void IPCClient::reconnectLoop() {
    while (!stop_reconnect_.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(RECONNECT_INTERVAL);
        
        if (stop_reconnect_.load(std::memory_order_acquire)) {
            break;
        }
        
        if (state_.load(std::memory_order_acquire) != ConnectionState::Connected) {
            Result result = connectInternal(socket_path_, static_cast<int>(connect_timeout_.load()), false);
            
            if (result == Result::Success) {
                break;
            }
        }
    }
}

void IPCClient::closeSocket() {
    if (socket_fd_ != INVALID_FD) {
        close(socket_fd_);
        socket_fd_ = INVALID_FD;
    }
}

Result IPCClient::waitForConnection(int timeout_ms) {
    auto deadline = std::chrono::steady_clock::now() + 
                    std::chrono::milliseconds(timeout_ms);
    
    std::unique_lock<std::mutex> lock(mutex_);
    
    while (state_.load(std::memory_order_acquire) != ConnectionState::Connected) {
        if (cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
            return Result::Timeout;
        }
    }
    
    return Result::Success;
}

} // namespace ipc
