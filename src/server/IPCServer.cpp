/**
 * @file IPCServer.cpp
 * @brief Implementation of high-performance IPC Server
 */

#include "ipc/server/IPCServer.h"
#include "ipc/common/Utils.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <stdexcept>

// Include concurrent queue
#include "concurrentqueue/concurrentqueue.h"

namespace ipc {

IPCServer::IPCServer()
    : queue_capacity_(DEFAULT_QUEUE_CAPACITY)
    , message_queue_(std::make_unique<moodycamel::ConcurrentQueue<Message>>(
          static_cast<moodycamel::ConcurrentQueue<Message>::size_t>(queue_capacity_))) {
}

IPCServer::~IPCServer() {
    stop();
}

IPCServer::IPCServer(IPCServer&& other) noexcept
    : socket_path_(std::move(other.socket_path_))
    , queue_capacity_(other.queue_capacity_.exchange(0))
    , running_(other.running_.load())
    , stopped_(other.stopped_.load())
    , server_fd_(other.server_fd_)
    , epoll_fd_(other.epoll_fd_)
    , network_thread_(std::move(other.network_thread_))
    , processor_thread_(std::move(other.processor_thread_))
    , message_queue_(std::move(other.message_queue_))
    , clients_(std::move(other.clients_))
    , next_client_id_(other.next_client_id_.load())
    , next_connection_id_(other.next_connection_id_.load())
    , callback_(std::move(other.callback_))
    , total_messages_(other.total_messages_.load())
    , total_clients_(other.total_clients_.load()) {
    
    other.server_fd_ = INVALID_FD;
    other.epoll_fd_ = INVALID_FD;
    other.queue_capacity_ = 0;
}

IPCServer& IPCServer::operator=(IPCServer&& other) noexcept {
    if (this != &other) {
        stop();
        
        socket_path_ = std::move(other.socket_path_);
        queue_capacity_ = other.queue_capacity_.exchange(0);
        running_ = other.running_.load();
        stopped_ = other.stopped_.load();
        server_fd_ = other.server_fd_;
        epoll_fd_ = other.epoll_fd_;
        network_thread_ = std::move(other.network_thread_);
        processor_thread_ = std::move(other.processor_thread_);
        message_queue_ = std::move(other.message_queue_);
        
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            clients_ = std::move(other.clients_);
        }
        
        next_client_id_ = other.next_client_id_.load();
        next_connection_id_ = other.next_connection_id_.load();
        
        {
            std::lock_guard<std::mutex> lock(callback_mutex_);
            callback_ = std::move(other.callback_);
        }
        
        total_messages_ = other.total_messages_.load();
        total_clients_ = other.total_clients_.load();
        
        other.server_fd_ = INVALID_FD;
        other.epoll_fd_ = INVALID_FD;
        other.queue_capacity_ = 0;
    }
    return *this;
}

Result IPCServer::start(std::string_view socket_path) {
    if (running_.load(std::memory_order_acquire)) {
        return Result::InvalidState;
    }
    
    if (socket_path.empty() || socket_path.size() >= MAX_SOCKET_PATH) {
        return Result::Error;
    }
    
    socket_path_ = std::string(socket_path);
    stopped_ = false;
    
    // Create server socket
    server_fd_ = utils::createSocket();
    if (server_fd_ == INVALID_FD) {
        return Result::Error;
    }
    
    // Bind socket
    if (utils::bindSocket(server_fd_, socket_path_) != Result::Success) {
        close(server_fd_);
        server_fd_ = INVALID_FD;
        return Result::Error;
    }
    
    // Listen
    if (listen(server_fd_, SOCKET_BACKLOG) == -1) {
        cleanup();
        return Result::Error;
    }
    
    // Set non-blocking
    if (utils::setNonBlocking(server_fd_) != Result::Success) {
        cleanup();
        return Result::Error;
    }
    
    // Create epoll
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ == INVALID_FD) {
        cleanup();
        return Result::Error;
    }
    
    // Add server socket to epoll
    struct epoll_event ev {};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = server_fd_;
    
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &ev) == -1) {
        cleanup();
        return Result::Error;
    }
    
    // Start threads
    running_.store(true, std::memory_order_release);
    
    try {
        network_thread_ = std::thread(&IPCServer::networkLoop, this);
        processor_thread_ = std::thread(&IPCServer::processorLoop, this);
    } catch (...) {
        running_.store(false, std::memory_order_release);
        cleanup();
        return Result::Error;
    }
    
    return Result::Success;
}

Result IPCServer::stop() {
    if (!running_.load(std::memory_order_acquire)) {
        return Result::Success;
    }
    
    running_.store(false, std::memory_order_release);
    stopped_.store(true, std::memory_order_release);
    
    // Close epoll to wake up network thread
    if (epoll_fd_ != INVALID_FD) {
        close(epoll_fd_);
        epoll_fd_ = INVALID_FD;
    }
    
    // Join threads
    if (network_thread_.joinable()) {
        network_thread_.join();
    }
    
    if (processor_thread_.joinable()) {
        processor_thread_.join();
    }
    
    cleanup();
    return Result::Success;
}

void IPCServer::setCallback(MessageCallback callback) {
    std::lock_guard<std::mutex> lock(callback_mutex_);
    callback_ = std::move(callback);
}

size_t IPCServer::clientCount() const {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    size_t count = 0;
    for (const auto& [id, info] : clients_) {
        if (info.active) {
            ++count;
        }
    }
    return count;
}

size_t IPCServer::queueSize() const {
    if (message_queue_) {
        return message_queue_->size_approx();
    }
    return 0;
}

bool IPCServer::isRunning() const {
    return running_.load(std::memory_order_acquire);
}

void IPCServer::networkLoop() {
    std::vector<struct epoll_event> events(EPOLL_MAX_EVENTS);
    std::vector<uint8_t> buffer(MAX_MESSAGE_SIZE + 256);
    
    while (running_.load(std::memory_order_acquire)) {
        int nfds = epoll_wait(epoll_fd_, events.data(), EPOLL_MAX_EVENTS, EPOLL_TIMEOUT_MS);
        
        if (nfds == -1) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        
        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;
            
            if (fd == server_fd_) {
                // New connection
                struct sockaddr_un client_addr {};
                socklen_t client_len = sizeof(client_addr);
                
                int client_fd = accept4(server_fd_, 
                    reinterpret_cast<struct sockaddr*>(&client_addr),
                    &client_len, SOCK_NONBLOCK | CLOEXEC);
                
                if (client_fd != -1) {
                    ClientId client_id = handleNewClient(client_fd);
                    if (client_id != INVALID_CLIENT_ID) {
                        if (addToEpoll(client_fd) != Result::Success) {
                            handleClientDisconnect(client_id);
                        }
                    } else {
                        close(client_fd);
                    }
                }
            } else {
                // Client data
                ClientId client_id = INVALID_CLIENT_ID;
                {
                    std::lock_guard<std::mutex> lock(clients_mutex_);
                    for (const auto& [id, info] : clients_) {
                        if (info.fd == fd && info.active) {
                            client_id = id;
                            break;
                        }
                    }
                }
                
                if (client_id != INVALID_CLIENT_ID) {
                    if (events[i].events & (EPOLLERR | EPOLLHUP)) {
                        handleClientDisconnect(client_id);
                    } else if (events[i].events & EPOLLIN) {
                        readFromClient(client_id, fd);
                    }
                }
            }
        }
    }
}

void IPCServer::processorLoop() {
    Message message;
    
    while (running_.load(std::memory_order_acquire) || queueSize() > 0) {
        if (message_queue_->try_dequeue(message)) {
            std::lock_guard<std::mutex> lock(callback_mutex_);
            if (callback_) {
                callback_(message.clientId(), message.span());
            }
            total_messages_.fetch_add(1, std::memory_order_relaxed);
        } else {
            // No messages, sleep briefly to avoid busy waiting
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
}

ClientId IPCServer::handleNewClient(int fd) {
    ClientId client_id = next_client_id_.fetch_add(1, std::memory_order_relaxed);
    ConnectionId conn_id = next_connection_id_.fetch_add(1, std::memory_order_relaxed);
    
    ClientInfo info;
    info.fd = fd;
    info.id = client_id;
    info.connection_id = conn_id;
    info.active = true;
    
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        clients_[client_id] = info;
    }
    
    total_clients_.fetch_add(1, std::memory_order_relaxed);
    return client_id;
}

void IPCServer::handleClientDisconnect(ClientId client_id) {
    ClientInfo info;
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        auto it = clients_.find(client_id);
        if (it != clients_.end()) {
            info = it->second;
            it->second.active = false;
            
            if (info.fd != INVALID_FD) {
                removeFromEpoll(info.fd);
                close(info.fd);
            }
            
            clients_.erase(it);
        }
    }
}

Result IPCServer::readFromClient(ClientId client_id, int fd) {
    std::vector<uint8_t> buffer(MAX_MESSAGE_SIZE + 256);
    
    // Read with MSG_TRUNC to detect oversized messages
    ssize_t n = recv(fd, buffer.data(), buffer.size(), MSG_TRUNC);
    
    if (n <= 0) {
        if (n == 0 || errno == ECONNRESET || errno == EPIPE) {
            handleClientDisconnect(client_id);
        }
        return Result::Success;  // Will be handled by disconnect
    }
    
    if (static_cast<size_t>(n) > MAX_MESSAGE_SIZE) {
        // Message too large, discard
        return Result::BufferTooSmall;
    }
    
    // Get connection ID
    ConnectionId conn_id = INVALID_CONNECTION_ID;
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        auto it = clients_.find(client_id);
        if (it != clients_.end() && it->second.active) {
            conn_id = it->second.connection_id;
        } else {
            return Result::Error;
        }
    }
    
    // Create message and push to queue
    Message msg(client_id, conn_id, buffer.data(), static_cast<size_t>(n));
    
    if (!message_queue_->try_enqueue(std::move(msg))) {
        // Queue full, message dropped
        return Result::QueueFull;
    }
    
    return Result::Success;
}

Result IPCServer::addToEpoll(int fd) {
    struct epoll_event ev {};
    ev.events = EPOLLIN | EPOLLET | EPOLLHUP | EPOLLERR;
    ev.data.fd = fd;
    
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) == -1) {
        return Result::Error;
    }
    
    return Result::Success;
}

Result IPCServer::removeFromEpoll(int fd) {
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) == -1) {
        return Result::Error;
    }
    return Result::Success;
}

void IPCServer::cleanup() {
    if (server_fd_ != INVALID_FD) {
        close(server_fd_);
        server_fd_ = INVALID_FD;
    }
    
    if (epoll_fd_ != INVALID_FD) {
        close(epoll_fd_);
        epoll_fd_ = INVALID_FD;
    }
    
    // Remove socket file
    if (!socket_path_.empty()) {
        unlink(socket_path_.c_str());
    }
    
    // Clear clients
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        for (auto& [id, info] : clients_) {
            if (info.fd != INVALID_FD) {
                close(info.fd);
            }
        }
        clients_.clear();
    }
}

} // namespace ipc
