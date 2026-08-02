#include "ipc/server/IPCServer.h"
#include "ipc/common/Message.h"
#include "ipc/common/Utils.h"
#include "ipc/common/Constants.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <chrono>

namespace ipc {

IPCServer::IPCServer() 
    : socket_path_()
    , queue_capacity_(DEFAULT_QUEUE_CAPACITY)
    , max_clients_(MAX_CONCURRENT_CLIENTS)
    , running_(false)
    , server_fd_(INVALID_FD)
    , queue_()
    , clients_()
    , client_threads_()
    , callback_() {
}

IPCServer::~IPCServer() {
    stop();
}

IPCServer::IPCServer(IPCServer&& other) noexcept
    : socket_path_(std::move(other.socket_path_))
    , queue_capacity_(other.queue_capacity_)
    , max_clients_(other.max_clients_)
    , running_(other.running_.exchange(false))
    , server_fd_(other.server_fd_)
    , acceptor_thread_(std::move(other.acceptor_thread_))
    , processor_thread_(std::move(other.processor_thread_))
    , queue_(std::move(other.queue_))
    , clients_(std::move(other.clients_))
    , client_threads_(std::move(other.client_threads_))
    , callback_(std::move(other.callback_)) {
    
    other.server_fd_ = INVALID_FD;
}

IPCServer& IPCServer::operator=(IPCServer&& other) noexcept {
    if (this != &other) {
        stop();
        
        socket_path_ = std::move(other.socket_path_);
        queue_capacity_ = other.queue_capacity_;
        max_clients_ = other.max_clients_;
        running_.store(other.running_.exchange(false));
        server_fd_ = other.server_fd_;
        acceptor_thread_ = std::move(other.acceptor_thread_);
        processor_thread_ = std::move(other.processor_thread_);
        queue_ = std::move(other.queue_);
        clients_ = std::move(other.clients_);
        client_threads_ = std::move(other.client_threads_);
        callback_ = std::move(other.callback_);
        
        other.server_fd_ = INVALID_FD;
    }
    return *this;
}

Result IPCServer::start(std::string_view socket_path) {
    if (running_.load()) {
        return Result::ErrorAlreadyRunning;
    }

    // Cleanup old socket if exists
    std::string path_str(socket_path);
    ::unlink(path_str.c_str());

    server_fd_ = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
    if (server_fd_ == -1) {
        return Result::ErrorSocketCreation;
    }

    struct sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    
    if (path_str.size() >= sizeof(addr.sun_path)) {
        ::close(server_fd_);
        server_fd_ = INVALID_FD;
        return Result::ErrorPathTooLong;
    }
    
    std::memcpy(addr.sun_path, path_str.c_str(), path_str.size());
    socket_path_ = path_str;

    if (::bind(server_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == -1) {
        ::close(server_fd_);
        server_fd_ = INVALID_FD;
        return Result::ErrorBind;
    }

    if (::listen(server_fd_, SOMAXCONN) == -1) {
        ::close(server_fd_);
        server_fd_ = INVALID_FD;
        return Result::ErrorListen;
    }

    running_.store(true);
    
    // Start the acceptor thread
    acceptor_thread_ = std::thread(&IPCServer::acceptorLoop, this);
    
    // Start the processor thread
    processor_thread_ = std::thread(&IPCServer::processorLoop, this);

    return Result::Success;
}

void IPCServer::stop() {
    if (!running_.exchange(false)) {
        return;
    }

    // Close server socket to unblock accept()
    if (server_fd_ != INVALID_FD) {
        ::shutdown(server_fd_, SHUT_RDWR);
        ::close(server_fd_);
        server_fd_ = INVALID_FD;
    }

    // Join acceptor thread
    if (acceptor_thread_.joinable()) {
        acceptor_thread_.join();
    }

    // Force disconnect all clients
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        for (auto& [id, info] : clients_) {
            if (info.fd != INVALID_FD) {
                ::shutdown(info.fd, SHUT_RDWR);
                ::close(info.fd);
                info.fd = INVALID_FD;
            }
        }
    }
    
    // Join all client threads
    {
        std::lock_guard<std::mutex> lock(threads_mutex_);
        for (auto& [id, thread] : client_threads_) {
            if (thread && thread->joinable()) {
                thread->join();
            }
        }
        client_threads_.clear();
    }
    
    clients_.clear();

    // Join processor thread
    if (processor_thread_.joinable()) {
        processor_thread_.join();
    }

    if (!socket_path_.empty()) {
        ::unlink(socket_path_.c_str());
    }
}

void IPCServer::set_callback(MessageCallback callback) {
    callback_ = std::move(callback);
}

size_t IPCServer::client_count() const {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    return clients_.size();
}

size_t IPCServer::queue_size() const {
    return queue_.size_approx();
}

bool IPCServer::is_running() const {
    return running_.load();
}

void IPCServer::acceptorLoop() {
    while (running_.load()) {
        struct sockaddr_un client_addr {};
        socklen_t client_len = sizeof(client_addr);
        
        // Blocking accept
        int client_fd = ::accept4(server_fd_, reinterpret_cast<struct sockaddr*>(&client_addr), 
                                  &client_len, SOCK_CLOEXEC);
        
        if (client_fd == -1) {
            if (errno == EINTR || errno == EAGAIN) {
                continue;
            }
            if (!running_.load()) break; // Shutdown requested
            
            // Error accepting
            continue;
        }

        // Check max clients limit
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            if (clients_.size() >= max_clients_) {
                ::close(client_fd);
                continue;
            }
        }

        ClientId client_id = utils::generateClientId();
        ConnectionId conn_id = utils::generateConnectionId();

        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            clients_[client_id] = {client_fd, conn_id};
        }

        // Spawn a dedicated thread for this client
        try {
            auto t = std::make_unique<std::thread>(&IPCServer::clientHandler, this, client_id, conn_id, client_fd);
            std::lock_guard<std::mutex> lock(threads_mutex_);
            client_threads_[client_id] = std::move(t);
        } catch (const std::exception& e) {
            // Failed to create thread, cleanup
            ::close(client_fd);
            std::lock_guard<std::mutex> lock(clients_mutex_);
            clients_.erase(client_id);
        }
    }
}

void IPCServer::clientHandler(ClientId client_id, ConnectionId conn_id, int client_fd) {
    std::vector<uint8_t> buffer(MAX_PACKET_SIZE);
    
    // Loop until disconnect or error
    while (running_.load()) {
        // Blocking recv - waits for a complete packet (SEQPACKET)
        ssize_t bytes_read = ::recv(client_fd, buffer.data(), buffer.size(), 0);
        
        if (bytes_read <= 0) {
            // Connection closed or error
            break;
        }

        // Create message
        auto msg = std::make_unique<Message>();
        msg->client_id = client_id;
        msg->connection_id = conn_id;
        msg->timestamp = std::chrono::system_clock::now();
        msg->payload_size = static_cast<size_t>(bytes_read);
        msg->payload.assign(buffer.begin(), buffer.begin() + bytes_read);

        // Push to queue (blocking if full)
        queue_.enqueue(std::move(msg));
    }

    // Cleanup
    ::close(client_fd);
    
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        clients_.erase(client_id);
    }
    
    {
        std::lock_guard<std::mutex> lock(threads_mutex_);
        client_threads_.erase(client_id);
    }
    
    // Thread ends here, unique_ptr will automatically join and cleanup
}

void IPCServer::processorLoop() {
    while (running_.load() || queue_.size_approx() > 0) {
        Message::Ptr msg;
        
        if (queue_.try_dequeue(msg)) {
            if (callback_) {
                try {
                    callback_(msg->client_id, std::span<const uint8_t>(msg->payload.data(), msg->payload_size));
                } catch (const std::exception& e) {
                    // Log callback exception
                }
            }
        } else {
            // No message, sleep briefly to avoid busy waiting
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
}

} // namespace ipc
