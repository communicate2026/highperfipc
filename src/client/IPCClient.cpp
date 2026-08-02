#include "ipc/client/IPCClient.h"
#include "ipc/common/Utils.h"

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <cstring>

namespace ipc {

IPCClient::IPCClient()
    : fd_(-1)
    , connected_(false)
    , send_timeout_ms_(DEFAULT_SEND_TIMEOUT_MS)
    , recv_timeout_ms_(DEFAULT_RECV_TIMEOUT_MS)
    , connect_timeout_ms_(DEFAULT_CONNECT_TIMEOUT_MS)
    , auto_reconnect_(false)
    , socket_path_() {
}

IPCClient::~IPCClient() {
    disconnect();
}

IPCClient::IPCClient(IPCClient&& other) noexcept
    : fd_(other.fd_)
    , connected_(other.connected_.exchange(false))
    , send_timeout_ms_(other.send_timeout_ms_.load())
    , recv_timeout_ms_(other.recv_timeout_ms_.load())
    , connect_timeout_ms_(other.connect_timeout_ms_.load())
    , auto_reconnect_(other.auto_reconnect_.load())
    , socket_path_(std::move(other.socket_path_)) {
    
    other.fd_ = -1;
}

IPCClient& IPCClient::operator=(IPCClient&& other) noexcept {
    if (this != &other) {
        disconnect();
        
        fd_ = other.fd_;
        connected_.store(other.connected_.exchange(false));
        send_timeout_ms_.store(other.send_timeout_ms_.load());
        recv_timeout_ms_.store(other.recv_timeout_ms_.load());
        connect_timeout_ms_.store(other.connect_timeout_ms_.load());
        auto_reconnect_.store(other.auto_reconnect_.load());
        socket_path_ = std::move(other.socket_path_);
        
        other.fd_ = -1;
    }
    return *this;
}

Result IPCClient::connect(std::string_view socket_path) {
    return connect(socket_path, connect_timeout_ms_.load());
}

Result IPCClient::connect(std::string_view socket_path, int timeout_ms) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (connected_.load()) {
        return Result::ErrorAlreadyRunning;
    }
    
    fd_ = utils::createSocket();
    if (fd_ == -1) {
        return Result::ErrorSocketCreation;
    }
    
    socket_path_ = std::string(socket_path);
    
    Result res = utils::connectSocket(fd_, socket_path, timeout_ms, false);
    if (res != Result::Success) {
        ::close(fd_);
        fd_ = -1;
        return res;
    }
    
    connected_.store(true);
    return Result::Success;
}

Result IPCClient::disconnect() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!connected_.exchange(false)) {
        return Result::ErrorNotRunning;
    }
    
    if (fd_ != -1) {
        ::close(fd_);
        fd_ = -1;
    }
    
    return Result::Success;
}

Result IPCClient::reconnect() {
    disconnect();
    return connect(socket_path_);
}

Result IPCClient::send(const uint8_t* data, size_t size) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!connected_.load()) {
        return Result::ErrorNotRunning;
    }
    
    if (fd_ == -1) {
        if (auto_reconnect_.load()) {
            Result res = reconnect();
            if (res != Result::Success) {
                return res;
            }
        } else {
            return Result::ErrorNotRunning;
        }
    }
    
    ssize_t sent = ::send(fd_, data, size, MSG_EOR);
    if (sent == -1) {
        if (errno == EPIPE || errno == ECONNRESET) {
            connected_.store(false);
            if (auto_reconnect_.load()) {
                Result res = reconnect();
                if (res == Result::Success) {
                    sent = ::send(fd_, data, size, MSG_EOR);
                    if (sent >= 0) {
                        return Result::Success;
                    }
                }
            }
            return Result::ErrorDisconnect;
        }
        return Result::ErrorSend;
    }
    
    return Result::Success;
}

bool IPCClient::is_connected() const {
    return connected_.load();
}

void IPCClient::set_send_timeout(int milliseconds) {
    send_timeout_ms_.store(milliseconds);
}

void IPCClient::set_receive_timeout(int milliseconds) {
    recv_timeout_ms_.store(milliseconds);
}

void IPCClient::enable_auto_reconnect(bool enable) {
    auto_reconnect_.store(enable);
}

} // namespace ipc
