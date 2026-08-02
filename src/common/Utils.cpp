#include "ipc/common/Utils.h"
#include "ipc/common/Constants.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <cstring>
#include <chrono>
#include <thread>

namespace ipc::utils {

Result setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        return Result::ErrorSocketOption;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        return Result::ErrorSocketOption;
    }
    return Result::Success;
}

Result setSocketOptions(int fd) {
    // Set socket buffer sizes
    int buf_size = 256 * 1024;  // 256 KB
    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size)) == -1) {
        return Result::ErrorSocketOption;
    }
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size)) == -1) {
        return Result::ErrorSocketOption;
    }
    return Result::Success;
}

int createSocket() {
    return socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
}

Result bindSocket(int fd, std::string_view path) {
    struct sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    
    if (path.size() >= sizeof(addr.sun_path)) {
        return Result::ErrorPathTooLong;
    }
    
    std::memcpy(addr.sun_path, path.data(), path.size());
    
    if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == -1) {
        return Result::ErrorBind;
    }
    
    return Result::Success;
}

Result connectSocket(int fd, std::string_view path, int timeout_ms, bool blocking) {
    struct sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    
    if (path.size() >= sizeof(addr.sun_path)) {
        return Result::ErrorPathTooLong;
    }
    
    std::memcpy(addr.sun_path, path.data(), path.size());
    
    if (blocking) {
        if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) == -1) {
            return Result::ErrorConnect;
        }
        return Result::Success;
    }
    
    // Non-blocking connect with timeout
    if (setNonBlocking(fd) != Result::Success) {
        return Result::ErrorSocketOption;
    }
    
    int ret = ::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    if (ret == 0) {
        return Result::Success;
    }
    
    if (errno != EINPROGRESS) {
        return Result::ErrorConnect;
    }
    
    // Wait for connection with timeout
    fd_set fds;
    FD_ZERO(&fds);
    FD_SET(fd, &fds);
    
    struct timeval tv {};
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    
    ret = select(fd + 1, nullptr, &fds, nullptr, &tv);
    if (ret == 0) {
        return Result::ErrorTimeout;
    }
    if (ret == -1) {
        return Result::ErrorConnect;
    }
    
    // Check if connection succeeded
    int error = 0;
    socklen_t len = sizeof(error);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) == -1 || error != 0) {
        return Result::ErrorConnect;
    }
    
    return Result::Success;
}

ClientId generateClientId() {
    static std::atomic<ClientId> counter{1};
    return counter.fetch_add(1, std::memory_order_relaxed);
}

ConnectionId generateConnectionId() {
    static std::atomic<ConnectionId> counter{1};
    return counter.fetch_add(1, std::memory_order_relaxed);
}

} // namespace ipc::utils
