/**
 * @file Utils.cpp
 * @brief Implementation of utility functions
 */

#include "ipc/common/Utils.h"
#include "ipc/common/Constants.h"

#include <fcntl.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/epoll.h>
#include <unistd.h>
#include <cstring>
#include <random>
#include <chrono>
#include <system_error>

namespace ipc {
namespace utils {

Result setNonBlocking(int fd) {
    if (fd == INVALID_FD) {
        return Result::Error;
    }
    
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        return Result::Error;
    }
    
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        return Result::Error;
    }
    
    return Result::Success;
}

Result setSocketOptions(int fd) {
    if (fd == INVALID_FD) {
        return Result::Error;
    }
    
    // Set SO_REUSEADDR
    int optval = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) == -1) {
        return Result::Error;
    }
    
    // Set buffer sizes for high performance
    int buffer_size = 256 * 1024;  // 256 KB
    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buffer_size, sizeof(buffer_size)) == -1) {
        // Non-fatal, continue
    }
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buffer_size, sizeof(buffer_size)) == -1) {
        // Non-fatal, continue
    }
    
    return Result::Success;
}

int createSocket() {
    int fd = socket(AF_UNIX, SOCK_SEQPACKET, 0);
    if (fd == -1) {
        return INVALID_FD;
    }
    
    if (setSocketOptions(fd) != Result::Success) {
        close(fd);
        return INVALID_FD;
    }
    
    return fd;
}

Result bindSocket(int fd, std::string_view path) {
    if (fd == INVALID_FD || path.empty()) {
        return Result::Error;
    }
    
    if (path.size() >= MAX_SOCKET_PATH) {
        return Result::Error;
    }
    
    // Remove existing socket file
    unlink(std::string(path).c_str());
    
    struct sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    std::memcpy(addr.sun_path, path.data(), path.size());
    
    socklen_t addr_len = sizeof(sa_family_t) + path.size() + 1;
    
    if (bind(fd, reinterpret_cast<struct sockaddr*>(&addr), addr_len) == -1) {
        return Result::Error;
    }
    
    return Result::Success;
}

Result connectSocket(int fd, std::string_view path, int timeout_ms, bool blocking) {
    if (fd == INVALID_FD || path.empty()) {
        return Result::Error;
    }
    
    if (!blocking) {
        if (setNonBlocking(fd) != Result::Success) {
            return Result::Error;
        }
    }
    
    struct sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    std::memcpy(addr.sun_path, path.data(), path.size());
    
    socklen_t addr_len = sizeof(sa_family_t) + path.size() + 1;
    
    int result = connect(fd, reinterpret_cast<struct sockaddr*>(&addr), addr_len);
    
    if (result == 0) {
        // Connected immediately
        if (!blocking) {
            // Ensure non-blocking mode is set
            setNonBlocking(fd);
        }
        return Result::Success;
    }
    
    if (errno == EINPROGRESS && !blocking) {
        // Connection in progress (non-blocking)
        // Use epoll to wait for connection with timeout
        int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd == -1) {
            return Result::Error;
        }
        
        struct epoll_event ev {};
        ev.events = EPOLLOUT;
        ev.data.fd = fd;
        
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
            close(epoll_fd);
            return Result::Error;
        }
        
        struct epoll_event events[1];
        int ret = epoll_wait(epoll_fd, events, 1, timeout_ms);
        
        close(epoll_fd);
        
        if (ret == -1) {
            return Result::Error;
        }
        
        if (ret == 0) {
            return Result::Timeout;
        }
        
        // Check if connection succeeded
        int error = 0;
        socklen_t len = sizeof(error);
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) == -1 || error != 0) {
            return Result::Error;
        }
        
        // Set non-blocking mode
        setNonBlocking(fd);
        return Result::Success;
    }
    
    return Result::Error;
}

std::error_code getLastError() {
    return std::error_code(errno, std::system_category());
}

bool isRecoverableError(const std::error_code& err) {
    // Errors that indicate the server might restart
    return err.value() == ECONNREFUSED || 
           err.value() == ENOENT ||
           err.value() == ECONNRESET ||
           err.value() == EPIPE;
}

ClientId generateClientId() {
    static std::atomic<ClientId> counter{1};
    return counter.fetch_add(1, std::memory_order_relaxed);
}

ConnectionId generateConnectionId() {
    static std::atomic<ConnectionId> counter{1};
    return counter.fetch_add(1, std::memory_order_relaxed);
}

uint64_t getCurrentTimestamp() {
    auto now = std::chrono::steady_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

} // namespace utils
} // namespace ipc
