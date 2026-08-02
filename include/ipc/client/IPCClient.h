#ifndef IPC_CLIENT_IPCCLIENT_H
#define IPC_CLIENT_IPCCLIENT_H

#include "ipc/common/Types.h"
#include "ipc/common/Constants.h"
#include <string>
#include <atomic>
#include <chrono>
#include <mutex>

namespace ipc {

class IPCClient {
public:
    IPCClient();
    ~IPCClient();
    
    IPCClient(const IPCClient&) = delete;
    IPCClient& operator=(const IPCClient&) = delete;
    IPCClient(IPCClient&&) noexcept;
    IPCClient& operator=(IPCClient&&) noexcept;
    
    Result connect(std::string_view socket_path);
    Result connect(std::string_view socket_path, int timeout_ms);
    Result disconnect();
    Result reconnect();
    Result send(const uint8_t* data, size_t size);
    bool is_connected() const;
    
    void set_send_timeout(int milliseconds);
    void set_receive_timeout(int milliseconds);
    void enable_auto_reconnect(bool enable);
    
private:
    int fd_;
    std::atomic<bool> connected_;
    std::atomic<int> send_timeout_ms_;
    std::atomic<int> recv_timeout_ms_;
    std::atomic<int> connect_timeout_ms_;
    std::atomic<bool> auto_reconnect_;
    std::string socket_path_;
    mutable std::mutex mutex_;
};

} // namespace ipc

#endif
