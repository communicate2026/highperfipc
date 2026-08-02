/**
 * @file Constants.h
 * @brief Common constants for IPC library
 */

#ifndef IPC_COMMON_CONSTANTS_H
#define IPC_COMMON_CONSTANTS_H

#include <cstddef>
#include <chrono>

namespace ipc {

// Socket configuration
constexpr size_t MAX_SOCKET_PATH = 108;  // Unix domain socket path limit
constexpr int SOCKET_BACKLOG = 256;       // Listen backlog
constexpr size_t MAX_CONCURRENT_CLIENTS = 512;  // Maximum concurrent clients (support 200+)

// Message configuration
constexpr size_t MIN_MESSAGE_SIZE = 10;
constexpr size_t MAX_PACKET_SIZE = 8192;  // 8 KB
constexpr size_t DEFAULT_BUFFER_SIZE = MAX_PACKET_SIZE + 256;

// Timeouts
constexpr long DEFAULT_SEND_TIMEOUT_MS = 5000;
constexpr long DEFAULT_RECV_TIMEOUT_MS = 5000;
constexpr long DEFAULT_CONNECT_TIMEOUT_MS = 5000;
constexpr std::chrono::milliseconds RECONNECT_INTERVAL{100};
constexpr std::chrono::milliseconds SHUTDOWN_TIMEOUT{10000};

// Queue configuration
constexpr size_t DEFAULT_QUEUE_CAPACITY = 1000000;  // Increased for high throughput

// Client identification
using ClientId = uint64_t;
using ConnectionId = uint64_t;

// Special values
constexpr ClientId INVALID_CLIENT_ID = 0;
constexpr ConnectionId INVALID_CONNECTION_ID = 0;
constexpr int INVALID_FD = -1;

} // namespace ipc

#endif // IPC_COMMON_CONSTANTS_H
