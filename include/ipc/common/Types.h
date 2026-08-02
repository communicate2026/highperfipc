/**
 * @file Types.h
 * @brief Common type definitions for IPC library
 */

#ifndef IPC_COMMON_TYPES_H
#define IPC_COMMON_TYPES_H

#include <cstdint>
#include <functional>
#include <string_view>
#include <span>

namespace ipc {

// Message callback type
using MessageCallback = std::function<void(ClientId client_id, std::span<const uint8_t> data)>;

// Result codes
enum class Result {
    Success = 0,
    Error = -1,
    Timeout = -2,
    Disconnected = -3,
    InvalidState = -4,
    BufferTooSmall = -5,
    QueueFull = -6
};

// Connection state
enum class ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting
};

} // namespace ipc

#endif // IPC_COMMON_TYPES_H
