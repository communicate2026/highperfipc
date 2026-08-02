/**
 * @file Types.h
 * @brief Common type definitions for IPC library
 */

#ifndef IPC_COMMON_TYPES_H
#define IPC_COMMON_TYPES_H

#include <functional>
#include <span>
#include <cstdint>

namespace ipc {

// Forward declarations from Constants.h
using ClientId = uint64_t;
using ConnectionId = uint64_t;

/**
 * @brief Callback function type for received messages
 * @param client_id ID of the client that sent the message
 * @param data Message payload as a span
 */
using MessageCallback = std::function<void(ClientId client_id, std::span<const uint8_t> data)>;

/**
 * @brief Result codes for IPC operations
 */
enum class Result {
    Success = 0,
    ErrorAlreadyRunning,
    ErrorNotRunning,
    ErrorSocketCreation,
    ErrorSocketOption,
    ErrorBind,
    ErrorListen,
    ErrorConnect,
    ErrorSend,
    ErrorRecv,
    ErrorDisconnect,
    ErrorTimeout,
    ErrorPathTooLong,
    ErrorEpollCreate,
    ErrorEpollCtl,
    ErrorQueueFull,
    ErrorInvalidState,
    ErrorUnknown
};

} // namespace ipc

#endif // IPC_COMMON_TYPES_H
