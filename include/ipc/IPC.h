/**
 * @file IPC.h
 * @brief Main header file for IPC library
 * 
 * Include this header to use the IPC library.
 */

#ifndef IPC_IPC_H
#define IPC_IPC_H

// Common types and utilities
#include "ipc/common/Constants.h"
#include "ipc/common/Types.h"
#include "ipc/common/Message.h"
#include "ipc/common/Utils.h"

// Server
#include "ipc/server/IPCServer.h"

// Client
#include "ipc/client/IPCClient.h"

/**
 * @namespace ipc
 * @brief High-performance IPC library namespace
 * 
 * This library provides:
 * - IPCServer: High-performance server using Unix Domain Sockets
 * - IPCClient: Thread-safe client with auto-reconnect support
 * 
 * Key Features:
 * - AF_UNIX with SOCK_SEQPACKET for message boundary preservation
 * - epoll (edge-triggered) for scalable I/O
 * - Non-blocking sockets
 * - Lock-free concurrent queue
 * - Support for 200+ concurrent clients
 * - Packet sizes from 10 bytes to 8 KB
 * - Hundreds of thousands of packets per second
 */
namespace ipc {

/**
 * @brief Library version
 */
constexpr int VERSION_MAJOR = 1;
constexpr int VERSION_MINOR = 0;
constexpr int VERSION_PATCH = 0;

/**
 * @brief Get library version string
 * @return Version string in format "major.minor.patch"
 */
inline const char* getVersion() {
    return "1.0.0";
}

} // namespace ipc

#endif // IPC_IPC_H
