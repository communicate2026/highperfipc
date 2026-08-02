/**
 * @file Utils.h
 * @brief Utility functions for IPC library
 */

#ifndef IPC_COMMON_UTILS_H
#define IPC_COMMON_UTILS_H

#include "ipc/common/Types.h"
#include <string>
#include <system_error>

namespace ipc {
namespace utils {

/**
 * @brief Set a file descriptor to non-blocking mode
 * @param fd File descriptor
 * @return Result code
 */
Result setNonBlocking(int fd);

/**
 * @brief Set socket options for Unix domain socket
 * @param fd File descriptor
 * @return Result code
 */
Result setSocketOptions(int fd);

/**
 * @brief Create a Unix domain socket with SOCK_SEQPACKET
 * @return File descriptor or INVALID_FD on error
 */
int createSocket();

/**
 * @brief Bind socket to a path
 * @param fd File descriptor
 * @param path Socket path
 * @return Result code
 */
Result bindSocket(int fd, std::string_view path);

/**
 * @brief Connect socket to a path
 * @param fd File descriptor
 * @param path Socket path
 * @param timeout_ms Connection timeout in milliseconds
 * @param blocking Whether to block until connection
 * @return Result code
 */
Result connectSocket(int fd, std::string_view path, 
                     int timeout_ms, bool blocking);

/**
 * @brief Get last socket error
 * @return Error code
 */
std::error_code getLastError();

/**
 * @brief Check if error is recoverable
 * @param err Error code
 * @return true if recoverable
 */
bool isRecoverableError(const std::error_code& err);

/**
 * @brief Generate unique client ID
 * @return Client ID
 */
ClientId generateClientId();

/**
 * @brief Generate unique connection ID
 * @return Connection ID
 */
ConnectionId generateConnectionId();

/**
 * @brief Get current timestamp in milliseconds
 * @return Timestamp
 */
uint64_t getCurrentTimestamp();

} // namespace utils
} // namespace ipc

#endif // IPC_COMMON_UTILS_H
