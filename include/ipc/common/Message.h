/**
 * @file Message.h
 * @brief Message structures for IPC communication
 */

#ifndef IPC_COMMON_MESSAGE_H
#define IPC_COMMON_MESSAGE_H

#include "ipc/common/Types.h"
#include "ipc/common/Constants.h"
#include <cstdint>
#include <chrono>
#include <vector>
#include <memory>

namespace ipc {

/**
 * @brief Represents a message received from a client
 */
struct Message {
    using Ptr = std::unique_ptr<Message>;
    
    ClientId client_id;
    ConnectionId connection_id;
    std::chrono::system_clock::time_point timestamp;
    std::vector<uint8_t> payload;
    size_t payload_size;
    
    Message() 
        : client_id(INVALID_CLIENT_ID)
        , connection_id(INVALID_CONNECTION_ID)
        , timestamp(std::chrono::system_clock::now())
        , payload_size(0) {}
};

} // namespace ipc

#endif // IPC_COMMON_MESSAGE_H
