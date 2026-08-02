/**
 * @file Message.h
 * @brief Message structure for IPC communication
 */

#ifndef IPC_COMMON_MESSAGE_H
#define IPC_COMMON_MESSAGE_H

#include "ipc/common/Types.h"
#include "ipc/common/Constants.h"
#include <cstdint>
#include <chrono>
#include <vector>
#include <cstring>

namespace ipc {

/**
 * @brief Represents a message received from a client
 * 
 * Each message contains:
 * - client_id: Unique identifier for the client
 * - connection_id: Connection instance identifier (changes on reconnect)
 * - timestamp: Time when message was received
 * - payload: Message data
 * - payload_size: Size of the payload
 */
class Message {
public:
    Message() 
        : client_id_(INVALID_CLIENT_ID)
        , connection_id_(INVALID_CONNECTION_ID)
        , timestamp_(std::chrono::steady_clock::now())
        , payload_size_(0) {}
    
    Message(ClientId client_id, ConnectionId connection_id, 
            const uint8_t* data, size_t size)
        : client_id_(client_id)
        , connection_id_(connection_id)
        , timestamp_(std::chrono::steady_clock::now())
        , payload_size_(size) {
        if (size > 0 && data != nullptr) {
            payload_.assign(data, data + size);
        }
    }
    
    // Getters
    ClientId clientId() const noexcept { return client_id_; }
    ConnectionId connectionId() const noexcept { return connection_id_; }
    std::chrono::steady_clock::time_point timestamp() const noexcept { return timestamp_; }
    const uint8_t* data() const noexcept { return payload_.data(); }
    size_t size() const noexcept { return payload_size_; }
    std::span<const uint8_t> span() const noexcept { 
        return std::span<const uint8_t>(payload_.data(), payload_size_); 
    }
    
    // Move semantics
    Message(Message&&) noexcept = default;
    Message& operator=(Message&&) noexcept = default;
    
    // Copy semantics
    Message(const Message&) = default;
    Message& operator=(const Message&) = default;
    
private:
    ClientId client_id_;
    ConnectionId connection_id_;
    std::chrono::steady_clock::time_point timestamp_;
    std::vector<uint8_t> payload_;
    size_t payload_size_;
};

} // namespace ipc

#endif // IPC_COMMON_MESSAGE_H
