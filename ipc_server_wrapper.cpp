/*# Compile both implementation files
g++ -shared -fPIC -O3 -std=c++17 \
  ipc_server.cpp \
  ipc_server_c.cpp \
  -o libipc_server.so \
  -pthread

# Verify symbols are exported (should show the C functions)
nm -D libipc_server.so | grep IPCServer_new*/

#include "ipc_server.hpp"
#include "ipc_server_wrapper.hpp"
#include <cstring>

extern "C" {

/* Opaque handle cast helpers */
static IPCServer* handle_cast(IPCServerHandle h) {
    return reinterpret_cast<IPCServer*>(h);
}

static IPCServerHandle handle_from_ptr(IPCServer* ptr) {
    return reinterpret_cast<IPCServerHandle>(ptr);
}

/* Constructor */
IPCServerHandle IPCServer_new(const char* socket_path) {
    try {
        std::string path = socket_path ? socket_path : "/tmp/ipc_seqpacket.sock";
        IPCServer* server = new IPCServer(path);
        return handle_from_ptr(server);
    } catch (...) {
        return nullptr;
    }
}

/* Destructor */
void IPCServer_delete(IPCServerHandle handle) {
    if (handle) {
        delete handle_cast(handle);
    }
}

/* Start server */
bool IPCServer_start(IPCServerHandle handle, int num_workers) {
    if (!handle) return false;
    try {
        return handle_cast(handle)->start(num_workers);
    } catch (...) {
        return false;
    }
}

/* Stop server */
void IPCServer_stop(IPCServerHandle handle) {
    if (!handle) return;
    try {
        handle_cast(handle)->stop();
    } catch (...) {
    }
}

/* Get message */
bool IPCServer_get_message(
    IPCServerHandle handle,
    uint8_t* out_data,
    size_t* out_size,
    uint64_t* out_timestamp_us,
    int timeout_ms)
{
    if (!handle || !out_data || !out_size) return false;
    
    try {
        IPCServer* server = handle_cast(handle);
        Message msg;
        
        if (!server->get_message(msg, timeout_ms)) {
            return false;
        }
        
        /* Copy message data */
        size_t copy_size = msg.data.size();
        if (copy_size > 16384) copy_size = 16384;  /* MAX_MSG_SIZE */
        
        std::memcpy(out_data, msg.data.data(), copy_size);
        *out_size = copy_size;
        *out_timestamp_us = msg.timestamp_us;
        
        return true;
    } catch (...) {
        return false;
    }
}

/* Get statistics */
void IPCServer_get_statistics(IPCServerHandle handle, Statistics* out_stats) {
    if (!handle || !out_stats) return;
    
    try {
        IPCServer* server = reinterpret_cast<IPCServer*>(handle);
        auto stats = server->get_statistics();  // C++ struct
        
        /* Convert C++ struct to C struct */
        out_stats->total_messages = stats.total_messages;
        out_stats->total_bytes = stats.total_bytes;
        out_stats->active_clients = stats.active_clients;
        out_stats->dropped_messages = stats.dropped_messages;
        out_stats->queue_size = stats.queue_size;
        out_stats->callback_call_count = stats.callback_call_count;
        out_stats->get_message_count = stats.get_message_count;
        out_stats->throughput_mbps = stats.throughput_mbps;
        out_stats->msg_rate_per_sec = stats.msg_rate_per_sec;
        out_stats->avg_msg_size = stats.avg_msg_size;
    } catch (...) {
        std::memset(out_stats, 0, sizeof(Statistics));
    }
}

/* Is running */
bool IPCServer_is_running(IPCServerHandle handle) {
    if (!handle) return false;
    try {
        return handle_cast(handle)->is_running();
    } catch (...) {
        return false;
    }
}

/* Set message callback */
void IPCServer_set_message_callback(
    IPCServerHandle handle,
    void (*cb)(const uint8_t* data, size_t size, uint64_t timestamp_us))
{
    if (!handle) return;
    
    try {
        IPCServer* server = handle_cast(handle);
        
        if (!cb) {
            /* Clear callback */
            server->set_message_callback(nullptr);
            return;
        }
        
        /* Wrap C callback in C++ lambda */
        server->set_message_callback(
            [cb](const Message& msg) {
                cb(msg.data.data(), msg.data.size(), msg.timestamp_us);
            }
        );
    } catch (...) {
    }
}

}  /* extern "C" */
