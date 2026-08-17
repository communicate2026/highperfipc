#ifndef IPC_SERVER_WRAPPER_H
#define IPC_SERVER_WRAPPER_H

#include "ipc_server.hpp"  // Include the C++ header first
#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void* IPCServerHandle;

/* Reuse Statistics from C++ (no duplicate definition) */
/* The C++ struct Statistics is compatible with C */

/* Constructor/Destructor */
IPCServerHandle IPCServer_new(const char* socket_path);
void            IPCServer_delete(IPCServerHandle handle);

/* Methods */
bool            IPCServer_start(IPCServerHandle handle, int num_workers);
void            IPCServer_stop(IPCServerHandle handle);
bool            IPCServer_get_message(
                    IPCServerHandle handle,
                    uint8_t* out_data,
                    size_t* out_size,
                    uint64_t* out_timestamp_us,
                    int timeout_ms
                );
void            IPCServer_get_statistics(IPCServerHandle handle, Statistics* out_stats);
bool            IPCServer_is_running(IPCServerHandle handle);
void            IPCServer_set_message_callback(
                    IPCServerHandle handle,
                    void (*cb)(const uint8_t* data, size_t size, uint64_t timestamp_us)
                );

#ifdef __cplusplus
}
#endif

#endif /* IPC_SERVER_WRAPPER_H */
