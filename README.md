# High-Performance IPC Library for Linux

A production-ready, high-performance Inter-Process Communication (IPC) library for Linux using Unix Domain Sockets with SOCK_SEQPACKET.

## Features

- **Modern C++20**: Built with the latest C++ standards
- **Unix Domain Sockets**: Uses `AF_UNIX` with `SOCK_SEQPACKET` for message boundary preservation
- **High Performance**: 
  - Supports 200+ concurrent client processes
  - Handles hundreds of thousands of packets per second
  - Packet sizes from 10 bytes to 8 KB
- **epoll (edge-triggered)**: Scalable I/O multiplexing
- **Non-blocking sockets**: Efficient resource utilization
- **Lock-free queue**: Uses [concurrentqueue](https://github.com/cameron314/concurrentqueue) for message passing
- **Thread-safe**: Both server and client are fully thread-safe

## Architecture

### Server
```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ Network Thread  │────▶│ Concurrent Queue │────▶│ Processor Thread│
│ - accept()      │     │ - Lock-free      │     │ - Callback      │
│ - epoll_wait()  │     │ - Message objects│     │ - User function │
│ - recv()        │     │                  │     │                 │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

### Client
```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ Application     │────▶│ Socket Layer     │────▶│ Auto-Reconnect  │
│ - send()        │     │ - SOCK_SEQPACKET │     │ - Background    │
│ - connect()     │     │ - Non-blocking   │     │ - Thread        │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

## Building

### Prerequisites

- Linux x64 (ADM x64)
- CMake 3.20 or higher
- GCC 10+ or Clang 11+ with C++20 support
- pthread

### Build Instructions

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j$(nproc)
```

### Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `BUILD_TESTS` | ON | Build unit tests |
| `BUILD_EXAMPLES` | ON | Build example programs |
| `BUILD_SHARED_LIBS` | OFF | Build shared libraries |
| `ENABLE_SANITIZERS` | ON | Enable sanitizers in debug mode |

## Usage

### Server Example

```cpp
#include "ipc/IPC.h"

int main() {
    ipc::IPCServer server;
    
    // Set message callback
    server.setCallback([](ipc::ClientId client_id, std::span<const uint8_t> data) {
        std::cout << "Received from client " << client_id 
                  << ": " << std::string(data.begin(), data.end()) << std::endl;
    });
    
    // Start server
    if (server.start("/tmp/my_ipc.sock") != ipc::Result::Success) {
        std::cerr << "Failed to start server" << std::endl;
        return 1;
    }
    
    // Run until stopped
    while (server.isRunning()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    
    return 0;
}
```

### Client Example

```cpp
#include "ipc/IPC.h"

int main() {
    ipc::IPCClient client;
    
    // Connect to server
    if (client.connect("/tmp/my_ipc.sock", 5000) != ipc::Result::Success) {
        std::cerr << "Failed to connect" << std::endl;
        return 1;
    }
    
    // Send message (each send() produces exactly one packet)
    std::string message = "Hello, Server!";
    client.send(std::as_bytes(std::span{message}));
    
    // Enable auto-reconnect
    client.enableAutoReconnect(true);
    
    // Disconnect when done
    client.disconnect();
    
    return 0;
}
```

## API Reference

### IPCServer

| Method | Description |
|--------|-------------|
| `start(socket_path)` | Start the server on the specified socket path |
| `stop()` | Stop the server gracefully |
| `setCallback(callback)` | Set the message callback function |
| `clientCount()` | Get number of connected clients |
| `queueSize()` | Get current message queue size |
| `isRunning()` | Check if server is running |

**Callback Signature:**
```cpp
void on_message(ipc::ClientId client_id, std::span<const uint8_t> data);
```

### IPCClient

| Method | Description |
|--------|-------------|
| `connect(socket_path)` | Connect to server (blocking) |
| `connect(socket_path, timeout_ms)` | Connect with timeout |
| `connect(socket_path, blocking)` | Connect in blocking/non-blocking mode |
| `disconnect()` | Disconnect from server |
| `reconnect()` | Reconnect to server |
| `send(data)` | Send data (thread-safe) |
| `isConnected()` | Check connection status |
| `setSendTimeout(ms)` | Set send timeout |
| `setReceiveTimeout(ms)` | Set receive timeout |
| `enableAutoReconnect(enable)` | Enable/disable auto-reconnect |

## Message Object

Each received message contains:

| Field | Type | Description |
|-------|------|-------------|
| `client_id` | `uint64_t` | Unique client identifier |
| `connection_id` | `uint64_t` | Connection instance ID (changes on reconnect) |
| `timestamp` | `time_point` | Message reception time |
| `payload` | `span<uint8_t>` | Message data |
| `size` | `size_t` | Payload size |

## Configuration Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `MAX_CLIENTS` | 512 | Maximum concurrent clients |
| `MAX_MESSAGE_SIZE` | 8192 | Maximum message size (8 KB) |
| `MIN_MESSAGE_SIZE` | 10 | Minimum message size |
| `DEFAULT_QUEUE_CAPACITY` | 100000 | Default queue capacity |

## Testing

```bash
cd build
ctest --output-on-failure
```

Or run individual test executables:
```bash
./ipc_tests
```

## Benchmarking

```bash
./benchmark
```

Example output:
```
=== IPC Library Benchmark ===
Clients: 50
Messages per client: 10000
Message size: 256 bytes
Total messages: 500000

=== Results ===
Duration: 2.5 seconds
Messages received: 500000
Throughput: 200000 msg/sec
Bandwidth: 48.8 MB/sec
```

## License

This project is provided as-is for production use.

## Platform Support

- **OS**: Linux only
- **Architecture**: x86_64 (AMD64)
- **Compiler**: GCC 10+ or Clang 11+
- **C++ Standard**: C++20
