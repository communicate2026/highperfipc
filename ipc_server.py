import ctypes
import os
import sys
from pathlib import Path
from typing import Callable, Optional, Tuple
from dataclasses import dataclass
from enum import IntEnum

# Load the shared library
def load_ipc_server_lib(lib_path: Optional[str] = None) -> ctypes.CDLL:
    """Load the IPC server shared library."""
    if lib_path:
        try:
            return ctypes.CDLL(lib_path)
        except OSError as e:
            raise FileNotFoundError(f"Failed to load {lib_path}: {e}")
    
    possible_paths = [
        "./libipc_server.so",
        "/usr/local/lib/libipc_server.so",
        "/usr/lib/libipc_server.so",
    ]
    
    for path in possible_paths:
        try:
            if os.path.exists(path):
                return ctypes.CDLL(path)
        except OSError:
            continue
    
    raise FileNotFoundError(
        f"Could not find libipc_server.so. Tried: {possible_paths}"
    )

# Data structures
@dataclass
class Statistics:
    """Statistics from the IPC server."""
    total_messages: int
    total_bytes: int
    active_clients: int
    dropped_messages: int
    queue_size: int
    callback_call_count: int
    get_message_count: int
    throughput_mbps: float
    msg_rate_per_sec: float
    avg_msg_size: float


class _CStatistics(ctypes.Structure):
    """C-compatible Statistics structure."""
    _fields_ = [
        ("total_messages", ctypes.c_uint64),
        ("total_bytes", ctypes.c_uint64),
        ("active_clients", ctypes.c_uint64),
        ("dropped_messages", ctypes.c_uint64),
        ("queue_size", ctypes.c_uint64),
        ("callback_call_count", ctypes.c_uint64),
        ("get_message_count", ctypes.c_uint64),
        ("throughput_mbps", ctypes.c_double),
        ("msg_rate_per_sec", ctypes.c_double),
        ("avg_msg_size", ctypes.c_double),
    ]
    
    def to_python(self) -> Statistics:
        """Convert C structure to Python dataclass."""
        return Statistics(
            total_messages=self.total_messages,
            total_bytes=self.total_bytes,
            active_clients=self.active_clients,
            dropped_messages=self.dropped_messages,
            queue_size=self.queue_size,
            callback_call_count=self.callback_call_count,
            get_message_count=self.get_message_count,
            throughput_mbps=self.throughput_mbps,
            msg_rate_per_sec=self.msg_rate_per_sec,
            avg_msg_size=self.avg_msg_size,
        )


@dataclass
class Message:
    """Received message from IPC server."""
    data: bytes
    timestamp_us: int
    
    @property
    def size(self) -> int:
        """Return message size in bytes."""
        return len(self.data)


# Constants
MAX_MSG_SIZE = 16384


class IPCServerError(Exception):
    """Exception raised for IPC server errors."""
    pass


class IPCServer:
    """
    Python wrapper for the C IPC Server.
    
    This class provides a Pythonic interface to the high-performance
    Unix domain socket IPC server written in C++.
    """
    
    def __init__(
        self,
        socket_path: str = "/tmp/ipc_seqpacket.sock",
        lib_path: Optional[str] = None,
    ):
        """
        Initialize the IPC server wrapper.
        
        Args:
            socket_path: Path to the Unix domain socket.
            lib_path: Path to the compiled .so file. If None, searches defaults.
        
        Raises:
            FileNotFoundError: If the library cannot be found.
        """
        self._lib = load_ipc_server_lib(lib_path)
        self._socket_path = socket_path
        self._handle: Optional[ctypes.c_void_p] = None
        self._callback: Optional[Callable[[bytes, int], None]] = None
        self._callback_c = None  # Keep reference to C callback
        
        self._setup_c_interface()
        self._create_handle()
    
    def _setup_c_interface(self) -> None:
        """Configure ctypes function signatures."""
        # Constructor
        self._lib.IPCServer_new.argtypes = [ctypes.c_char_p]
        self._lib.IPCServer_new.restype = ctypes.c_void_p
        
        # Destructor
        self._lib.IPCServer_delete.argtypes = [ctypes.c_void_p]
        self._lib.IPCServer_delete.restype = None
        
        # Start
        self._lib.IPCServer_start.argtypes = [ctypes.c_void_p, ctypes.c_int]
        self._lib.IPCServer_start.restype = ctypes.c_bool
        
        # Stop
        self._lib.IPCServer_stop.argtypes = [ctypes.c_void_p]
        self._lib.IPCServer_stop.restype = None
        
        # Get message
        self._lib.IPCServer_get_message.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_uint8),
            ctypes.POINTER(ctypes.c_size_t),
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.c_int,
        ]
        self._lib.IPCServer_get_message.restype = ctypes.c_bool
        
        # Get statistics
        self._lib.IPCServer_get_statistics.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(_CStatistics),
        ]
        self._lib.IPCServer_get_statistics.restype = None
        
        # Is running
        self._lib.IPCServer_is_running.argtypes = [ctypes.c_void_p]
        self._lib.IPCServer_is_running.restype = ctypes.c_bool
        
        # Set message callback
        callback_signature = ctypes.CFUNCTYPE(
            None,
            ctypes.POINTER(ctypes.c_uint8),
            ctypes.c_size_t,
            ctypes.c_uint64,
        )
        self._lib.IPCServer_set_message_callback.argtypes = [
            ctypes.c_void_p,
            callback_signature,
        ]
        self._lib.IPCServer_set_message_callback.restype = None
        self._callback_signature = callback_signature
    
    def _create_handle(self) -> None:
        """Create the C++ server instance."""
        socket_path_bytes = self._socket_path.encode('utf-8')
        self._handle = self._lib.IPCServer_new(socket_path_bytes)
        if not self._handle:
            raise IPCServerError(f"Failed to create IPC server at {self._socket_path}")
    
    def start(self, num_workers: int = 4) -> bool:
        """
        Start the IPC server.
        
        Args:
            num_workers: Number of worker threads to spawn (default: 4).
        
        Returns:
            True if server started successfully, False otherwise.
        
        Raises:
            IPCServerError: If the server is not initialized.
        """
        if not self._handle:
            raise IPCServerError("Server handle not initialized")
        
        success = self._lib.IPCServer_start(self._handle, num_workers)
        if not success:
            raise IPCServerError(f"Failed to start IPC server")
        return success
    
    def stop(self) -> None:
        """
        Stop the IPC server gracefully.
        
        Raises:
            IPCServerError: If the server is not initialized.
        """
        if not self._handle:
            raise IPCServerError("Server handle not initialized")
        self._lib.IPCServer_stop(self._handle)
    
    def is_running(self) -> bool:
        """
        Check if the server is currently running.
        
        Returns:
            True if server is running, False otherwise.
        
        Raises:
            IPCServerError: If the server is not initialized.
        """
        if not self._handle:
            raise IPCServerError("Server handle not initialized")
        return self._lib.IPCServer_is_running(self._handle)
    
    def get_message(self, timeout_ms: int = 100) -> Optional[Message]:
        """
        Retrieve a message from the queue.
        
        Args:
            timeout_ms: Timeout in milliseconds (default: 100).
        
        Returns:
            Message object if available, None if timeout or error.
        
        Raises:
            IPCServerError: If the server is not initialized.
        """
        if not self._handle:
            raise IPCServerError("Server handle not initialized")
        
        # Allocate buffer for message data
        buffer = (ctypes.c_uint8 * MAX_MSG_SIZE)()
        out_size = ctypes.c_size_t(0)
        out_timestamp = ctypes.c_uint64(0)
        
        success = self._lib.IPCServer_get_message(
            self._handle,
            buffer,
            ctypes.byref(out_size),
            ctypes.byref(out_timestamp),
            timeout_ms,
        )
        
        if not success:
            return None
        
        # Convert buffer to bytes
        data = bytes(buffer[:out_size.value])
        return Message(data=data, timestamp_us=out_timestamp.value)
    
    def get_statistics(self) -> Statistics:
        """
        Retrieve current server statistics.
        
        Returns:
            Statistics object with current metrics.
        
        Raises:
            IPCServerError: If the server is not initialized.
        """
        if not self._handle:
            raise IPCServerError("Server handle not initialized")
        
        c_stats = _CStatistics()
        self._lib.IPCServer_get_statistics(self._handle, ctypes.byref(c_stats))
        return c_stats.to_python()
    
    def set_message_callback(
        self,
        callback: Callable[[bytes, int], None],
    ) -> None:
        """
        Set a callback function to be invoked when messages arrive.
        
        The callback receives:
            - data: bytes - The message data
            - timestamp_us: int - The message timestamp in microseconds
        
        Args:
            callback: Callable that accepts (bytes, int) parameters.
        
        Raises:
            IPCServerError: If the server is not initialized.
        
        Example:
            >>> def on_message(data: bytes, timestamp_us: int):
            ...     print(f"Received {len(data)} bytes at {timestamp_us}")
            >>> server.set_message_callback(on_message)
        """
        if not self._handle:
            raise IPCServerError("Server handle not initialized")
        
        self._callback = callback
        
        # Create C callback wrapper that keeps Python reference
        def c_callback(
            data_ptr: ctypes.POINTER(ctypes.c_uint8),
            size: ctypes.c_size_t,
            timestamp_us: ctypes.c_uint64,
        ) -> None:
            try:
                data = bytes(data_ptr[:size])
                callback(data, timestamp_us)
            except Exception as e:
                print(f"Error in message callback: {e}", file=sys.stderr)
        
        # Store reference to prevent garbage collection
        self._callback_c = self._callback_signature(c_callback)
        self._lib.IPCServer_set_message_callback(self._handle, self._callback_c)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures proper cleanup."""
        self.cleanup()
        return False
    
    def cleanup(self) -> None:
        """Clean up resources and destroy the server."""
        if self._handle:
            try:
                if self.is_running():
                    self.stop()
            except:
                pass
            
            self._lib.IPCServer_delete(self._handle)
            self._handle = None
    
    def __del__(self):
        """Destructor - ensures cleanup on garbage collection."""
        self.cleanup()


# Example usage and helper functions

def example_basic_usage():
    """Example: Basic message retrieval."""
    server = IPCServer(socket_path="/tmp/ipc_seqpacket.sock")
    if not server.start(num_workers=4):
        print("Failed to start server")
        exit(1)

    print("Server started. Waiting for messages...")
    
    try:
        # Poll for messages
        while server.is_running():
            msg = server.get_message(timeout_ms=100)
            if msg:
                pass
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        server.stop()


def example_with_callback():
    """Example: Using message callback."""
    def on_message(data: bytes, timestamp_us: int):
        #print(f"Callback: received {len(data)} bytes at {timestamp_us} µs")
        pass
    
    server = IPCServer(socket_path="/tmp/ipc_seqpacket.sock")
    server.set_message_callback(on_message)
    server.start(num_workers=4)
    
    try:
        # Server processes messages via callback
        import time
        time.sleep(100)
    finally:
        server.stop()


def example_context_manager():
    """Example: Using context manager for automatic cleanup."""
    with IPCServer() as server:
        server.start(num_workers=4)
        
        # Poll messages
        msg = server.get_message(timeout_ms=1000)
        if msg:
            print(f"Got message: {msg.data}")
        
        # Get statistics
        stats = server.get_statistics()
        print(f"Active clients: {stats.active_clients}")
        print(f"Throughput: {stats.throughput_mbps:.2f} MB/s")

import time
def example_with_callback_fast():
    """Faster callback that doesn't block."""
    server = IPCServer(socket_path="/tmp/ipc_seqpacket.sock")
    if not server.start(num_workers=4):
        print("Failed to start server")
        exit(1)

    msg_count = 0
    start_time = time.time()
    
    def fast_callback(msg, timestamp_us: int):
        pass
        nonlocal msg_count
        msg_count += 1
        # DO NOT do heavy work here—callback must be fast
        # Just log/buffer; process asynchronously if needed
        if msg_count % 10000 == 0:
            elapsed = time.time() - start_time
            rate = msg_count / elapsed
            print(f"Processed {msg_count} msgs @ {rate:.0f} msg/s")
    
    server.set_message_callback(fast_callback)
    
    try:
        while server.is_running():
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        server.stop()

if __name__ == "__main__":
    # Uncomment examples to test
    example_basic_usage()
    #example_with_callback()
    #example_with_callback_fast()
    # example_context_manager()
    pass
