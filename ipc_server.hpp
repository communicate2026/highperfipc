#ifndef IPC_SERVER_HPP
#define IPC_SERVER_HPP

#include <cstdint>
#include <cstring>
#include <thread>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <chrono>
#include <vector>
#include <memory>
#include <iostream>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>

#define MAX_MSG_SIZE      16384
#define BACKLOG           1024
#define MAX_EVENTS        512
#define MAX_QUEUE_SIZE    1000000
#define QUEUE_DROP_PERCENT 10
#define NUM_SHARDS        8        /* Sharded queue to reduce contention */

/* Cache-line padding to prevent false sharing */
#define CACHE_LINE 64

/* Message: movable, non-copyable-in-hot-path.
 * Timestamp is optional (set explicitly) to save a clock read per message. */
struct Message {
    std::vector<uint8_t> data;
    size_t   bytes_received = 0;
    uint64_t timestamp_us   = 0;

    Message() = default;

    Message(const uint8_t* buf, size_t len, uint64_t ts_us = 0)
        : data(buf, buf + len), bytes_received(len), timestamp_us(ts_us) {}

    /* Enable cheap move; disable implicit copy in hot paths by using std::move */
    Message(Message&&) noexcept            = default;
    Message& operator=(Message&&) noexcept = default;
    Message(const Message&)                = default;   /* still allowed if needed */
    Message& operator=(const Message&)     = default;
};

struct Statistics {
    uint64_t total_messages;
    uint64_t total_bytes;
    uint64_t active_clients;
    uint64_t dropped_messages;
    uint64_t queue_size;
    uint64_t callback_call_count;
    uint64_t get_message_count;
    double   throughput_mbps;
    double   msg_rate_per_sec;
    double   avg_msg_size;
};

/* Ring-buffer speed meter (fixed size, no allocations, no erase()) */
class SpeedMeter {
    struct Sample {
        uint64_t messages;
        uint64_t bytes;
        std::chrono::steady_clock::time_point time;
    };
    static constexpr int WINDOW_SECONDS = 5;
    static constexpr int CAPACITY       = 16;   /* > WINDOW_SECONDS */
    Sample   ring_[CAPACITY]{};
    size_t   head_ = 0;
    size_t   count_ = 0;
    std::mutex m_;

public:
    void record(uint64_t msgs, uint64_t bytes) {
        std::lock_guard<std::mutex> lk(m_);
        auto now = std::chrono::steady_clock::now();
        ring_[head_] = {msgs, bytes, now};
        head_ = (head_ + 1) % CAPACITY;
        if (count_ < CAPACITY) ++count_;
    }

    void get_smoothed_rates(double& msg_rate, double& mbps) {
        std::lock_guard<std::mutex> lk(m_);
        msg_rate = mbps = 0.0;
        if (count_ < 2) return;
        auto now    = std::chrono::steady_clock::now();
        auto cutoff = now - std::chrono::seconds(WINDOW_SECONDS);
        uint64_t tmsg = 0, tby = 0;
        std::chrono::steady_clock::time_point first{}, last{};
        bool have_first = false;
        for (size_t i = 0; i < count_; ++i) {
            const auto& s = ring_[(head_ + CAPACITY - 1 - i) % CAPACITY];
            if (s.time < cutoff) break;
            tmsg += s.messages;
            tby  += s.bytes;
            if (!have_first) { last = s.time; have_first = true; }
            first = s.time;
        }
        double elapsed = std::chrono::duration<double>(last - first).count();
        if (elapsed > 0.0) {
            msg_rate = tmsg / elapsed;
            mbps     = (tby / (1024.0 * 1024.0)) / elapsed;
        }
    }
};

/* One shard of the message queue: its own mutex + cv + deque, cache-aligned */
struct alignas(CACHE_LINE) QueueShard {
    std::deque<Message>     q;
    std::mutex              m;
    std::condition_variable cv;
    char pad_[CACHE_LINE];  /* avoid false sharing between shards */
};

class IPCServer {
private:
    std::string socket_path;
    int         server_sock;

    /* Threading */
    std::vector<std::thread> worker_threads;
    std::thread              stats_thread;
    std::thread              callback_thread;

    /* Sharded queue - reduces lock contention N-fold */
    std::unique_ptr<QueueShard[]> shards;
    alignas(CACHE_LINE) std::atomic<uint64_t> queue_depth{0};
    std::atomic<uint32_t>         rr_producer{0};   /* round-robin producer counter */
    std::atomic<uint32_t>         rr_consumer{0};   /* round-robin consumer counter */

    /* Statistics (cache-aligned, relaxed on hot path) */
    alignas(CACHE_LINE) std::atomic<uint64_t> total_messages{0};
    alignas(CACHE_LINE) std::atomic<uint64_t> total_bytes{0};
    alignas(CACHE_LINE) std::atomic<uint64_t> active_clients{0};
    alignas(CACHE_LINE) std::atomic<uint64_t> dropped_messages{0};
    alignas(CACHE_LINE) std::atomic<uint64_t> callback_call_count{0};
    alignas(CACHE_LINE) std::atomic<uint64_t> get_message_count{0};
    alignas(CACHE_LINE) std::atomic<bool>     running{false};

    SpeedMeter speed_meter;

    /* Callback stored via shared_ptr for lock-free reads */
    using CallbackFn = std::function<void(const Message&)>;
    using BatchCallbackFn = std::function<void(const std::vector<Message>&)>;
    std::shared_ptr<CallbackFn> callback_ptr;  /* atomic ops via std::atomic_load/store */
    std::shared_ptr<BatchCallbackFn> batch_callback_ptr;  /* atomic ops via std::atomic_load/store */
    std::condition_variable callback_set_cv;
    std::mutex                   callback_set_lock;
    std::atomic<uint32_t> stats_interval_sec{1};  // Configurable interval

    /* Helpers */
    static int  set_nonblocking(int fd);
    void        worker_loop(int worker_id);
    void        stats_loop();
    void        callback_loop();
    void        enqueue_message(Message&& msg);
    bool        dequeue_one(Message& out, int timeout_ms);
    size_t      dequeue_batch(std::vector<Message>& out_messages, size_t max_count, int timeout_ms);
    void        wake_all_shards();

public:
    explicit IPCServer(const std::string& path = "/tmp/ipc_seqpacket.sock");
    ~IPCServer();

    bool start(int num_workers = 4);
    void stop();

    void set_message_callback(CallbackFn cb);
    void set_batch_message_callback(BatchCallbackFn cb, size_t batch_size = 100, int batch_timeout_ms = 10);
    bool get_message(Message& msg, int timeout_ms = 100);
    size_t get_messages(std::vector<Message>& messages, size_t max_count, int timeout_ms = 100);
    Statistics get_statistics();
    bool is_running() const { return running.load(std::memory_order_acquire); }
    void set_stats_interval(uint32_t seconds) {
        if (seconds < 1) seconds = 1;
        if (seconds > 3600) seconds = 3600;
        stats_interval_sec.store(seconds, std::memory_order_relaxed);
    }
};

#ifdef __cplusplus
extern "C" {
#endif

typedef void* IPCServerHandle;

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
size_t          IPCServer_get_messages(
                    IPCServerHandle handle,
                    uint8_t* out_data,      /* Pointer to contiguous buffer */
                    size_t* out_sizes,      /* Array of message sizes */
                    uint64_t* out_timestamps, /* Array of timestamps */
                    size_t max_count,       /* Max messages to retrieve */
                    int timeout_ms
                );
void            IPCServer_get_statistics(IPCServerHandle handle, Statistics* out_stats);
bool            IPCServer_is_running(IPCServerHandle handle);
void            IPCServer_set_message_callback(
                    IPCServerHandle handle,
                    void (*cb)(const uint8_t* data, size_t size, uint64_t timestamp_us)
                );
void            IPCServer_set_batch_message_callback(
                    IPCServerHandle handle,
                    void (*cb)(const uint8_t** data_array, const size_t* sizes, 
                               const uint64_t* timestamps, size_t count)
                );

#ifdef __cplusplus
}
#endif

#endif /* IPC_SERVER_HPP */
