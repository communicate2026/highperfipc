// g++ -O3 -std=c++17 -fPIC -shared -o libipc_server.so ipc_server.cpp -lpthread

#include "ipc_server.hpp"
#include <algorithm>
#include <errno.h>
#include <sys/uio.h>
#include <time.h>
#include <iomanip>
#include <ctime>


/* -------- Time helper: coarse monotonic, ~microsecond, no syscall on Linux -------- */
static inline uint64_t now_us_coarse() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC_COARSE, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000ULL +
           static_cast<uint64_t>(ts.tv_nsec) / 1000ULL;
}

IPCServer::IPCServer(const std::string& path, size_t max_queue_size_val, size_t queue_drop_percent_val, size_t num_shards_val)
    : socket_path(path), server_sock(-1),
      max_queue_size(max_queue_size_val),
      queue_drop_percent(queue_drop_percent_val),
      num_shards(num_shards_val),
      max_msg_size(DEFAULT_MAX_MSG_SIZE),
      backlog(DEFAULT_BACKLOG),
      max_events(DEFAULT_MAX_EVENTS),
      shards(std::make_unique<QueueShard[]>(num_shards_val)) {}

IPCServer::~IPCServer() {
    stop();
    /* server_sock already closed & set to -1 by stop() */
}

int IPCServer::set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

/* Producer: pick a shard by round-robin, enqueue with bounded drop-oldest policy */
void IPCServer::enqueue_message(Message&& msg) {
    uint32_t idx = rr_producer.fetch_add(1, std::memory_order_relaxed) % num_shards;
    QueueShard& sh = shards[idx];

    size_t per_shard_limit = max_queue_size / num_shards;

    {
        std::lock_guard<std::mutex> lk(sh.m);
        if (sh.q.size() >= per_shard_limit) {
            size_t drop_count = (per_shard_limit * queue_drop_percent) / 100;
            if (drop_count == 0) drop_count = 1;
            for (size_t i = 0; i < drop_count && !sh.q.empty(); ++i) {
                sh.q.pop_front();
                dropped_messages.fetch_add(1, std::memory_order_relaxed);
                queue_depth.fetch_sub(1, std::memory_order_relaxed);  /* Track drop */
            }
        }
        sh.q.emplace_back(std::move(msg));
        queue_depth.fetch_add(1, std::memory_order_relaxed);  /* Increment on enqueue */
    }
    sh.cv.notify_one();
}


/* Consumer: round-robin scan for a non-empty shard, then block on the "hint" shard */
bool IPCServer::dequeue_one(Message& out, int timeout_ms) {
    uint32_t start = rr_consumer.fetch_add(1, std::memory_order_relaxed);

    /* First: fast non-blocking scan of all shards */
    for (uint32_t i = 0; i < num_shards; ++i) {
        QueueShard& sh = shards[(start + i) % num_shards];
        std::unique_lock<std::mutex> lk(sh.m, std::try_to_lock);
        if (!lk.owns_lock()) continue;
        if (!sh.q.empty()) {
            out = std::move(sh.q.front());
            sh.q.pop_front();
            queue_depth.fetch_sub(1, std::memory_order_relaxed);  /* Decrement on dequeue */
            return true;
        }
    }

    /* Nothing available: block on one shard with timeout */
    QueueShard& sh = shards[start % num_shards];
    std::unique_lock<std::mutex> lk(sh.m);
    if (!sh.cv.wait_for(lk, std::chrono::milliseconds(timeout_ms),
                        [&] { return !sh.q.empty() ||
                                     !running.load(std::memory_order_acquire); })) {
        return false;
    }
    if (sh.q.empty()) return false;
    out = std::move(sh.q.front());
    sh.q.pop_front();
    queue_depth.fetch_sub(1, std::memory_order_relaxed);  /* Decrement on dequeue */
    return true;
}

/* Consumer: dequeue multiple messages in batch */
size_t IPCServer::dequeue_batch(std::vector<Message>& out_messages, size_t max_count, int timeout_ms) {
    if (max_count == 0 || !running.load(std::memory_order_acquire)) {
        return 0;
    }

    out_messages.clear();
    out_messages.reserve(max_count);

    /* First: fast non-blocking scan of all shards */
    uint32_t start = rr_consumer.fetch_add(1, std::memory_order_relaxed);
    size_t count = 0;

    for (uint32_t i = 0; i < num_shards && count < max_count; ++i) {
        QueueShard& sh = shards[(start + i) % num_shards];
        std::unique_lock<std::mutex> lk(sh.m, std::try_to_lock);
        if (!lk.owns_lock()) continue;
        
        while (!sh.q.empty() && count < max_count) {
            out_messages.emplace_back(std::move(sh.q.front()));
            sh.q.pop_front();
            queue_depth.fetch_sub(1, std::memory_order_relaxed);
            ++count;
        }
    }

    if (count > 0) {
        return count;
    }

    /* Nothing available: block on one shard with timeout */
    QueueShard& sh = shards[start % num_shards];
    std::unique_lock<std::mutex> lk(sh.m);
    
    if (!sh.cv.wait_for(lk, std::chrono::milliseconds(timeout_ms),
                        [&] { return !sh.q.empty() ||
                                     !running.load(std::memory_order_acquire); })) {
        return 0;
    }

    /* Drain as many as possible up to max_count */
    while (!sh.q.empty() && count < max_count && running.load(std::memory_order_acquire)) {
        out_messages.emplace_back(std::move(sh.q.front()));
        sh.q.pop_front();
        queue_depth.fetch_sub(1, std::memory_order_relaxed);
        ++count;
    }

    return count;
}


void IPCServer::wake_all_shards() {
    for (int i = 0; i < num_shards; ++i) shards[i].cv.notify_all();
}

bool IPCServer::start(int num_workers, size_t max_msg_size_val, int backlog_val) {
    if (running.load(std::memory_order_acquire)) {
        std::cerr << "Server already running\n";
        return false;
    }
    if (num_workers <= 0)  num_workers = 1;
    if (num_workers > 64)  num_workers = 64;

    /* Update configurable parameters */
    max_msg_size = max_msg_size_val;
    backlog = backlog_val;

    /* Ignore SIGPIPE globally so a client vanishing never kills us */
    struct sigaction sa{};
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, nullptr);

    server_sock = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (server_sock < 0) { perror("socket"); return false; }

    unlink(socket_path.c_str());

    struct sockaddr_un addr{};
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, socket_path.c_str(), sizeof(addr.sun_path) - 1);

    if (bind(server_sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind"); close(server_sock); server_sock = -1; return false;
    }
    if (listen(server_sock, backlog) < 0) {
        perror("listen"); close(server_sock); server_sock = -1; return false;
    }

    /* Larger kernel recv buffer for high-throughput scenarios */
    int rcvbuf = 8 * 1024 * 1024;
    setsockopt(server_sock, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    running.store(true, std::memory_order_release);

    for (int i = 0; i < num_workers; ++i) {
        worker_threads.emplace_back([this, i]() { worker_loop(i); });
    }
    callback_thread = std::thread([this]() { callback_loop(); });
    stats_thread    = std::thread([this]() { stats_loop(); });

    std::cout << "IPCServer started with " << num_workers
              << " workers, " << num_shards << " queue shards\n"
              << "Socket: " << socket_path << "\n"
              << "Max queue: " << max_queue_size
              << " (drop " << queue_drop_percent << "% when full)\n";
    return true;
}

void IPCServer::stop() {
    if (!running.exchange(false, std::memory_order_acq_rel)) return;

    /* Wake every consumer */   
    wake_all_shards();        
    /* ← ADD THIS LINE: Wake the callback thread */    
    callback_set_cv.notify_one();

    if (callback_thread.joinable()) callback_thread.join();
    if (stats_thread.joinable())    stats_thread.join();
    for (auto& t : worker_threads)  if (t.joinable()) t.join();
    worker_threads.clear();

    if (server_sock >= 0) {
        close(server_sock);
        server_sock = -1;
    }
    unlink(socket_path.c_str());
}

void IPCServer::set_message_callback(CallbackFn cb) {
    auto sp = std::make_shared<CallbackFn>(std::move(cb));
    {
        std::lock_guard<std::mutex> lk(callback_set_lock);
        std::atomic_store_explicit(&callback_ptr, sp, std::memory_order_release);
    }
    /* Wake the callback thread immediately (even if already running) */
    callback_set_cv.notify_one();
}

void IPCServer::set_batch_message_callback(BatchCallbackFn cb, size_t batch_size, int batch_timeout_ms) {
    auto sp = std::make_shared<BatchCallbackFn>(std::move(cb));
    {
        std::lock_guard<std::mutex> lk(callback_set_lock);
        std::atomic_store_explicit(&batch_callback_ptr, sp, std::memory_order_release);
    }
    /* Wake the callback thread immediately */
    callback_set_cv.notify_one();
    
    /* Store batch parameters in a member or use lambda capture - for now use simple approach */
    /* The callback_loop will read batch_callback_ptr and use fixed batch params */
    /* For more flexibility, could store batch_size/timeout as atomics */
}


bool IPCServer::get_message(Message& msg, int timeout_ms) {
    bool rt = dequeue_one(msg, timeout_ms);
    if(rt == true)    
        get_message_count.fetch_add(1, std::memory_order_relaxed);    
    return rt;
}

size_t IPCServer::get_messages(std::vector<Message>& messages, size_t max_count, int timeout_ms) {
    size_t count = dequeue_batch(messages, max_count, timeout_ms);
    if (count > 0) {
        get_message_count.fetch_add(count, std::memory_order_relaxed);
    }
    return count;
}

Statistics IPCServer::get_statistics() {
    double msg_rate = 0.0, throughput_mbps = 0.0;
    speed_meter.get_smoothed_rates(msg_rate, throughput_mbps);

    uint64_t total_msgs = total_messages.load(std::memory_order_relaxed);
    uint64_t total_bts  = total_bytes.load(std::memory_order_relaxed);
    uint64_t dropped    = dropped_messages.load(std::memory_order_relaxed);
    uint64_t avg_size   = (total_msgs > 0) ? (total_bts / total_msgs) : 0;
    uint64_t callback_call = callback_call_count.load(std::memory_order_relaxed);
    uint64_t get_message_call  = get_message_count.load(std::memory_order_relaxed);
    
    /* Use atomic counter instead of lock-free scan */
    uint64_t depth = queue_depth.load(std::memory_order_relaxed);
    
    return Statistics{
        total_msgs,
        total_bts,
        active_clients.load(std::memory_order_relaxed),
        dropped,
        depth,  /* Now exact, not approximate */
        callback_call,
        get_message_call,
        throughput_mbps,
        msg_rate,
        static_cast<double>(avg_size)
    };
}


/* ---------------- worker ---------------- */
void IPCServer::worker_loop(int /*worker_id*/) {
    int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd < 0) { perror("epoll_create1"); return; }

    /* EPOLLEXCLUSIVE stops the thundering herd across worker epoll sets.
     * Kernel wakes only ONE epoll waiter per new connection. */
    struct epoll_event ev{};
    ev.events  = EPOLLIN | EPOLLEXCLUSIVE;
    ev.data.fd = server_sock;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_sock, &ev) < 0) {
        perror("epoll_ctl add server_sock (EPOLLEXCLUSIVE)");
        /* Fallback without EPOLLEXCLUSIVE for old kernels */
        ev.events = EPOLLIN;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_sock, &ev) < 0) {
            perror("epoll_ctl add server_sock"); close(epoll_fd); return;
        }
    }

    /* Track FDs owned by this worker so we can close them on shutdown (no leak) */
    std::vector<int> owned_fds;
    owned_fds.reserve(1024);

    struct epoll_event events[max_events];

    /* Reusable read buffer sits in Message.data directly to avoid extra copy */
    while (running.load(std::memory_order_acquire)) {
        int nfds = epoll_wait(epoll_fd, events, max_events, 100);
        if (nfds < 0) {
            if (errno != EINTR) perror("epoll_wait");
            continue;
        }

        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;

            /* Accept path */
            if (fd == server_sock) {
                for (;;) {
                    int client_fd = accept4(server_sock, nullptr, nullptr,
                                            SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (client_fd < 0) {
                        if (errno != EAGAIN && errno != EWOULDBLOCK) perror("accept4");
                        break;  /* drained */
                    }
                    int rcv = 4 * 1024 * 1024;
                    setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv));

                    struct epoll_event cev{};
                    cev.events  = EPOLLIN | EPOLLET | EPOLLRDHUP;
                    cev.data.fd = client_fd;
                    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &cev) < 0) {
                        perror("epoll_ctl add client");
                        close(client_fd);
                        continue;
                    }
                    owned_fds.push_back(client_fd);
                    active_clients.fetch_add(1, std::memory_order_relaxed);
                }
                continue;
            }

            /* Peer hang-up */
            if (events[i].events & (EPOLLHUP | EPOLLRDHUP | EPOLLERR)) {
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                close(fd);
                owned_fds.erase(std::remove(owned_fds.begin(), owned_fds.end(), fd),
                                owned_fds.end());
                active_clients.fetch_sub(1, std::memory_order_relaxed);
                continue;
            }

            /* Edge-triggered: drain the socket */
            bool close_client = false;
            for (;;) {
                Message msg;
                msg.data.resize(max_msg_size);   /* one allocation, then move */

                /* Use recvmsg to detect truncation cheaply (MSG_TRUNC flag) */
                struct iovec iov{};
                iov.iov_base = msg.data.data();
                iov.iov_len  = max_msg_size;
                struct msghdr mh{};
                mh.msg_iov    = &iov;
                mh.msg_iovlen = 1;

                ssize_t n = recvmsg(fd, &mh, MSG_DONTWAIT);
                if (n < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) break;   /* drained */
                    std::cerr << "[ERROR] recvmsg fd=" << fd << ": "
                              << strerror(errno) << "\n";
                    close_client = true;
                    break;
                }
                if (n == 0) {              /* peer closed */
                    close_client = true;
                    break;
                }
                if (mh.msg_flags & MSG_TRUNC) {
                    std::cerr << "[ERROR] Truncated SEQPACKET on fd=" << fd
                              << " (>" << max_msg_size << " B). Dropped.\n";
                    dropped_messages.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }

                msg.data.resize(static_cast<size_t>(n));   /* no reallocation, just size */
                msg.bytes_received = static_cast<size_t>(n);
                msg.timestamp_us   = now_us_coarse();

                total_messages.fetch_add(1, std::memory_order_relaxed);
                total_bytes.fetch_add(static_cast<uint64_t>(n),
                                      std::memory_order_relaxed);

                enqueue_message(std::move(msg));
            }

            if (close_client) {
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                close(fd);
                owned_fds.erase(std::remove(owned_fds.begin(), owned_fds.end(), fd),
                                owned_fds.end());
                active_clients.fetch_sub(1, std::memory_order_relaxed);
            }
        }
    }

    /* --- Shutdown: close every client FD this worker owns (fixes leak) --- */
    for (int fd : owned_fds) {
        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
    }
    close(epoll_fd);
}

/* ---------------- callback thread ---------------- */
void IPCServer::callback_loop() {
    while (running.load(std::memory_order_acquire)) {  
        /* Load callbacks lock-free */
        auto cb = std::atomic_load_explicit(&callback_ptr, std::memory_order_acquire);
        auto batch_cb = std::atomic_load_explicit(&batch_callback_ptr, std::memory_order_acquire);
        
        bool has_single_cb = (cb && *cb);
        bool has_batch_cb = (batch_cb && *batch_cb);
        
        if (!has_single_cb && !has_batch_cb) {
            /* No callback set: wait for one to be set or server to stop */
            std::unique_lock<std::mutex> lk(callback_set_lock);
            callback_set_cv.wait(lk, [this] {
                auto ptr = std::atomic_load_explicit(&callback_ptr, std::memory_order_acquire);
                auto batch_ptr = std::atomic_load_explicit(&batch_callback_ptr, std::memory_order_acquire);
                return static_cast<bool>((ptr && *ptr) || (batch_ptr && *batch_ptr)) 
                       || !running.load(std::memory_order_acquire);
            });
            continue;
        }

        if (has_batch_cb) {
            /* Batch callback mode: collect messages and invoke with batch */
            std::vector<Message> batch;
            size_t count = dequeue_batch(batch, 100, 10);  /* Default batch params */
            if (count == 0) continue;
            
            callback_call_count.fetch_add(count, std::memory_order_relaxed);
            try {
                (*batch_cb)(batch);
            } catch (const std::exception& e) {
                std::cerr << "[ERROR] Batch callback exception: " << e.what() << "\n";
            } catch (...) {
                std::cerr << "[ERROR] Batch callback: unknown exception\n";
            }
        } else {
            /* Single message callback mode */
            Message msg;
            if (!dequeue_one(msg, 100)) continue;
            
            callback_call_count.fetch_add(1, std::memory_order_relaxed);
            try {
                (*cb)(msg);
            } catch (const std::exception& e) {
                std::cerr << "[ERROR] Callback exception: " << e.what() << "\n";
            } catch (...) {
                std::cerr << "[ERROR] Callback: unknown exception\n";
            }
        }
    }

    /* Drain remaining messages on shutdown */
    if (auto batch_cb = std::atomic_load_explicit(&batch_callback_ptr, std::memory_order_acquire);
        batch_cb && *batch_cb) {
        /* Drain in batches */
        std::vector<Message> batch;
        while (dequeue_batch(batch, 100, 0) > 0) {
            try {
                (*batch_cb)(batch);
            } catch (...) {
                /* Suppress exceptions during shutdown drain */
            }
        }
    } else if (auto cb = std::atomic_load_explicit(&callback_ptr, std::memory_order_acquire);
               cb && *cb) {
        /* Drain single messages */
        Message msg;
        while (dequeue_one(msg, 0)) {
            try {
                (*cb)(msg);
            } catch (...) {
                /* Suppress exceptions during shutdown drain */
            }
        }
    }
}

/* ---------------- stats thread ---------------- */
void IPCServer::stats_loop() {
    uint64_t prev_msgs = 0, prev_bytes = 0;

    while (running.load(std::memory_order_acquire)) {
        uint32_t interval = stats_interval_sec.load(std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::seconds(interval));

        uint64_t cur_msgs  = total_messages.load(std::memory_order_relaxed);
        uint64_t cur_bytes = total_bytes.load(std::memory_order_relaxed);

        speed_meter.record(cur_msgs - prev_msgs, cur_bytes - prev_bytes);
        prev_msgs  = cur_msgs;
        prev_bytes = cur_bytes;

        Statistics s = get_statistics();

        std::cout << "[STATS @ " << std::time(nullptr) << "] "
                  << "Rate: "     << static_cast<uint64_t>(s.msg_rate_per_sec) << " msg/s | "
                  << "Tput: "     << std::fixed << std::setprecision(2) 
                  << s.throughput_mbps                                        << " MB/s | "
                  << "Avg size: " << s.avg_msg_size                          << " B | "
                  << "Clients: "  << s.active_clients                        << " | "
                  << "Total: "    << s.total_messages;
        if (s.dropped_messages > 0) {
            std::cout << " | DROPPED: " << s.dropped_messages;
        }
        std::cout << " | Queue: " << s.queue_size << "/" << max_queue_size 
                  << " | CB calls: " << s.callback_call_count
                  << " | Get calls: " << s.get_message_count << "\n";
        std::cout.flush();
    }
}
