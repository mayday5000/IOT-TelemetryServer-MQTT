// client.cpp
// Purpose: MQTT-aware edge-triggered epoll reactor for IoT telemetry.
// How it works: reactor accepts, parses MQTT CONNECT, queues packets for workers;
// non-blocking write pipeline handles outbound CONNACK and other responses.

#include <iostream>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <cstring>
#include <cstdint>
#include <chrono>
#include <csignal>
#include <cerrno>
#include <expected>
#include <span>

// Linux Native System Headers
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>

constexpr size_t MAX_EVENTS        = 8;
constexpr size_t TARGET_STACK_SIZE = 64 * 1024; // Low-power 64 KB thread micro-stack
constexpr int    FLUSH_TIMEOUT_MS  = 1000;      // Max time to flush pending writes on shutdown

// ============================================================================
// 🌊 1. EDGE-TRIGGERED NON-BLOCKING ASYNC WRITE PIPELINE
// ============================================================================
class ClientWritePipeline {
private:
    int socket_fd_;
    int epoll_fd_;
    std::queue<std::vector<uint8_t>> outbound_queue_;
    size_t pending_offset_ = 0;
    std::mutex write_mutex_;
    std::atomic<bool> broken_{false}; // Set once a send fails hard (EPIPE, ECONNRESET, ...)

    void update_epoll_mask(bool enable_write) {
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP; 
        if (enable_write) {
            ev.events |= EPOLLOUT; 
        }
        ev.data.fd = socket_fd_;
        epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, socket_fd_, &ev);
    }

public:
    bool is_broken() const { return broken_.load(std::memory_order_relaxed); }

    bool has_pending() {
        std::lock_guard<std::mutex> lock(write_mutex_);
        return !outbound_queue_.empty();
    }

    ClientWritePipeline(int socket_fd, int epoll_fd) 
        : socket_fd_(socket_fd), epoll_fd_(epoll_fd) {}

    // Returns false on a hard socket error (EPIPE, ECONNRESET, ...): the connection is gone
    bool send_payload(const uint8_t* data, size_t length) {
        std::lock_guard<std::mutex> lock(write_mutex_);
        if (broken_.load(std::memory_order_relaxed)) return false;

        if (!outbound_queue_.empty()) {
            outbound_queue_.push(std::vector<uint8_t>(data, data + length));
            return true;
        }

        ssize_t bytes_written;
        do { // MSG_NOSIGNAL: a reset peer yields EPIPE instead of a process-killing SIGPIPE
            bytes_written = send(socket_fd_, data, length, MSG_NOSIGNAL);
        } while (bytes_written == -1 && errno == EINTR);

        if (bytes_written >= 0) {
            size_t written = static_cast<size_t>(bytes_written);
            if (written < length) {
                std::vector<uint8_t> remaining(data + written, data + length);
                outbound_queue_.push(std::move(remaining));
                pending_offset_ = 0;
                update_epoll_mask(true);
            }
        } else {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                outbound_queue_.push(std::vector<uint8_t>(data, data + length));
                pending_offset_ = 0;
                update_epoll_mask(true);
            } else {
                broken_.store(true, std::memory_order_relaxed);
                return false;
            }
        }
        return true;
    }

    // Returns false on a hard socket error (EPIPE, ECONNRESET, ...)
    bool handle_write_ready() {
        std::lock_guard<std::mutex> lock(write_mutex_);
        if (broken_.load(std::memory_order_relaxed)) return false;

        while (!outbound_queue_.empty()) {
            const auto& current_chunk = outbound_queue_.front();
            size_t bytes_left = current_chunk.size() - pending_offset_;

            ssize_t bytes_written = send(socket_fd_, current_chunk.data() + pending_offset_, bytes_left, MSG_NOSIGNAL);

            if (bytes_written >= 0) {
                size_t written = static_cast<size_t>(bytes_written);
                if (written < bytes_left) {
                    pending_offset_ += written;
                    return true; 
                } else {
                    outbound_queue_.pop();
                    pending_offset_ = 0;
                }
            } else {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) return true;
                broken_.store(true, std::memory_order_relaxed); // EPIPE / ECONNRESET: peer is gone
                return false;
            }
        }
        update_epoll_mask(false); // Queue dry. Stop spinning.
        return true;
    }
};

#include "mqtt_connect_frame.hpp"

// Utility function to modify socket control descriptor settings
bool make_socket_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) != -1;
}

// ============================================================================
// 🏛️ 3. MAIN RUNTIME EXECUTION GATEWAY
// ============================================================================

// Global stop flag reachable by signal handler
std::atomic<bool> g_client_active{true};

// Shutdown "doorbell": eventfd watched by epoll so the reactor can sleep with no timeout
int g_wake_fd = -1;

void signal_handler(int) {
    int saved_errno = errno;
    g_client_active.store(false, std::memory_order_relaxed);
    uint64_t one = 1;
    if (g_wake_fd != -1) (void)!write(g_wake_fd, &one, sizeof(one)); // async-signal-safe wake
    errno = saved_errno;
}

int main(int argc, char* argv[]) {
    std::string device_name = (argc > 1) ? argv[1] : "IoT_Edge_Node_01";
    std::string server_ip   = (argc > 2) ? argv[2] : "127.0.0.1";
    int server_port         = 8883;

    // Create the shutdown eventfd before handlers can fire
    g_wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (g_wake_fd == -1) {
        std::cerr << "[CRITICAL] eventfd creation failed.\n";
        return 1;
    }

    // Install graceful shutdown handlers
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Belt and braces with MSG_NOSIGNAL: writing to a reset server must never kill the client
    struct sigaction ignore_pipe{};
    ignore_pipe.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &ignore_pipe, nullptr);

    // Allocate connection socket
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd == -1) {
        std::cerr << "[CRITICAL] Failed generating socket structures.\n";
        return 1;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    inet_pton(AF_INET, server_ip.c_str(), &server_addr.sin_addr);

    std::cout << "[CLIENT] Attempting connection link to server: " << server_ip << ":" << server_port << "\n";
    
    // Connect synchronously first, then shift to non-blocking edge pipelines
    if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "[ERROR] Physical remote connection link refused or timeout reached.\n";
        close(sock_fd);
        return 1;
    }

    if (!make_socket_non_blocking(sock_fd)) {
        std::cerr << "[ERROR] Failed configuring async flags.\n";
        close(sock_fd);
        return 1;
    }

    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        close(sock_fd);
        return 1;
    }

    // Register our runtime interface with Edge-Triggered connection drop monitoring configurations
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
    ev.data.fd = sock_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock_fd, &ev);

    // Register the shutdown eventfd so a signal wakes the reactor instantly
    epoll_event wake_ev{};
    wake_ev.events = EPOLLIN;
    wake_ev.data.fd = g_wake_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, g_wake_fd, &wake_ev);

    auto pipeline = std::make_shared<ClientWritePipeline>(sock_fd, epoll_fd);

    // Build CONNECT frame once
    auto connect_packet = MqttPacketFactory::create_connect_frame(device_name);

    // Lets the reactor interrupt the telemetry thread's 2 s sleep on shutdown
    std::mutex sleep_mutex;
    std::condition_variable sleep_cv;

    // ============================================================================
    // 🔒 4. DECOUPLED TELEMETRY HARNESS LOOP (LOW-POWER BACKGROUND THREAD)
    // ============================================================================
    std::thread telemetry_worker([pipeline, connect_packet, device_name, &sleep_mutex, &sleep_cv]() {
        // Step A: Send MQTT CONNECT
        std::cout << "[TELEMETRY] Sending CONNECT for device: " << device_name << "\n";
        if (!pipeline->send_payload(connect_packet.data(), connect_packet.size())) {
            std::cerr << "[TELEMETRY] CONNECT send failed (connection reset). Stopping.\n";
            g_client_active.store(false, std::memory_order_relaxed);
            uint64_t one = 1;
            (void)!write(g_wake_fd, &one, sizeof(one)); // Wake the reactor so it shuts down
            return;
        }

        uint32_t message_counter = 0;

        // Step B: Loop simulation representing ongoing low-overhead physical telemetry streams
        while (g_client_active.load(std::memory_order_relaxed)) {
            // Read physical sensors every 2 seconds; sleep, but wake at once on shutdown
            std::unique_lock<std::mutex> lock(sleep_mutex);
            if (sleep_cv.wait_for(lock, std::chrono::seconds(2),
                    [] { return !g_client_active.load(std::memory_order_relaxed); })) break;
            lock.unlock();

            std::string metric = "{\"msg_idx\":" + std::to_string(message_counter++) + ",\"temp_c\":24.85}";
            
            // Raw inline transfer to background writer pipeline
            if (!pipeline->send_payload(reinterpret_cast<const uint8_t*>(metric.data()), metric.length())) {
                std::cerr << "[TELEMETRY] Send failed (connection reset). Stopping.\n";
                g_client_active.store(false, std::memory_order_relaxed);
                uint64_t one = 1;
                (void)!write(g_wake_fd, &one, sizeof(one)); // Wake the reactor so it shuts down
                break;
            }
        }
        std::cout << "[TELEMETRY] Background worker detached cleanly.\n";
    });

    // ============================================================================
    // ⚡ 5. REACTOR COMPONENT: MONITORING LOOP FOR SYSTEM EVENTS
    // ============================================================================
    std::vector<epoll_event> events(MAX_EVENTS);
    std::cout << "[REACTOR] Client network loop activated safely.\n";
    bool peer_closed = false;

    while (g_client_active.load(std::memory_order_relaxed)) {
        // Sleep until socket I/O or the shutdown eventfd wakes us
        int num_events = epoll_wait(epoll_fd, events.data(), MAX_EVENTS, -1);
        if (num_events == -1) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < num_events; ++i) {
            if (events[i].data.fd == g_wake_fd) {
                uint64_t counter = 0;
                (void)!read(g_wake_fd, &counter, sizeof(counter)); // Drain the doorbell
                g_client_active.store(false, std::memory_order_relaxed);
                break;
            }
            if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                std::cerr << "[CLIENT] Connection closed by peer or error.\n";
                peer_closed = true;
                g_client_active.store(false, std::memory_order_relaxed);
                break;
            }
            if (events[i].events & EPOLLOUT) {
                if (!pipeline->handle_write_ready()) {
                    std::cerr << "[CLIENT] Write failed (connection reset by peer).\n";
                    peer_closed = true;
                    g_client_active.store(false, std::memory_order_relaxed);
                    break;
                }
            }
            if (events[i].events & EPOLLIN) {
                uint8_t buf[256];
                ssize_t n;
                while ((n = read(sock_fd, buf, sizeof(buf))) > 0) {
                    // Minimal CONNACK / data handling (teaching stub)
                    std::cout << "[REACTOR] Received " << n << " bytes from server\n";
                }
                if (n == 0) {
                    std::cerr << "[CLIENT] Peer closed connection.\n";
                    peer_closed = true;
                    g_client_active.store(false, std::memory_order_relaxed);
                    break;
                }
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(sleep_mutex);
        g_client_active.store(false, std::memory_order_relaxed);
    }
    sleep_cv.notify_all(); // Wake the telemetry thread from its 2 s sleep
    if (telemetry_worker.joinable()) telemetry_worker.join();
    if (pipeline->is_broken()) peer_closed = true; // A failed send means the server is gone

    // Bounded flush: give queued telemetry up to FLUSH_TIMEOUT_MS to reach the server.
    // Skipped if the peer is gone; a second Ctrl-C (eventfd) aborts it.
    if (!peer_closed && pipeline->has_pending()) {
        std::cout << "[CLIENT] Flushing pending telemetry before exit...\n";
        uint64_t counter = 0;
        (void)!read(g_wake_fd, &counter, sizeof(counter)); // Clear the first Ctrl-C (loop may have exited via EINTR)
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(FLUSH_TIMEOUT_MS);
        bool abort_flush = false;
        while (!abort_flush && pipeline->has_pending()) {
            auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
            if (left <= 0) break;
            int n = epoll_wait(epoll_fd, events.data(), MAX_EVENTS, static_cast<int>(left));
            if (n == -1) {
                if (errno == EINTR) continue;
                break;
            }
            for (int i = 0; i < n; ++i) {
                if (events[i].data.fd == g_wake_fd ||
                    (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))) { abort_flush = true; break; }
                if ((events[i].events & EPOLLOUT) && !pipeline->handle_write_ready()) { abort_flush = true; break; }
            }
        }
        if (pipeline->has_pending()) std::cerr << "[CLIENT] Unsent telemetry discarded at shutdown.\n";
    }

    close(sock_fd);
    close(epoll_fd);
    close(g_wake_fd);
    std::cout << "[CLIENT] Shutdown complete.\n";
    return 0;
}
