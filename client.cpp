// client.cpp
// Purpose: Low-power MQTT telemetry client for IoT edge devices.
// How it works: connect -> send CONNECT -> await CONNACK (chained with std::expected::and_then);
// telemetry starts only after the server accepts the client. On any failure or a dropped
// connection the client logs the reason and reconnects with exponential backoff.

#include <iostream>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <stop_token>
#include <atomic>
#include <memory>
#include <string>
#include <string_view>
#include <cstring>
#include <cstdint>
#include <chrono>
#include <csignal>
#include <cerrno>
#include <expected>
#include <span>
#include <algorithm>

// Linux Native System Headers
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#include "net_util.hpp"
#include "mqtt_connect_frame.hpp"

using namespace std::chrono_literals;

constexpr size_t   MAX_EVENTS       = 8;
constexpr uint16_t SERVER_PORT      = 8883;
constexpr auto     TELEMETRY_PERIOD = 2s;     // Sensor sample interval
constexpr auto     CONNECT_TIMEOUT  = 5s;     // TCP connect
constexpr auto     CONNACK_TIMEOUT  = 5s;     // Server must answer the CONNECT within this time
constexpr auto     BACKOFF_MIN      = 1s;     // First retry delay, doubled after each failure...
constexpr auto     BACKOFF_MAX      = 30s;    // ...up to this cap; reset after a successful handshake
constexpr auto     FLUSH_TIMEOUT    = 1000ms; // Max time to flush pending writes on shutdown

// ============================================================================
// Edge-triggered non-blocking async write pipeline (does not own the descriptors)
// ============================================================================
class ClientWritePipeline {
private:
    int socket_fd_;
    int epoll_fd_;
    std::queue<std::vector<uint8_t>> outbound_queue_;
    size_t pending_offset_ = 0;
    std::mutex write_mutex_;
    std::atomic<bool> broken_{false}; // Set once a send fails hard (EPIPE, ECONNRESET, ...)

    [[nodiscard]] bool update_epoll_mask(bool enable_write) {
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
        if (enable_write) ev.events |= EPOLLOUT;
        ev.data.fd = socket_fd_;
        return epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, socket_fd_, &ev) != -1;
    }

    bool fail() {
        broken_.store(true, std::memory_order_relaxed);
        return false;
    }

public:
    ClientWritePipeline(int socket_fd, int epoll_fd) : socket_fd_(socket_fd), epoll_fd_(epoll_fd) {}

    [[nodiscard]] bool is_broken() const { return broken_.load(std::memory_order_relaxed); }

    [[nodiscard]] bool has_pending() {
        std::lock_guard<std::mutex> lock(write_mutex_);
        return !outbound_queue_.empty();
    }

    // Returns false on a hard socket error (EPIPE, ECONNRESET, ...): the connection is gone
    [[nodiscard]] bool send_payload(std::span<const uint8_t> data) {
        std::lock_guard<std::mutex> lock(write_mutex_);
        if (is_broken()) return false;

        if (!outbound_queue_.empty()) {
            outbound_queue_.emplace(data.begin(), data.end());
            return true;
        }

        ssize_t bytes_written;
        do { // MSG_NOSIGNAL: a reset peer yields EPIPE instead of a process-killing SIGPIPE
            bytes_written = send(socket_fd_, data.data(), data.size(), MSG_NOSIGNAL);
        } while (bytes_written == -1 && errno == EINTR);

        if (bytes_written >= 0) {
            const auto rest = data.subspan(static_cast<size_t>(bytes_written));
            if (rest.empty()) return true;
            outbound_queue_.emplace(rest.begin(), rest.end());
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            outbound_queue_.emplace(data.begin(), data.end());
        } else {
            return fail();
        }
        pending_offset_ = 0;
        return update_epoll_mask(true) || fail();
    }

    // Returns false on a hard socket error (EPIPE, ECONNRESET, ...)
    [[nodiscard]] bool handle_write_ready() {
        std::lock_guard<std::mutex> lock(write_mutex_);
        if (is_broken()) return false;

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
                return fail(); // EPIPE / ECONNRESET: peer is gone
            }
        }
        return update_epoll_mask(false) || fail(); // Queue dry. Stop asking for EPOLLOUT.
    }
};

// ============================================================================
// Signals
// ============================================================================

// Global stop flag reachable by signal handler
std::atomic<bool> g_client_active{true};

// Plain copy of the shutdown "doorbell" eventfd (owned by a UniqueFd in main) for the signal handler
int g_wake_fd = -1;

void signal_handler(int) {
    int saved_errno = errno;
    g_client_active.store(false, std::memory_order_relaxed);
    uint64_t one = 1;
    if (g_wake_fd != -1) (void)!write(g_wake_fd, &one, sizeof(one)); // async-signal-safe wake
    errno = saved_errno;
}

// ============================================================================
// One accepted session: telemetry thread + reactor until shutdown or connection loss
// ============================================================================
enum class SessionEnd { Shutdown, ConnectionLost };

SessionEnd run_session(UniqueFd sock, uint32_t& message_counter) {
    // Session epoll set: socket, shutdown eventfd, and a "lost" eventfd the telemetry thread rings
    auto epoll = make_epoll();
    auto lost  = make_eventfd();
    if (!epoll || !lost) {
        std::cerr << "[CLIENT] Session setup failed: " << (!epoll ? epoll.error() : lost.error()) << "\n";
        return SessionEnd::ConnectionLost;
    }
    const int epoll_fd = epoll->get();
    const int lost_fd  = lost->get();
    for (auto [fd, mask] : {std::pair{sock.get(), uint32_t{EPOLLIN | EPOLLET | EPOLLRDHUP}},
                            std::pair{g_wake_fd, uint32_t{EPOLLIN}}, std::pair{lost_fd, uint32_t{EPOLLIN}}}) {
        if (auto added = epoll_add(epoll_fd, fd, mask); !added) {
            std::cerr << "[CLIENT] Session setup failed: " << added.error() << "\n";
            return SessionEnd::ConnectionLost;
        }
    }

    ClientWritePipeline pipeline(sock.get(), epoll_fd);

    // Low-power telemetry thread: sleeps between samples; request_stop() wakes it at once
    std::jthread telemetry([&](std::stop_token stop) {
        std::mutex sleep_mutex;
        std::condition_variable_any sleep_cv;
        while (true) {
            {
                std::unique_lock lock(sleep_mutex);
                (void)sleep_cv.wait_for(lock, stop, TELEMETRY_PERIOD, [] { return false; });
            }
            if (stop.stop_requested()) break;

            std::string metric = "{\"msg_idx\":" + std::to_string(message_counter++) + ",\"temp_c\":24.85}";
            if (!pipeline.send_payload(std::span(reinterpret_cast<const uint8_t*>(metric.data()), metric.size()))) {
                std::cerr << "[TELEMETRY] Send failed: " << std::strerror(errno) << ".\n";
                uint64_t one = 1;
                (void)!write(lost_fd, &one, sizeof(one)); // Tell the reactor the connection is gone
                break;
            }
        }
    });

    std::vector<epoll_event> events(MAX_EVENTS);
    const char* reason = nullptr;

    while (reason == nullptr && g_client_active.load(std::memory_order_relaxed)) {
        // Sleep until socket I/O or an eventfd wakes us
        int num_events = epoll_wait(epoll_fd, events.data(), MAX_EVENTS, -1);
        if (num_events == -1) {
            if (errno == EINTR) continue;
            reason = "epoll_wait failed";
            break;
        }

        for (int i = 0; i < num_events && reason == nullptr; ++i) {
            const int fd = events[i].data.fd;
            const uint32_t revents = events[i].events;

            if (fd == g_wake_fd) {
                uint64_t counter = 0;
                (void)!read(g_wake_fd, &counter, sizeof(counter)); // Drain the doorbell
                g_client_active.store(false, std::memory_order_relaxed);
                break;
            }
            if (fd == lost_fd) { reason = "telemetry send failed"; break; }

            // Read first, so data that arrives together with a hang-up is not lost
            if (revents & EPOLLIN) {
                uint8_t buf[256];
                ssize_t n;
                while ((n = read(fd, buf, sizeof(buf))) > 0) {
                    std::cout << "[REACTOR] Received " << n << " bytes from server\n";
                }
                if (n == 0) { reason = "server closed the connection"; break; }
                if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) { reason = std::strerror(errno); break; }
            }
            if (revents & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) { reason = "connection closed by peer or error"; break; }
            if ((revents & EPOLLOUT) && !pipeline.handle_write_ready()) { reason = "write failed (connection reset by peer)"; break; }
        }
    }

    telemetry.request_stop();
    telemetry.join();

    if (reason != nullptr) {
        std::cerr << "[CLIENT] Connection lost: " << reason << ".\n";
        return SessionEnd::ConnectionLost;
    }

    // Shutdown: bounded flush, so queued telemetry gets up to FLUSH_TIMEOUT to reach the server.
    // Skipped if the connection is broken; a second Ctrl-C (eventfd) aborts it.
    if (!pipeline.is_broken() && pipeline.has_pending()) {
        std::cout << "[CLIENT] Flushing pending telemetry before exit...\n";
        uint64_t counter = 0;
        (void)!read(g_wake_fd, &counter, sizeof(counter)); // Clear the first Ctrl-C if the loop exited before draining it
        auto deadline = std::chrono::steady_clock::now() + FLUSH_TIMEOUT;
        bool abort_flush = false;
        while (!abort_flush && pipeline.has_pending()) {
            auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()).count();
            if (left <= 0) break;
            int n = epoll_wait(epoll_fd, events.data(), MAX_EVENTS, static_cast<int>(left));
            if (n == -1) {
                if (errno == EINTR) continue;
                break;
            }
            for (int i = 0; i < n; ++i) {
                if (events[i].data.fd != sock.get() ||
                    (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))) { abort_flush = true; break; }
                if ((events[i].events & EPOLLOUT) && !pipeline.handle_write_ready()) { abort_flush = true; break; }
            }
        }
        if (pipeline.has_pending()) std::cerr << "[CLIENT] Unsent telemetry discarded at shutdown.\n";
    }
    return SessionEnd::Shutdown;
}

// ============================================================================
// Main: configuration, then handshake + reconnect loop with exponential backoff
// ============================================================================
int main(int argc, char* argv[]) {
    std::span<char*> args(argv, static_cast<size_t>(argc));
    std::string_view device_name = (args.size() > 1) ? std::string_view{args[1]} : "IoT_Edge_Node_01";
    const char* server_ip        = (args.size() > 2) ? args[2] : "127.0.0.1"; // NUL-terminated for inet_pton

    // Configuration errors cannot be fixed by retrying: report and exit non-zero
    auto connect_frame = MqttPacketFactory::create_connect_frame(device_name);
    if (!connect_frame) {
        std::cerr << "[CONFIG] Client id is too long (" << device_name.size() << " bytes, max "
                  << MqttPacketFactory::kMaxClientIdLen << ").\n";
        return 1;
    }
    if (auto addr = parse_ipv4(server_ip, SERVER_PORT); !addr) {
        std::cerr << "[CONFIG] '" << server_ip << "': " << addr.error() << ".\n";
        return 1;
    }

    // Create the shutdown eventfd before handlers can fire
    auto wake = make_eventfd();
    if (!wake) {
        std::cerr << "[CRITICAL] " << wake.error() << "\n";
        return 1;
    }
    g_wake_fd = wake->get();

    // Install graceful shutdown handlers
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Belt and braces with MSG_NOSIGNAL: writing to a reset server must never kill the client
    struct sigaction ignore_pipe{};
    ignore_pipe.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &ignore_pipe, nullptr);

    std::chrono::milliseconds backoff = BACKOFF_MIN;
    uint32_t message_counter = 0;

    while (g_client_active.load(std::memory_order_relaxed)) {
        std::cout << "[CLIENT] Connecting to " << server_ip << ":" << SERVER_PORT << " as '" << device_name << "'...\n";

        // connect -> send CONNECT -> await CONNACK; the first failing step short-circuits to the end
        auto session = connect_to(server_ip, SERVER_PORT, g_wake_fd, CONNECT_TIMEOUT)
            .and_then([&](UniqueFd fd) { return send_connect(std::move(fd), *connect_frame, g_wake_fd, CONNECT_TIMEOUT); })
            .and_then([&](UniqueFd fd) { return await_connack(std::move(fd), g_wake_fd, CONNACK_TIMEOUT); });

        if (session) {
            std::cout << "[CLIENT] CONNACK accepted: streaming telemetry.\n";
            backoff = BACKOFF_MIN; // Reset after a successful handshake
            if (run_session(std::move(*session), message_counter) == SessionEnd::Shutdown) break;
        } else {
            if (session.error().op == Op::Interrupted) break;
            std::cerr << "[CLIENT] Handshake failed: " << session.error() << ".\n";
        }

        // Interruptible backoff sleep: poll() on the eventfd only, so it wakes once, or at Ctrl+C
        std::cerr << "[CLIENT] Retrying in " << backoff.count() / 1000.0 << " s.\n";
        if (wait_fd(-1, 0, g_wake_fd, backoff) == Wait::Woken) break;
        backoff = std::min(backoff * 2, std::chrono::milliseconds(BACKOFF_MAX));
    }

    std::cout << "[CLIENT] Shutdown complete.\n";
    return 0;
}
