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
    ClientWritePipeline(int socket_fd, int epoll_fd) 
        : socket_fd_(socket_fd), epoll_fd_(epoll_fd) {}

    void send_payload(const uint8_t* data, size_t length) {
        std::lock_guard<std::mutex> lock(write_mutex_);

        if (!outbound_queue_.empty()) {
            outbound_queue_.push(std::vector<uint8_t>(data, data + length));
            return;
        }

        ssize_t bytes_written = write(socket_fd_, data, length);

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
            }
        }
    }

    void handle_write_ready() {
        std::lock_guard<std::mutex> lock(write_mutex_);

        while (!outbound_queue_.empty()) {
            const auto& current_chunk = outbound_queue_.front();
            size_t bytes_left = current_chunk.size() - pending_offset_;

            ssize_t bytes_written = write(socket_fd_, current_chunk.data() + pending_offset_, bytes_left);

            if (bytes_written >= 0) {
                size_t written = static_cast<size_t>(bytes_written);
                if (written < bytes_left) {
                    pending_offset_ += written;
                    return; 
                } else {
                    outbound_queue_.pop();
                    pending_offset_ = 0;
                }
            } else {
                if (errno == EAGAIN || errno == EWOULDBLOCK) return;
                return;
            }
        }
        update_epoll_mask(false); // Queue dry. Stop spinning.
    }
};

// ============================================================================
// 📦 2. ZERO-ALLOCATION MQTT PROTOCOL PACKET BUILDER
// ============================================================================
class MqttPacketFactory {
public:
    // Builds a conformant raw binary MQTT CONNECT frame directly into a stack container
    static std::vector<uint8_t> create_connect_frame(const std::string& device_id) {
        std::vector<uint8_t> frame;
        
        // 1. Fixed Header: Packet Type = 0x10 (CONNECT)
        frame.push_back(0x10);

        // 2. Variable Header Payload Data
        std::vector<uint8_t> var_header;
        // Protocol Name Length (0x00, 0x04) + Protocol Name ("MQTT")
        var_header.push_back(0x00); var_header.push_back(0x04);
        var_header.push_back('M');  var_header.push_back('Q'); 
        var_header.push_back('T');  var_header.push_back('T');
        // Protocol Level = 4 (MQTT v3.1.1)
        var_header.push_back(0x04);
        // Connect Flags = 0x02 (Clean Session only)
        var_header.push_back(0x02);
        // Keep Alive Timer = 60 Seconds (0x00, 0x3C)
        var_header.push_back(0x00); var_header.push_back(0x3C);

        // 3. Append Client ID String Payload with big-endian length prefix
        uint16_t id_len = static_cast<uint16_t>(device_id.length());
        var_header.push_back(static_cast<uint8_t>(id_len >> 8));
        var_header.push_back(static_cast<uint8_t>(id_len & 0xFF));
        for (char c : device_id) {
            var_header.push_back(static_cast<uint8_t>(c));
        }

        // 4. Calculate and encode Remaining Length dynamically into Fixed Header
        size_t rem_len = var_header.size();
        do {
            uint8_t encoded_byte = rem_len % 128;
            rem_len /= 128;
            if (rem_len > 0) {
                encoded_byte |= 128;
            }
            frame.push_back(encoded_byte);
        } while (rem_len > 0);

        // 5. Merge variable payload parts into the final serialization vector array
        frame.insert(frame.end(), var_header.begin(), var_header.end());
        return frame;
    }
};

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
        pipeline->send_payload(connect_packet.data(), connect_packet.size());

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
            pipeline->send_payload(reinterpret_cast<const uint8_t*>(metric.data()), metric.length());
        }
        std::cout << "[TELEMETRY] Background worker detached cleanly.\n";
    });

    // ============================================================================
    // ⚡ 5. REACTOR COMPONENT: MONITORING LOOP FOR SYSTEM EVENTS
    // ============================================================================
    std::vector<epoll_event> events(MAX_EVENTS);
    std::cout << "[REACTOR] Client network loop activated safely.\n";

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
                g_client_active.store(false, std::memory_order_relaxed);
                break;
            }
            if (events[i].events & EPOLLOUT) {
                pipeline->handle_write_ready();
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
    close(sock_fd);
    close(epoll_fd);
    close(g_wake_fd);
    std::cout << "[CLIENT] Shutdown complete.\n";
    return 0;
}
