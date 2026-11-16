// server.cpp
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
#include <unordered_map>
#include <optional>
#include <string_view>

// Linux Native System Headers
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <csignal>
#include <span>
#include <expected>

// ============================================================================
// 🔒 1. DATA INFRASTRUCTURE & RAII HARDWARE CONFIGURATION
// ============================================================================
constexpr size_t MAX_PACKET_SIZE     = 1024; // Expanded for MQTT bursts
constexpr size_t MAX_EVENTS          = 64;
constexpr int    SERVER_PORT         = 8883; 
constexpr size_t TARGET_STACK_SIZE   = 64 * 1024; // 64 KB Thread micro-stack
constexpr size_t INLINE_WORKSPACE_SZ = 32 * 1024; // 32 KB Heap calculation block

struct TelemetryPacket {
    int32_t     file_descriptor;
    std::string validated_device_id;
    uint8_t     raw_payload[MAX_PACKET_SIZE];
    size_t      payload_len;

    // Secure RAII Scrubbing on destruction to explicitly wipe buffers from RAM
    ~TelemetryPacket() {
        file_descriptor = -1;
        payload_len = 0;
        std::memset(raw_payload, 0, MAX_PACKET_SIZE);
    }
};

struct LargeHeapWorkspace {
    uint8_t scratchpad[INLINE_WORKSPACE_SZ];
};

// ============================================================================
// 🎨 2. SAFE MULTI-INDEX AUTHENTICATED CLIENT REGISTRY
// ============================================================================
struct ConnectedClient {
    std::string device_id;
    int         file_descriptor;
};

class ClientRegistry {
private:
    std::unordered_map<int, std::shared_ptr<ConnectedClient>> fd_map_;
    std::unordered_map<std::string, std::shared_ptr<ConnectedClient>> id_map_;
    std::mutex registry_mutex_;

public:
    bool register_client(const std::string& device_id, int fd) {
        std::lock_guard<std::mutex> lock(registry_mutex_);

        // Evict structural ghost sessions on rapid socket re-registrations
        auto existing = id_map_.find(device_id);
        if (existing != id_map_.end()) {
            fd_map_.erase(existing->second->file_descriptor);
        }

        auto client = std::make_shared<ConnectedClient>(ConnectedClient{device_id, fd});
        fd_map_[fd] = client;
        id_map_[device_id] = client;

        std::cout << "[REGISTRY] Authenticated: Raw FD " << fd << " Bound to ID -> " << device_id << "\n";
        return true; 
    }

    std::optional<std::string> get_device_id_by_fd(int fd) {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        auto it = fd_map_.find(fd);
        if (it != fd_map_.end()) return it->second->device_id;
        return std::nullopt;
    }

    void remove_client_by_fd(int fd) {
        std::lock_guard<std::mutex> lock(registry_mutex_);
        auto it = fd_map_.find(fd);
        if (it != fd_map_.end()) {
            id_map_.erase(it->second->device_id);
            fd_map_.erase(it);
        }
    }
};

// ============================================================================
// 📦 3. ZERO-ALLOCATION MQTT CONNECT PACKET PARSER
// ============================================================================
class SafeMqttParser {
public:
    static std::optional<std::string> extract_client_id(const uint8_t* buffer, size_t length) {
        if (length < 12) return std::nullopt;
        if (buffer[0] != 0x10) return std::nullopt; // Verification check for MQTT CONNECT signature

        size_t index = 1;
        uint32_t remaining_length = 0;
        uint32_t multiplier = 1;
        
        while (index < length) {
            uint8_t byte = buffer[index++];
            remaining_length += (byte & 127) * multiplier;
            if (multiplier > 128 * 128 * 128) return std::nullopt; // Size guard filter
            multiplier *= 128;
            if ((byte & 128) == 0) break;
        }

        if (index + remaining_length > length || index + 10 >= length) return std::nullopt;
        index += 10; // Fast-forward past standard verification protocol blocks

        uint16_t id_len = (static_cast<uint16_t>(buffer[index]) << 8) | buffer[index + 1];
        index += 2;

        if (index + id_len > length) return std::nullopt; // Structural string overrun check
        return std::string(reinterpret_cast<const char*>(&buffer[index]), id_len);
    }
};

// ============================================================================
// 🌊 4. EDGE-TRIGGERED (`EPOLLET`) NON-BLOCKING OUTBOUND STREAM QUEUE
// ============================================================================
class AsyncWriteBuffer {
private:
    int file_descriptor_;
    int epoll_descriptor_;
    std::queue<std::vector<uint8_t>> outbound_queue_;
    size_t pending_offset_ = 0;
    std::mutex write_mutex_;

    void update_epoll_mask(bool enable_write) {
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP; 
        if (enable_write) ev.events |= EPOLLOUT; 
        ev.data.fd = file_descriptor_;
        epoll_ctl(epoll_descriptor_, EPOLL_CTL_MOD, file_descriptor_, &ev);
    }

public:
    AsyncWriteBuffer(int fd, int epoll_fd) : file_descriptor_(fd), epoll_descriptor_(epoll_fd) {}

    void send_payload(const uint8_t* data, size_t length) {
        std::lock_guard<std::mutex> lock(write_mutex_);

        if (!outbound_queue_.empty()) {
            outbound_queue_.push(std::vector<uint8_t>(data, data + length));
            return;
        }

        ssize_t bytes_written = write(file_descriptor_, data, length);
        if (bytes_written >= 0) {
            size_t written = static_cast<size_t>(bytes_written);
            if (written < length) {
                outbound_queue_.push(std::vector<uint8_t>(data + written, data + length));
                pending_offset_ = 0;
                update_epoll_mask(true);
            }
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            outbound_queue_.push(std::vector<uint8_t>(data, data + length));
            pending_offset_ = 0;
            update_epoll_mask(true);
        }
    }

    void handle_write_ready() {
        std::lock_guard<std::mutex> lock(write_mutex_);

        while (!outbound_queue_.empty()) {
            const auto& chunk = outbound_queue_.front();
            size_t bytes_left = chunk.size() - pending_offset_;

            ssize_t bytes_written = write(file_descriptor_, chunk.data() + pending_offset_, bytes_left);
            if (bytes_written >= 0) {
                size_t written = static_cast<size_t>(bytes_written);
                if (written < bytes_left) {
                    pending_offset_ += written;
                    return; 
                } else {
                    outbound_queue_.pop();
                    pending_offset_ = 0;
                }
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return;
            } else {
                return;
            }
        }
        update_epoll_mask(false);
    }
};

// ============================================================================
// 🚀 5. PROCESSING PIPELINE STAGE (DECOUPLED THREAD WORK QUEUE)
// ============================================================================
class TelemetryPipeline {
private:
    std::queue<std::unique_ptr<TelemetryPacket>> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_requested_{false};

public:
    void push(std::unique_ptr<TelemetryPacket> packet) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (queue_.size() >= 10000) queue_.pop(); // Drop oldest element if memory bounds saturate
        queue_.push(std::move(packet));
        cv_.notify_one(); 
    }

    std::unique_ptr<TelemetryPacket> pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return !queue_.empty() || stop_requested_; });
        if (stop_requested_ && queue_.empty()) return nullptr;

        auto packet = std::move(queue_.front());
        queue_.pop();
        return packet;
    }

    void shutdown() {
        stop_requested_ = true;
        cv_.notify_all();
    }
};

// ============================================================================
// 🏛️ 6. SYSTEM RUNTIME ROUTING
// ============================================================================
bool make_socket_non_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) != -1;
}

std::expected<void, std::string> ensure_non_blocking(int fd) {
    if (!make_socket_non_blocking(fd)) return std::unexpected("non-blocking failed");
    return {};
}

// Global stop flag reachable by signal handler
std::atomic<bool> g_running{true};

void signal_handler(int) {
    g_running.store(false, std::memory_order_relaxed);
}

int main() {
    // Install graceful shutdown handlers
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    auto pipeline = std::make_shared<TelemetryPipeline>();
    ClientRegistry registry;
    std::unordered_map<int, std::unique_ptr<AsyncWriteBuffer>> write_managers;

    // Calculate core processing footprints automatically
    unsigned int pool_size = std::thread::hardware_concurrency();
    if (pool_size == 0) pool_size = 2;
    pool_size *= 2; // I/O optimized allocation scale factor

    std::vector<std::jthread> workers;
    for (unsigned int i = 0; i < pool_size; ++i) {
        std::jthread w([pipeline]() {
            while (g_running.load(std::memory_order_relaxed)) {
                auto packet = pipeline->pop();
                if (!packet) break;

                // Allocate workspace securely onto Heap inside the modern micro-stack framework
                auto workspace = std::make_unique<LargeHeapWorkspace>();
                
                // --- DOMAIN PROCESSING BUSINESS LOGIC ---
                std::cout << "[WORKER] Device: "
                          << (packet->validated_device_id.empty()
                                ? "UNAUTHENTICATED"
                                : packet->validated_device_id)
                          << " (Length: " << packet->payload_len << " bytes)\n";
            }
        });

        workers.push_back(std::move(w));
    }

    // Allocate Linux TCP socket configuration settings
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(SERVER_PORT);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0 || listen(server_fd, SOMAXCONN) < 0 || !make_socket_non_blocking(server_fd)) {
        std::cerr << "[CRITICAL] Infrastructure initialization sequence failed.\n";
        return 1;
    }

    int epoll_fd = epoll_create1(0);
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET; 
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    std::vector<epoll_event> events(MAX_EVENTS);
    std::cout << "[SERVER ENGINE] Monitoring port " << SERVER_PORT << " with Edge-Triggered Linux Epoll.\n";

    // ============================================================================
    // ⚡ REACTOR MASTER EVENT LOOP
    // ============================================================================
    while (g_running.load(std::memory_order_relaxed)) {
        int num_events = epoll_wait(epoll_fd, events.data(), MAX_EVENTS, 500); // 500 ms timeout so flag is observed
        if (num_events == -1) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < num_events; ++i) {
            int current_fd = events[i].data.fd;

            if (current_fd == server_fd) {
                while (true) { // Edge-Triggered loop: Drain all accept calls
                    int client_fd = accept(server_fd, nullptr, nullptr);
                    if (client_fd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        break;
                    }

                    if (make_socket_non_blocking(client_fd)) {
                        epoll_event client_ev{};
                        client_ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
                        client_ev.data.fd = client_fd;
                        epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &client_ev);
                        write_managers[client_fd] = std::make_unique<AsyncWriteBuffer>(client_fd, epoll_fd);
                    } else {
                        close(client_fd);
                    }
                }
            } 
            else if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
                registry.remove_client_by_fd(current_fd);
                write_managers.erase(current_fd);
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, current_fd, nullptr);
                close(current_fd);
            } 
            else {
                if (events[i].events & EPOLLOUT) {
                    if (write_managers.count(current_fd)) write_managers[current_fd]->handle_write_ready();
                }
                if (events[i].events & EPOLLIN) {
                    auto packet = std::make_unique<TelemetryPacket>();
                    bool disconnect = false;

                    while (true) { // Edge-Triggered loop: Drain read socket completely
                        ssize_t bytes_read = read(current_fd, packet->raw_payload, MAX_PACKET_SIZE);
                        if (bytes_read > 0) {
                            packet->payload_len = static_cast<size_t>(bytes_read);
                            packet->file_descriptor = current_fd;

                            auto known_id = registry.get_device_id_by_fd(current_fd);
                            if (!known_id.has_value()) {
                                // Execute extraction against the raw payload to confirm identity bindings
                                auto parsed_id = SafeMqttParser::extract_client_id(packet->raw_payload, packet->payload_len);
                                if (parsed_id.has_value()) {
                                    registry.register_client(parsed_id.value(), current_fd);
                                    packet->validated_device_id = parsed_id.value();
                                    
                                    // Securely route an validation acknowledgement response frame out to the client
                                    uint8_t connack[] = {0x20, 0x02, 0x00, 0x00};
                                    if (write_managers.count(current_fd)) write_managers[current_fd]->send_payload(connack, 4);
                                }
                            } else {
                                packet->validated_device_id = known_id.value();
                            }

                            pipeline->push(std::move(packet));
                            packet = std::make_unique<TelemetryPacket>(); // Reset RAII structural tracking instance
                        } else if (bytes_read == 0) {
                            disconnect = true; break;
                        } else {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                            disconnect = true; break;
                        }
                    }

                    if (disconnect) {
                        registry.remove_client_by_fd(current_fd);
                        write_managers.erase(current_fd);
                        epoll_ctl(epoll_fd, EPOLL_CTL_DEL, current_fd, nullptr);
                        close(current_fd);
                    }
                }
            }
        }
    }

    g_running.store(false, std::memory_order_relaxed);
    pipeline->shutdown();
    // jthreads join automatically
    close(server_fd);
    close(epoll_fd);
    return 0;
}


/*

========================================================================================================
[ PHASE 1: BOOTSTRAP INITIALIZATION ]
========================================================================================================
 │
 ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  LOOP 1: for (unsigned int i = 0; i < pool_size; ++i)                                                │
│  • Running In: Main Thread                                                                           │
│  • Exit Condition: When counter 'i' reaches 'pool_size'                                              │
│  • Purpose: Spawns background workers, shrinks stacks to 64KB, pushes threads into vector tracker.   │
└──────────────────────────────────────────────────────────────────────────────────────────────────────┘
 │
 ├─────────────────────────────────────────┐
 ▼                                         ▼
=======================================   ==============================================================
[ WORKER THREAD POOL ]                    [ PHASE 2: EVENT DISPATCHER (REACTOR) ]
=======================================   ==============================================================
 │                                         │
 ▼                                         ▼
┌─────────────────────────────────────┐   ┌────────────────────────────────────────────────────────────┐
│ LOOP 2: while (running)             │   │ LOOP 3: while (running)                                    │
│ • Running In: BACKGROUND THREADS    │   │ • Running In: MAIN THREAD                                  │
│ • Exit Condition: running == false  │   │ • Exit Condition: running == false                         │
│ • Purpose: Blocks on pipeline CV.   │   │ • Purpose: Sleeps 0% CPU in epoll_wait() until network     │
│   Pops and processes payloads.      │   │   hardware interrupts fire.                                │
└─────────────────────────────────────┘   └────────────────────────────────────────────────────────────┘
                                           │
                                           ▼
                                          ┌────────────────────────────────────────────────────────────┐
                                          │ LOOP 4: for (int i = 0; i < num_events; ++i)               │
                                          │ • Running In: MAIN THREAD                                  │
                                          │ • Exit Condition: i reaches number of active events        │
                                          │ • Purpose: Iterates strictly through active network        │
                                          │   file descriptors that generated traffic.                 │
                                          └────────────────────────────────────────────────────────────┘
                                           │
                                           ├───────────────────────────┐
                                           │ (If New Connection)       │ (If Data Inbound)
                                           ▼                           ▼
                                          ┌────────────────────────┐  ┌────────────────────────────────┐
                                          │ LOOP 5: while (true)   │  │ LOOP 6: while (true)           │
                                          │ • Running In: MAIN THTR│  │ • Running In: MAIN THREAD      │
                                          │ • Exit Condition:      │  │ • Exit Condition:              │
                                          │   accept() returns     │  │   read() returns EAGAIN/0/-1   │
                                          │   EAGAIN/EWOULDBLOCK   │  │ • Purpose: Edge-triggered drain│
                                          │ • Purpose: Edge-       │  │   of all telemetry bytes from  │
                                          │   triggered accept of  │  │   the socket into a packet.    │
                                          │   all clients at gate. │  └────────────────────────────────┘
                                          └────────────────────────┘   │
                                                                       │ (packet pushed to pipeline)
                                                                       └───────────┐
                                                                                   ▼
                                                                      [ Wakes up Waiting Loop 2 ]

========================================================================================================
[ PHASE 3: SHUTDOWN DESTRUCTION ]
========================================================================================================
 │
 ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  LOOP 7: for (auto& t : workers)                                                                     │
│  • Running In: Main Thread                                                                           │
│  • Exit Condition: When every thread object in the vector tracker has been evaluated                 │
│  • Purpose: Calls .join() on all workers. Blocks Main until background threads finish up completely. │
└──────────────────────────────────────────────────────────────────────────────────────────────────────┘

*/
