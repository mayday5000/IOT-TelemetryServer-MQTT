// server_core.hpp
// Purpose: Server building blocks shared by server.cpp and tests.cpp:
// packet type, client registry, bounds-checked MQTT CONNECT parser,
// non-blocking write buffer and the bounded worker queue.
#pragma once

#include <iostream>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <string>
#include <cstring>
#include <cstdint>
#include <cerrno>
#include <unordered_map>
#include <optional>
#include <array>

#include <sys/epoll.h>
#include <sys/socket.h>

// MQTT CONNACK: packet type 0x20, remaining length 2, no session present, return code 0 (accepted)
constexpr std::array<uint8_t, 4> MQTT_CONNACK_ACCEPTED = {0x20, 0x02, 0x00, 0x00};

// ============================================================================
// Data structures and RAII packet scrubbing
// ============================================================================
constexpr size_t MAX_PACKET_SIZE     = 1024; // Expanded for MQTT bursts
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
// Bidirectional client registry
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
// Bounds-checked MQTT CONNECT parser
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
// Non-blocking outbound write queue
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

    // Returns false on a hard socket error (EPIPE, ECONNRESET, ...): caller treats it as a disconnect
    bool send_payload(const uint8_t* data, size_t length) {
        std::lock_guard<std::mutex> lock(write_mutex_);

        if (!outbound_queue_.empty()) {
            outbound_queue_.push(std::vector<uint8_t>(data, data + length));
            return true;
        }

        ssize_t bytes_written;
        do { // MSG_NOSIGNAL: a reset peer yields EPIPE instead of a process-killing SIGPIPE
            bytes_written = send(file_descriptor_, data, length, MSG_NOSIGNAL);
        } while (bytes_written == -1 && errno == EINTR);
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
        } else {
            return false;
        }
        return true;
    }

    // Returns false on a hard socket error (EPIPE, ECONNRESET, ...)
    bool handle_write_ready() {
        std::lock_guard<std::mutex> lock(write_mutex_);

        while (!outbound_queue_.empty()) {
            const auto& chunk = outbound_queue_.front();
            size_t bytes_left = chunk.size() - pending_offset_;

            ssize_t bytes_written = send(file_descriptor_, chunk.data() + pending_offset_, bytes_left, MSG_NOSIGNAL);
            if (bytes_written >= 0) {
                size_t written = static_cast<size_t>(bytes_written);
                if (written < bytes_left) {
                    pending_offset_ += written;
                    return true; 
                } else {
                    outbound_queue_.pop();
                    pending_offset_ = 0;
                }
            } else if (errno == EINTR) {
                continue;
            } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                return true;
            } else {
                return false; // EPIPE / ECONNRESET: peer is gone
            }
        }
        update_epoll_mask(false);
        return true;
    }
};

// ============================================================================
// Thread-safe work queue
// ============================================================================
class TelemetryPipeline {
private:
    std::queue<std::unique_ptr<TelemetryPacket>> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_requested_{false};

public:
    static constexpr size_t kCapacity = 10000;

    void push(std::unique_ptr<TelemetryPacket> packet) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (queue_.size() >= kCapacity) queue_.pop(); // Drop oldest element if memory bounds saturate
        queue_.push(std::move(packet));
        cv_.notify_one(); 
    }

    // Returns queued packets until the queue is empty; nullptr only once shutdown
    // was requested AND every queued packet has been handed out (drain on exit).
    std::unique_ptr<TelemetryPacket> pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this]() { return !queue_.empty() || stop_requested_; });
        if (queue_.empty()) return nullptr;

        auto packet = std::move(queue_.front());
        queue_.pop();
        return packet;
    }

    // Returns how many packets were still queued; workers drain them before exiting
    size_t shutdown() {
        size_t pending;
        {
            std::lock_guard<std::mutex> lock(mutex_); // Set under the lock so no worker misses the wake-up
            stop_requested_ = true;
            pending = queue_.size();
        }
        cv_.notify_all();
        return pending;
    }
};
