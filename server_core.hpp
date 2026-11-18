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
#include <string_view>
#include <cstring>
#include <cstdint>
#include <cerrno>
#include <unordered_map>
#include <optional>
#include <array>
#include <span>
#include <expected>
#include <algorithm>

#include <string.h> // explicit_bzero
#include <sys/epoll.h>
#include <sys/socket.h>

// MQTT CONNACK: packet type 0x20, remaining length 2, no session present, return code
constexpr std::array<uint8_t, 4> make_connack(uint8_t return_code) { return {0x20, 0x02, 0x00, return_code}; }
constexpr std::array<uint8_t, 4> MQTT_CONNACK_ACCEPTED = make_connack(0x00);
constexpr uint8_t CONNACK_BAD_PROTOCOL = 0x01; // Unacceptable protocol version
constexpr uint8_t CONNACK_ID_REJECTED  = 0x02; // Identifier rejected

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

    [[nodiscard]] std::span<const uint8_t> payload() const { return {raw_payload, payload_len}; }

    // Secure RAII scrubbing on destruction. explicit_bzero, unlike memset, cannot be
    // removed by the optimizer as a dead store just before the memory is freed.
    ~TelemetryPacket() {
        file_descriptor = -1;
        payload_len = 0;
        explicit_bzero(raw_payload, sizeof(raw_payload));
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
    void register_client(const std::string& device_id, int fd) {
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
    }

    // Returns a copy: a view into the map would dangle once the lock is released
    [[nodiscard]] std::optional<std::string> get_device_id_by_fd(int fd) {
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
enum class ConnectError : uint8_t {
    TooShort,           // Empty buffer, or remaining length below the 12-byte minimum
    NotConnect,         // First byte is not the CONNECT packet type (0x10)
    BadRemainingLength, // Remaining-length field longer than 4 bytes
    Truncated,          // Frame ends before the declared remaining length
    BadProtocol,        // Protocol name is not "MQTT" or level is not 4 (3.1.1)
    IdOverrun,          // Declared client-id length runs past the CONNECT payload
    EmptyId             // Zero-length client id (this server does not assign ids)
};

constexpr std::string_view to_string(ConnectError e) {
    switch (e) {
        case ConnectError::TooShort:           return "frame too short";
        case ConnectError::NotConnect:         return "not a CONNECT packet";
        case ConnectError::BadRemainingLength: return "malformed remaining length";
        case ConnectError::Truncated:          return "truncated CONNECT frame";
        case ConnectError::BadProtocol:        return "unsupported protocol name or level";
        case ConnectError::IdOverrun:          return "client id longer than the CONNECT payload";
        case ConnectError::EmptyId:            return "empty client id";
    }
    return "unknown";
}

// CONNACK return code to send before closing, if MQTT defines one for this error
constexpr std::optional<uint8_t> connack_code_for(ConnectError e) {
    if (e == ConnectError::BadProtocol) return CONNACK_BAD_PROTOCOL;
    if (e == ConnectError::EmptyId) return CONNACK_ID_REJECTED;
    return std::nullopt;
}

class SafeMqttParser {
public:
    // Returns a view into `buffer`: copy it before the buffer is reused
    [[nodiscard]] static std::expected<std::string_view, ConnectError> extract_client_id(std::span<const uint8_t> buffer) {
        if (buffer.empty()) return std::unexpected(ConnectError::TooShort);
        if (buffer[0] != 0x10) return std::unexpected(ConnectError::NotConnect);

        // Remaining length: 1 to 4 bytes, 7 bits each, high bit = continuation
        size_t index = 1;
        uint32_t remaining_length = 0;
        for (int i = 0;; ++i) {
            if (i == 4) return std::unexpected(ConnectError::BadRemainingLength);
            if (index >= buffer.size()) return std::unexpected(ConnectError::Truncated);
            const uint8_t byte = buffer[index++];
            remaining_length |= static_cast<uint32_t>(byte & 0x7F) << (7 * i);
            if ((byte & 0x80) == 0) break;
        }

        // 10-byte variable header plus the 2-byte client-id length prefix
        if (remaining_length < 12) return std::unexpected(ConnectError::TooShort);
        if (remaining_length > buffer.size() - index) return std::unexpected(ConnectError::Truncated);
        const auto body = buffer.subspan(index, remaining_length);

        constexpr std::array<uint8_t, 7> kProtocol = {0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04};
        if (!std::equal(kProtocol.begin(), kProtocol.end(), body.begin()))
            return std::unexpected(ConnectError::BadProtocol);

        const size_t id_len = (static_cast<size_t>(body[10]) << 8) | body[11];
        if (id_len > body.size() - 12) return std::unexpected(ConnectError::IdOverrun);
        if (id_len == 0) return std::unexpected(ConnectError::EmptyId);
        return std::string_view(reinterpret_cast<const char*>(body.data() + 12), id_len);
    }
};

// ============================================================================
// Non-blocking outbound write queue (does not own the descriptors)
// ============================================================================
class AsyncWriteBuffer {
private:
    int file_descriptor_;
    int epoll_descriptor_;
    std::queue<std::vector<uint8_t>> outbound_queue_;
    size_t pending_offset_ = 0;
    std::mutex write_mutex_;

    [[nodiscard]] bool update_epoll_mask(bool enable_write) {
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
        if (enable_write) ev.events |= EPOLLOUT;
        ev.data.fd = file_descriptor_;
        return epoll_ctl(epoll_descriptor_, EPOLL_CTL_MOD, file_descriptor_, &ev) != -1;
    }

public:
    AsyncWriteBuffer(int fd, int epoll_fd) : file_descriptor_(fd), epoll_descriptor_(epoll_fd) {}

    // Returns false on a hard socket error (EPIPE, ECONNRESET, ...): caller treats it as a disconnect
    [[nodiscard]] bool send_payload(std::span<const uint8_t> data) {
        std::lock_guard<std::mutex> lock(write_mutex_);

        if (!outbound_queue_.empty()) {
            outbound_queue_.emplace(data.begin(), data.end());
            return true;
        }

        ssize_t bytes_written;
        do { // MSG_NOSIGNAL: a reset peer yields EPIPE instead of a process-killing SIGPIPE
            bytes_written = send(file_descriptor_, data.data(), data.size(), MSG_NOSIGNAL);
        } while (bytes_written == -1 && errno == EINTR);
        if (bytes_written >= 0) {
            const auto rest = data.subspan(static_cast<size_t>(bytes_written));
            if (rest.empty()) return true;
            outbound_queue_.emplace(rest.begin(), rest.end());
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            outbound_queue_.emplace(data.begin(), data.end());
        } else {
            return false;
        }
        pending_offset_ = 0;
        return update_epoll_mask(true);
    }

    // Returns false on a hard socket error (EPIPE, ECONNRESET, ...)
    [[nodiscard]] bool handle_write_ready() {
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
        return update_epoll_mask(false);
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
    [[nodiscard]] std::unique_ptr<TelemetryPacket> pop() {
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
