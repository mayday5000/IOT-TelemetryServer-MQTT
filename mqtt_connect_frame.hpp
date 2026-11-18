// mqtt_connect_frame.hpp
// Purpose: Client side of the MQTT 3.1.1 handshake (shared with tests.cpp):
// CONNECT frame builder, CONNACK parser, and the send_connect / await_connack steps
// chained after connect_to() with std::expected::and_then.
#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <expected>
#include <span>
#include <string_view>
#include <vector>

#include "net_util.hpp"

enum class FrameError : uint8_t { IdTooLong };

class MqttPacketFactory {
public:
    static constexpr size_t kMaxClientIdLen = 0xFFFF; // MQTT strings carry a 16-bit length prefix

    // Builds a raw binary MQTT 3.1.1 CONNECT frame (clean session, 60 s keep-alive)
    [[nodiscard]] static std::expected<std::vector<uint8_t>, FrameError> create_connect_frame(std::string_view device_id) {
        if (device_id.size() > kMaxClientIdLen) return std::unexpected(FrameError::IdTooLong);

        const auto id_len = static_cast<uint16_t>(device_id.size());
        std::vector<uint8_t> body = {
            0x00, 0x04, 'M', 'Q', 'T', 'T', // Protocol name
            0x04,                           // Protocol level 4 (MQTT 3.1.1)
            0x02,                           // Connect flags: clean session only
            0x00, 0x3C,                     // Keep-alive 60 s
            static_cast<uint8_t>(id_len >> 8), static_cast<uint8_t>(id_len & 0xFF)};
        body.insert(body.end(), device_id.begin(), device_id.end());

        std::vector<uint8_t> frame = {0x10}; // Fixed header: CONNECT
        size_t rem_len = body.size();        // Remaining length, 7 bits per byte
        do {
            uint8_t encoded = rem_len % 128;
            rem_len /= 128;
            if (rem_len > 0) encoded |= 128;
            frame.push_back(encoded);
        } while (rem_len > 0);

        frame.insert(frame.end(), body.begin(), body.end());
        return frame;
    }

    // CONNACK = 20 02 <flags: 0 or 1 (session present)> <return code 0..5>
    [[nodiscard]] static std::expected<void, SysError> parse_connack(std::span<const uint8_t, 4> f) {
        if (f[0] != 0x20 || f[1] != 0x02 || f[2] > 0x01 || f[3] > 5)
            return std::unexpected(SysError{Op::ConnackMalformed});
        if (f[3] != 0) return std::unexpected(SysError{Op::Rejected, f[3]});
        return {};
    }
};

// Step 2 of the handshake: write the whole CONNECT frame (socket is non-blocking)
[[nodiscard]] inline std::expected<UniqueFd, SysError> send_connect(UniqueFd fd, std::span<const uint8_t> frame,
                                                                  int wake_fd, std::chrono::milliseconds timeout) {
    while (!frame.empty()) {
        ssize_t n = ::send(fd.get(), frame.data(), frame.size(), MSG_NOSIGNAL);
        if (n >= 0) { frame = frame.subspan(static_cast<size_t>(n)); continue; }
        if (errno == EINTR) continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK) return std::unexpected(SysError{Op::SendConnect, errno});
        switch (wait_fd(fd.get(), POLLOUT, wake_fd, timeout)) {
            case Wait::Woken:   return std::unexpected(SysError{Op::Interrupted});
            case Wait::Timeout: return std::unexpected(SysError{Op::SendConnect, ETIMEDOUT});
            case Wait::Error:   return std::unexpected(SysError{Op::SendConnect, errno});
            case Wait::Ready:   break;
        }
    }
    return fd;
}

// Step 3: wait (poll on the socket plus the shutdown eventfd) for the 4-byte CONNACK
[[nodiscard]] inline std::expected<UniqueFd, SysError> await_connack(UniqueFd fd, int wake_fd,
                                                                   std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::array<uint8_t, 4> reply{};
    size_t got = 0;
    while (got < reply.size()) {
        auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        switch (wait_fd(fd.get(), POLLIN, wake_fd, std::max(left, std::chrono::milliseconds(0)))) {
            case Wait::Woken:   return std::unexpected(SysError{Op::Interrupted});
            case Wait::Timeout: return std::unexpected(SysError{Op::ConnackTimeout});
            case Wait::Error:   return std::unexpected(SysError{Op::PeerClosed, errno});
            case Wait::Ready:   break;
        }
        ssize_t n = ::recv(fd.get(), reply.data() + got, reply.size() - got, 0);
        if (n > 0) { got += static_cast<size_t>(n); continue; }
        if (n == 0) return std::unexpected(SysError{Op::PeerClosed});
        if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) return std::unexpected(SysError{Op::PeerClosed, errno});
    }
    return MqttPacketFactory::parse_connack(reply).transform([&] { return std::move(fd); });
}
