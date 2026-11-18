// net_util.hpp
// Purpose: Small Linux networking toolkit shared by server.cpp, client.cpp and tests.cpp:
// UniqueFd (RAII file descriptor), SysError (allocation-free error value) and
// [[nodiscard]] std::expected helpers for sockets, epoll, eventfd and poll waits.
#pragma once

#include <array>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <expected>
#include <ostream>
#include <utility>

#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

// ============================================================================
// UniqueFd: owns one file descriptor and closes it exactly once
// ============================================================================
class UniqueFd {
    int fd_ = -1;

public:
    UniqueFd() = default;
    explicit UniqueFd(int fd) noexcept : fd_(fd) {}
    UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}
    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) reset(std::exchange(other.fd_, -1));
        return *this;
    }
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;
    ~UniqueFd() { reset(); }

    [[nodiscard]] int get() const noexcept { return fd_; }
    [[nodiscard]] explicit operator bool() const noexcept { return fd_ != -1; }
    [[nodiscard]] int release() noexcept { return std::exchange(fd_, -1); }

    void reset(int fd = -1) noexcept {
        if (fd_ != -1) ::close(fd_);
        fd_ = fd;
    }
};

// ============================================================================
// SysError: which step failed plus errno (or the CONNACK return code for Rejected).
// Two small integers, so error paths never allocate.
// ============================================================================
enum class Op : uint8_t {
    Socket, SetSockOpt, Bind, Listen, EpollCreate, EpollCtl, EventFd,      // setup
    BadAddress, Connect, ConnectTimeout, SendConnect,                      // client connect
    ConnackTimeout, ConnackMalformed, PeerClosed, Rejected, Interrupted    // MQTT handshake
};

struct SysError {
    Op  op;
    int code = 0; // errno, or the CONNACK return code when op == Op::Rejected
};

inline const char* connack_reason(int code) {
    switch (code) {
        case 1:  return "server rejected protocol version";
        case 2:  return "server rejected client id";
        case 3:  return "server unavailable";
        case 4:  return "server rejected user name or password";
        case 5:  return "client not authorized";
        default: return "server rejected the connection";
    }
}

inline std::ostream& operator<<(std::ostream& os, const SysError& e) {
    auto sys = [&](const char* what) -> std::ostream& { return os << what << ": " << std::strerror(e.code); };
    switch (e.op) {
        case Op::Socket:           return sys("socket() failed");
        case Op::SetSockOpt:       return sys("setsockopt() failed");
        case Op::Bind:             return sys("bind() failed");
        case Op::Listen:           return sys("listen() failed");
        case Op::EpollCreate:      return sys("epoll_create1() failed");
        case Op::EpollCtl:         return sys("epoll_ctl() failed");
        case Op::EventFd:          return sys("eventfd() failed");
        case Op::BadAddress:       return os << "bad IP address (expected IPv4 dotted quad, e.g. 192.168.1.10)";
        case Op::Connect:
            if (e.code == ECONNREFUSED) return os << "connection refused (is the server running?)";
            return sys("connect failed");
        case Op::ConnectTimeout:   return os << "connect timeout";
        case Op::SendConnect:      return sys("sending CONNECT failed");
        case Op::ConnackTimeout:   return os << "CONNACK timeout (server did not answer the CONNECT)";
        case Op::ConnackMalformed: return os << "malformed CONNACK from server";
        case Op::PeerClosed:
            if (e.code != 0) return sys("server closed the connection during the handshake");
            return os << "server closed the connection during the handshake";
        case Op::Rejected:         return os << connack_reason(e.code) << " (CONNACK return code " << e.code << ")";
        case Op::Interrupted:      return os << "interrupted by shutdown";
    }
    return os << "unknown error";
}

// ============================================================================
// Resource helpers
// ============================================================================
[[nodiscard]] inline std::expected<UniqueFd, SysError> make_epoll() {
    UniqueFd fd{::epoll_create1(EPOLL_CLOEXEC)};
    if (!fd) return std::unexpected(SysError{Op::EpollCreate, errno});
    return fd;
}

[[nodiscard]] inline std::expected<UniqueFd, SysError> make_eventfd() {
    UniqueFd fd{::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)};
    if (!fd) return std::unexpected(SysError{Op::EventFd, errno});
    return fd;
}

[[nodiscard]] inline std::expected<void, SysError> epoll_add(int epoll_fd, int fd, uint32_t events) {
    epoll_event ev{};
    ev.events = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) return std::unexpected(SysError{Op::EpollCtl, errno});
    return {};
}

// Non-blocking IPv4 listening socket bound to all interfaces
[[nodiscard]] inline std::expected<UniqueFd, SysError> create_listener(uint16_t port) {
    UniqueFd fd{::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
    if (!fd) return std::unexpected(SysError{Op::Socket, errno});

    int opt = 1;
    if (::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1)
        return std::unexpected(SysError{Op::SetSockOpt, errno});

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port);
    if (::bind(fd.get(), reinterpret_cast<sockaddr*>(&address), sizeof(address)) == -1)
        return std::unexpected(SysError{Op::Bind, errno});
    if (::listen(fd.get(), SOMAXCONN) == -1)
        return std::unexpected(SysError{Op::Listen, errno});
    return fd;
}

// ip must be NUL-terminated: inet_pton is a C API
[[nodiscard]] inline std::expected<sockaddr_in, SysError> parse_ipv4(const char* ip, uint16_t port) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, ip, &addr.sin_addr) != 1) return std::unexpected(SysError{Op::BadAddress});
    return addr;
}

// ============================================================================
// Waiting without busy loops: poll() on one fd plus the shutdown eventfd
// ============================================================================
enum class Wait : uint8_t { Ready, Timeout, Woken, Error };

// Sleeps until `fd` reports `events`, `wake_fd` becomes readable (shutdown), or the
// timeout expires. A negative `fd` is ignored by poll(), which turns this into an
// interruptible sleep. The wake eventfd is left undrained so callers still see it.
[[nodiscard]] inline Wait wait_fd(int fd, short events, int wake_fd, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (true) {
        auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        if (left.count() < 0) left = std::chrono::milliseconds(0);

        std::array<pollfd, 2> fds{{{fd, events, 0}, {wake_fd, POLLIN, 0}}};
        int n = ::poll(fds.data(), fds.size(), static_cast<int>(left.count()));
        if (n == -1) {
            if (errno == EINTR) continue; // The signal handler also rang wake_fd; poll again to see it
            return Wait::Error;
        }
        if (fds[1].revents & POLLIN) return Wait::Woken;
        if (n == 0) return Wait::Timeout;
        if (fds[0].revents) return Wait::Ready; // Includes POLLERR/POLLHUP: the next syscall reports why
    }
}

// Non-blocking TCP connect with a timeout; Ctrl+C (wake_fd) aborts it at once
[[nodiscard]] inline std::expected<UniqueFd, SysError> connect_to(const char* ip, uint16_t port, int wake_fd,
                                                                std::chrono::milliseconds timeout) {
    auto addr = parse_ipv4(ip, port);
    if (!addr) return std::unexpected(addr.error());

    UniqueFd fd{::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0)};
    if (!fd) return std::unexpected(SysError{Op::Socket, errno});

    if (::connect(fd.get(), reinterpret_cast<const sockaddr*>(&*addr), sizeof(*addr)) == 0) return fd;
    if (errno != EINPROGRESS) return std::unexpected(SysError{Op::Connect, errno});

    switch (wait_fd(fd.get(), POLLOUT, wake_fd, timeout)) {
        case Wait::Woken:   return std::unexpected(SysError{Op::Interrupted});
        case Wait::Timeout: return std::unexpected(SysError{Op::ConnectTimeout});
        case Wait::Error:   return std::unexpected(SysError{Op::Connect, errno});
        case Wait::Ready:   break;
    }
    int so_error = 0;
    socklen_t len = sizeof(so_error);
    if (::getsockopt(fd.get(), SOL_SOCKET, SO_ERROR, &so_error, &len) == -1) so_error = errno;
    if (so_error != 0) return std::unexpected(SysError{Op::Connect, so_error});
    return fd;
}
