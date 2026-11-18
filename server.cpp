// server.cpp
// Purpose: MQTT-aware edge-triggered epoll reactor for IoT telemetry.
// How it works: reactor accepts, parses MQTT CONNECT, queues packets for workers;
// non-blocking write pipeline handles outbound CONNACK and other responses.

#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <memory>
#include <cstring>
#include <cstdint>
#include <unordered_map>
#include <span>
#include <array>
#include <algorithm>
#include <csignal>
#include <cerrno>

// Linux Native System Headers
#include <unistd.h>
#include <fcntl.h>
#include <string.h> // explicit_bzero
#include <sys/epoll.h>
#include <sys/socket.h>

#include "net_util.hpp"
#include "server_core.hpp"

constexpr size_t   MAX_EVENTS  = 64;
constexpr uint16_t SERVER_PORT = 8883;

// One accepted TCP connection. Declaration order matters: the writer is destroyed
// before the descriptor it writes to is closed.
struct Connection {
    UniqueFd         fd;
    AsyncWriteBuffer writer;
    bool             warned_unauthenticated = false;

    Connection(UniqueFd socket, int epoll_fd) : fd(std::move(socket)), writer(fd.get(), epoll_fd) {}
};

// Global stop flag reachable by signal handler
std::atomic<bool> g_running{true};

// Plain copy of the shutdown "doorbell" eventfd (owned by a UniqueFd in main) for the signal handler
int g_wake_fd = -1;

void signal_handler(int) {
    int saved_errno = errno;
    g_running.store(false, std::memory_order_relaxed);
    uint64_t one = 1;
    if (g_wake_fd != -1) (void)!write(g_wake_fd, &one, sizeof(one)); // async-signal-safe wake
    errno = saved_errno;
}

int main() {
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

    // Belt and braces with MSG_NOSIGNAL: writing to a reset peer must never kill the server
    struct sigaction ignore_pipe{};
    ignore_pipe.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &ignore_pipe, nullptr);

    // Listener, epoll set and the eventfd registration; any failure is reported and fatal
    auto setup_failed = [](const SysError& e) {
        std::cerr << "[CRITICAL] Server setup failed on port " << SERVER_PORT << ": " << e << "\n";
        return 1;
    };
    auto listener = create_listener(SERVER_PORT);
    if (!listener) return setup_failed(listener.error());
    auto epoll = make_epoll();
    if (!epoll) return setup_failed(epoll.error());
    const int server_fd = listener->get();
    const int epoll_fd  = epoll->get();
    if (auto added = epoll_add(epoll_fd, server_fd, EPOLLIN | EPOLLET); !added) return setup_failed(added.error());
    if (auto added = epoll_add(epoll_fd, g_wake_fd, EPOLLIN); !added) return setup_failed(added.error());

    auto pipeline = std::make_shared<TelemetryPipeline>();
    ClientRegistry registry;
    std::unordered_map<int, Connection> connections; // Owns every client socket

    // Calculate core processing footprints automatically
    unsigned int pool_size = std::thread::hardware_concurrency();
    if (pool_size == 0) pool_size = 2;
    pool_size *= 2; // I/O optimized allocation scale factor

    std::vector<std::jthread> workers;
    for (unsigned int i = 0; i < pool_size; ++i) {
        workers.emplace_back([pipeline]() {
            // Keep consuming until shutdown is requested and the queue is fully drained
            while (auto packet = pipeline->pop()) {
                // Allocate workspace securely onto Heap inside the modern micro-stack framework
                auto workspace = std::make_unique<LargeHeapWorkspace>();

                // --- DOMAIN PROCESSING BUSINESS LOGIC ---
                std::cout << "[WORKER] Device: "
                          << (packet->validated_device_id.empty()
                                ? "UNAUTHENTICATED"
                                : packet->validated_device_id)
                          << " (Length: " << packet->payload().size() << " bytes)\n";
            }
        });
    }

    // Spare descriptor released on EMFILE so a pending connection can be accepted and closed
    UniqueFd spare{open("/dev/null", O_RDONLY | O_CLOEXEC)};

    std::vector<epoll_event> events(MAX_EVENTS);
    std::cout << "[SERVER ENGINE] Monitoring port " << SERVER_PORT << " with Edge-Triggered Linux Epoll.\n";

    // ============================================================================
    // Reactor event loop
    // ============================================================================
    while (g_running.load(std::memory_order_relaxed)) {
        int num_events = epoll_wait(epoll_fd, events.data(), MAX_EVENTS, -1); // Sleep until I/O or shutdown eventfd
        if (num_events == -1) {
            if (errno == EINTR) continue;
            break;
        }

        for (int i = 0; i < num_events; ++i) {
            int current_fd = events[i].data.fd;

            if (current_fd == g_wake_fd) {
                uint64_t counter = 0;
                (void)!read(g_wake_fd, &counter, sizeof(counter)); // Drain the doorbell
                g_running.store(false, std::memory_order_relaxed);
                break;
            }

            if (current_fd == server_fd) {
                while (true) { // Edge-Triggered loop: Drain all accept calls
                    UniqueFd client{accept4(server_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC)};
                    if (!client) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        if (errno == EINTR || errno == ECONNABORTED) continue;
                        if ((errno == EMFILE || errno == ENFILE) && spare) {
                            // Out of descriptors: free the spare, accept and close the pending connection,
                            // so it does not sit in the backlog with no further edge to wake us.
                            // (Linux reports EMFILE even when the backlog is empty, so stop on EAGAIN.)
                            spare.reset();
                            UniqueFd rejected{accept(server_fd, nullptr, nullptr)};
                            const bool got_one = static_cast<bool>(rejected);
                            rejected.reset();
                            spare.reset(open("/dev/null", O_RDONLY | O_CLOEXEC));
                            if (!got_one) break;
                            std::cerr << "[SERVER ENGINE] Out of file descriptors; rejected a connection.\n";
                            continue;
                        }
                        std::cerr << "[SERVER ENGINE] accept() failed: " << std::strerror(errno) << "\n";
                        break;
                    }

                    const int client_fd = client.get();
                    if (auto added = epoll_add(epoll_fd, client_fd, EPOLLIN | EPOLLET | EPOLLRDHUP); !added) {
                        std::cerr << "[SERVER ENGINE] " << added.error() << "; dropping FD " << client_fd << "\n";
                        continue; // UniqueFd closes the socket
                    }
                    connections.try_emplace(client_fd, std::move(client), epoll_fd);
                }
            }
            else {
                auto conn_it = connections.find(current_fd);
                if (conn_it == connections.end()) continue;
                Connection& conn = conn_it->second;

                // Read first: a peer that sends data and then closes delivers EPOLLIN together with
                // EPOLLRDHUP/EPOLLHUP, and that final data must be drained before the socket is closed.
                const uint32_t revents = events[i].events;
                const bool hangup = revents & (EPOLLERR | EPOLLHUP | EPOLLRDHUP);
                bool disconnect = false;

                if (revents & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
                    // Read into a reactor-owned stack buffer; heap packets are created only for real data
                    std::array<uint8_t, MAX_PACKET_SIZE> buffer;
                    size_t used = 0;

                    while (true) { // Edge-Triggered loop: Drain read socket completely
                        ssize_t bytes_read = read(current_fd, buffer.data(), buffer.size());
                        if (bytes_read > 0) {
                            const auto data = std::span<const uint8_t>(buffer).first(static_cast<size_t>(bytes_read));
                            used = std::max(used, data.size());

                            std::string device_id;
                            if (auto known_id = registry.get_device_id_by_fd(current_fd)) {
                                device_id = std::move(*known_id);
                            } else if (auto parsed = SafeMqttParser::extract_client_id(data)) {
                                device_id = std::string(*parsed); // Copy: the view points into `buffer`
                                registry.register_client(device_id, current_fd);
                                if (!conn.writer.send_payload(MQTT_CONNACK_ACCEPTED)) {
                                    std::cerr << "[SERVER ENGINE] CONNACK send failed on FD " << current_fd << " (peer reset). Disconnecting.\n";
                                    disconnect = true; // CONNECT already read; still hand it to the workers below
                                }
                            } else if (parsed.error() == ConnectError::NotConnect) {
                                // Raw data before any CONNECT: processed as UNAUTHENTICATED (logged once)
                                if (!conn.warned_unauthenticated) {
                                    std::cerr << "[SERVER ENGINE] FD " << current_fd << " sent data before a CONNECT; treating as UNAUTHENTICATED.\n";
                                    conn.warned_unauthenticated = true;
                                }
                            } else {
                                // Malformed or refused CONNECT: tell the client why when MQTT has a code, then close
                                std::cerr << "[SERVER ENGINE] Rejected CONNECT on FD " << current_fd << ": " << to_string(parsed.error()) << "\n";
                                if (auto code = connack_code_for(parsed.error()))
                                    (void)conn.writer.send_payload(make_connack(*code)); // Best effort: closing anyway
                                disconnect = true; break;
                            }

                            auto packet = std::make_unique_for_overwrite<TelemetryPacket>();
                            packet->file_descriptor = current_fd;
                            packet->validated_device_id = std::move(device_id);
                            packet->payload_len = data.size();
                            std::memcpy(packet->raw_payload, data.data(), data.size());
                            pipeline->push(std::move(packet));
                            if (disconnect) break;
                        } else if (bytes_read == 0) {
                            disconnect = true; break;
                        } else {
                            if (errno == EINTR) continue;
                            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                            disconnect = true; break;
                        }
                    }
                    explicit_bzero(buffer.data(), used); // Scrub the plaintext copy left on the stack
                }

                if (hangup) disconnect = true; // Peer closed or socket error: everything readable is drained

                if (!disconnect && (revents & EPOLLOUT) && !conn.writer.handle_write_ready()) {
                    std::cerr << "[SERVER ENGINE] Write failed on FD " << current_fd << " (peer reset). Disconnecting.\n";
                    disconnect = true;
                }

                if (disconnect) {
                    registry.remove_client_by_fd(current_fd);
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, current_fd, nullptr);
                    connections.erase(conn_it); // Closes the socket
                }
            }
        }
    }

    // Reactor has stopped reading; let workers finish everything already queued
    g_running.store(false, std::memory_order_relaxed);
    size_t pending = pipeline->shutdown();
    std::cout << "[SERVER ENGINE] Shutdown requested. Draining " << pending << " queued packet(s)...\n";
    workers.clear(); // jthreads join here once the queue is empty
    std::cout << "[SERVER ENGINE] Closing " << connections.size() << " client connection(s).\n";
    connections.clear(); // UniqueFd closes every client socket; listener, epoll and eventfd close at scope exit
    std::cout << "[SERVER ENGINE] Shutdown complete.\n";
    return 0;
}
