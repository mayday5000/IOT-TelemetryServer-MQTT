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
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <csignal>
#include <cerrno>
#include <span>
#include <expected>

#include "server_core.hpp"

constexpr size_t MAX_EVENTS  = 64;
constexpr int    SERVER_PORT = 8883;

// ============================================================================
// Runtime helpers and main
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

// Shutdown "doorbell": eventfd watched by epoll so the reactor can sleep with no timeout
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
    g_wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (g_wake_fd == -1) {
        std::cerr << "[CRITICAL] eventfd creation failed.\n";
        return 1;
    }

    // Install graceful shutdown handlers
    std::signal(SIGINT,  signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Belt and braces with MSG_NOSIGNAL: writing to a reset peer must never kill the server
    struct sigaction ignore_pipe{};
    ignore_pipe.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &ignore_pipe, nullptr);

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
            // Keep consuming until shutdown is requested and the queue is fully drained
            while (auto packet = pipeline->pop()) {
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
        pipeline->shutdown(); // Release workers so their jthreads can join
        return 1;
    }

    int epoll_fd = epoll_create1(0);
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET; 
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    // Register the shutdown eventfd so a signal wakes the reactor instantly
    epoll_event wake_ev{};
    wake_ev.events = EPOLLIN;
    wake_ev.data.fd = g_wake_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, g_wake_fd, &wake_ev);

    // Spare descriptor released on EMFILE so a pending connection can be accepted and closed
    int spare_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);

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
                    int client_fd = accept(server_fd, nullptr, nullptr);
                    if (client_fd == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        if (errno == EINTR || errno == ECONNABORTED) continue;
                        if ((errno == EMFILE || errno == ENFILE) && spare_fd != -1) {
                            // Out of descriptors: free the spare, accept and close the pending connection,
                            // so it does not sit in the backlog with no further edge to wake us.
                            // (Linux reports EMFILE even when the backlog is empty, so stop on EAGAIN.)
                            close(spare_fd);
                            int rejected = accept(server_fd, nullptr, nullptr);
                            if (rejected != -1) close(rejected);
                            spare_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
                            if (rejected == -1) break;
                            std::cerr << "[SERVER ENGINE] Out of file descriptors; rejected a connection.\n";
                            continue;
                        }
                        std::cerr << "[SERVER ENGINE] accept() failed: " << std::strerror(errno) << "\n";
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
            else {
                // Read first: a peer that sends data and then closes delivers EPOLLIN together with
                // EPOLLRDHUP/EPOLLHUP, and that final data must be drained before the socket is closed.
                const uint32_t revents = events[i].events;
                const bool hangup = revents & (EPOLLERR | EPOLLHUP | EPOLLRDHUP);
                bool disconnect = false;

                if (revents & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
                    auto packet = std::make_unique<TelemetryPacket>();

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
                                    if (write_managers.count(current_fd) &&
                                        !write_managers[current_fd]->send_payload(MQTT_CONNACK_ACCEPTED.data(), MQTT_CONNACK_ACCEPTED.size())) {
                                        std::cerr << "[SERVER ENGINE] CONNACK send failed on FD " << current_fd << " (peer reset). Disconnecting.\n";
                                        pipeline->push(std::move(packet)); // CONNECT already read; still hand it to the workers
                                        disconnect = true; break;
                                    }
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
                }

                if (hangup) disconnect = true; // Peer closed or socket error: everything readable is drained

                if (!disconnect && (revents & EPOLLOUT)) {
                    if (write_managers.count(current_fd) && !write_managers[current_fd]->handle_write_ready()) {
                        std::cerr << "[SERVER ENGINE] Write failed on FD " << current_fd << " (peer reset). Disconnecting.\n";
                        disconnect = true;
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

    // Reactor has stopped reading; let workers finish everything already queued
    g_running.store(false, std::memory_order_relaxed);
    size_t pending = pipeline->shutdown();
    std::cout << "[SERVER ENGINE] Shutdown requested. Draining " << pending << " queued packet(s)...\n";
    workers.clear(); // jthreads join here once the queue is empty
    close(server_fd);
    close(epoll_fd);
    close(g_wake_fd);
    if (spare_fd != -1) close(spare_fd);
    std::cout << "[SERVER ENGINE] Shutdown complete.\n";
    return 0;
}
