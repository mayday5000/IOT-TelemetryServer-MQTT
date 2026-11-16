// tests.cpp
// Purpose: Dependency-free unit tests for the real server and client building blocks
// (server_core.hpp, mqtt_connect_frame.hpp). No sockets to a live server are needed.

#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <thread>
#include <chrono>
#include <atomic>
#include <cstdint>
#include <cerrno>

#include <unistd.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#include "server_core.hpp"
#include "mqtt_connect_frame.hpp"

int passed = 0, failed = 0;
#define CHECK(name, cond) do { if (cond) { std::cout << "[PASS] " << name << "\n"; ++passed; } else { std::cout << "[FAIL] " << name << "\n"; ++failed; } } while(0)

static std::optional<std::string> parse(const std::vector<uint8_t>& frame) {
    return SafeMqttParser::extract_client_id(frame.data(), frame.size());
}

static std::unique_ptr<TelemetryPacket> make_packet(size_t marker) {
    auto p = std::make_unique<TelemetryPacket>();
    p->file_descriptor = 0;
    p->payload_len = marker; // Used as a sequence number in these tests
    return p;
}

static void test_connect_frame_builder() {
    auto frame = MqttPacketFactory::create_connect_frame("AB");
    const std::vector<uint8_t> expected = {
        0x10, 0x0E,                         // CONNECT, remaining length 14
        0x00, 0x04, 'M', 'Q', 'T', 'T',     // protocol name
        0x04,                               // protocol level 4 (MQTT 3.1.1)
        0x02,                               // clean session
        0x00, 0x3C,                         // keep-alive 60 s
        0x00, 0x02, 'A', 'B'};              // client id
    CHECK("builder: exact CONNECT bytes for id \"AB\"", frame == expected);

    auto big = MqttPacketFactory::create_connect_frame(std::string(200, 'x'));
    CHECK("builder: 2-byte remaining length for 200-char id (212 = 0xD4 0x01)",
          big.size() == 215 && big[1] == 0xD4 && big[2] == 0x01);
}

static void test_parser() {
    CHECK("parser: round-trips builder output", parse(MqttPacketFactory::create_connect_frame("Edge_Node_A")) == "Edge_Node_A");
    CHECK("parser: multi-byte remaining length (200-char id)",
          parse(MqttPacketFactory::create_connect_frame(std::string(200, 'x'))) == std::string(200, 'x'));
    CHECK("parser: empty client id", parse(MqttPacketFactory::create_connect_frame("")) == std::string());

    auto wrong_type = MqttPacketFactory::create_connect_frame("dev");
    wrong_type[0] = 0x30; // PUBLISH
    CHECK("parser: rejects non-CONNECT packet type", !parse(wrong_type));

    auto truncated = MqttPacketFactory::create_connect_frame("device-01");
    truncated.pop_back();
    CHECK("parser: rejects truncated frame (id shorter than declared)", !parse(truncated));

    CHECK("parser: rejects frame shorter than 12 bytes", !parse({0x10, 0x0A, 0x00, 0x04, 'M', 'Q', 'T', 'T'}));

    std::vector<uint8_t> five_byte_len = {0x10, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    CHECK("parser: rejects 5-byte remaining length (oversize guard)", !parse(five_byte_len));

    std::vector<uint8_t> huge_len = {0x10, 0xFF, 0xFF, 0xFF, 0x7F, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    CHECK("parser: rejects remaining length larger than buffer", !parse(huge_len));

    auto lying_id = MqttPacketFactory::create_connect_frame("abc");
    lying_id[12] = 0x7F; // id length high byte -> ~32 KB id declared in a 17-byte frame
    CHECK("parser: rejects id length past end of buffer", !parse(lying_id));
}

static void test_connack() {
    CHECK("CONNACK bytes are 20 02 00 00",
          MQTT_CONNACK_ACCEPTED == (std::array<uint8_t, 4>{0x20, 0x02, 0x00, 0x00}));
}

static void test_registry() {
    ClientRegistry registry;
    registry.register_client("sensor-1", 5);
    CHECK("registry: fd -> device id lookup", registry.get_device_id_by_fd(5) == "sensor-1");

    registry.register_client("sensor-1", 9); // Same device reconnects on a new socket
    CHECK("registry: ghost session evicted (old fd no longer mapped)", !registry.get_device_id_by_fd(5));
    CHECK("registry: new fd mapped to the device", registry.get_device_id_by_fd(9) == "sensor-1");

    registry.remove_client_by_fd(5); // Old socket finally hangs up: must not remove the live session
    CHECK("registry: removing stale fd keeps the live session", registry.get_device_id_by_fd(9) == "sensor-1");

    registry.remove_client_by_fd(9);
    CHECK("registry: remove by fd", !registry.get_device_id_by_fd(9));
}

static void test_pipeline_drop_oldest_and_drain() {
    TelemetryPipeline pipeline;
    const size_t total = TelemetryPipeline::kCapacity + 5;
    for (size_t i = 0; i < total; ++i) pipeline.push(make_packet(i));

    size_t pending = pipeline.shutdown();
    CHECK("pipeline: queue capped at 10,000", pending == TelemetryPipeline::kCapacity);

    size_t count = 0, first = SIZE_MAX, last = 0;
    bool ordered = true;
    while (auto p = pipeline.pop()) { // After shutdown, pop() still drains every queued packet
        if (count == 0) first = p->payload_len;
        else if (p->payload_len != last + 1) ordered = false;
        last = p->payload_len;
        ++count;
    }
    CHECK("pipeline: drains every queued packet after shutdown", count == TelemetryPipeline::kCapacity);
    CHECK("pipeline: oldest 5 dropped, FIFO order kept", first == 5 && last == total - 1 && ordered);
    CHECK("pipeline: pop() returns nullptr once drained", pipeline.pop() == nullptr);
}

static void test_pipeline_shutdown_wakes_workers() {
    TelemetryPipeline pipeline;
    std::atomic<int> processed{0};
    std::vector<std::jthread> workers;
    for (int i = 0; i < 4; ++i)
        workers.emplace_back([&] { while (auto p = pipeline.pop()) processed++; });
    for (size_t i = 0; i < 100; ++i) pipeline.push(make_packet(i));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    auto t0 = std::chrono::steady_clock::now();
    pipeline.shutdown();
    workers.clear(); // Joins: would hang if a worker missed the wake-up
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    CHECK("pipeline: blocked workers wake on shutdown and join", ms < 1000 && processed == 100);
}

static void test_async_write_buffer() {
    int sv[2];
    socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sv);
    int ep = epoll_create1(0);
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
    ev.data.fd = sv[0];
    epoll_ctl(ep, EPOLL_CTL_ADD, sv[0], &ev);

    AsyncWriteBuffer writer(sv[0], ep);
    bool ok = writer.send_payload(MQTT_CONNACK_ACCEPTED.data(), MQTT_CONNACK_ACCEPTED.size());
    uint8_t buf[8] = {};
    ssize_t n = read(sv[1], buf, sizeof(buf));
    CHECK("write buffer: delivers CONNACK to peer", ok && n == 4 && buf[0] == 0x20 && buf[1] == 0x02);

    close(sv[1]); // Peer gone: must report failure, not raise SIGPIPE
    bool failed_send = !writer.send_payload(MQTT_CONNACK_ACCEPTED.data(), MQTT_CONNACK_ACCEPTED.size());
    CHECK("write buffer: closed peer returns false (EPIPE, no SIGPIPE)", failed_send);

    close(sv[0]);
    close(ep);
}

int main() {
    test_connect_frame_builder();
    test_parser();
    test_connack();
    test_registry();
    test_pipeline_drop_oldest_and_drain();
    test_pipeline_shutdown_wakes_workers();
    test_async_write_buffer();

    std::cout << "passed=" << passed << " failed=" << failed << "\n";
    return failed ? 1 : 0;
}
