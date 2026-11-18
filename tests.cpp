// tests.cpp
// Purpose: Dependency-free unit tests for the real server and client building blocks
// (net_util.hpp, server_core.hpp, mqtt_connect_frame.hpp). Sockets are local only
// (socketpair and 127.0.0.1); no running server is needed.

#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <memory>
#include <thread>
#include <chrono>
#include <atomic>
#include <cstdint>
#include <cerrno>

#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#include "net_util.hpp"
#include "server_core.hpp"
#include "mqtt_connect_frame.hpp"

using namespace std::chrono_literals;

int passed = 0, failed = 0;
#define CHECK(name, cond) do { if (cond) { std::cout << "[PASS] " << name << "\n"; ++passed; } else { std::cout << "[FAIL] " << name << "\n"; ++failed; } } while(0)

static std::vector<uint8_t> frame_for(std::string_view id) { return MqttPacketFactory::create_connect_frame(id).value(); }

static std::expected<std::string, ConnectError> parse(const std::vector<uint8_t>& frame) {
    return SafeMqttParser::extract_client_id(frame).transform([](std::string_view v) { return std::string(v); });
}

static bool parse_fails_with(const std::vector<uint8_t>& frame, ConnectError e) {
    auto r = parse(frame);
    return !r && r.error() == e;
}

static std::string text(const SysError& e) {
    std::ostringstream os;
    os << e;
    return os.str();
}

static bool fd_is_open(int fd) { return fcntl(fd, F_GETFD) != -1; }

static std::unique_ptr<TelemetryPacket> make_packet(size_t marker) {
    auto p = std::make_unique<TelemetryPacket>();
    p->file_descriptor = 0;
    p->payload_len = marker; // Used as a sequence number in these tests
    return p;
}

static void test_unique_fd() {
    int raw = -1;
    {
        UniqueFd a{eventfd(0, EFD_CLOEXEC)};
        raw = a.get();
        UniqueFd b = std::move(a);
        CHECK("UniqueFd: move transfers ownership", !a && b.get() == raw && fd_is_open(raw));
    }
    CHECK("UniqueFd: closes the descriptor at scope exit", !fd_is_open(raw));
}

static void test_connect_frame_builder() {
    auto frame = frame_for("AB");
    const std::vector<uint8_t> expected = {
        0x10, 0x0E,                         // CONNECT, remaining length 14
        0x00, 0x04, 'M', 'Q', 'T', 'T',     // protocol name
        0x04,                               // protocol level 4 (MQTT 3.1.1)
        0x02,                               // clean session
        0x00, 0x3C,                         // keep-alive 60 s
        0x00, 0x02, 'A', 'B'};              // client id
    CHECK("builder: exact CONNECT bytes for id \"AB\"", frame == expected);

    auto big = frame_for(std::string(200, 'x'));
    CHECK("builder: 2-byte remaining length for 200-char id (212 = 0xD4 0x01)",
          big.size() == 215 && big[1] == 0xD4 && big[2] == 0x01);

    const std::string max_id(MqttPacketFactory::kMaxClientIdLen, 'x');
    CHECK("builder: accepts a 65,535-byte id", MqttPacketFactory::create_connect_frame(max_id).has_value());
    auto too_long = MqttPacketFactory::create_connect_frame(max_id + "x");
    CHECK("builder: rejects a 65,536-byte id (IdTooLong)", !too_long && too_long.error() == FrameError::IdTooLong);
}

static void test_parser() {
    CHECK("parser: round-trips builder output", parse(frame_for("Edge_Node_A")) == "Edge_Node_A");
    CHECK("parser: multi-byte remaining length (200-char id)", parse(frame_for(std::string(200, 'x'))) == std::string(200, 'x'));

    CHECK("parser: TooShort on an empty buffer", parse_fails_with({}, ConnectError::TooShort));
    CHECK("parser: TooShort when remaining length < 12",
          parse_fails_with({0x10, 0x0A, 0x00, 0x04, 'M', 'Q', 'T', 'T'}, ConnectError::TooShort));

    // remaining_length = 11: 10-byte variable header + only one of the two id-length bytes.
    CHECK("parser: TooShort for 11-byte remaining payload (truncated id length field)",
          parse_fails_with({0x10, 11, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3C, 0x00}, ConnectError::TooShort));

    auto wrong_type = frame_for("dev");
    wrong_type[0] = 0x30; // PUBLISH
    CHECK("parser: NotConnect for a PUBLISH packet", parse_fails_with(wrong_type, ConnectError::NotConnect));
    CHECK("parser: NotConnect for raw JSON", parse_fails_with({'{', '"', 'a', '"', ':', '1', '}'}, ConnectError::NotConnect));

    CHECK("parser: BadRemainingLength for a 5-byte length field",
          parse_fails_with({0x10, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, ConnectError::BadRemainingLength));

    CHECK("parser: Truncated when remaining length exceeds the buffer",
          parse_fails_with({0x10, 0xFF, 0xFF, 0xFF, 0x7F, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, ConnectError::Truncated));
    auto truncated = frame_for("device-01");
    truncated.pop_back();
    CHECK("parser: Truncated when the id is shorter than declared", parse_fails_with(truncated, ConnectError::Truncated));
    CHECK("parser: Truncated when the length field is cut off", parse_fails_with({0x10, 0x80}, ConnectError::Truncated));

    auto bad_level = frame_for("dev");
    bad_level[8] = 0x03; // MQTT 3.1 level
    CHECK("parser: BadProtocol for protocol level 3", parse_fails_with(bad_level, ConnectError::BadProtocol));
    auto bad_name = frame_for("dev");
    bad_name[4] = 'X';
    CHECK("parser: BadProtocol for protocol name \"MXTT\"", parse_fails_with(bad_name, ConnectError::BadProtocol));

    auto lying_id = frame_for("abc");
    lying_id[12] = 0x7F; // id length high byte -> ~32 KB id declared in a 17-byte frame
    CHECK("parser: IdOverrun for id length past end of buffer", parse_fails_with(lying_id, ConnectError::IdOverrun));

    // Empty-id CONNECT (remaining_length 12) plus two extra bytes in the same buffer,
    // with id_len patched to 2 so a buffer-only check would accept "XY" as the id.
    auto id_past_remaining = frame_for("");
    id_past_remaining.push_back('X');
    id_past_remaining.push_back('Y');
    id_past_remaining[13] = 2; // id length low byte
    CHECK("parser: IdOverrun for id length past remaining length (still inside buffer)",
          parse_fails_with(id_past_remaining, ConnectError::IdOverrun));

    CHECK("parser: EmptyId for a zero-length client id", parse_fails_with(frame_for(""), ConnectError::EmptyId));

    CHECK("parser: CONNACK code 0x01 for BadProtocol, 0x02 for EmptyId, none for malformed",
          connack_code_for(ConnectError::BadProtocol) == 0x01 && connack_code_for(ConnectError::EmptyId) == 0x02 &&
          !connack_code_for(ConnectError::Truncated));
}

static void test_connack_parse() {
    CHECK("CONNACK bytes are 20 02 00 00",
          MQTT_CONNACK_ACCEPTED == (std::array<uint8_t, 4>{0x20, 0x02, 0x00, 0x00}));
    CHECK("connack: accepted (code 0)", MqttPacketFactory::parse_connack(MQTT_CONNACK_ACCEPTED).has_value());

    auto rejected = MqttPacketFactory::parse_connack(make_connack(0x02));
    CHECK("connack: code 2 -> Rejected, \"server rejected client id\"",
          !rejected && rejected.error().op == Op::Rejected && rejected.error().code == 2 &&
          text(rejected.error()).starts_with("server rejected client id"));
    auto bad_proto = MqttPacketFactory::parse_connack(make_connack(0x01));
    CHECK("connack: code 1 -> Rejected (protocol version)", !bad_proto && bad_proto.error().code == 1);

    const std::array<std::array<uint8_t, 4>, 4> garbage = {{
        {0x30, 0x02, 0x00, 0x00},   // wrong packet type
        {0x20, 0x03, 0x00, 0x00},   // wrong remaining length
        {0x20, 0x02, 0x02, 0x00},   // reserved flag bits set
        {0x20, 0x02, 0x00, 0x09}}}; // return code out of range
    bool all_malformed = true;
    for (const auto& g : garbage) {
        auto r = MqttPacketFactory::parse_connack(g);
        all_malformed = all_malformed && !r && r.error().op == Op::ConnackMalformed;
    }
    CHECK("connack: garbage frames -> ConnackMalformed", all_malformed);
}

// Runs await_connack on one end of a socketpair after `server` acts on the other end
static std::expected<UniqueFd, SysError> handshake_with(auto server, std::chrono::milliseconds timeout = 500ms) {
    int sv[2];
    socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sv);
    UniqueFd client{sv[0]}, peer{sv[1]};
    auto wake = make_eventfd().value();
    server(peer, wake.get());
    return await_connack(std::move(client), wake.get(), timeout);
}

static void test_await_connack() {
    auto send_bytes = [](std::vector<uint8_t> bytes) {
        return [bytes](UniqueFd& peer, int) { (void)!write(peer.get(), bytes.data(), bytes.size()); };
    };

    CHECK("await_connack: accepted CONNACK returns the socket", handshake_with(send_bytes({0x20, 0x02, 0x00, 0x00})).has_value());

    auto rejected = handshake_with(send_bytes({0x20, 0x02, 0x00, 0x02}));
    CHECK("await_connack: rejected client id -> Rejected(2)", !rejected && rejected.error().op == Op::Rejected && rejected.error().code == 2);

    auto garbage = handshake_with(send_bytes({'H', 'T', 'T', 'P'}));
    CHECK("await_connack: garbage reply -> ConnackMalformed", !garbage && garbage.error().op == Op::ConnackMalformed);

    auto t0 = std::chrono::steady_clock::now();
    auto silent = handshake_with([](UniqueFd&, int) {}, 100ms);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0).count();
    CHECK("await_connack: silent server -> ConnackTimeout after ~100 ms",
          !silent && silent.error().op == Op::ConnackTimeout && ms >= 90 && ms < 1000);

    auto closed = handshake_with([](UniqueFd& peer, int) { peer.reset(); });
    CHECK("await_connack: server closes -> PeerClosed", !closed && closed.error().op == Op::PeerClosed);

    {
        int sv[2];
        socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sv);
        UniqueFd client{sv[0]}, peer{sv[1]};
        auto wake = make_eventfd().value();
        (void)!write(peer.get(), "\x20\x02", 2);
        std::jthread late([&] { std::this_thread::sleep_for(30ms); (void)!write(peer.get(), "\x00\x00", 2); });
        auto split = await_connack(std::move(client), wake.get(), 1000ms);
        CHECK("await_connack: CONNACK split across two reads", split.has_value());
    }

    auto woken = handshake_with([](UniqueFd&, int wake) { uint64_t one = 1; (void)!write(wake, &one, sizeof(one)); }, 5000ms);
    CHECK("await_connack: shutdown eventfd -> Interrupted at once", !woken && woken.error().op == Op::Interrupted);
}

static void test_connect_to() {
    auto wake = make_eventfd().value();

    bool all_bad = true;
    for (const char* ip : {"999.1.1.1", "localhost", "", "10.0.0", "::1"}) {
        auto r = connect_to(ip, 8883, wake.get(), 100ms);
        all_bad = all_bad && !r && r.error().op == Op::BadAddress && !parse_ipv4(ip, 8883);
    }
    CHECK("connect_to: bad IPs -> BadAddress (config error, no socket opened)", all_bad);
    CHECK("connect_to: BadAddress message says \"bad IP address\"", text(SysError{Op::BadAddress}).starts_with("bad IP address"));

    // Find a free port: bind to an ephemeral port, read it back, close the listener
    uint16_t port = 0;
    {
        UniqueFd probe{socket(AF_INET, SOCK_STREAM, 0)};
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        socklen_t len = sizeof(a);
        (void)!bind(probe.get(), reinterpret_cast<sockaddr*>(&a), sizeof(a));
        getsockname(probe.get(), reinterpret_cast<sockaddr*>(&a), &len);
        port = ntohs(a.sin_port);
    }
    auto refused = connect_to("127.0.0.1", port, wake.get(), 1000ms);
    CHECK("connect_to: closed port -> \"connection refused\"",
          !refused && refused.error().op == Op::Connect && refused.error().code == ECONNREFUSED &&
          text(refused.error()).starts_with("connection refused"));

    // Full chain against a real listener that answers CONNECT with an accepted CONNACK
    auto listener = create_listener(port);
    CHECK("create_listener: binds and listens", listener.has_value());
    if (!listener) return;
    std::string seen_id;
    std::jthread fake_server([&] {
        pollfd p{listener->get(), POLLIN, 0};
        if (poll(&p, 1, 2000) != 1) return;
        UniqueFd c{accept(listener->get(), nullptr, nullptr)};
        uint8_t buf[64];
        ssize_t n = -1;
        for (int i = 0; i < 100 && n <= 0; ++i) {
            n = recv(c.get(), buf, sizeof(buf), MSG_DONTWAIT);
            if (n <= 0) std::this_thread::sleep_for(10ms);
        }
        if (n > 0)
            if (auto id = SafeMqttParser::extract_client_id(std::span<const uint8_t>(buf, static_cast<size_t>(n)))) seen_id = *id;
        (void)!write(c.get(), MQTT_CONNACK_ACCEPTED.data(), MQTT_CONNACK_ACCEPTED.size());
        std::this_thread::sleep_for(50ms);
    });
    auto frame = frame_for("chain-test");
    auto session = connect_to("127.0.0.1", port, wake.get(), 1000ms)
        .and_then([&](UniqueFd fd) { return send_connect(std::move(fd), frame, wake.get(), 1000ms); })
        .and_then([&](UniqueFd fd) { return await_connack(std::move(fd), wake.get(), 1000ms); });
    fake_server.join();
    CHECK("handshake chain: connect -> CONNECT -> CONNACK succeeds", session.has_value() && seen_id == "chain-test");

    auto busy = create_listener(port);
    CHECK("create_listener: port in use -> Bind error", !busy && busy.error().op == Op::Bind && busy.error().code == EADDRINUSE);
}

static void test_wait_fd_sleep() {
    auto wake = make_eventfd().value();
    auto t0 = std::chrono::steady_clock::now();
    auto r = wait_fd(-1, 0, wake.get(), 50ms);
    auto slept = std::chrono::steady_clock::now() - t0;
    CHECK("wait_fd: backoff sleep times out", r == Wait::Timeout && slept >= 45ms);

    uint64_t one = 1;
    (void)!write(wake.get(), &one, sizeof(one));
    t0 = std::chrono::steady_clock::now();
    r = wait_fd(-1, 0, wake.get(), 30s);
    CHECK("wait_fd: eventfd ends a 30 s backoff sleep at once", r == Wait::Woken && std::chrono::steady_clock::now() - t0 < 100ms);
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
    bool ok = writer.send_payload(MQTT_CONNACK_ACCEPTED);
    uint8_t buf[8] = {};
    ssize_t n = read(sv[1], buf, sizeof(buf));
    CHECK("write buffer: delivers CONNACK to peer", ok && n == 4 && buf[0] == 0x20 && buf[1] == 0x02);

    close(sv[1]); // Peer gone: must report failure, not raise SIGPIPE
    bool failed_send = !writer.send_payload(MQTT_CONNACK_ACCEPTED);
    CHECK("write buffer: closed peer returns false (EPIPE, no SIGPIPE)", failed_send);

    close(sv[0]);
    close(ep);
}

int main() {
    test_unique_fd();
    test_connect_frame_builder();
    test_parser();
    test_connack_parse();
    test_await_connack();
    test_connect_to();
    test_wait_fd_sleep();
    test_registry();
    test_pipeline_drop_oldest_and_drain();
    test_pipeline_shutdown_wakes_workers();
    test_async_write_buffer();

    std::cout << "passed=" << passed << " failed=" << failed << "\n";
    return failed ? 1 : 0;
}
