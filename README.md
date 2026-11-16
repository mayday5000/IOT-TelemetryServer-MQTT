# IoT Telemetry Server (MQTT)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A high-performance, low-power MQTT telemetry server and client written in modern C++23 for Linux IoT edge environments, where every CPU wake-up costs battery. It is designed for battery-powered, solar-powered and other energy-constrained edge gateways and devices.

Built on a non-blocking, edge-triggered epoll reactor architecture, the server accepts concurrent TCP connections, parses MQTT CONNECT frames with bounds checks, maps devices via an O(1) bidirectional registry, issues CONNACK responses, and offloads payload processing to a static worker thread pool. Outbound bytes are queued in a non-blocking write pipeline so slow cellular or Wi-Fi clients never block the main network thread.

**How it stays low-power:** nothing in the system busy-waits or polls on a timer. The reactor sleeps inside the kernel in `epoll_wait` with no timeout and is woken only when a socket is ready or when the shutdown `eventfd` is signaled, so an idle server adds zero periodic wake-ups. Idle worker threads block on a condition variable instead of polling the queue, and write readiness (`EPOLLOUT`) is requested only while outbound data is pending. The worker pool is created once at startup (no thread churn), and fixed-size packet buffers plus a capped queue keep memory bounded. Between bursts of telemetry, the process gives the CPU nothing to do, so it can stay in low-power idle states.

The companion client builds an MQTT 3.1.1 CONNECT frame (clean session, 60 s keep-alive), sends it, then emits a small JSON telemetry message every 2 seconds. The telemetry messages are written as raw JSON bytes on the TCP stream, not wrapped in MQTT PUBLISH packets (see [Limitations](#limitations)). Both processes support deterministic, graceful shutdown on SIGINT and SIGTERM, woken instantly through an `eventfd` instead of a polling timeout.

## Key Features

- Zero Idle Wake-Ups (eventfd Shutdown): The reactor calls `epoll_wait(..., -1)` with no timeout. SIGINT/SIGTERM handlers write to an `eventfd` registered in the epoll set, so shutdown no longer depends on a periodic 500 ms wake-up to check a flag.
- Sleep-Driven Event Loop: The reactor blocks in `epoll_wait` and is woken by the kernel only when a socket is ready or shutdown is requested, so the CPU can drop into idle states instead of spinning.
- Zero Busy-Waiting Workers: Worker threads sleep on a `std::condition_variable` and wake only when a packet is queued or shutdown is requested.
- On-Demand Write Interest: `EPOLLOUT` is enabled only while outbound data is pending, so the kernel never reports useless "writable" events that would wake the CPU.
- No Runtime Thread Churn: The fixed worker pool is created once at startup, avoiding the CPU and memory cost of spawning threads per connection.
- Bounded Memory Footprint: Fixed-size 1 KB packet buffers and a queue capped at 10,000 packets (roughly 10 MB of payload at most) keep RAM usage predictable on constrained edge hardware.
- Low-Duty-Cycle Client: The client sends small JSON payloads every 2 seconds and sleeps in between; its sleep is interruptible, so Ctrl-C stops it immediately.
- Edge-Triggered epoll Reactor: Single-threaded I/O multiplexing with EPOLLET and EPOLLRDHUP.
- Non-Blocking Accept & Read Loops: Exhaustive EAGAIN drain loops ensure the reactor never stalls on slow connections. accept() also retries EINTR/ECONNABORTED and handles descriptor exhaustion (EMFILE/ENFILE) with a spare descriptor, so the edge-triggered listener never stalls or spins.
- Bounds-Checked MQTT CONNECT Parsing: SafeMqttParser checks the CONNECT packet type, decodes the remaining length with an overflow guard, and extracts the client identifier without reading past the receive buffer; the reactor then queues a CONNACK.
- Bidirectional Client Registry: Dual std::unordered_map indexing (fd ↔ device_id) provides average O(1) lookups; when a device id re-registers on a new socket, the stale fd mapping is evicted.
- Asynchronous Write Pipeline: Dynamic EPOLLOUT interest toggling prevents partial-write stalls and preserves strict message ordering.
- Static Worker Thread Pool: Sized to 2 × std::thread::hardware_concurrency() (4 if the core count is unknown) to absorb bursty workloads without dynamic thread creation overhead.
- Bounded Queue (Drop-Oldest): Capped at 10,000 packets; when full, the oldest packet is dropped so memory cannot grow without limit and the reactor never stalls.
- RAII Memory Scrubbing: The TelemetryPacket destructor zeroes the payload buffer with std::memset whenever a packet is destroyed (processed, dropped on overflow, or unwound by an exception).
- Modern C++ Primitives: std::jthread worker pool, std::atomic stop flags with explicit memory ordering, std::unique_ptr packet ownership, and a std::expected-based helper (C++23).
- Reset-Safe Socket Writes: All socket writes use send(..., MSG_NOSIGNAL) and SIGPIPE is ignored at startup, so a peer that resets or closes the connection can never kill the process. EPIPE/ECONNRESET is treated as a normal disconnect: the server drops that client from the registry, closes its fd and keeps serving the others; the client logs the failure and shuts down cleanly.
- Graceful Shutdown: SIGINT/SIGTERM set an atomic stop flag and ring the shutdown `eventfd`, waking the reactor instantly (no polling delay) from whichever thread received the signal; the server's workers then finish every packet already queued before they are joined, and the client flushes pending telemetry for up to 1 second before closing.

## System Architecture

### Control Flow Overview

```
Bootstrap (main thread)
  create shutdown eventfd
  install signal handlers
  spawn jthread worker pool
        │
        ▼
Reactor loop (main thread)
  while (g_running)
    epoll_wait(-1)              // sleep until I/O or shutdown
    for each ready event
      ├─ eventfd    → drain it, stop the loop
      ├─ listen fd  → drain accept until EAGAIN
      ├─ EPOLLIN    → drain read until EAGAIN
      │                 parse CONNECT if new, else tag + push
      ├─ EPOLLOUT   → handle_write_ready
      └─ hang-up    → drain remaining reads first, then registry remove + close
        │
        ▼
Workers (background)
  while (packet = pipeline->pop())   // blocks on condition variable
    process and scrub                // nullptr only after shutdown AND queue empty
        │
        ▼
Shutdown
  signal sets g_running = false and writes 1 to the eventfd
  reactor wakes immediately, drains eventfd, exits loop
  pipeline->shutdown() + notify_all()
  workers drain every queued packet, then exit; jthreads join
  close listening socket, epoll fd and eventfd
```

### 1. Reactor & Worker Pool Model

Instead of blocking one thread per client, one reactor thread monitors all file descriptors and hands off packet ownership (std::unique_ptr) to background workers.

```
MAIN THREAD (Reactor)
  g_running = true
  create shutdown eventfd
  install SIGINT/SIGTERM handlers
  spawn std::jthread worker pool
  ┌─────────────────────────────────────────┐
  │ while (g_running)                       │
  │   epoll_wait(..., -1)                   │ ◄── Sleeps in kernel, zero idle wake-ups
  │   handle eventfd / accept / read /      │
  │          write / hangups                │
  └─────────────────────────────────────────┘
  g_running = false
  pipeline->shutdown() ──► notify_all()
  workers.clear() joins the std::jthread pool

WORKER THREADS
  ┌─────────────────────────────────────────┐
  │ while (packet = pipeline->pop())        │ ◄── Blocks on condition_variable
  │   process payload & scrub memory (RAII) │
  └─────────────────────────────────────────┘
  pop() returns nullptr only after shutdown() AND once the queue is empty
```

### 2. Reactor State Machine

The reactor never blocks on application work; all CPU-heavy or blocking operations live in the worker pool.

```
          ┌──────────────┐
          │   epoll_wait │◄────────────────┐
          └──────┬───────┘                 │
                 │ events                  │
     ┌───────────┼───────────┐             │
     ▼           ▼           ▼             │
  accept      EPOLLIN     hang-up          │
  (drain)     (drain      (drain read,     │
              read +       then DEL +      │
              MQTT parse,  close)          │
              push)                        │
                 │                         │
                 └─────────────────────────┘
```

1. Setup: Create a non-blocking TCP listening socket, initialize an epoll instance, register the socket with EPOLLIN | EPOLLET, and register the shutdown eventfd with EPOLLIN.
2. Wait: epoll_wait() blocks with no timeout until at least one registered file descriptor becomes ready.
3. Dispatch:
   - Shutdown eventfd Ready: Read (drain) the eventfd counter and leave the loop.
   - Listening Socket Ready: Drain accept() in a while (true) loop until EAGAIN. Configure each accepted client socket as non-blocking, register it with EPOLLIN | EPOLLET | EPOLLRDHUP, and create an AsyncWriteBuffer. On EMFILE/ENFILE the reserved spare descriptor is released, the pending connection is accepted and closed, and the spare is reopened; the loop stops at EAGAIN, because Linux reports EMFILE even when no connection is waiting.
   - Socket Readable (EPOLLIN): Drain read() completely until EAGAIN; each read() of up to 1,024 bytes becomes one TelemetryPacket. If the fd is unregistered, attempt to parse an MQTT CONNECT; on success register the device id and queue a CONNACK (0x20 0x02 0x00 0x00). Otherwise tag the packet with the known device id. Every packet is then pushed to TelemetryPipeline (packets from a connection that never sent a valid CONNECT are processed as UNAUTHENTICATED).
   - Socket Error / Hang-Up (EPOLLERR, EPOLLHUP, EPOLLRDHUP): These often arrive together with EPOLLIN when a device sends its last data and closes, so the socket is always drained first and that data is processed normally. Then the descriptor is removed from the registry and from epoll (EPOLL_CTL_DEL) and closed.
   - Socket Writable (EPOLLOUT): If the connection is still open after reading, call handle_write_ready() to flush the outbound queue.
4. Loop: Return to epoll_wait().

### 3. Producer-Consumer Pipeline & Client Architecture

```
SERVER PIPELINE:
Reactor Thread                  Worker Threads
     │                               │
     │  read() + MQTT parse          │
     │  pipeline->push() ──────────► │  pop() (blocks until data or shutdown)
     │  (O(1), drop-oldest when full)│  process & scrub payload
     │                               │  loop

CLIENT ARCHITECTURE:
┌─────────────────────────────┐     ┌──────────────────────────────┐
│  Telemetry Worker Thread    │     │  Main Thread (Reactor)       │
│                             │     │                              │
│  Every 2 seconds:           │     │  epoll_wait(-1)              │
│    Build JSON metric        │────►│  EPOLLOUT ──► Flush writes   │
│    pipeline->send_payload() │     │  EPOLLIN  ──► Read CONNACK   │
│  Sleeps on condition_var,   │◄────│  Hangup   ──► Trigger stop   │
│  woken at once on shutdown  │     │  eventfd  ──► Trigger stop   │
└─────────────────────────────┘     └──────────────────────────────┘
                 │                                  │
                 └──── Shared ClientWritePipeline ──┘
                         (Non-blocking FIFO queue)
```

### 4. Asynchronous Write Pipeline

```
send_payload()
  ├─ Queue empty? ──► Attempt direct send(MSG_NOSIGNAL)
  │     ├─ All bytes written ──► Done
  │     ├─ Partial / EAGAIN  ──► Queue remainder + enable EPOLLOUT
  │     └─ EPIPE / ECONNRESET ──► return false ──► caller disconnects
  └─ Queue non-empty ──► Append to queue (preserves FIFO order)

handle_write_ready() [Triggered by EPOLLOUT]
  └─ While queue not empty:
        send(MSG_NOSIGNAL) next chunk from offset
        ├─ Partial write ──► Update offset, yield (wait for next EPOLLOUT)
        ├─ Full chunk    ──► Pop chunk, continue
        └─ EPIPE / ECONNRESET ──► return false ──► caller disconnects
  └─ Queue empty ──► Disable EPOLLOUT (prevents busy-looping)
```

## Project Structure

```
├── README.md     # Project documentation
├── LICENSE       # MIT License
├── deploy.sh     # Idempotent Linux dependency & build script
├── server.cpp           # Epoll reactor, eventfd shutdown, accept/read/write dispatch and jthread worker pool
├── server_core.hpp      # TelemetryPacket, ClientRegistry, SafeMqttParser, AsyncWriteBuffer, TelemetryPipeline, CONNACK bytes
├── client.cpp           # Device simulator: MQTT CONNECT + periodic JSON telemetry over an epoll reactor
├── mqtt_connect_frame.hpp  # MqttPacketFactory: builds the MQTT 3.1.1 CONNECT frame
└── tests.cpp            # Unit tests for the code in server_core.hpp and mqtt_connect_frame.hpp
```

## Quick Start

### Prerequisites

- Compiler: g++ 13 or newer with C++23 support (`-std=c++23`, `<expected>`, `std::jthread`); verified with g++ 14.2. Other C++23 toolchains should work but are untested.
- OS: Linux only. Uses epoll (`epoll_create1`, `EPOLLET`, `EPOLLRDHUP`) and `eventfd` with `EFD_NONBLOCK | EFD_CLOEXEC`, so kernel 2.6.27 or newer.
- Libraries: pthread (via `-pthread`); no third-party dependencies.

### Automated Setup & Build (deploy.sh)

deploy.sh is the single, idempotent entry point on Linux:

1. Inspects the system for g++ and make, installing only what is missing (build-essential on Debian/Ubuntu, gcc-c++ on Fedora/RHEL/yum). It does not check the compiler version, so on distributions whose default g++ is older than 13, install a newer g++ first.
2. Compiles server, client, and tests with -std=c++23 -O2 -pthread (make is checked for but not used).
3. Runs ./tests and stops on failure (set -euo pipefail).

```bash
chmod +x deploy.sh && ./deploy.sh
```

### Manual Installation & Build

Debian / Ubuntu:

```bash
sudo apt-get update && sudo apt-get install -y build-essential g++ make
```

Fedora / RHEL:

```bash
sudo dnf install -y gcc-c++ make
```

Compile & Run Tests:

```bash
g++ -std=c++23 -O2 -Wall -Wextra -DNDEBUG -pthread -o server server.cpp
g++ -std=c++23 -O2 -Wall -Wextra -DNDEBUG -pthread -o client client.cpp
g++ -std=c++23 -O2 -Wall -Wextra -pthread -o tests tests.cpp && ./tests
```

### Running the Server & Client

Terminal 1 (Start Server):

```bash
./server
```

Terminal 2 (Start Client):

```bash
./client MyDevice_01 127.0.0.1
```

Usage:
- `./server` takes no arguments and listens on TCP port 8883 on all IPv4 interfaces (`SERVER_PORT` in server.cpp).
- `./client [device_id] [server_ipv4]` defaults to `IoT_Edge_Node_01` and `127.0.0.1`; the port is fixed at 8883.

Expected Output (abridged):

```
# server
[SERVER ENGINE] Monitoring port 8883 with Edge-Triggered Linux Epoll.
[REGISTRY] Authenticated: Raw FD 7 Bound to ID -> MyDevice_01
[WORKER] Device: MyDevice_01 (Length: 25 bytes)
[WORKER] Device: MyDevice_01 (Length: 28 bytes)
...
[SERVER ENGINE] Shutdown requested. Draining 0 queued packet(s)...
[SERVER ENGINE] Shutdown complete.

# client
[CLIENT] Attempting connection link to server: 127.0.0.1:8883
[REACTOR] Client network loop activated safely.
[TELEMETRY] Sending CONNECT for device: MyDevice_01
[REACTOR] Received 4 bytes from server
[TELEMETRY] Background worker detached cleanly.
[CLIENT] Shutdown complete.
```

The first worker line is the CONNECT frame itself. Each later one is a telemetry message such as `{"msg_idx":0,"temp_c":24.85}` (the temperature is a fixed simulated value). The client does not print the messages it sends; "Received 4 bytes" is the CONNACK.

Press Ctrl-C in either terminal to trigger a graceful shutdown. Stopping the server also makes the client exit, because it sees the connection close.

## C++23 Language Dialect & Contracts

The codebase is compiled as C++23 and uses standard-library facilities for ownership and thread safety:

- std::jthread: Workers join automatically when the std::vector<std::jthread> is cleared or destroyed, so there are no manual join loops.
- std::unique_ptr<TelemetryPacket>: Each packet has exactly one owner as it moves from the reactor to a worker.
- std::atomic<bool>: The stop flags use explicit (relaxed) memory ordering and are safe to write from a signal handler.
- std::expected<void, std::string>: server.cpp defines an ensure_non_blocking() helper returning std::expected (C++23); the current setup path calls make_socket_non_blocking() directly.
- `<span>` is included but not used yet.

## Ownership & Memory Scrubbing Flow

1. Reactor allocates std::unique_ptr<TelemetryPacket>.
2. Reactor fills the payload, performs MQTT parsing if needed, and moves ownership into TelemetryPipeline::push().
3. Worker thread receives exclusive ownership from TelemetryPipeline::pop().
4. When the packet is destroyed (after processing, when evicted by the drop-oldest policy, or during exception unwinding), ~TelemetryPacket() zeroes the raw payload buffer via std::memset before the memory returns to the allocator. Only the payload buffer is scrubbed, not the device-id string, and because a plain memset of memory that is about to be freed can legally be optimized away, use explicit_bzero() if scrubbing must be guaranteed.
5. On shutdown, workers drain the queue and the std::jthread pool is joined.

## Edge-Triggered & Overflow Contracts

- Edge-Triggered Drain Contract: Because sockets are registered with EPOLLET, every ready file descriptor must be drained until EAGAIN. Both accept() and read() are wrapped in while (true) loops that exit only on EAGAIN/EWOULDBLOCK or a hard error; failing to drain would silently lose subsequent data until the next edge transition.
- Overflow Policy: TelemetryPipeline is capped at 10,000 packets. When full, the oldest packet is dropped before inserting the newest sample, prioritizing fresh telemetry values while bounding memory without stalling the reactor. No back-pressure is applied to clients; dropped packets are not counted or logged.
- Queue Wake-Up Policy: push() calls notify_one() to wake a single worker and minimize context switches; shutdown() calls notify_all() to unblock all waiting threads simultaneously.
- Write-Pipeline Ordering: Partial writes resume from a saved offset; new data is always appended so FIFO order is preserved. EPOLLOUT is enabled only while data remains queued.

## Detailed Shutdown Sequence

The reactor waits with no timeout, so shutdown is delivered through an eventfd registered in the epoll set. This costs one extra file descriptor and removes both the periodic idle wake-ups and the shutdown delay:

1. SIGINT/SIGTERM handler performs only async-signal-safe work: it stores g_running = false and write()s a uint64_t 1 to the eventfd (preserving errno).
2. The eventfd becomes readable, so epoll_wait returns immediately, no matter which thread received the signal. If the signal arrives just before epoll_wait is entered, the counter is already set and epoll_wait returns at once, so the signal cannot be missed. If epoll_wait is interrupted with EINTR, the loop re-checks the flag.
3. The reactor drains the eventfd and exits its loop.
4. The reactor has stopped reading, so no new packets are queued. pipeline->shutdown() sets the internal stop flag under the queue mutex (so no worker can miss the wake-up), reports how many packets are still queued, and broadcasts notify_all().
5. Workers keep calling pop() and process every packet that was already queued. pop() returns nullptr only when shutdown was requested and the queue is empty, and then each worker exits.
6. workers.clear() joins the std::jthread pool.
7. Listening socket, epoll file descriptor and eventfd are closed; accepted client sockets are closed by the kernel at process exit. CONNACKs still waiting in a server-side write buffer are not flushed.

The client follows the same pattern. Its reactor wakes on the eventfd, then notifies a condition variable so the telemetry thread leaves its 2-second sleep immediately instead of finishing it. If telemetry is still queued in the client's write pipeline (the socket was not writable), the client keeps flushing it for up to 1 second (FLUSH_TIMEOUT_MS). The flush is skipped if the server already closed the connection, and a second Ctrl-C aborts it.

Draining is unbounded in time on the server: with a large backlog and slow processing, shutdown takes as long as the workers need to finish the queue (at most 10,000 packets). Process managers such as systemd (TimeoutStopSec, 90 s by default) or Kubernetes (terminationGracePeriodSeconds, 30 s by default) send SIGKILL if that limit is exceeded.

## Testing, Troubleshooting & Production Hardening

### Unit Testing (tests.cpp)

tests.cpp includes the same headers the server and client are built from (server_core.hpp and mqtt_connect_frame.hpp), so it tests the real code. It uses no external framework and needs no running server. Its 24 checks cover:

- CONNECT frame builder: exact bytes for a short id, and 2-byte remaining-length encoding for a 200-character id.
- SafeMqttParser: round-trip of builder output (including a multi-byte remaining length and an empty id), and rejection of a wrong packet type, a truncated frame, a frame under 12 bytes, a 5-byte remaining length, a remaining length larger than the buffer, a remaining payload too short to hold the 2-byte client-id length field, and an id length that points past the end of the remaining payload or the buffer.
- CONNACK bytes: `20 02 00 00`.
- ClientRegistry: lookup, ghost-session eviction when a device reconnects on a new socket, removing the stale fd without dropping the live session, and removal.
- TelemetryPipeline: the 10,000-packet cap with drop-oldest in FIFO order, draining every queued packet after shutdown(), nullptr once drained, and blocked workers waking and joining on shutdown().
- AsyncWriteBuffer (over a socketpair): delivering a CONNACK, and returning false instead of raising SIGPIPE when the peer has closed.

The epoll reactor, eventfd shutdown, client reactor and flush logic live in server.cpp and client.cpp and are verified by running the server and client together.

### Troubleshooting

| Symptom | Likely Cause & Fix |
|---------|--------------------|
| Address already in use | Another process holds port 8883. Stop the conflicting process or update SERVER_PORT. |
| "Out of file descriptors; rejected a connection" | The server hit its descriptor limit (`ulimit -n`); raise it or reduce concurrent clients. |
| Client cannot connect | Confirm ./server is running and local firewall rules allow TCP traffic on port 8883. |
| High idle CPU usage | Ensure epoll_wait is not called with a zero timeout (it should block with -1), that EPOLLOUT is disabled once the write queue is empty, and that accept()/read() loops exit cleanly on EAGAIN. |
| No CONNACK | The first bytes on a new connection must be a CONNECT (0x10) frame of at least 12 bytes; otherwise the data is processed as UNAUTHENTICATED and no CONNACK is sent. |
| Ctrl-C takes a long time on the server | Workers are draining a large backlog. Each packet already queued is processed before exit. |
| Fewer [WORKER] lines than messages sent | TCP is a byte stream: several small messages that arrive together are read as one packet (up to 1,024 bytes). There is no per-message framing. |

### Scaling & Production Hardening

- Horizontal Scaling: Run multiple server processes behind a load balancer, or set SO_REUSEPORT (not set today) and adopt a multi-reactor design (one epoll instance per core) for extreme connection rates.
- Security & Observability: Wrap sockets in TLS, add device authentication beyond the client identifier, redirect stdout/stderr to a logging daemon, and expose atomic metrics for accepted connections, dropped packets and queue depth.
- Ghost Session Handling: When the same device id re-registers, the registry drops the previous fd mapping so lookups route to the new socket. The old socket itself is not closed until its own hang-up is detected.

## Scripts

deploy.sh checks for g++ and make, installs any missing packages, builds the three binaries (server, client, tests) with -std=c++23 -O2 -pthread, and runs the tests. See [Automated Setup & Build](#automated-setup--build-deploysh) for details.

Example:

```bash
./deploy.sh
```

## Further Operational Notes

The server listens on TCP port 8883 on all IPv4 interfaces. 8883 is the port conventionally used for MQTT over TLS, but this server speaks plain TCP. A failed bind almost always indicates that another process already owns the port; ss -tulpn | grep 8883 identifies the occupant. The client accepts a device name and an optional server address.

All diagnostic output appears on standard output and standard error. In production these streams should be captured by the system logging facility. The reactor thread performs no heavy work; it only accepts, reads, parses the minimal CONNECT header, enqueues, and flushes the write pipeline.

The worker count is computed as twice the hardware concurrency reported by the standard library (4 when the query returns zero). Each worker allocates a 32 KB scratch workspace per packet it processes; the current processing step only logs the device id and payload length. This sizing provides enough concurrency to absorb short processing bursts while remaining modest for a pure I/O-bound workload.

SafeMqttParser performs bounds-checked remaining-length decoding before extracting the client-identifier string, so a malformed CONNECT cannot overrun the receive buffer. The fixed CONNACK indicates connection success and is sufficient for the teaching client.

## Limitations

This is a compact teaching and reference implementation, not a full MQTT broker:

- MQTT subset: only CONNECT is parsed and the CONNACK is always "accepted". PUBLISH, SUBSCRIBE, PINGREQ, DISCONNECT, QoS levels, retained messages and will messages are not implemented. The protocol name and level are not validated, and the keep-alive value is sent by the client but not enforced by the server.
- Not interoperable with standard MQTT brokers or clients for telemetry, because the client sends raw JSON bytes after CONNECT instead of PUBLISH packets.
- No TLS and no authentication: traffic is plain TCP on port 8883, and the client identifier is trusted as-is. Data from connections that never send a valid CONNECT is still queued and processed as UNAUTHENTICATED.
- No message framing: packets are whatever a single read() returns (up to 1,024 bytes).
- Client: IPv4 only, port fixed at 8883, blocking connect() at startup, no reconnect logic, simulated constant sensor value.
- Dropped packets (queue overflow) are not counted or reported, and every processed packet is logged to stdout.

## License

This project is licensed under the [MIT License](LICENSE). Copyright (c) 2026 Damian Andres Ulanowicz.
