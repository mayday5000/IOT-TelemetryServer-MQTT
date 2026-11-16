# IoT Telemetry Server (MQTT)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A high-performance, low-power MQTT telemetry server and client written in modern C++23 for Linux IoT edge environments.

Built on a non-blocking, edge-triggered epoll reactor architecture, the server accepts concurrent TCP connections, validates binary MQTT CONNECT frames, maps devices via an O(1) bidirectional registry, issues CONNACK responses, and offloads payload processing to a static worker thread pool. Outbound traffic is throttled through an asynchronous write pipeline so slow cellular or Wi-Fi clients never block the main network thread.

The companion client builds a proper MQTT CONNECT frame, sends it, then periodically emits small JSON telemetry messages. Both processes support deterministic, graceful shutdown on SIGINT and SIGTERM.

## Key Features

- Edge-Triggered epoll Reactor: Single-threaded I/O multiplexing with EPOLLET and EPOLLRDHUP for near-zero idle CPU usage (~0%).
- Non-Blocking Accept & Read Loops: Exhaustive EAGAIN drain loops ensure the reactor never stalls on slow connections.
- Zero-Allocation MQTT Parsing: Bounds-checked SafeMqttParser extracts client identifiers from CONNECT frames and dispatches CONNACK replies without buffer over-reads.
- Bidirectional Client Registry: Dual std::unordered_map indexing (fd ↔ device_id) provides O(1) lookups and automatically evicts ghost sessions on rapid device reconnects.
- Asynchronous Write Pipeline: Dynamic EPOLLOUT interest toggling prevents partial-write stalls and preserves strict message ordering.
- Static Worker Thread Pool: Sized to 2 × std::thread::hardware_concurrency() to absorb bursty I/O workloads without dynamic thread creation overhead.
- Bounded Queue with Back-Pressure: Capped at 10,000 packets with a drop-oldest eviction policy to prevent unbounded memory growth under load.
- RAII Memory Scrubbing: TelemetryPacket destructors automatically zero out payload buffers via std::memset upon scope exit—even if an exception is thrown.
- Modern C++23 Primitives: Uses std::jthread, std::expected, std::span, and std::atomic with explicit memory ordering for safe resource management and explicit error handling.
- Graceful Shutdown: Signal-driven (SIGINT/SIGTERM) atomic teardown drains queues and joins all worker threads cleanly.

## System Architecture

### Control Flow Overview

```
Bootstrap (main thread)
  install signal handlers
  spawn jthread worker pool
        │
        ▼
Reactor loop (main thread)
  while (g_running)
    epoll_wait(500 ms)
    for each ready event
      ├─ listen fd  → drain accept until EAGAIN
      ├─ EPOLLIN    → drain read until EAGAIN
      │                 parse CONNECT if new, else tag + push
      ├─ EPOLLOUT   → handle_write_ready
      └─ hang-up    → registry remove + close
        │
        ▼
Workers (background)
  while (g_running)
    packet = pipeline->pop()   // blocks on condition variable
    if (!packet) break
    process and scrub
        │
        ▼
Shutdown
  signal sets g_running = false
  pipeline->shutdown() + notify_all()
  jthread destructors join workers
  close listening socket and epoll fd
```

### 1. Reactor & Worker Pool Model

Instead of blocking one thread per client, one reactor thread monitors all file descriptors and hands off packet ownership (std::unique_ptr) to background workers.

```
MAIN THREAD (Reactor)
  g_running = true
  install SIGINT/SIGTERM handlers
  spawn std::jthread worker pool
  ┌─────────────────────────────────────────┐
  │ while (g_running)                       │
  │   epoll_wait(..., 500 ms)               │ ◄── ~0% CPU while waiting
  │   handle accept / read / write / hangups│
  └─────────────────────────────────────────┘
  g_running = false
  pipeline->shutdown() ──► notify_all()
  workers join automatically (std::jthread)

WORKER THREADS
  ┌─────────────────────────────────────────┐
  │ while (g_running)                       │
  │   packet = pipeline->pop()              │ ◄── Blocks on condition_variable
  │   if (!packet) break                    │ ◄── Woken by shutdown()
  │   process payload & scrub memory (RAII) │
  └─────────────────────────────────────────┘
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
  (drain)     (drain      (DEL + close)    │
              read +                       │
              MQTT parse,                  │
              push)                        │
                 │                         │
                 └─────────────────────────┘
```

1. Setup: Create a non-blocking TCP listening socket, initialize an epoll instance, and register the socket with EPOLLIN | EPOLLET.
2. Wait: epoll_wait() blocks until at least one registered file descriptor becomes ready or the 500 ms timeout expires.
3. Dispatch:
   - Listening Socket Ready: Drain accept() in a while (true) loop until EAGAIN. Configure each accepted client socket as non-blocking, register it with EPOLLIN | EPOLLET | EPOLLRDHUP, and create an AsyncWriteBuffer.
   - Socket Error / Hang-Up: Remove the descriptor from the registry and from epoll (EPOLL_CTL_DEL), then close it immediately.
   - Socket Readable (EPOLLIN): Drain read() completely until EAGAIN. If the fd is unregistered, attempt to parse an MQTT CONNECT, register the device id, and enqueue a CONNACK. Otherwise tag the packet with the known device id and push it to TelemetryPipeline.
   - Socket Writable (EPOLLOUT): Call handle_write_ready() to flush the outbound queue.
4. Loop: Return to epoll_wait().

### 3. Producer-Consumer Pipeline & Client Architecture

```
SERVER PIPELINE:
Reactor Thread                  Worker Threads
     │                               │
     │  read() + MQTT parse          │
     │  pipeline->push() ──────────► │  pop() (blocks until data or shutdown)
     │  (O(1) non-blocking)          │  process & scrub payload
     │                               │  loop

CLIENT ARCHITECTURE:
┌─────────────────────────────┐     ┌──────────────────────────────┐
│  Telemetry Worker Thread    │     │  Main Thread (Reactor)       │
│                             │     │                              │
│  Every 2 seconds:           │     │  epoll_wait(500 ms)          │
│    Build JSON metric        │────►│  EPOLLOUT ──► Flush writes   │
│    pipeline->send_payload() │     │  EPOLLIN  ──► Read CONNACK   │
│                             │     │  Hangup   ──► Trigger stop   │
└─────────────────────────────┘     └──────────────────────────────┘
                 │                                  │
                 └──── Shared ClientWritePipeline ──┘
                         (Non-blocking FIFO queue)
```

### 4. Asynchronous Write Pipeline

```
send_payload()
  ├─ Queue empty? ──► Attempt direct write()
  │     ├─ All bytes written ──► Done
  │     └─ Partial / EAGAIN  ──► Queue remainder + enable EPOLLOUT
  └─ Queue non-empty ──► Append to queue (preserves FIFO order)

handle_write_ready() [Triggered by EPOLLOUT]
  └─ While queue not empty:
        write() next chunk from offset
        ├─ Partial write ──► Update offset, yield (wait for next EPOLLOUT)
        └─ Full chunk    ──► Pop chunk, continue
  └─ Queue empty ──► Disable EPOLLOUT (prevents busy-looping)
```

## Project Structure

```
├── README.md     # Project documentation
├── LICENSE       # MIT License
├── deploy.sh     # Idempotent Linux dependency & build script
├── server.cpp    # Epoll reactor, MQTT parser, registry, write pipeline, and jthread worker pool
├── client.cpp    # Async MQTT client simulator
└── tests.cpp     # Deterministic unit tests for shutdown and packet helpers
```

## Quick Start

### Prerequisites

- Compiler: C++23-capable compiler (g++ 12+ or clang++ 16+)
- OS: Linux (Kernel 2.6.17+ for epoll and EPOLLRDHUP).
- Libraries: pthread

### Automated Setup & Build (deploy.sh)

deploy.sh is the single, idempotent entry point on Linux:

1. Inspects the system for g++ and make, installing only what is missing (build-essential on Debian/Ubuntu, gcc-c++ on Fedora/RHEL).
2. Compiles server, client, and tests with -std=c++23 -O2 -pthread.
3. Runs the test binary to verify the build.

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

Expected Output:
- Server: Logs device authentication followed by worker-thread payload processing.
- Client: Sends CONNECT, then prints periodic JSON telemetry every 2 seconds.

Press Ctrl-C in either terminal to trigger a graceful shutdown.

## C++23 Language Dialect & Contracts

The codebase targets C++23 and leverages standard-library facilities to enforce memory safety and clear ownership:

- std::jthread: Workers join automatically when the std::vector<std::jthread> is destroyed, eliminating manual join loops and preventing forgotten joins.
- std::expected<void, std::string>: Fallible setup helpers return explicit success or an explanatory error string without out-parameters or scattered errno checks.
- std::span<const uint8_t>: Provides non-owning, bounds-aware views over packet buffers instead of raw pointer-and-length pairs.
- std::atomic: Uses explicit memory orders on the stop flag to guarantee cross-core visibility without data races.

## Ownership & Memory Scrubbing Flow

1. Reactor allocates std::unique_ptr<TelemetryPacket>.
2. Reactor fills the payload, performs MQTT parsing if needed, and moves ownership into TelemetryPipeline::push().
3. Worker thread receives exclusive ownership from TelemetryPipeline::pop().
4. On scope exit, ~TelemetryPacket() zeros the entire raw buffer via std::memset before memory returns to the allocator—guaranteeing sensitive bytes are cleared even if an exception is thrown.
5. std::jthread destructor automatically joins the worker on shutdown.

## Edge-Triggered & Back-Pressure Contracts

- Edge-Triggered Drain Contract: Because sockets are registered with EPOLLET, every ready file descriptor must be drained until EAGAIN. Both accept() and read() are wrapped in while (true) loops that exit only on EAGAIN/EWOULDBLOCK or a hard error; failing to drain would silently lose subsequent data until the next edge transition.
- Back-Pressure Policy: TelemetryPipeline is capped at 10,000 packets. When full, the oldest packet is dropped before inserting the newest sample—prioritizing fresh telemetry values while bounding memory without stalling the reactor.
- Queue Wake-Up Policy: push() calls notify_one() to wake a single worker and minimize context switches; shutdown() calls notify_all() to unblock all waiting threads simultaneously.
- Write-Pipeline Ordering: Partial writes resume from a saved offset; new data is always appended so FIFO order is preserved. EPOLLOUT is enabled only while data remains queued.

## Detailed Shutdown Sequence

The 500 ms epoll_wait timeout caps shutdown latency at half a second while avoiding the overhead of an extra eventfd:

1. SIGINT/SIGTERM handler executes an async-signal-safe store (g_running = false).
2. Next epoll_wait returns (via timeout or EINTR).
3. Reactor loop condition evaluates to false and exits.
4. pipeline->shutdown() sets the internal stop flag and broadcasts notify_all().
5. Workers blocked in pop() wake, receive nullptr, and exit their loops.
6. std::vector<std::jthread> destruction automatically joins every worker.
7. Listening socket and epoll file descriptor are closed.

## Testing, Troubleshooting & Production Hardening

### Unit Testing (tests.cpp)

tests.cpp validates the deterministic stop-flag contract, worker thread teardown, and basic packet-construction helpers without requiring live network sockets.

### Troubleshooting

| Symptom | Likely Cause & Fix |
|---------|--------------------|
| Address already in use | Another process holds port 8883. Stop the conflicting process or update SERVER_PORT. |
| Client cannot connect | Confirm ./server is running and local firewall rules allow TCP traffic on port 8883. |
| High idle CPU usage | Ensure epoll_wait uses a non-zero timeout (500 ms) and that accept()/read() loops exit cleanly on EAGAIN. |
| No CONNACK | Verify the CONNECT frame is well-formed and that the write pipeline is flushing on EPOLLOUT. |

### Scaling & Production Hardening

- Horizontal Scaling: Run multiple server processes behind a load balancer (SO_REUSEPORT), or adopt a multi-reactor design (one epoll instance per core) for extreme connection rates.
- Security & Observability: Wrap sockets in TLS, add device authentication beyond the client identifier, redirect stdout/stderr to a logging daemon, and expose atomic metrics for accepted connections, dropped packets and queue depth.
- Ghost Session Handling: The registry automatically evicts a previous file descriptor when the same device id re-registers, preventing stale routes after rapid reconnects.

## Scripts

deploy.sh checks for g++ and make, installs any missing packages, builds the three binaries with -std=c++23 -O2 -pthread, and runs the tests.

Example:

```bash
./deploy.sh
```

## Further Operational Notes

The server listens on TCP port 8883. A failed bind almost always indicates that another process already owns the port; ss -tulpn | grep 8883 identifies the occupant. The client accepts a device name and an optional server address.

All diagnostic output appears on standard output and standard error. In production these streams should be captured by the system logging facility. The reactor thread performs no heavy work; it only accepts, reads, parses the minimal CONNECT header, enqueues, and flushes the write pipeline.

The worker count is computed as twice the hardware concurrency reported by the standard library, with a fallback when the query returns zero. This sizing provides enough concurrency to absorb short processing bursts while remaining modest for a pure I/O-bound workload.

SafeMqttParser performs bounds-checked remaining-length decoding before extracting the client-identifier string, so a malformed CONNECT cannot overrun the receive buffer. The fixed CONNACK indicates connection success and is sufficient for the teaching client.

## License

This project is licensed under the [MIT License](LICENSE). Copyright (c) 2026 Damian Andres Ulanowicz.
