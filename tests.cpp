#include <iostream>
#include <atomic>
#include <thread>
#include <chrono>
#include <string>

std::atomic<bool> g_running{true};

int passed = 0, failed = 0;
#define CHECK(name, cond) do { if (cond) { std::cout << "[PASS] " << name << "\n"; ++passed; } else { std::cout << "[FAIL] " << name << "\n"; ++failed; } } while(0)

int main() {
    CHECK("flag starts true", g_running.load());
    g_running.store(false);
    CHECK("flag can be set false", !g_running.load());

    g_running.store(true);
    bool exited = false;
    std::thread t([&]() {
        while (g_running.load(std::memory_order_relaxed))
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        exited = true;
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    g_running.store(false);
    t.join();
    CHECK("worker exits on flag", exited);

    std::string sample = "0,24.5,55\n";
    CHECK("payload format", sample.find(',') != std::string::npos && sample.back() == '\n');

    std::cout << "passed=" << passed << " failed=" << failed << "\n";
    return failed ? 1 : 0;
}
