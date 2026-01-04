
#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>
#include "futu_core/session.hpp"

static std::atomic<bool> g_stop{false};

static void on_sigint(int) { g_stop.store(true); }

int main (int argc, char *argv[]) {
    std::signal(SIGINT, on_sigint);

    FutuSessionConfig cfg;
    FutuQuoteSession sess(cfg);

    if(!sess.Start()) {
        std::cerr << "Session Start failed.\n";
        return 1;
    }
    
    while (!g_stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    sess.Stop();
    return 0;
}
