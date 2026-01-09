
#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>
#include "futu_core/FutuQuoteSession.hpp"
#include "futu_core/SubscrbeManager.hpp"

static std::atomic<bool> g_stop{false};

static void on_sigint(int) { g_stop.store(true); }

int main (int argc, char *argv[]) {
    std::signal(SIGINT, on_sigint);

    FutuQuoteSession sess;
    SubscribeManager subManager(sess);
    sess.SetSubscribeManager(&subManager);

    /*
    if(!sess.Start()) {
        std::cerr << "Session Start failed.\n";
        return 1;
    }
    */
    
    std::thread t(&FutuQuoteSession::Run, &sess);

    while (!g_stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    
    t.join();
    return 0;
}
