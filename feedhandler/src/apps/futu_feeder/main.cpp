
#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>
#include "futu_core/FutuQuoteSession.hpp"
#include "futu_core/SubscriptionManager.hpp"
#include "futu_core/YamlSubscribeConfigLoader.hpp"

static std::atomic<bool> g_stop{false};

static void on_sigint(int) { g_stop.store(true, std::memory_order_relaxed); }

struct DemoPublisher {
    static void submit(void*, Envelope&& e) {
        std::cout << "[demopublish] topic=" << e.topic
                << " key=" << e.key
                << " size=" << e.payload.size() << "\n";
    }
    
    static void flush(void*, int) {}
};

int main (int argc, char *argv[]) {
    std::signal(SIGINT, on_sigint);

    Sink sink;
    sink.ctx = nullptr;
    sink.submit = &DemoPublisher::submit;
    sink.flush = &DemoPublisher::flush;

    std::string config_path =
        argc > 1 ? argv[1] : "feedhandler/config/subscribe.example.yaml";

    YamlSubscribeConfigLoader cfgloader;

    SubscriptionManager subman(sink,
    [&](const std::string& path) { return cfgloader.load(config_path); },
        SubscriptionManager::Options
        {
            config_path,
            std::chrono::milliseconds(5000)
        });
   
    SessionCallbacks cbs;
    cbs.ctx = &subman;
    cbs.on_connected = [](void* ctx, int64_t e, const char* d){
        static_cast<SubscriptionManager*>(ctx)->on_connected(e, d);
    };
    cbs.on_sub_reply  = [](void* ctx, int32_t s, const Qot_Sub::Response& r){
        static_cast<SubscriptionManager*>(ctx)->on_sub_reply(s, r);
    };

    cbs.on_push_basicqot = [](void* ctx, const Qot_UpdateBasicQot::Response& r){
        static_cast<SubscriptionManager*>(ctx)->on_push_basicqot(r);
    };
    cbs.on_push_orderbook = [](void* ctx, const Qot_UpdateOrderBook::Response& r){
        static_cast<SubscriptionManager*>(ctx)->on_push_orderbook(r);
    };
    cbs.on_push_ticker = [](void* ctx, const Qot_UpdateTicker::Response& r){
        static_cast<SubscriptionManager*>(ctx)->on_push_ticker(r);
    };
    cbs.on_push_kl = [](void* ctx, const Qot_UpdateKL::Response& r){
        static_cast<SubscriptionManager*>(ctx)->on_push_kl(r);
    };
    cbs.on_push_rt = [](void* ctx, const Qot_UpdateRT::Response& r){
        static_cast<SubscriptionManager*>(ctx)->on_push_rt(r);
    };
    cbs.on_push_broker = [](void* ctx, const Qot_UpdateBroker::Response& r){
        static_cast<SubscriptionManager*>(ctx)->on_push_broker(r);
    };
    FutuQuoteSession session(cbs);
    subman.bind_session(&session);

    std::thread t([&]{
        session.start("127.0.0.1", 11111);
        subman.run(g_stop);
        session.stop();
    });

    while (!g_stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    t.join();
    return 0;
}
