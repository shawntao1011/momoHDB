
#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>
#include "futu_core/FutuQuoteSession.hpp"
#include "futu_core/SubscriptionManager.hpp"
#include "config/SubscriptionLoader.hpp"
#include "config/ConfigLoader.hpp"
#include "runtime/Dispatcher.hpp"
#include "sinks/DemoLogSink.hpp"
#include "sinks/RedPandaSink.hpp"

static std::atomic<bool> g_stop{false};
static void on_sigint(int) { g_stop.store(true, std::memory_order_relaxed); }

static queue::OverflowPolicy to_queue_overflow(cfg::OverflowPolicy p) {
    switch (p) {
        case cfg::OverflowPolicy::Block: return queue::OverflowPolicy::Block;
        case cfg::OverflowPolicy::DropOldest: return queue::OverflowPolicy::DropOldest;
    }
    std::abort();
}

int main (int argc, char *argv[]) {
    std::signal(SIGINT, on_sigint);

    namespace fs = std::filesystem;
    fs::path exe_dir = fs::canonical("/proc/self/exe").parent_path();
    fs::path default_cfg =
        (exe_dir / "../../../../config/apps.default.yaml")
        .lexically_normal();
    std::string config_path = (argc > 1) ? argv[1] : default_cfg.string();
    if (!std::filesystem::exists(config_path)) {
        std::cerr << "[FATAL] config_not_found: " << config_path << "\n";
        return 1;
    }

    auto cfg_res = cfg::ConfigLoader::load(config_path);
    if (!cfg_res) {
        std::cerr << "[FATAL] " << to_string(cfg_res.error()) << "\n";
        return 1;
    }
    auto& cfg = cfg_res.value();

    // logger
    logger::init(cfg.logger);

    Dispatcher dispatcher;

    for (const auto& ds : cfg.downstreams) {
        if (std::holds_alternative<cfg::RedpandaSinkCfg>(ds.sink)) {
            const auto& rc = std::get<cfg::RedpandaSinkCfg>(ds.sink);

            RedPandaSink::Options opts;
            opts.brokers = rc.brokers;
            opts.acks = rc.acks;

            dispatcher.add_downstream(
                ds.name,
                std::make_unique<RedPandaSink>(ds.name, opts));
        }
    }

    Sink sink{
        .ctx = &dispatcher,
        .submit = &Dispatcher::submit,
        .flush = &Dispatcher::flush
    };

    cfg::SubscriptionLoader subsloader;

    SubscriptionManager subman(sink,
        [&](const std::string& path) { return subsloader.load(path); },
        SubscriptionManager::Options
        {
            cfg.subscription.path,
            std::chrono::milliseconds{cfg.submanager.refresh_ms},
            cfg.submanager.capacity,
            to_queue_overflow(cfg.submanager.overflow)
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
        auto start_result = session.start(cfg.futu.opend_ip.c_str(), cfg.futu.opend_port);
        if (!start_result) {
            logger::error("main", "session_start_failed",
              {logger::field("error", start_result.error())});
            g_stop.store(true, std::memory_order_relaxed);
        }
        subman.run(g_stop);
        session.stop();
    });

    while (!g_stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    t.join();

    return 0;
}
