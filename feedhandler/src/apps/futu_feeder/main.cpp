
#include <atomic>
#include <chrono>
#include <csignal>
#include <thread>
#include "futu_core/FutuQuoteSession.hpp"
#include "futu_core/SubscriptionManager.hpp"
#include "../../../include/config/YamlSubscribeConfigLoader.hpp"
#include "runtime/Dispatcher.hpp"
#include "sinks/DemoLogSink.hpp"

static std::atomic<bool> g_stop{false};

static void on_sigint(int) { g_stop.store(true, std::memory_order_relaxed); }

int main (int argc, char *argv[]) {
    std::signal(SIGINT, on_sigint);

    auto opt = logger::default_options_for("futu_feeder");
    logger::init(opt);

    std::vector<std::unique_ptr<QueuedDownstream>> downstreams;
    downstreams.emplace_back(
        std::make_unique<QueuedDownstream>(
        "redpanda-prod",
        std::make_unique<DemoLogSink>("redpanda-prod"),
        8192   // capacity, DROP_OLDEST
        )
    );
    Dispatcher dispatcher(std::move(downstreams));

    Sink sink;
    sink.ctx = &dispatcher;
    sink.submit = &Dispatcher::submit;
    sink.flush = &Dispatcher::flush;

    namespace fs = std::filesystem;
    fs::path exe_dir = fs::canonical("/proc/self/exe").parent_path();
    fs::path default_cfg =
        (exe_dir / "../../../../config/subscriptions.example.yaml")
        .lexically_normal();
    std::string config_path = (argc > 1) ? argv[1] : default_cfg.string();
    if (!std::filesystem::exists(config_path)) {
        logger::error("main", "config_not_found",
                      {logger::field("path", config_path)});
        std::exit(1);
    }

    YamlSubscribeConfigLoader cfgloader;

    SubscriptionManager subman(sink,
    [&](const std::string& path) { return cfgloader.load(path); },
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
        auto start_result = session.start("127.0.0.1", 11111);
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
