#pragma once
#include "futu_quote/FutuQuoteSession.hpp"
#include <FTSPI.h>
#include <FTAPIChannel_Define.h>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <optional>

#include "SubscriptionTools.hpp"
#include "Subscription.hpp"
#include "common/Envelope.hpp"
#include "common/SPSCQueue.hpp"
#include "sinks/Sink.hpp"

using SubscriptionLoadFn = std::function<std::expected<SubscribeConfig, std::string>(const std::string&)>;
using SteadyTP = std::chrono::steady_clock::time_point;

class SubscriptionManager {
  public:
    struct Options {
        std::string subscription_path;
        std::chrono::milliseconds refresh_interval{5000};
        std::size_t capacity{32768};
        queue::OverflowPolicy overflow{queue::OverflowPolicy::Block};
    };

    // -----------------------------------------------------------------------------
    // Warmup gate (restart-safe-by-behavior, no persistent state)
    //
    // Problem we address:
    // - After (re)subscribe, OpenD often immediately pushes a snapshot which is
    //   frequently the "last" cached value (duplicate) even outside trading hours.
    // - We don't want that single snapshot to look like "new data" during deploy.
    //
    // Strategy (per topic+key):
    // - After we (re)apply subscriptions, open a short warmup window (default 1s).
    // - During warmup, buffer all messages instead of forwarding.
    // - When window ends:
    //     * if buffered count == 1: DROP it (snapshot-only, typical deploy/restart)
    //     * if buffered count >= 2: FORWARD ALL buffered messages (no data loss)
    // - After warmup ends, forward messages normally.
    //
    // This is intentionally lightweight: no protobuf decode, no heavy hashing,
    // no RocksDB, no extra processes.
    // -----------------------------------------------------------------------------
    struct WarmupSlot {
        SteadyTP until{};
        bool active{false};
        std::vector<Envelope> buf; // keep all during warmup to avoid data loss
    };

    SubscriptionManager(Sink sink,
        SubscriptionLoadFn cfgloader,
        Options opts);

    void bind_session(FutuQuoteSession* s) { session_ = s; };
    
    void on_connected(Futu::i64_t err, const char* desc);
    void on_disconnected(Futu::i64_t err);
    void on_sub_reply(Futu::u32_t nSerialNo, const Qot_Sub::Response &stRsp);
    
    void on_push_basicqot(const Qot_UpdateBasicQot::Response &stRsp);
    void on_push_orderbook(const Qot_UpdateOrderBook::Response &stRsp);
    void on_push_ticker(const Qot_UpdateTicker::Response &stRsp);
    void on_push_kl(const Qot_UpdateKL::Response &stRsp);
    void on_push_rt(const Qot_UpdateRT::Response &stRsp);
    void on_push_broker(const Qot_UpdateBroker::Response &stRsp);
    
    void run(std::atomic<bool>& stop);

    void force_resubscribe() { force_resubscribe_ = true; }

private:
    bool should_check_file();
    std::expected<bool, std::string> load_if_changed();

    void apply_pending();

    void do_full_resubscribe(const SubState& target); // called when force_resubscribe
    void execute_diff(const SubState& current,
                  const SubState& pending);

    std::expected<void, std::string> call_subscribe(const SecurityId& id, SubMask mask);
    std::expected<void, std::string> call_unsubscribe(const SecurityId& id, SubMask mask);

    Futu::u32_t subscribe_api(const SecurityId& id, const std::vector<Qot_Common::SubType>& subs);
    Futu::u32_t unsubscribe_api(const SecurityId& id, const std::vector<Qot_Common::SubType>& subs);

    static int64_t now_ms();
    bool enqueue(Envelope&& e);

private:
    Sink sink_;
    FutuQuoteSession* session_{nullptr};
    queue::SPSCQueue<Envelope> inbox_{1u << 16};

    SubscriptionLoadFn cfgloader_{nullptr};
    Options opts_;

    std::atomic<bool> connected_{false};
    std::atomic<bool> force_resubscribe_{false}; // used when init & reconnect

    std::optional<std::chrono::steady_clock::time_point> last_check_{};
    std::filesystem::file_time_type last_mtime_{};

    SubState current_state_{};
    SubState pending_state_{};
    bool has_current_{false}; // used when reconnect
    bool has_pending_{false};
};
