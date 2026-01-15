#pragma once
#include "futu_core/FutuQuoteSession.hpp"
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
#include "common/SPSCRing.hpp"

struct Sink {
    void* ctx{};
    void (*submit)(void*, Envelope&&) = nullptr;
    void (*flush)(void*, int) = nullptr;
};

using ConfigLoaderFn = std::function<SubscribeConfig(const std::string&)>;

class SubscriptionManager {
  public:
    struct Options {
        std::string config_path;
        std::chrono::milliseconds refresh_interval{5000};
    };

    SubscriptionManager(Sink sink,
        ConfigLoaderFn cfgloader,
        Options opts);

    void bind_session(FutuQuoteSession* s) { session_ = s; };
    
    void on_connected(Futu::i64_t err, const char* desc);
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
    bool load_if_chanegd();

    void apply_pending();

    void do_full_resubscribe(const SubState& target); // called when force_resubscribe
    void execute_diff(const SubState& current,
                  const SubState& pending);

    bool call_subscribe(const SecurityId& id, SubMask mask);
    bool call_unsubscribe(const SecurityId& id, SubMask mask);

    Futu::u32_t subscribe_api(const SecurityId& id, const std::vector<Qot_Common::SubType>& subs);
    Futu::u32_t unsubscribe_api(const SecurityId& id, const std::vector<Qot_Common::SubType>& subs);

    static int64_t now_ns();

private:
    Sink sink_;
    FutuQuoteSession* session_{nullptr};
    SPSCRing<Envelope> inbox_{1u << 16};

    ConfigLoaderFn cfgloader_{nullptr};
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
