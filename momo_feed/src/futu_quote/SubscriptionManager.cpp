#include "futu_quote/SubscriptionManager.hpp"
#include <FTAPI.h>
#include <FTSPI.h>
#include <Proto/Qot_Common.pb.h>
#include <Proto/Qot_Sub.pb.h>
#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <utility>

#include "common/JsonLogger.hpp"

static std::size_t next_pow2(std::size_t v) {
    if (v < 2) return 2;
    std::size_t p = 1;
    while (p < v) p <<= 1;
    return p;
}

static std::string market_prefix(int market) {
    // Prefer human-friendly keys like HK.00700 / US.AAPL / SH.600519.
    const std::string name = Qot_Common::QotMarket_Name(
        static_cast<Qot_Common::QotMarket>(market)
    );
    constexpr std::string_view prefix = "QotMarket_";
    constexpr std::string_view suffix = "_Security";

    if (!name.starts_with(prefix) || !name.ends_with(suffix)) {
        return name; //fallback
    }
    const auto begin = prefix.size();
    const auto len   = name.size() - prefix.size() - suffix.size();

    if (len == 0) {
        return name; //fallback
    }

    return name.substr(begin, len);
}

static std::string make_key(int market, const std::string& code) {
    return market_prefix(market) + "." + code;
}

SubscriptionManager::SubscriptionManager(Sink sink, SubscriptionLoadFn cfgloader, Options opts)
    : sink_(sink)
    , inbox_(next_pow2(opts.capacity))
    , cfgloader_(cfgloader)
    , opts_(opts)
{}

void SubscriptionManager::on_connected(Futu::i64_t err, const char *desc)
{
    logger::warn("subman", "connected",
                 {logger::num("err", err), logger::field("desc", desc ? desc : "")});
    connected_.store(err == 0);
    if (err == 0) force_resubscribe_.store(true, std::memory_order_release);
}

void SubscriptionManager::on_disconnected(Futu::i64_t err)
{
    connected_.store(false, std::memory_order_release);
    logger::warn("subman", "disconnected", {logger::num("err", err)});
}

void SubscriptionManager::on_sub_reply(Futu::u32_t nSerialNo, const Qot_Sub::Response &stRsp)
{
    logger::info("subman", "sub_reply",
             {logger::num("serial", nSerialNo),
              logger::num("retType", stRsp.rettype()),
              logger::field("retMsg", stRsp.retmsg())});
}

void SubscriptionManager::on_push_basicqot(const Qot_UpdateBasicQot::Response &stRsp)
{
    const Qot_UpdateBasicQot::S2C &pbS2C = stRsp.s2c();
    const int list_size = pbS2C.basicqotlist_size();
    if (list_size <= 0) return;

    for (int i = 0; i < list_size; ++i) {
        const auto& item = pbS2C.basicqotlist(i);
        const auto& sec = item.security();

        Qot_UpdateBasicQot::Response single_rsp;
        single_rsp.set_rettype(stRsp.rettype());
        single_rsp.set_retmsg(stRsp.retmsg());
        single_rsp.mutable_s2c()->CopyFrom(stRsp.s2c());
        single_rsp.mutable_s2c()->clear_basicqotlist();
        single_rsp.mutable_s2c()->add_basicqotlist()->CopyFrom(item);

        Envelope e;
        e.topic = "futu.basicqot.pb";
        e.key = make_key(sec.market(), sec.code());
        e.ts_ns = now_ns();
        e.payload.resize(static_cast<size_t>(single_rsp.ByteSizeLong()));
        if (!single_rsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
            continue;
        }
        enqueue(std::move(e));
    }
}
void SubscriptionManager::on_push_orderbook(const Qot_UpdateOrderBook::Response &stRsp)
{
    Envelope e;
    e.topic = "futu.orderbook.pb";
    const auto& sec = stRsp.s2c().security();
    e.key = make_key(sec.market(), sec.code());
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    enqueue(std::move(e));
}
void SubscriptionManager::on_push_ticker(const Qot_UpdateTicker::Response &stRsp)
{
    Envelope e;
    e.topic = "futu.ticker.pb";
    const auto& sec = stRsp.s2c().security();
    e.key = make_key(sec.market(), sec.code());
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    enqueue(std::move(e));
}
void SubscriptionManager::on_push_kl(const Qot_UpdateKL::Response &stRsp)
{
    Envelope e;
    e.topic = "futu.kl1min.pb";
    const auto& sec = stRsp.s2c().security();
    e.key = make_key(sec.market(), sec.code());
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    enqueue(std::move(e));
}
void SubscriptionManager::on_push_rt(const Qot_UpdateRT::Response &stRsp)
{
    Envelope e;
    e.topic = "futu.rt.pb";
    const auto& sec = stRsp.s2c().security();
    e.key = make_key(sec.market(), sec.code());
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    enqueue(std::move(e));
}
void SubscriptionManager::on_push_broker(const Qot_UpdateBroker::Response &stRsp)
{
    Envelope e;
    e.topic = "futu.broker.pb";
    const auto& sec = stRsp.s2c().security();
    e.key = make_key(sec.market(), sec.code());
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    enqueue(std::move(e));
}

void SubscriptionManager::run(std::atomic<bool> &stop) {
    std::vector<Envelope> batch;
    batch.reserve(1024);

    while (!stop.load(std::memory_order_relaxed)) {
        if (session_) {
            session_->ensure_connected();
        }

        // --------- subscription control plane ----------
        const bool connected = connected_.load(std::memory_order_acquire) && session_;

        // 1) Decide whether we should attempt a (re)load this loop.
        const bool want_force = force_resubscribe_.load(std::memory_order_acquire);
        const bool want_poll  = should_check_file();

        if (connected && (want_force || want_poll)) {
            logger::info("subman", "control_cycle",
                         {logger::b("want_force", want_force),
                          logger::b("want_poll", want_poll)});
            // 2) Try load config -> produce pending_state_ if changed
            const auto changed = load_if_changed();
            if (!changed) {
                logger::error("subman", "load_failed",
              {logger::field("error", changed.error())});
            }

            const bool changed_value = changed.value_or(false);
            if (!changed_value && want_force && has_current_ && !has_pending_) {
                pending_state_ = current_state_;
                has_pending_ = true;
                logger::info("subman", "force_resubscribe_current_state");
            }

            // 3) If we have pending, apply it (diff or full)
            if (has_pending_) {
                apply_pending();
            }

            // 4) Only clear force after we've at least attempted a connected-cycle load.
            //    (Optionally: clear only when apply succeeded; here apply_pending clears pending itself)
            if (want_force) {
                // if you want stricter semantics, only clear when has_current_ becomes true
                if (has_current_) {
                    force_resubscribe_.store(false, std::memory_order_release);
                }
            }
        }

        // --------- data plane (push -> sink) ----------
        batch.clear();
        auto n = inbox_.pop_many(batch, 1024);

        if (n == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }

        if (sink_.submit) {
            for (auto& e : batch) {
                sink_.submit(sink_.ctx, std::move(e));
            }
        }
    }

    if (sink_.flush) sink_.flush(sink_.ctx, 3000);
}

bool SubscriptionManager::should_check_file() {
    const auto now = std::chrono::steady_clock::now();
    if (!last_check_ || (now - *last_check_) >= opts_.refresh_interval) {
        last_check_ = now;
        return true;
    }
    return false;
}
std::expected<bool, std::string> SubscriptionManager::load_if_changed() {
    if (!cfgloader_) {
        return std::unexpected("no config loader set");
    }
    std::error_code ec;
    auto mtime = std::filesystem::last_write_time(opts_.subscription_path,  ec);
    if (ec) {
        return std::unexpected("last_write_time failed: " + ec.message());
    }

    if (has_current_ && mtime == last_mtime_) return false;

    logger::info("subman", "load_mtime_ok",
                 {logger::field("path", opts_.subscription_path)});

    auto cfg_result = cfgloader_(opts_.subscription_path);
    if (!cfg_result) {
        return std::unexpected(cfg_result.error());
    }
    const SubscribeConfig& cfg = *cfg_result;
    logger::info("subman", "load_cfg",
             {logger::num("securities", cfg.securities.size()),
              logger::num("default_subtypes", cfg.default_subtypes.size())});

    SubState st = canonicalize(cfg);
    logger::info("subman", "load_canonical",
                 {logger::num("count", st.size())});

    if (has_current_ && st_equal(current_state_, st)) {
        last_mtime_ = mtime;
        return false;
    }

    pending_state_ = std::move(st);
    has_pending_ = true;
    last_mtime_ = mtime;
    logger::info("subman", "load_pending_ready");
    return true;
}

void SubscriptionManager::apply_pending() {
    logger::info("subman", "apply_pending",
             {logger::b("has_pending", has_pending_),
              logger::num("target_n", pending_state_.size())});

    // call bind session in main
    if (!has_pending_) return;

    if (force_resubscribe_.exchange(false, std::memory_order_acq_rel)) {
        do_full_resubscribe(pending_state_);
    } else {
        if (has_current_) execute_diff(current_state_, pending_state_);
        else do_full_resubscribe(pending_state_);
    }

    current_state_ = pending_state_;
    has_current_ = true;
    has_pending_ = false;
}

void SubscriptionManager::do_full_resubscribe(const SubState& target) {
    if (has_current_) {
        for (const auto&[id, mask] : current_state_) {
            auto res = call_unsubscribe(id, mask);
            if (!res) {
                logger::error("subman", "unsubscribe_failed",
                              {logger::field("code", id.code),
                               logger::field("error", res.error())});
            }
        }
    }
    for (const auto&[id, mask] : target) {
        auto res = call_subscribe(id, mask);
        if (!res) {
            logger::error("subman", "subscribe_failed",
                          {logger::field("code", id.code),
                           logger::field("error", res.error())});
        }
    }
}
void SubscriptionManager::execute_diff(const SubState& current, const SubState& pending) {
    SubscriptionTools stdiff = diff_state(current, pending);
    for (const auto&[id, mask] : stdiff.remove_security) {
        auto res = call_unsubscribe(id, mask);
        if (!res) {
            logger::error("subman", "unsubscribe_failed",
                          {logger::field("code", id.code),
                           logger::field("error", res.error())});
        }
    }
    for (const auto&[id, mask] : stdiff.del_subtypes) {
        auto res = call_unsubscribe(id, mask);
        if (!res) {
            logger::error("subman", "unsubscribe_failed",
                          {logger::field("code", id.code),
                           logger::field("error", res.error())});
        }
    }

    for (const auto&[id, mask] : stdiff.add_subtypes) {
        auto res = call_subscribe(id, mask);
        if (!res) {
            logger::error("subman", "subscribe_failed",
                          {logger::field("code", id.code),
                           logger::field("error", res.error())});
        }
    }
    for (const auto&[id, mask] : stdiff.add_security) {
        auto res = call_subscribe(id, mask);
        if (!res) {
            logger::error("subman", "subscribe_failed",
                          {logger::field("code", id.code),
                           logger::field("error", res.error())});
        }
    }
}

std::expected<void, std::string> SubscriptionManager::call_subscribe(const SecurityId& id, SubMask mask) {
    if (!session_) {
        return std::unexpected("session not bound");
    }
    std::vector<Qot_Common::SubType> subtypes;
    mask_to_vec(mask, subtypes);

    auto sn = subscribe_api(id, subtypes);
    if (sn == 0) {
        return std::unexpected("subscribe failed for " + id.code);
    }
    return {};
}
std::expected<void, std::string> SubscriptionManager::call_unsubscribe(const SecurityId& id, SubMask mask) {
    if (!session_) {
        return std::unexpected("session not bound");
    }
    std::vector<Qot_Common::SubType> unsubtypes;
    mask_to_vec(mask, unsubtypes);

    auto sn = unsubscribe_api(id, unsubtypes);
    if (sn == 0) {
        return std::unexpected("unsubscribe failed for " + id.code);
    }
    return {};
}

Futu::u32_t SubscriptionManager::subscribe_api(const SecurityId& id, const std::vector<Qot_Common::SubType>& subs) {
    logger::info("subman", "subscribe",
                 {logger::field("code", id.code),
                  logger::num("subtypes", subs.size())});
    Qot_Sub::Request pbSub;
    Qot_Sub::C2S *pSubC2S = pbSub.mutable_c2s();

    Qot_Common::Security *pSubSec = pSubC2S->add_securitylist();
    pSubSec->set_code(id.code);
    pSubSec->set_market(id.market);

    for (auto &sub : subs)
    {
        pSubC2S->add_subtypelist(sub);
    }

    pSubC2S->set_issuborunsub(true);
    pSubC2S->set_isregorunregpush(true);
    auto sn = session_->sub(pbSub);
    logger::info("subman", "subscribe_sent",
                 {logger::num("serial", sn),
                  logger::field("code", id.code),
                  logger::num("subtypes", subs.size())});
    return sn;
}
Futu::u32_t SubscriptionManager::unsubscribe_api(const SecurityId& id, const std::vector<Qot_Common::SubType>& subs) {
    logger::info("subman", "unsubscribe",
             {logger::field("code", id.code),
              logger::num("subtypes", subs.size())});
    Qot_Sub::Request pbSub;
    Qot_Sub::C2S *pSubC2S = pbSub.mutable_c2s();

    Qot_Common::Security *pSubSec = pSubC2S->add_securitylist();
    pSubSec->set_code(id.code);
    pSubSec->set_market(id.market);

    for (auto &sub : subs)
    {
        pSubC2S->add_subtypelist(sub);
    }

    pSubC2S->set_issuborunsub(false);
    pSubC2S->set_isregorunregpush(false);
    auto sn = session_->sub(pbSub);
    logger::info("subman", "unsubscribe_sent",
                 {logger::num("serial", sn),
                  logger::field("code", id.code),
                  logger::num("subtyps", subs.size())});
    return sn;
}

int64_t SubscriptionManager::now_ns() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count();
}

bool SubscriptionManager::enqueue(Envelope&& e) {
    if (inbox_.try_push(std::move(e))) {
        return true;
    }
    if (opts_.overflow == queue::OverflowPolicy::DropOldest) {
        Envelope dropped;
        (void)inbox_.try_pop(dropped);
        return inbox_.try_push(std::move(e));
    }
    while (!inbox_.try_push(std::move(e))) {
        std::this_thread::yield();
    }
    return true;
}