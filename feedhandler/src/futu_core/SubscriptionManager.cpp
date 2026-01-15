#include "futu_core/SubscriptionManager.hpp"
#include <FTAPI.h>
#include <FTSPI.h>
#include <Proto/Qot_Common.pb.h>
#include <Proto/Qot_Sub.pb.h>
#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <utility>

SubscriptionManager::SubscriptionManager(Sink sink, ConfigLoaderFn cfgloader, Options opts)
    : sink_(sink)
    , cfgloader_(cfgloader)
    , opts_(opts)
{}

void SubscriptionManager::on_connected(Futu::i64_t err, const char *desc)
{
    std::cout << "[subman] connected err=" << err
              << " desc=" << (desc ? desc : "") << "\n";
    connected_.store(err == 0);
    if (err == 0) force_resubscribe_.store(true, std::memory_order_release);
}
void SubscriptionManager::on_sub_reply(Futu::u32_t nSerialNo, const Qot_Sub::Response &stRsp)
{
    std::cout << "[subman] sub reply serial=" << nSerialNo
              << " retType=" << stRsp.rettype() << " retMsg=" << stRsp.retmsg()
              << "\n";
}

void SubscriptionManager::on_push_basicqot(const Qot_UpdateBasicQot::Response &stRsp)
{
    const Qot_UpdateBasicQot::S2C &pbS2C = stRsp.s2c();
    if (pbS2C.basicqotlist_size() <= 0) return;;

    Envelope e;
    e.topic = "futu.basicqot.pb";
    e.key = stRsp.s2c().basicqotlist(0).security().code();
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    inbox_.try_push(std::move(e));
}
void SubscriptionManager::on_push_orderbook(const Qot_UpdateOrderBook::Response &stRsp)
{
    Envelope e;
    e.topic = "futu.orderbook.pb";
    e.key = stRsp.s2c().security().code();
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    inbox_.try_push(std::move(e));
}
void SubscriptionManager::on_push_ticker(const Qot_UpdateTicker::Response &stRsp)
{
    Envelope e;
    e.topic = "futu.ticker.pb";
    e.key = stRsp.s2c().security().code();
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    inbox_.try_push(std::move(e));
}
void SubscriptionManager::on_push_kl(const Qot_UpdateKL::Response &stRsp)
{
    Envelope e;
    e.topic = "futu.kl_1m.pb";
    e.key = stRsp.s2c().security().code();
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    inbox_.try_push(std::move(e));
}
void SubscriptionManager::on_push_rt(const Qot_UpdateRT::Response &stRsp)
{
    Envelope e;
    e.topic = "futu.rt.pb";
    e.key = stRsp.s2c().security().code();
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    inbox_.try_push(std::move(e));
}
void SubscriptionManager::on_push_broker(const Qot_UpdateBroker::Response &stRsp)
{
    Envelope e;
    e.topic = "futu.broker.pb";
    e.key = stRsp.s2c().security().code();
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    inbox_.try_push(std::move(e));
}

void SubscriptionManager::run(std::atomic<bool> &stop) {
    std::vector<Envelope> batch;
    batch.reserve(1024);

    while (!stop.load(std::memory_order_relaxed)) {

        // --------- subscription control plane ----------
        const bool connected = connected_.load(std::memory_order_acquire) && session_;

        // 1) Decide whether we should attempt a (re)load this loop.
        const bool want_force = force_resubscribe_.load(std::memory_order_acquire);
        const bool want_poll  = should_check_file();

        if (connected && (want_force || want_poll)) {
            std::cout << "[subman] ctrl connected=1 want_force=" << want_force
          << " want_poll=" << want_poll << "\n";
            // 2) Try load config -> produce pending_state_ if changed
            const bool changed = load_if_changed();
            if (!changed && want_force && has_current_ && !has_pending_) {
                pending_state_ = current_state_;
                has_pending_ = true;
                std::cout << "[subman] load: force resubscribe using current state\n";
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
bool SubscriptionManager::load_if_changed() {
    if (!cfgloader_) {
        std::cout << "[subman] load: no cfgloader\n";
        return false;
    }
    std::error_code ec;
    auto mtime = std::filesystem::last_write_time(opts_.config_path,  ec);
    if (ec) {
        std::cout << "[subman] load: last_write_time failed path=" << opts_.config_path
              << " ec=" << ec.message() << "\n";
        return false;
    }

    if (has_current_ && mtime == last_mtime_) return false;

    std::cout << "[subman] load: mtime ok path=" << opts_.config_path << "\n";

    SubscribeConfig cfg = cfgloader_(opts_.config_path);
    std::cout << "[subman] load: cfg securities=" << cfg.securities.size()
                  << " default_subtypes=" << cfg.default_subtypes.size() << "\n";

    SubState st = canonicalize(cfg);
    std::cout << "[subman] load: canonical n=" << st.size() << "\n";

    if (has_current_ && st_equal(current_state_, st)) {
        last_mtime_ = mtime;
        return false;
    }

    pending_state_ = std::move(st);
    has_pending_ = true;
    last_mtime_ = mtime;
    std::cout << "[subman] load: pending ready\n";
    return true;
}

void SubscriptionManager::apply_pending() {
    std::cout << "[subman] apply_pending enter pending=" << has_pending_
          << " target_n=" << pending_state_.size() << "\n";

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
            call_unsubscribe(id, mask);
        }
    }
    for (const auto&[id, mask] : target) {
        call_subscribe(id, mask);
    }
}
void SubscriptionManager::execute_diff(const SubState& current, const SubState& pending) {
    SubscriptionTools stdiff = diff_state(current, pending);
    for (const auto&[id, mask] : stdiff.remove_security) {
        call_unsubscribe(id, mask);
    }
    for (const auto&[id, mask] : stdiff.del_subtypes) {
        call_unsubscribe(id, mask);
    }

    for (const auto&[id, mask] : stdiff.add_subtypes) {
        call_subscribe(id, mask);
    }
    for (const auto&[id, mask] : stdiff.add_security) {
        call_subscribe(id, mask);
    }
}

bool SubscriptionManager::call_subscribe(const SecurityId& id, SubMask mask) {
    std::vector<Qot_Common::SubType> subtypes;
    mask_to_vec(mask, subtypes);

    return 0 != subscribe_api(id, subtypes);
}
bool SubscriptionManager::call_unsubscribe(const SecurityId& id, SubMask mask) {
    std::vector<Qot_Common::SubType> unsubtypes;
    mask_to_vec(mask, unsubtypes);

    return 0 != unsubscribe_api(id, unsubtypes);
}

Futu::u32_t SubscriptionManager::subscribe_api(const SecurityId& id, const std::vector<Qot_Common::SubType>& subs) {
    std::cout << "[subman] subscribe " << id.code << " subs=" << subs.size() << "\n";
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
    std::cout << "[subman] sub serial=" << sn
          << " code=" << id.code << " subs=" << subs.size() << "\n";
    return sn;
}
Futu::u32_t SubscriptionManager::unsubscribe_api(const SecurityId& id, const std::vector<Qot_Common::SubType>& subs) {
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
    return session_->sub(pbSub);
}

int64_t SubscriptionManager::now_ns() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
}