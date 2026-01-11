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

SubscriptionManager::SubscriptionManager(Sink sink) : sink_(sink) {}

void SubscriptionManager::on_connected(Futu::i64_t err, const char *desc)
{
    std::cout << "[subman] connected err=" << err
              << " desc=" << (desc ? desc : "") << "\n";
    connected_.store(err == 0);
    if (err == 0) force_resubscribe_ = true;
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
        if (connected_.load(std::memory_order_relaxed) && session_) {
            drive_subscriptions();
        }

        inbox_.pop_many(batch, 1024);

        for (auto& e : batch) {
            sink_.submit(sink_.ctx, std::move(e));
        }
        batch.clear();

        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    if (sink_.flush) sink_.flush(sink_.ctx, 3000);
}

bool SubscriptionManager::should_check_file() {

}
bool SubscriptionManager::load_if_chanegd() {

}

void SubscriptionManager::apply_pending() {

}

void SubscriptionManager::do_full_resubscribe(const SubState& target) {

}
bool SubscriptionManager::execute_diff(const SubState& current, const SubState& pending) {

}

bool SubscriptionManager::call_subscribe(const SecurityId& id, SubMask mask) {

}
bool SubscriptionManager::call_unsubscribe(const SecurityId& id, SubMask mask) {

}

Futu::u32_t SubscriptionManager::subscribe_api(const SecurityId& id, const std::vector<Qot_Common::SubType>& subs) {
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
    return session_->sub(pbSub);
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