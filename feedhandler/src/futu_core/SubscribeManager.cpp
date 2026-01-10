#include "futu_core/SubscrbeManager.hpp"
#include <FTAPI.h>
#include <FTSPI.h>
#include <Proto/Qot_Common.pb.h>
#include <Proto/Qot_Sub.pb.h>
#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <utility>

SubscribeManager::SubscribeManager(Sink sink) : sink_(sink) {}

void SubscribeManager::on_connected(Futu::i64_t err, const char *desc)
{
    std::cout << "[subman] connected err=" << err
              << " desc=" << (desc ? desc : "") << "\n";
    connected_.store(err == 0);
}
void SubscribeManager::on_sub_reply(Futu::u32_t nSerialNo, const Qot_Sub::Response &stRsp) 
{
    std::cout << "[subman] sub reply serial=" << nSerialNo
              << " retType=" << stRsp.rettype() << " retMsg=" << stRsp.retmsg()
              << "\n";
}

void SubscribeManager::on_push_basicqot(const Qot_UpdateBasicQot::Response &stRsp)
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

    if (!inbox_.try_push(std::move(e))) {
        dropped_basicqot_.fetch_add(1, std::memory_order_relaxed);
    }
}
void SubscribeManager::on_push_orderbook(const Qot_UpdateOrderBook::Response &stRsp)
{
    Envelope e;
    e.topic = "futu.orderbook.pb";
    e.key = stRsp.s2c().security().code();
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    if (!inbox_.try_push(std::move(e))) {
        dropped_orderbook_.fetch_add(1, std::memory_order_relaxed);
    }
}
void SubscribeManager::on_push_ticker(const Qot_UpdateTicker::Response &stRsp)
{
    Envelope e;
    e.topic = "futu.ticker.pb";
    e.key = stRsp.s2c().security().code();
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    if (!inbox_.try_push(std::move(e))) {
        dropped_ticker_.fetch_add(1, std::memory_order_relaxed);
    }
}
void SubscribeManager::on_push_kl(const Qot_UpdateKL::Response &stRsp)
{
    Envelope e;
    e.topic = "futu.kl_1m.pb";
    e.key = stRsp.s2c().security().code();
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    if (!inbox_.try_push(std::move(e))) {
        dropped_kl_.fetch_add(1, std::memory_order_relaxed);
    }
}
void SubscribeManager::on_push_rt(const Qot_UpdateRT::Response &stRsp)
{
    Envelope e;
    e.topic = "futu.rt.pb";
    e.key = stRsp.s2c().security().code();
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    if (!inbox_.try_push(std::move(e))) {
        dropped_rt_.fetch_add(1, std::memory_order_relaxed);
    }
}
void SubscribeManager::on_push_broker(const Qot_UpdateBroker::Response &stRsp) 
{
    Envelope e;
    e.topic = "futu.broker.pb";
    e.key = stRsp.s2c().security().code();
    e.ts_ns = now_ns();
    e.payload.resize(static_cast<size_t>(stRsp.ByteSizeLong()));
    if (!stRsp.SerializeToArray(e.payload.data(), static_cast<int>(e.payload.size()))) {
        return;
    }

    if (!inbox_.try_push(std::move(e))) {
        dropped_broker_.fetch_add(1, std::memory_order_relaxed);
    }
}

void SubscribeManager::run(std::atomic<bool> &stop) {
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

void SubscribeManager::drive_subscriptions() {
    static bool subdone = false;
    if (subdone)
        return;

    Qot_Sub::Request req;
    Qot_Sub::C2S *pSubC2S = req.mutable_c2s();

    Qot_Common::Security *pSubSec = pSubC2S->add_securitylist();
    pSubSec->set_code("00700");
    pSubSec->set_market(Qot_Common::QotMarket_HK_Security);

    pSubC2S->add_subtypelist(Qot_Common::SubType_Basic);
    pSubC2S->add_subtypelist(Qot_Common::SubType_OrderBook);
    pSubC2S->add_subtypelist(Qot_Common::SubType_Ticker);
    pSubC2S->add_subtypelist(Qot_Common::SubType_KL_1Min);
    pSubC2S->add_subtypelist(Qot_Common::SubType_RT);
    pSubC2S->add_subtypelist(Qot_Common::SubType_Broker);

    pSubC2S->set_issuborunsub(true);
    pSubC2S->set_isregorunregpush(true);

    session_->sub(req);
    subdone = true;
}

int64_t SubscribeManager::now_ns() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
}