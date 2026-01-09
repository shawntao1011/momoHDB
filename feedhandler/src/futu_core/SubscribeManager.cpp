#include "futu_core/SubscrbeManager.hpp"
#include <FTAPI.h>
#include <FTSPI.h>
#include <Proto/Qot_Common.pb.h>
#include <Proto/Qot_Sub.pb.h>
#include <atomic>
#include <chrono>
#include <thread>
#include <utility>

SubscribeManager::SubscribeManager(Sink sink) : sink_(sink) {}

void SubscribeManager::on_connected(Futu::i64_t err, const char *desc) {
    std::cout << "[subman] connected err=" << err
              << " desc=" << (desc ? desc : "") << "\n";
    connected_.store(err == 0);
}
void SubscribeManager::on_sub_reply(Futu::u32_t nSerialNo,
                                    const Qot_Sub::Response &stRsp) {
    std::cout << "[subman] sub reply serial=" << nSerialNo
              << " retType=" << stRsp.rettype() << " retMsg=" << stRsp.retmsg()
              << "\n";
}

void SubscribeManager::on_push_basicqot(
    const Qot_UpdateBasicQot::Response &stRsp) {
    publish_basicqot(stRsp);
}
void SubscribeManager::on_push_orderbook(
    const Qot_UpdateOrderBook::Response &stRsp) {
    publish_orderbook(stRsp);
}
void SubscribeManager::on_push_ticker(const Qot_UpdateTicker::Response &stRsp) {
    publish_ticker(stRsp);
}
void SubscribeManager::on_push_kl(const Qot_UpdateKL::Response &stRsp) {
    publish_kl(stRsp);
}
void SubscribeManager::on_push_rt(const Qot_UpdateRT::Response &stRsp) {
    publish_rt(stRsp);
}
void SubscribeManager::on_push_broker(const Qot_UpdateBroker::Response &stRsp) {
    publish_broker(stRsp);
}

void SubscribeManager::run(std::atomic<bool> &stop) {
    while (!stop.load()) {
        if (connected_.load() && session_) {
            drive_subscriptions();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    if (sink_.flush)
        sink_.flush(sink_.ctx, 3000);
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

    pSubC2S->set_issuborunsub(true);
    pSubC2S->set_isregorunregpush(true);

    session_->sub(req);
    subdone = true;
}

void SubscribeManager::publish_basicqot(
    const Qot_UpdateBasicQot::Response &stRsp) {
    Envelope env;
    env.topic = "futu.basicqot.raw";
    env.key = stRsp.s2c().basicqotlist(0).security().code();
    stRsp.SerializeToString(&env.payload);
    env.ts_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();

    sink_.submit(sink_.ctx, std::move(env));
}
void SubscribeManager::publish_orderbook(
    const Qot_UpdateOrderBook::Response &stRsp) {}
void SubscribeManager::publish_ticker(const Qot_UpdateTicker::Response &stRsp) {

}
void SubscribeManager::publish_kl(const Qot_UpdateKL::Response &stRsp) {}
void SubscribeManager::publish_rt(const Qot_UpdateRT::Response &stRsp) {}
void SubscribeManager::publish_broker(const Qot_UpdateBroker::Response &stRsp) {

}
