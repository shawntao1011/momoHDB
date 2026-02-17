#include "futu_quote/FutuQuoteSession.hpp"
#include <FTAPIChannel_Define.h>
#include <Proto/Qot_Sub.pb.h>
#include <chrono>
#include <cstdint>
#include <iostream>

#include "JsonLogger.hpp"

FutuQuoteSession::FutuQuoteSession(SessionCallbacks cbs) : cbs_(cbs) {
    qot_ = new Futu::FTAPI_Qot();
    qot_->RegisterQotSpi(this);
    qot_->RegisterConnSpi(this);
}

FutuQuoteSession::~FutuQuoteSession() {
    stop();
    delete qot_;
}

std::expected<void, std::string> FutuQuoteSession::start(const char *ip, uint16_t port) {
    if (!qot_) return std::unexpected("qot_ is null");

    ip_ = ip ? ip : "";
    port_ = port;
    next_reconnect_at_ns_.store(0, std::memory_order_release);

    (void)qot_->InitConnect(ip, port, false);

    logger::info("session", "init_connect_requested",
                 {logger::field("ip", ip ? ip : ""),
                  logger::num("port", port)});
    return {};
}

void FutuQuoteSession::ensure_connected() {
    if (!qot_ || connected_.load(std::memory_order_acquire)) {
        return;
    }
    if (ip_.empty() || port_ == 0) {
        return;
    }

    const auto now_ns = steady_now_ns();
    auto next_at = next_reconnect_at_ns_.load(std::memory_order_acquire);
    if (now_ns < next_at) {
        return;
    }

    const auto interval_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        reconnect_interval_).count();
    if (!next_reconnect_at_ns_.compare_exchange_strong(
            next_at, now_ns + interval_ns,
            std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
        return;
            }

    logger::info("session", "reconnect_attempt",
                 {logger::field("ip", ip_), logger::num("port", port_)});
    (void)qot_->InitConnect(ip_.c_str(), port_, false);
}

void FutuQuoteSession::stop() {
    if (qot_)
        qot_->Close();
}

Futu::u32_t FutuQuoteSession::sub(const Qot_Sub::Request &req) {
    return qot_->Sub(req);
}

void FutuQuoteSession::OnInitConnect(Futu::FTAPI_Conn *pConn,
                                     Futu::i64_t nErrCode,
                                     const char *strDesc) {
    connected_.store(nErrCode == 0);
    if (nErrCode != 0) {
        next_reconnect_at_ns_.store(steady_now_ns(), std::memory_order_release);
    }
    if (cbs_.on_connected) {
        cbs_.on_connected(cbs_.ctx, nErrCode, strDesc);
    }
}

void FutuQuoteSession::OnDisConnect(Futu::FTAPI_Conn *pConn,
                                    Futu::i64_t nErrCode) {
    connected_.store(false);
    next_reconnect_at_ns_.store(steady_now_ns(), std::memory_order_release);
    if (cbs_.on_disconnected) {
        cbs_.on_disconnected(cbs_.ctx, nErrCode);
    }
}

int64_t FutuQuoteSession::steady_now_ns() {
    using namespace std::chrono;
    return duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
}

void FutuQuoteSession::OnReply_Sub(Futu::u32_t nSerialNo,
                                   const Qot_Sub::Response &stRsp) {
    if (cbs_.on_sub_reply) {
        cbs_.on_sub_reply(cbs_.ctx, nSerialNo, stRsp);
    }
}

void FutuQuoteSession::OnPush_UpdateBasicQot(
    const Qot_UpdateBasicQot::Response &stRsp) {
    logger::debug("session", "push_basicqot");
    if (cbs_.on_push_basicqot) {
        cbs_.on_push_basicqot(cbs_.ctx, stRsp);
    }
}
void FutuQuoteSession::OnPush_UpdateOrderBook(
    const Qot_UpdateOrderBook::Response &stRsp) {
    logger::debug("session", "push_orderbook");
    if (cbs_.on_push_orderbook) {
        cbs_.on_push_orderbook(cbs_.ctx, stRsp);
    }
}
void FutuQuoteSession::OnPush_UpdateTicker(
    const Qot_UpdateTicker::Response &stRsp) {
    logger::debug("session", "push_ticker");
    if (cbs_.on_push_ticker) {
        cbs_.on_push_ticker(cbs_.ctx, stRsp);
    }
}
void FutuQuoteSession::OnPush_UpdateKL(const Qot_UpdateKL::Response &stRsp) {
    logger::debug("session", "push_kl");
    if (cbs_.on_push_kl) {
        cbs_.on_push_kl(cbs_.ctx, stRsp);
    }
}
void FutuQuoteSession::OnPush_UpdateRT(const Qot_UpdateRT::Response &stRsp) {
    logger::debug("session", "push_rt");
    if (cbs_.on_push_rt) {
        cbs_.on_push_rt(cbs_.ctx, stRsp);
    }
}
void FutuQuoteSession::OnPush_UpdateBroker(const Qot_UpdateBroker::Response &stRsp) {
    logger::debug("session", "push_broker");
    if (cbs_.on_push_broker) {
        cbs_.on_push_broker(cbs_.ctx, stRsp);
    }
}
