#pragma once
#include "futu_core/FutuQuoteSession.hpp"
#include <FTSPI.h>
#include <FTAPIChannel_Define.h>
#include <atomic>
#include <cstdint>

#include "common/Envelope.h"
#include "common/SPSCRing.h"

struct Sink {
    void* ctx{};
    void (*submit)(void*, Envelope&&) = nullptr;
    void (*flush)(void*, int) = nullptr;
};

class SubscribeManager {
  public:
    explicit SubscribeManager(Sink sink);

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

private:
    void drive_subscriptions();

private:
    Sink sink_;
    FutuQuoteSession* session_{nullptr};
    SPSCRing<Envelope> inbox_{1u << 16};

    std::atomic<bool> connected_{false};
    std::atomic<int64_t> dropped_basicqot_{0};
    std::atomic<int64_t> dropped_orderbook_{0};
    std::atomic<int64_t> dropped_ticker_{0};
    std::atomic<int64_t> dropped_kl_{0};
    std::atomic<int64_t> dropped_rt_{0};
    std::atomic<int64_t> dropped_broker_{0};

    static int64_t now_ns();
};
