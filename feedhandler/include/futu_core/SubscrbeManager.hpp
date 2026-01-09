#pragma once
#include "futu_core/FutuQuoteSession.hpp"
#include <FTAPI.h>
#include <FTSPI.h>
#include <FTAPIChannel_Define.h>
#include <atomic>
#include <cstdint>

struct Envelope {
    std::string topic;      // futu.quote.raw
    std::string key;        // symbol
    std::string payload;    // bytes
    std::int64_t ts_ns{0};  // ingest time
};

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

    void publish_basicqot(const Qot_UpdateBasicQot::Response &stRsp);
    void publish_orderbook(const Qot_UpdateOrderBook::Response &stRsp);
    void publish_ticker(const Qot_UpdateTicker::Response &stRsp);
    void publish_kl(const Qot_UpdateKL::Response &stRsp);
    void publish_rt(const Qot_UpdateRT::Response &stRsp);
    void publish_broker(const Qot_UpdateBroker::Response &stRsp);

private:
    Sink sink_;
    FutuQuoteSession* session_{nullptr};

    std::atomic<bool> connected_{false};
};
