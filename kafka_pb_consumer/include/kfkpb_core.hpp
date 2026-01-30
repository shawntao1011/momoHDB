#pragma once
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>
#include <librdkafka/rdkafka.h>

#include "k.h"
#include "momoDB_types.hpp"

enum class KfkpbMsgType {
    BasicQuote,
    OrderBook,
    Ticker,
    Kline1M,
    Unknown
};

class KfkpbClient {
public:
    struct ThreadCfg {
        std::size_t decode_threads{4};
        std::size_t max_raw_queue{20000};   // backpressure
        std::size_t max_evt_queue{20000};
        int poll_ms{50};
    };

    KfkpbClient(rd_kafka_t* rk, int notify_fd, ThreadCfg cfg);
    ~KfkpbClient();

    KfkpbClient(const KfkpbClient&) = delete;
    KfkpbClient& operator=(const KfkpbClient&) = delete;

    void subscribe(std::unordered_map<std::string, KfkpbMsgType> topics);
    void subscribeFromTime(std::unordered_map<std::string, KfkpbMsgType> topics, std::int64_t ts_ms);

    // q thread
    void drainTo(std::vector<K>& out);

private:
    struct RawMsg {
        std::string topic;
        std::string key;
        std::vector<std::uint8_t> payload;
        std::int64_t ts_ms{0}; // rd_kafka_message_timestamp
    };

    void consumerLoop();
    void decodeLoop();

    void notify();

    bool popRaw(RawMsg& out);
    void pushRaw(RawMsg&& m);

    void pushEvent(K ev);

private:
    rd_kafka_t* rk_{nullptr};
    int notify_fd_{-1};
    ThreadCfg cfg_;

    std::atomic<bool> stop_{false};

    // subscription state
    std::mutex cfg_mu_;
    std::condition_variable cfg_cv_;
    std::unordered_map<std::string, KfkpbMsgType> topicTypes_;
    std::optional<std::int64_t> seekFromMs_;
    std::int64_t cfgEpoch_{0};

    // consumer thread
    std::thread consumer_th_;

    // raw queue
    std::mutex raw_mu_;
    std::condition_variable raw_cv_;
    std::deque<RawMsg> raw_q_;

    // decode threads
    std::vector<std::thread> dec_ths_;

    // event queue (K objects)
    std::mutex evt_mu_;
    std::deque<K> evt_q_;
};