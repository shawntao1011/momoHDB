#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <librdkafka/rdkafka.h>

#include "SPSCQueue.hpp"

enum class KfkpbMsgType : std::uint8_t {
    BasicQuote = 0,
    OrderBook  = 1,
    Ticker     = 2,
    Kline1M    = 3,
    Unknown    = 255
};

// core outputs only kbytes + minimal meta (NO variant/batch, NO K objects)
struct KfkpbEvent {
    enum class Kind : std::uint8_t { Data = 0, Error = 1 };

    Kind kind{Kind::Data};
    KfkpbMsgType msg_type{KfkpbMsgType::Unknown};

    std::string topic;
    std::string key;
    std::int64_t ingest_ns{0};

    // success: decoded kbytes (for q-thread to wrap as KG)
    std::vector<std::uint8_t> kbytes;

    // error: fixed size message
    char err_msg[96]{0};
};

// Minimal internal decoder signature (runs in C++ decode threads)
// Return true on success and fill out_kbytes. On failure, fill err_msg.
using DecodeFn = bool (*)(
    const std::uint8_t* key, std::size_t key_len,
    const std::uint8_t* payload, std::size_t payload_len,
    std::int64_t ts_ns,
    std::vector<std::uint8_t>& out_kbytes,
    char* err_msg, std::size_t err_cap
);

class KfkpbClient {
public:
    struct ThreadCfg {
        std::size_t decode_threads{4};
        std::size_t max_raw_queue{20000};
        std::size_t max_evt_queue{20000};
        int poll_ms{50};
    };

    KfkpbClient(rd_kafka_t* rk, int notify_fd, ThreadCfg cfg);
    ~KfkpbClient();

    KfkpbClient(const KfkpbClient&) = delete;
    KfkpbClient& operator=(const KfkpbClient&) = delete;

    // q-side: subscribe topic -> message type (decoder is chosen by internal registry)
    void subscribe(std::unordered_map<std::string, KfkpbMsgType> topics);
    void subscribeFromTime(std::unordered_map<std::string, KfkpbMsgType> topics, std::int64_t ts_ns);

    void drainTo(std::vector<KfkpbEvent>& out);

private:
    struct RawMsg {
        std::string topic;
        std::string key; // bytes copied into string (same as your current code)
        std::vector<std::uint8_t> payload;
        std::int64_t ts_ns{0};

        bool is_error{false};
        char err_msg[96]{0};
    };

    // internal decoder registry (type -> fn)
    struct DecoderRegistry {
        DecodeFn get(KfkpbMsgType t) const noexcept;
    };

    void consumerLoop();
    void decodeLoop(std::size_t worker_id);

    void notify();

    bool popRaw(std::size_t worker_id, RawMsg& out);
    void pushRaw(RawMsg&& m);

    void pushEvent(std::size_t worker_id, KfkpbEvent&& ev);

    std::size_t workerIndexFor(const RawMsg& msg);

private:
    rd_kafka_t* rk_{nullptr};
    int notify_fd_{-1};
    ThreadCfg cfg_;

    std::atomic<bool> stop_{false};

    // subscription config
    std::mutex cfg_mu_;
    std::condition_variable cfg_cv_;
    std::unordered_map<std::string, KfkpbMsgType> topicTypes_;
    std::optional<std::int64_t> seekFromMs_;
    std::int64_t cfgEpoch_{0};

    DecoderRegistry decoders_{};

    // threads
    std::thread consumer_th_;
    std::vector<std::thread> dec_ths_;

    // queues
    std::vector<std::unique_ptr<queue::SPSCQueue<RawMsg>>> raw_qs_;
    std::vector<std::unique_ptr<std::mutex>> raw_mus_;
    std::vector<std::unique_ptr<std::condition_variable>> raw_cvs_;

    std::vector<std::unique_ptr<queue::SPSCQueue<KfkpbEvent>>> evt_qs_;
    std::atomic<std::size_t> drain_rr_{0};
    std::atomic<std::size_t> fallback_rr_{0};
};
