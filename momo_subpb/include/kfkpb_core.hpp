#pragma once
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <unordered_map>
#include <librdkafka/rdkafka.h>

#include "SPSCQueue.hpp"
#include "schema.hpp"

enum class KfkpbMsgType {
    BasicQuote,
    OrderBook,
    Ticker,
    Kline1M,
    Unknown
};

struct KfkpbEvent {
    enum class Kind {
        Data,
        Error
    };

    using Payload = std::variant<std::monostate,
                                 TickerBatch,
                                 OrderBookBatch,
                                 BasicQuoteBatch,
                                 KL1MinBatch,
                                 std::vector<std::uint8_t>>;

    Kind kind{Kind::Data};
    KfkpbMsgType msg_type{KfkpbMsgType::Unknown};
    std::string topic;
    std::string key;
    std::int64_t ingest_ms{0};
    std::string reason;
    Payload payload;
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
    void drainTo(std::vector<KfkpbEvent>& out);

private:
    struct RawMsg {
        std::string topic;
        std::string key;
        std::vector<std::uint8_t> payload;
        std::int64_t ts_ms{0}; // rd_kafka_message_timestamp
        bool is_error{false};
        KfkpbMsgType error_type{KfkpbMsgType::Unknown};
        std::string error_reason;
    };

    void consumerLoop();
    void decodeLoop(std::size_t worker_id);

    void notify();

    bool popRaw(std::size_t worker_id, RawMsg& out);
    void pushRaw(RawMsg&& m);

    void pushEvent(std::size_t worker_id, KfkpbEvent ev);
    std::size_t workerIndexFor(const RawMsg& msg);

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

    // raw queues (consumer -> worker)
    std::vector<std::unique_ptr<queue::SPSCQueue<RawMsg>>> raw_qs_;
    std::vector<std::unique_ptr<std::mutex>> raw_mus_;
    std::vector<std::unique_ptr<std::condition_variable>> raw_cvs_;

    // decode threads
    std::vector<std::thread> dec_ths_;

    // event queues (worker -> writer)
    std::vector<std::unique_ptr<queue::SPSCQueue<KfkpbEvent>>> evt_qs_;
    std::atomic<std::size_t> drain_rr_{0};
    std::atomic<std::size_t> fallback_rr_{0};
};