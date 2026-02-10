#include <k.h>

#include <atomic>
#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <cstring>
#include <unordered_map>

#include <unistd.h>
#include <sys/socket.h>

#include <librdkafka/rdkafka.h>

#include "kfkpb_core.hpp"

// ------------------------------------------------------------
// Global singleton state
// ------------------------------------------------------------
static std::mutex g_mu;
static std::unique_ptr<KfkpbClient> g_client;

static int g_handle = 1;
static int g_fd[2] = {-1, -1};
static std::atomic<bool> g_inited{false};

// ------------------------------------------------------------
// Helpers
// ------------------------------------------------------------

static const char* msg_type_name(KfkpbMsgType t) {
    switch (t) {
        case KfkpbMsgType::BasicQuote: return "basicquote";
        case KfkpbMsgType::OrderBook:  return "orderbook";
        case KfkpbMsgType::Ticker:     return "ticker";
        case KfkpbMsgType::Kline1M:    return "kline1m";
        default:                       return "unknown";
    }
}

static const char* kind_name(KfkpbEvent::Kind k) {
    return (k == KfkpbEvent::Kind::Data) ? "data" : "error";
}

static KfkpbMsgType infer_type_from_topic(const std::string& topic) {
    if (topic.find("basicqot") != std::string::npos ||
        topic.find("basicqquote") != std::string::npos)
        return KfkpbMsgType::BasicQuote;
    if (topic.find("orderbook") != std::string::npos)
        return KfkpbMsgType::OrderBook;
    if (topic.find("ticker") != std::string::npos)
        return KfkpbMsgType::Ticker;
    if (topic.find("kline") != std::string::npos ||
        topic.find("kl1m")  != std::string::npos)
        return KfkpbMsgType::Kline1M;

    return KfkpbMsgType::Unknown;
}

static K make_kbytes(const std::vector<std::uint8_t>& buf) {
    K b = ktn(KG, (J)buf.size());
    if (!buf.empty()) std::memcpy(kG(b), buf.data(), buf.size());
    return b;
}

// accept:
//  symbol list: (`topic1;`topic2;...)
static std::unordered_map<std::string, KfkpbMsgType>
parse_topics(K topics) {
    std::unordered_map<std::string, KfkpbMsgType> out;

    if (topics->t != KS)
        throw std::runtime_error("topics must be symbol list");

    for (J i = 0; i < topics->n; ++i) {
        std::string topic = kS(topics)[i];
        auto mt = infer_type_from_topic(topic);

        if (mt == KfkpbMsgType::Unknown) {
            throw std::runtime_error(
                "cannot infer msg type from topic: " + topic
            );
        }

        out.emplace(std::move(topic), mt);
    }

    return out;
}

// ------------------------------------------------------------
// callback
// ------------------------------------------------------------
static K kfkpb_callback(int d) {
    std::vector<KfkpbEvent> evs;
    {
        std::lock_guard<std::mutex> lk(g_mu);
        if (!g_client) return (K)0;
        g_client->drainTo(evs);
    }

    for (const auto& ev : evs) {
        K data = make_kbytes(ev.data);
        k(0, (S)".kfkpb.consumecb", data, (K)0);
    }

    return (K)0;
}


static rd_kafka_t* build_default_consumer() {
    char errstr[512]{0};

    // 1. new conf
    rd_kafka_conf_t* conf = rd_kafka_conf_new();

    auto set = [&](const char* k, const char* v) {
        if (rd_kafka_conf_set(conf, k, v, errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            std::string msg = std::string("rd_kafka_conf_set failed: ")
                            + k + "=" + v + " : " + errstr;
            rd_kafka_conf_destroy(conf);
            throw std::runtime_error(msg);
        }
    };

    // 2. hard-coded minimal config
    set("bootstrap.servers", "192.168.2.209:9092");
    set("group.id", "momo-subpb-dev");
    set("enable.auto.commit", "true");
    set("auto.offset.reset", "earliest");

    // 3. create consumer
    rd_kafka_t* rk = rd_kafka_new(
        RD_KAFKA_CONSUMER,
        conf,
        errstr,
        sizeof(errstr)
    );

    if (!rk) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(
            std::string("rd_kafka_new consumer failed: ") + errstr
        );
    }

    // 4. required for consumer_poll()
    rd_kafka_poll_set_consumer(rk);

    return rk;
}

// ------------------------------------------------------------
// q ABI
// ------------------------------------------------------------
extern "C" {

K kfkpb_init() {
        if (g_inited.exchange(true)) return ki(g_handle);

        if (socketpair(AF_LOCAL, SOCK_STREAM, 0, g_fd) != 0)
            return krr((S)"socketpair");

        sd1(g_fd[1], kfkpb_callback);
        return ki(g_handle);
    }

K kfkpb_initConsumer(K conf) {
    if (conf->t != 99) return krr((S)"type");

    std::lock_guard<std::mutex> lk(g_mu);
    if (g_client) return ki(g_handle);

    try {
        rd_kafka_t* rk = build_default_consumer();
        KfkpbClient::ThreadCfg cfg; // default
        g_client = std::make_unique<KfkpbClient>(rk, g_fd[0], cfg);
    } catch (const std::exception& e) {
        return krr((S)e.what());
    }

    return ki(g_handle);
}

K kfkpb_close() {
    std::lock_guard<std::mutex> lk(g_mu);
    g_client.reset();
    return (K)0;
}

K kfkpb_subscribe(K handle, K topics) {
    if (handle->t != -6 || handle->i != g_handle) return krr((S)"handle");

    std::lock_guard<std::mutex> lk(g_mu);
    if (!g_client) return krr((S)"not initialized");

    try {
        g_client->subscribe(parse_topics(topics));
    } catch (const std::exception& e) {
        return krr((S)e.what());
    }

    return (K)0;
}

K kfkpb_subscribeFromTime(K handle, K topics, K ts) {
    if (handle->t != -7 || handle->j != g_handle) return krr((S)"handle");
    if (ts->t != -7) return krr((S)"type");

    std::lock_guard<std::mutex> lk(g_mu);
    if (!g_client) return krr((S)"not initialized");

    try {
        g_client->subscribeFromTime(parse_topics(topics), ts->j);
    } catch (const std::exception& e) {
        return krr((S)e.what());
    }

    return (K)0;
}

} // extern "C"
