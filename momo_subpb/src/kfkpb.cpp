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
static S sym(const char* s) { return ss(const_cast<char*>(s)); }

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

static K make_kbytes(const std::vector<std::uint8_t>& buf) {
    K b = ktn(KG, (J)buf.size());
    if (!buf.empty()) std::memcpy(kG(b), buf.data(), buf.size());
    return b;
}

// q dict: `topic`type  (topic->symbol, type->symbol)
// or `topic`type!(...) ; allow both dict and table(2 cols)
static std::unordered_map<std::string, KfkpbMsgType> parse_topics(K topics) {
    std::unordered_map<std::string, KfkpbMsgType> out;

    if (topics->t == 99) { // dict
        K k = kK(topics)[0];
        K v = kK(topics)[1];
        if (k->t != KS || v->t != KS) throw std::runtime_error("topics dict must be symbol->symbol");
        for (J i = 0; i < k->n; ++i) {
            std::string topic = kS(k)[i];
            std::string type  = kS(v)[i];

            KfkpbMsgType mt = KfkpbMsgType::Unknown;
            if (type == "basicquote") mt = KfkpbMsgType::BasicQuote;
            else if (type == "orderbook") mt = KfkpbMsgType::OrderBook;
            else if (type == "ticker") mt = KfkpbMsgType::Ticker;
            else if (type == "kline1m" || type == "kl1min") mt = KfkpbMsgType::Kline1M;

            out.emplace(std::move(topic), mt);
        }
        return out;
    }

    // table: flip of dict, expect cols `topic`type as symbols
    if (topics->t == 98) {
        K dict = kK(topics)[1]; // flip returns (cols)!data ; in k it's type 98 with .k? depends
        // 为了不纠结 flip 的内部布局，这里直接要求 dict 形式。你如果要 table，我再给你按真实 k.h 结构写。
        throw std::runtime_error("topics as table not supported yet; pass as dict `topic`type!");
    }

    throw std::runtime_error("topics must be dict");
}

// very small consumer builder: conf is dict of string->string
static rd_kafka_t* build_consumer_from_conf(K conf) {
    if (conf->t != 99) throw std::runtime_error("conf must be dict");

    K k = kK(conf)[0];
    K v = kK(conf)[1];
    if (k->t != KS) throw std::runtime_error("conf keys must be symbol");
    if (v->t != 0)  throw std::runtime_error("conf values must be mixed list");

    char errstr[512]{0};
    rd_kafka_conf_t* rkconf = rd_kafka_conf_new();

    for (J i = 0; i < k->n; ++i) {
        const char* key = kS(k)[i];
        K val = kK(v)[i];

        // accept char vector or symbol as value
        std::string sval;
        if (val->t == KS) sval = kS(val)[0];
        else if (val->t == KC) sval.assign((char*)kG(val), (size_t)val->n);
        else throw std::runtime_error("conf values must be symbol or char list");

        if (rd_kafka_conf_set(rkconf, key, sval.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            std::string msg = std::string("rd_kafka_conf_set failed: ") + key + ": " + errstr;
            rd_kafka_conf_destroy(rkconf);
            throw std::runtime_error(msg);
        }
    }

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, rkconf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(rkconf);
        throw std::runtime_error(std::string("rd_kafka_new failed: ") + errstr);
    }

    rd_kafka_poll_set_consumer(rk);
    return rk;
}

// ------------------------------------------------------------
// KfkpbEvent -> q dict  (MINIMAL, q-friendly)
// keys: kind, topic, key, msgType, ingestNs, kbytes, errMsg
// ------------------------------------------------------------
static K to_q_event(const KfkpbEvent& ev) {
    K keys = ktn(KS, 7);
    kS(keys)[0] = sym("kind");
    kS(keys)[1] = sym("topic");
    kS(keys)[2] = sym("key");
    kS(keys)[3] = sym("msgType");
    kS(keys)[4] = sym("ingestNs");
    kS(keys)[5] = sym("kbytes");
    kS(keys)[6] = sym("errMsg");

    K vals = ktn(0, 7);

    kK(vals)[0] = ks((S)kind_name(ev.kind));
    kK(vals)[1] = ev.topic.empty() ? ktj(-KJ, nj) : ks((S)ev.topic.c_str());
    kK(vals)[2] = ev.key.empty()   ? ktj(-KJ, nj) : ks((S)ev.key.c_str());
    kK(vals)[3] = ks((S)msg_type_name(ev.msg_type));

    // ingestNs: if you store ns in ev.ingest_ns
    kK(vals)[4] = kj(ev.ingest_ns);

    if (ev.kind == KfkpbEvent::Kind::Data) {
        kK(vals)[5] = make_kbytes(ev.kbytes);
        kK(vals)[6] = ktj(-KJ, nj);
    } else {
        kK(vals)[5] = ktj(-KJ, nj);
        kK(vals)[6] = ev.err_msg[0] ? ks((S)ev.err_msg) : ktj(-KJ, nj);
    }

    return xD(keys, vals);
}

// ------------------------------------------------------------
// callback
// ------------------------------------------------------------
static K kfkpb_callback(int) {
    std::vector<KfkpbEvent> evs;
    {
        std::lock_guard<std::mutex> lk(g_mu);
        if (!g_client) return (K)0;
        g_client->drainTo(evs);
    }

    for (const auto& ev : evs) {
        K qev = to_q_event(ev);
        k(0, (S)".kfkpb._dispatch", ki(g_handle), qev, (K)0);
        r0(qev);
    }

    return (K)0;
}

// ------------------------------------------------------------
// q ABI
// ------------------------------------------------------------
extern "C" {

K kfkpb_init() {
    if (g_inited.exchange(true)) return ki(g_handle);

    if (socketpair(AF_LOCAL, SOCK_STREAM, 0, g_fd) != 0) return krr((S)"socketpair");
    sd1(g_fd[1], kfkpb_callback);
    return ki(g_handle);
}

K kfkpb_initConsumer(K conf) {
    if (conf->t != 99) return krr((S)"type");

    std::lock_guard<std::mutex> lk(g_mu);
    if (g_client) return ki(g_handle);

    try {
        rd_kafka_t* rk = build_consumer_from_conf(conf);
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
    if (handle->t != -7 || handle->j != g_handle) return krr((S)"handle");

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
