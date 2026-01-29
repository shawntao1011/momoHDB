// kfkpb.cpp  (single-file: q ABI + registry + q->rdkafka config parsing)
// Build as C++ (e.g. -std=gnu++23). Requires k.h + librdkafka + your kfkpb_core.hpp.

#include "k.h"
#define K1(f) K f(K x)
#define K2(f) K f(K x,K y)
#define K3(f) K f(K x,K y,K z)
#define EXP __attribute__((visibility("default")))

#include <librdkafka/rdkafka.h>

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <memory>

#include "kfkpb_core.hpp"   // must provide: KfkpbClient, KfkpbMsgType, etc.

// -------------------- globals --------------------
static std::atomic<int> g_notify_fd{-1};
static std::mutex g_reg_mu;
static int g_next_handle = 1;
static std::unordered_map<int, std::unique_ptr<KfkpbClient>> g_clients;

// socketpair for q callback (sd1)
static int spair[2] = {-1, -1};
static int inited = 0;

using TopicMap = std::unordered_map<std::string, KfkpbMsgType>;

// -------------------- small helpers --------------------
static void set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0) fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static std::string k_to_string(K x) {
    if (!x) return {};
    if (x->t == -KS) return std::string(x->s);
    if (x->t == KC)  return std::string((char*)x->G0, (size_t)x->n);
    if (x->t == -KC) return std::string(1, (char)x->g);
    throw std::runtime_error("value must be symbol or char vector");
}

static int dict_find_key_index(K d, const char* key) {
    // d is dict (t==99), keys must be symbol list (KS)
    if (!d || d->t != 99) return -1;
    K keys = kK(d)[0];
    if (!keys || keys->t != KS) return -1;
    for (J i = 0; i < keys->n; ++i) {
        const char* s = kS(keys)[i];
        if (s && key && 0 == std::strcmp(s, key)) return (int)i;
    }
    return -1;
}

static rd_kafka_t* rk_create_consumer_from_qconf(K conf) {
    if (!conf || conf->t != 99)
        throw std::runtime_error("conf must be dict (symbol!value)");

    char errstr[512]{0};
    rd_kafka_conf_t* rkconf = rd_kafka_conf_new();

    K keys = kK(conf)[0];
    K vals = kK(conf)[1];

    if (!keys || keys->t != KS) {
        rd_kafka_conf_destroy(rkconf);
        throw std::runtime_error("conf keys must be symbol list");
    }
    // For simplicity & safety: expect mixed list values
    if (!vals || vals->t != 0) {
        rd_kafka_conf_destroy(rkconf);
        throw std::runtime_error("conf values must be mixed list");
    }
    if (keys->n != vals->n) {
        rd_kafka_conf_destroy(rkconf);
        throw std::runtime_error("conf dict keys/values length mismatch");
    }

    for (J i = 0; i < keys->n; ++i) {
        const char* k = kS(keys)[i];
        std::string v = k_to_string(kK(vals)[i]);

        if (rd_kafka_conf_set(rkconf, k, v.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
            rd_kafka_conf_destroy(rkconf);
            throw std::runtime_error(std::string("rd_kafka_conf_set failed: ") + errstr);
        }
    }

    // mimic kfk style
    (void)rd_kafka_conf_set(rkconf, "log.queue", "true", errstr, sizeof(errstr));

    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, rkconf, errstr, sizeof(errstr));
    if (!rk) {
        rd_kafka_conf_destroy(rkconf);
        throw std::runtime_error(std::string("rd_kafka_new consumer failed: ") + errstr);
    }
    rd_kafka_poll_set_consumer(rk);

    // Optional: enable queue IO event to our notify fd (helps “wake up”)
    int nfd = g_notify_fd.load();
    if (nfd >= 0) {
        rd_kafka_queue_io_event_enable(rd_kafka_queue_get_consumer(rk), nfd, "X", 1);
    }

    return rk;
}

static TopicMap parse_topics_q(K topics) {
    // topics: dict  `topicA`topicB ! (`ticker;`orderbook)
    if (!topics || topics->t != 99)
        throw std::runtime_error("topics must be dict: `topic`...!(`type`...)");

    K tk = kK(topics)[0];
    K tv = kK(topics)[1];

    if (!tk || tk->t != KS) throw std::runtime_error("topics keys must be symbol list");
    if (!tv || tv->t != 0)  throw std::runtime_error("topics values must be mixed list");
    if (tk->n != tv->n)      throw std::runtime_error("topics dict keys/values length mismatch");

    auto parse_type = [](K v) -> KfkpbMsgType {
        if (!v) throw std::runtime_error("topic type is null");
        if (v->t == -KS) {
            std::string s = v->s;
            if (s == "basicquote") return KfkpbMsgType::BasicQuote;
            if (s == "orderbook")  return KfkpbMsgType::OrderBook;
            if (s == "ticker")     return KfkpbMsgType::Ticker;
            if (s == "kline1m" || s == "kl1min") return KfkpbMsgType::Kline1M;
            throw std::runtime_error("unknown msg type: " + s);
        }
        if (v->t == -KI) {
            int i = v->i;
            if (i < 0 || i > 3) throw std::runtime_error("msg type int out of range (0..3)");
            return static_cast<KfkpbMsgType>(i);
        }
        throw std::runtime_error("topic type must be symbol or int");
    };

    TopicMap m;
    m.reserve((size_t)tk->n);
    for (J i = 0; i < tk->n; ++i) {
        std::string topic = kS(tk)[i];
        KfkpbMsgType type = parse_type(kK(tv)[i]);
        m.emplace(std::move(topic), type);
    }
    return m;
}

static int64_t parse_ts_ms_q(K ts) {
    if (!ts) throw std::runtime_error("ts is null");

    // q timestamp (-KP): ns since 2000-01-01
    if (ts->t == -KP) {
        const int64_t q_ns = ts->j;
        const int64_t q_ms = q_ns / 1000000LL;
        return q_ms + 946684800000LL; // 2000-01-01 UTC in unix ms
    }
    // long (-KJ): treat as unix epoch ms
    if (ts->t == -KJ) return ts->j;

    throw std::runtime_error("ts must be timestamp (-KP) or long ms (-KJ)");
}

// Drain all clients' decoded events into a mixed list.
// Also patch ev[`handle] to the right client handle (by key name, not index).
static K drain_all_events_patch_handle() {
    std::vector<K> all;

    std::lock_guard<std::mutex> lk(g_reg_mu);
    all.reserve(1024);

    for (auto& [h, c] : g_clients) {
        std::vector<K> tmp;
        c->drainTo(tmp);

        for (K ev : tmp) {
            if (!ev) continue;

            // expect dict
            if (ev->t != 99) { r0(ev); continue; }

            int idx = dict_find_key_index(ev, "handle");
            if (idx < 0) { r0(ev); continue; }

            K vals = kK(ev)[1];
            if (!vals || vals->t != 0) { r0(ev); continue; } // values must be mixed
            // replace placeholder
            r0(kK(vals)[idx]);
            kK(vals)[idx] = ki(h);

            all.push_back(ev);
        }
    }

    K lst = ktn(0, (J)all.size());
    for (J i = 0; i < lst->n; ++i) kK(lst)[i] = all[(size_t)i];
    return lst;
}

// q callback registered by sd1() - called when socketpair gets data
static K kfkpb_callback(I fd) {
    char buf[256];
    while (recv(fd, buf, sizeof(buf), 0) > 0) {}

    K evs = drain_all_events_patch_handle(); // mixed list of dict
    if (evs && evs->t == 0) {
        for (J i = 0; i < evs->n; ++i) {
            K ev = kK(evs)[i];
            if (!ev || ev->t != 99) continue;

            int idx = dict_find_key_index(ev, "handle");
            if (idx < 0) continue;
            K vals = kK(ev)[1];
            if (!vals || vals->t != 0) continue;

            K h = kK(vals)[idx]; // int atom
            // q dispatcher: .kfkpb._dispatch[handle; ev]
            K r = k(0, (S)".kfkpb._dispatch", h, ev, (K)0);
            if (r) r0(r);
        }
    }
    r0(evs);
    return (K)0;
}

#ifdef __cplusplus
extern "C" {
#endif

// -------------------- exported q ABI --------------------

EXP K1(kfkpbInit) {
    (void)x;
    if (inited) return (K)0;

    if (socketpair(AF_UNIX, SOCK_STREAM, 0, spair) != 0)
        return krr((S)"socketpair failed");

    set_nonblock(spair[0]);
    set_nonblock(spair[1]);

    // register callback on read-end
    K r = sd1(-spair[0], kfkpb_callback);
    if (!r) return krr((S)"sd1 failed");
    r0(r);

    g_notify_fd.store(spair[1]);
    inited = 1;
    return (K)0;
}

EXP K1(kfkpbInitConsumer) {
    try {
        int nfd = g_notify_fd.load();
        if (nfd < 0) return krr((S)"call kfkpbInit[] first");

        rd_kafka_t* rk = rk_create_consumer_from_qconf(x);

        std::lock_guard<std::mutex> lk(g_reg_mu);
        int h = g_next_handle++;
        KfkpbClient::ThreadCfg tcfg{};
        tcfg.decode_threads = 4;
        tcfg.max_raw_queue  = 10000;
        tcfg.max_evt_queue   = 10000;
        g_clients.emplace(h, std::make_unique<KfkpbClient>(rk, nfd, tcfg));
        return ki(h);
    } catch (const std::exception& e) {
        return krr((S)e.what());
    }
}

EXP K1(kfkpbCloseConsumer) {
    std::lock_guard<std::mutex> lk(g_reg_mu);
    g_clients.erase(xi);
    return (K)0;
}

// x: handle (int)
// y: topics dict topic->type
EXP K2(kfkpbSubscribe) {
    try {
        TopicMap m = parse_topics_q(y);

        std::lock_guard<std::mutex> lk(g_reg_mu);
        auto it = g_clients.find(xi);
        if (it == g_clients.end()) return krr((S)"bad handle");

        it->second->subscribe(std::move(m));
        return (K)0;
    } catch (const std::exception& e) {
        return krr((S)e.what());
    }
}

// x: handle (int)
// y: topics dict
// z: timestamp (-KP) or unix ms (-KJ)
EXP K3(kfkpbSubscribeFromTime) {
    try {
        TopicMap m = parse_topics_q(y);
        int64_t ms = parse_ts_ms_q(z);

        std::lock_guard<std::mutex> lk(g_reg_mu);
        auto it = g_clients.find(xi);
        if (it == g_clients.end()) return krr((S)"bad handle");

        it->second->subscribeFromTime(std::move(m), ms);
        return (K)0;
    } catch (const std::exception& e) {
        return krr((S)e.what());
    }
}

#ifdef __cplusplus
}
#endif
