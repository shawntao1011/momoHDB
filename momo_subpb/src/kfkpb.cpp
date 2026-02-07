#include <k.h>

#include <atomic>
#include <mutex>
#include <memory>
#include <vector>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>

#include "kfkpb_core.hpp"

// ------------------------------------------------------------
// Global singleton state
// ------------------------------------------------------------

static std::mutex g_mu;
static std::unique_ptr<KfkpbClient> g_client;

static int g_handle = 1;        // fixed handle for q side
static int g_fd[2] = {-1, -1};  // socketpair
static std::atomic<bool> g_inited{false};

// ------------------------------------------------------------
// Helpers: enum → q symbol
// ------------------------------------------------------------

static S sym(const char* s) {
    return ss(const_cast<char*>(s));
}

static const char* msg_type_name(KfkpbMsgType t) {
    switch (t) {
        case KfkpbMsgType::BasicQuote: return "basicquote";
        case KfkpbMsgType::OrderBook:  return "orderbook";
        case KfkpbMsgType::Ticker:     return "ticker";
        case KfkpbMsgType::Kline1M:    return "kline1m";
        default:                       return "unknown";
    }
}

// ------------------------------------------------------------
// KfkpbEvent -> q dict
// ------------------------------------------------------------

static K to_q_event(const KfkpbEvent& ev, int handle) {
    // keys
    K keys = ktn(KS, 9);
    kS(keys)[0] = sym("handle");
    kS(keys)[1] = sym("kind");
    kS(keys)[2] = sym("type");
    kS(keys)[3] = sym("topic");
    kS(keys)[4] = sym("key");
    kS(keys)[6] = sym("reason");
    kS(keys)[7] = sym("payload_kind");
    kS(keys)[8] = sym("payload");

    // values
    K vals = ktn(0, 9);

    // handle
    kK(vals)[0] = ki(handle);

    // kind
    kK(vals)[1] = ks(const_cast<char*>(
        ev.kind == KfkpbEvent::Kind::Data ? "data" : "error"
    ));

    // msg type
    kK(vals)[2] = ks(const_cast<char*>(msg_type_name(ev.msg_type)));

    // topic / key
    kK(vals)[3] = ks(const_cast<char*>(ev.topic.c_str()));
    kK(vals)[4] = ks(const_cast<char*>(ev.key.c_str()));

    // ingest timestamp
    kK(vals)[5] = kj(ev.ingest_ms);

    // reason
    if (!ev.reason.empty()) {
        kK(vals)[6] = ks(const_cast<char*>(ev.reason.c_str()));
    } else {
        kK(vals)[6] = ktj(-1); // null
    }

    // payload
    const auto& p = ev.payload;

    if (std::holds_alternative<std::vector<std::uint8_t>>(p)) {
        const auto& buf = std::get<std::vector<std::uint8_t>>(p);

        kK(vals)[7] = ks(const_cast<char*>("bytes"));

        K b = ktn(KG, buf.size());
        if (!buf.empty()) {
            std::memcpy(kG(b), buf.data(), buf.size());
        }
        kK(vals)[8] = b;
    } else if (std::holds_alternative<std::monostate>(p)) {
        kK(vals)[7] = ks(const_cast<char*>("none"));
        kK(vals)[8] = ktj(-1);
    } else {
        // typed batch (TickerBatch / OrderBookBatch / ...)
        kK(vals)[7] = ks(const_cast<char*>("batch"));
        kK(vals)[8] = ktj(-1);
    }

    return xD(keys, vals);
}

// ------------------------------------------------------------
// socket callback: drain events & dispatch into q
// ------------------------------------------------------------

static void kfkpb_callback(int) {
    std::vector<KfkpbEvent> evs;

    {
        std::lock_guard<std::mutex> lk(g_mu);
        if (!g_client) return;
        g_client->drainTo(evs);
    }

    for (const auto& ev : evs) {
        K qev = to_q_event(ev, g_handle);
        // .kfkpb._dispatch[handle; event]
        k(0, ".kfkpb._dispatch", ki(g_handle), qev, (K)0);
        r0(qev);
    }
}

// ------------------------------------------------------------
// q ABI
// ------------------------------------------------------------

extern "C" {

// .kfkpb.init[]
K kfkpb_init() {
    if (g_inited.exchange(true)) {
        return ki(g_handle);
    }

    if (socketpair(AF_LOCAL, SOCK_STREAM, 0, g_fd) != 0) {
        return krr("socketpair");
    }

    sd1(g_fd[1], kfkpb_callback);
    return ki(g_handle);
}

// .kfkpb.initConsumer[conf]
K kfkpb_initConsumer(K conf) {
    if (conf->t != 0) {
        return krr("type");
    }

    std::lock_guard<std::mutex> lk(g_mu);

    if (g_client) {
        // idempotent
        return ki(g_handle);
    }

    try {
        g_client = std::make_unique<KfkpbClient>(conf, g_fd[0]);
    } catch (const std::exception& e) {
        return krr(const_cast<char*>(e.what()));
    }

    return ki(g_handle);
}

// .kfkpb.close[]
K kfkpb_close() {
    std::lock_guard<std::mutex> lk(g_mu);
    g_client.reset();
    return (K)0;
}

// .kfkpb.subscribe[handle; topics]
K kfkpb_subscribe(K handle, K topics) {
    if (handle->t != -7 || handle->j != g_handle) {
        return krr("handle");
    }
    if (topics->t != 0) {
        return krr("type");
    }

    std::lock_guard<std::mutex> lk(g_mu);
    if (!g_client) return krr("not initialized");

    try {
        g_client->subscribe(topics);
    } catch (const std::exception& e) {
        return krr(const_cast<char*>(e.what()));
    }

    return (K)0;
}

// .kfkpb.subscribeFromTime[handle; topics; ts]
K kfkpb_subscribeFromTime(K handle, K topics, K ts) {
    if (handle->t != -7 || handle->j != g_handle) {
        return krr("handle");
    }
    if (topics->t != 0 || ts->t != -7) {
        return krr("type");
    }

    std::lock_guard<std::mutex> lk(g_mu);
    if (!g_client) return krr("not initialized");

    try {
        g_client->subscribeFromTime(topics, ts->j);
    } catch (const std::exception& e) {
        return krr(const_cast<char*>(e.what()));
    }

    return (K)0;
}

} // extern "C"
