#include "kfkpb_core.hpp"

#include <cstring>
#include <stdexcept>

#include "decoders.hpp"

static std::size_t next_pow2(std::size_t v) {
    if (v < 2) return 2;
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    if (sizeof(std::size_t) >= 8) v |= v >> 32;
    return v + 1;
}

static std::int64_t ns_to_ms(std::int64_t ts_ns) {
    constexpr std::int64_t kNsPerMs = 1'000'000;
    return ts_ns / kNsPerMs;
}

static void set_err(char* dst, std::size_t cap, const char* s) {
    if (!dst || cap == 0) return;
    if (!s) { dst[0] = '\0'; return; }
    std::snprintf(dst, cap, "%s", s);
    dst[cap - 1] = '\0';
}

// --------------------
// Temporary decoders for validation (pass-through):
// kbytes = raw payload
// Replace these with real protobuf->kbytes later, keeping signature stable.
// --------------------
static bool decode_passthrough(
    const std::uint8_t*, std::size_t,
    const std::uint8_t* payload, std::size_t payload_len,
    std::int64_t,
    std::vector<std::uint8_t>& out_kbytes,
    char*, std::size_t
) {
    out_kbytes.assign(payload, payload + payload_len);
    return true;
}

static bool decode_not_supported(
    const std::uint8_t*, std::size_t,
    const std::uint8_t*, std::size_t,
    std::int64_t,
    std::vector<std::uint8_t>&,
    char* err_msg, std::size_t err_cap
) {
    set_err(err_msg, err_cap, "decoder not implemented");
    return false;
}

DecodeFn KfkpbClient::DecoderRegistry::get(KfkpbMsgType t) const noexcept {
    // Phase-1 validation: all types pass-through.
    // Phase-2: swap to real decoders:
    //   case OrderBook: return decode_orderbook_kbytes; ...
    switch (t) {
        case KfkpbMsgType::BasicQuote:
            return &decode_basicquote_kbytes;
        case KfkpbMsgType::OrderBook:
            return &decode_orderbook_kbytes;
        case KfkpbMsgType::Ticker:
            return &decode_ticker_kbytes;
        case KfkpbMsgType::Kline1M:
            return &decode_kl1min_kbytes;
        default:
            return &decode_not_supported;
    }
}

KfkpbClient::KfkpbClient(rd_kafka_t* rk, int notify_fd, ThreadCfg cfg)
    : rk_(rk), notify_fd_(notify_fd), cfg_(cfg) {

    const std::size_t raw_cap = next_pow2(cfg_.max_raw_queue + 1);
    const std::size_t evt_cap = next_pow2(cfg_.max_evt_queue + 1);

    raw_qs_.reserve(cfg_.decode_threads);
    evt_qs_.reserve(cfg_.decode_threads);
    raw_epochs_.resize(cfg_.decode_threads);

    for (std::size_t i = 0; i < cfg_.decode_threads; ++i) {
        raw_qs_.emplace_back(
            std::make_unique<queue::SPSCQueue<std::unique_ptr<RawMsg>>>(raw_cap));
        evt_qs_.emplace_back(
            std::make_unique<queue::SPSCQueue<std::unique_ptr<KfkpbEvent>>>(evt_cap));
        raw_epochs_[i] = std::make_unique<std::atomic<std::uint64_t>>(0);
    }

    for (std::size_t i = 0; i < cfg_.decode_threads; ++i) {
        dec_ths_.emplace_back([this, i] { decodeLoop(i); });
    }

    consumer_th_ = std::thread([this] { consumerLoop(); });
}

KfkpbClient::~KfkpbClient() {
    stop_.store(true, std::memory_order_relaxed);

    cfg_cv_.notify_all();
    for (auto& ep : raw_epochs_) {
        if (!ep) continue;
        ep->fetch_add(1, std::memory_order_release);
        std::atomic_notify_all(ep.get());
    }

    if (consumer_th_.joinable()) consumer_th_.join();
    for (auto& t : dec_ths_) if (t.joinable()) t.join();

    if (rk_) {
        rd_kafka_consumer_close(rk_);
        rd_kafka_destroy(rk_);
        rk_ = nullptr;
    }
}

void KfkpbClient::subscribe(std::unordered_map<std::string, KfkpbMsgType> topics) {
    {
        std::lock_guard<std::mutex> lk(cfg_mu_);
        topicTypes_ = std::move(topics);
        seekFromMs_.reset();
        cfgEpoch_++;
    }
    cfg_cv_.notify_all();
}

void KfkpbClient::subscribeFromTime(std::unordered_map<std::string, KfkpbMsgType> topics, std::int64_t ts_ns) {
    {
        std::lock_guard<std::mutex> lk(cfg_mu_);
        topicTypes_ = std::move(topics);
        seekFromMs_ = ns_to_ms(ts_ns);
        cfgEpoch_++;
    }
    cfg_cv_.notify_all();
}

void KfkpbClient::drainTo(std::vector<KfkpbEvent>& out) {
    if (evt_qs_.empty()) return;

    const std::size_t qn = evt_qs_.size();
    const std::size_t start = drain_rr_.fetch_add(1, std::memory_order_relaxed) % qn;

    std::size_t drained = 0;
    for (std::size_t i = 0; i < qn; ++i) {
        const std::size_t idx = (start + i) % qn;
        auto& q = *evt_qs_[idx];
        std::unique_ptr<KfkpbEvent> ev;
        while (q.try_pop(ev)) {
            out.push_back(std::move(*ev));
            ++drained;
        }
    }

    if (drained == 0) {
        for (std::size_t i = 0; i < qn; ++i) {
            const std::size_t idx = (start + i) % qn;
            auto& q = *evt_qs_[idx];
            std::unique_ptr<KfkpbEvent> ev;
            if (q.try_pop(ev)) { out.push_back(std::move(*ev)); break; }
        }
    }
}

void KfkpbClient::consumerLoop() {
    std::int64_t appliedEpoch = -1;

    auto apply_subscription_locked = [&](std::unique_lock<std::mutex>& lk) {
        if (appliedEpoch == cfgEpoch_) return;

        if (topicTypes_.empty()) {
            rd_kafka_unsubscribe(rk_);
            appliedEpoch = cfgEpoch_;
            return;
        }

        rd_kafka_topic_partition_list_t* t =
            rd_kafka_topic_partition_list_new((int)topicTypes_.size());
        for (const auto& [topic, _] : topicTypes_) {
            rd_kafka_topic_partition_list_add(t, topic.c_str(), RD_KAFKA_PARTITION_UA);
        }

        rd_kafka_resp_err_t e = rd_kafka_subscribe(rk_, t);
        rd_kafka_topic_partition_list_destroy(t);

        if (e != RD_KAFKA_RESP_ERR_NO_ERROR) {
            auto err = std::make_unique<RawMsg>();
            err->is_error = true;
            set_err(err->err_msg, sizeof(err->err_msg), rd_kafka_err2str(e));
            pushRaw(std::move(err));
            appliedEpoch = cfgEpoch_;
            return;
        }

        if (seekFromMs_) {
            const int64_t ts_ms = *seekFromMs_;
            rd_kafka_topic_partition_list_t* assn = nullptr;
            bool got = false;

            lk.unlock();
            for (int i = 0; i < 60 && !stop_.load(std::memory_order_relaxed); ++i) {
                rd_kafka_poll(rk_, 0);
                if (rd_kafka_assignment(rk_, &assn) == RD_KAFKA_RESP_ERR_NO_ERROR &&
                    assn && assn->cnt > 0) {
                    got = true;
                    break;
                }
                if (assn) { rd_kafka_topic_partition_list_destroy(assn); assn = nullptr; }
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }

            if (!got) {
                if (assn) rd_kafka_topic_partition_list_destroy(assn);
                auto warn = std::make_unique<RawMsg>();
                warn->is_error = true;
                set_err(warn->err_msg, sizeof(warn->err_msg), "subscribeFromTime: assignment not ready, skip seek");
                pushRaw(std::move(warn));
                lk.lock();
                appliedEpoch = cfgEpoch_;
                return;
            }

            for (int i = 0; i < assn->cnt; ++i) assn->elems[i].offset = ts_ms;

            rd_kafka_resp_err_t oe = rd_kafka_offsets_for_times(rk_, assn, 5000);
            if (oe != RD_KAFKA_RESP_ERR_NO_ERROR) {
                rd_kafka_topic_partition_list_destroy(assn);
                auto warn= std::make_unique<RawMsg>();
                warn->is_error = true;
                set_err(warn->err_msg, sizeof(warn->err_msg), rd_kafka_err2str(oe));
                pushRaw(std::move(warn));
                lk.lock();
                appliedEpoch = cfgEpoch_;
                return;
            }

            rd_kafka_resp_err_t ae = rd_kafka_assign(rk_, assn);
            if (ae != RD_KAFKA_RESP_ERR_NO_ERROR) {
                rd_kafka_topic_partition_list_destroy(assn);
                auto warn = std::make_unique<RawMsg>();
                warn->is_error = true;
                set_err(warn->err_msg, sizeof(warn->err_msg), rd_kafka_err2str(ae));
                pushRaw(std::move(warn));
                lk.lock();
                appliedEpoch = cfgEpoch_;
                return;
            }

            for (int i = 0; i < assn->cnt; ++i) {
                auto& elem = assn->elems[i];
                if (elem.offset == RD_KAFKA_OFFSET_INVALID) continue;

                rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk_, elem.topic, nullptr);
                if (!rkt) continue;

                rd_kafka_resp_err_t se = rd_kafka_seek(rkt, elem.partition, elem.offset, 5000);
                rd_kafka_topic_destroy(rkt);

                if (se != RD_KAFKA_RESP_ERR_NO_ERROR) {
                    auto warn = std::make_unique<RawMsg>();
                    warn->is_error = true;
                    set_err(warn->err_msg, sizeof(warn->err_msg), rd_kafka_err2str(se));
                    pushRaw(std::move(warn));
                }
            }

            rd_kafka_topic_partition_list_destroy(assn);
            lk.lock();
        }

        appliedEpoch = cfgEpoch_;
    };

    while (!stop_.load(std::memory_order_relaxed)) {
        {
            std::unique_lock<std::mutex> lk(cfg_mu_);
            if (appliedEpoch != cfgEpoch_) apply_subscription_locked(lk);
        }

        rd_kafka_message_t* msg = rd_kafka_consumer_poll(rk_, cfg_.poll_ms);
        if (!msg) continue;

        if (msg->err) {
            rd_kafka_message_destroy(msg);
            continue;
        }

        auto rm = std::make_unique<RawMsg>();
        rm->topic = msg->rkt ? rd_kafka_topic_name(msg->rkt) : "";
        if (msg->key && msg->key_len > 0) {
            rm->key.assign((const char*)msg->key, (std::size_t)msg->key_len);
        }
        rm->ts_ns = rd_kafka_message_timestamp(msg, nullptr);

        rm->payload.resize((std::size_t)msg->len);
        if (msg->len > 0) std::memcpy(rm->payload.data(), msg->payload, (std::size_t)msg->len);

        rd_kafka_message_destroy(msg);
        pushRaw(std::move(rm));
    }
}

void KfkpbClient::decodeLoop(std::size_t worker_id) {
    while (!stop_.load(std::memory_order_relaxed)) {
        std::unique_ptr<RawMsg> m;
        if (!popRaw(worker_id, m)) continue;

        if (m->is_error) {
            auto ev = std::make_unique<KfkpbEvent>();
            ev->kind = KfkpbEvent::Kind::Error;
            ev->topic = std::move(m->topic);
            ev->key = std::move(m->key);
            ev->ingest_ns = m->ts_ns;
            std::memcpy(ev->err_msg, m->err_msg, sizeof(ev->err_msg));
            pushEvent(worker_id, std::move(ev));
            continue;
        }

        // Determine msg type by topic mapping
        KfkpbMsgType mt = KfkpbMsgType::Unknown;
        {
            std::lock_guard<std::mutex> lk(cfg_mu_);
            auto it = topicTypes_.find(m->topic);
            if (it != topicTypes_.end()) mt = it->second;
        }

        if (mt == KfkpbMsgType::Unknown) {
            auto ev = std::make_unique<KfkpbEvent>();
            ev->kind = KfkpbEvent::Kind::Error;
            ev->msg_type = KfkpbMsgType::Unknown;
            ev->topic = m->topic;
            ev->key = m->key;
            ev->ingest_ns = m->ts_ns;
            set_err(ev->err_msg, sizeof(ev->err_msg), "unknown topic mapping");
            pushEvent(worker_id, std::move(ev));
            continue;
        }

        // internal registry picks decoder by type
        DecodeFn fn = decoders_.get(mt);

        auto out = std::make_unique<KfkpbEvent>();
        out->kind = KfkpbEvent::Kind::Data;
        out->msg_type = mt;
        out->topic = m->topic;
        out->key = m->key;
        out->ingest_ns = m->ts_ns;

        char em[96]{0};
        bool ok = false;
        try {
            ok = fn(
                (const std::uint8_t*)m->key.data(), m->key.size(),
                m->payload.data(), m->payload.size(),
                m->ts_ns,
                out->data,
                em, sizeof(em)
            );
        } catch (...) {
            set_err(em, sizeof(em), "decoder threw exception");
            ok = false;
        }

        if (!ok) {
            auto err = std::make_unique<KfkpbEvent>();
            err->kind = KfkpbEvent::Kind::Error;
            err->msg_type = mt;
            err->topic = std::move(m->topic);
            err->key = std::move(m->key);
            err->ingest_ns = m->ts_ns;
            if (em[0] == '\0') set_err(err->err_msg, sizeof(err->err_msg), "decode failed");
            else std::memcpy(err->err_msg, em, sizeof(err->err_msg));
            pushEvent(worker_id, std::move(err));
            continue;
        }

        pushEvent(worker_id, std::move(out));
    }
}

void KfkpbClient::notify() {
    char c = 'X';
    ::send(notify_fd_, &c, 1, 0);
}

bool KfkpbClient::popRaw(std::size_t worker_id, std::unique_ptr<RawMsg>& out) {
    auto& q = *raw_qs_[worker_id];
    auto& epoch = *raw_epochs_[worker_id];

    while (!stop_.load()) {
        if (q.try_pop(out)) return true;
        const auto seen = epoch.load();
        std::atomic_wait(&epoch, seen);
    }
    return false;
}

void KfkpbClient::pushRaw(std::unique_ptr<RawMsg> m) {
    const std::size_t worker_id = workerIndexFor(*m);
    auto& q = *raw_qs_[worker_id];

    if (!q.try_push(std::move(m))) {
        std::unique_ptr<RawMsg> tmp;
        q.try_pop(tmp);
        q.try_push(std::move(m));
    }

    auto& epoch = *raw_epochs_[worker_id];
    epoch.fetch_add(1);
    std::atomic_notify_one(&epoch);
}

void KfkpbClient::pushEvent(std::size_t worker_id, std::unique_ptr<KfkpbEvent> ev) {
    auto& q = *evt_qs_[worker_id];

    if (!q.try_push(std::move(ev))) {
        std::unique_ptr<KfkpbEvent> tmp;
        q.try_pop(tmp);
        q.try_push(std::move(ev));
    }

    notify();
}

std::size_t KfkpbClient::workerIndexFor(const RawMsg& msg) {
    if (raw_qs_.empty()) return 0;
    if (!msg.key.empty()) {
        return std::hash<std::string>{}(msg.key) % raw_qs_.size();
    }
    return fallback_rr_.fetch_add(1, std::memory_order_relaxed) % raw_qs_.size();
}
