#include <cstring>
#include "kfkpb_core.hpp"

#include "decoders.hpp"

static KfkpbEvent make_error_event(KfkpbMsgType type,
                                   std::string topic,
                                   std::string key,
                                   int64_t ingest_ms,
                                   std::string reason,
                                   std::vector<std::uint8_t> raw = {}) {
    KfkpbEvent ev;
    ev.kind = KfkpbEvent::Kind::Error;
    ev.msg_type = type;
    ev.topic = std::move(topic);
    ev.key = std::move(key);
    ev.ingest_ms = ingest_ms;
    ev.reason = std::move(reason);
    if (!raw.empty()) {
        ev.payload = std::move(raw);
    }

    return ev;
}

static std::size_t next_pow2(std::size_t v) {
    if (v < 2) return 2;
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    if (sizeof(std::size_t) >= 8) {
        v |= v >> 32;
    }
    return v + 1;
}

KfkpbClient::KfkpbClient(rd_kafka_t* rk, int notify_fd, ThreadCfg cfg)
    : rk_(rk)
    , notify_fd_(notify_fd)
    , cfg_(cfg)
{
    if (!rk_) throw std::runtime_error("null consumer");
    if (notify_fd_ < 0) throw std::runtime_error("bad notify fd");
    if (cfg_.decode_threads == 0) cfg_.decode_threads = 1;

    raw_qs_.reserve(cfg_.decode_threads);

    raw_mus_.resize(cfg_.decode_threads);
    raw_cvs_.resize(cfg_.decode_threads);
    for (std::size_t i = 0; i < cfg_.decode_threads; ++i) {
        raw_mus_[i] = std::make_unique<std::mutex>();
        raw_cvs_[i] = std::make_unique<std::condition_variable>();
    }

    evt_qs_.reserve(cfg_.decode_threads);

    const std::size_t raw_cap = next_pow2(cfg_.max_raw_queue + 1);
    const std::size_t evt_cap = next_pow2(cfg_.max_evt_queue + 1);
    for (std::size_t i = 0; i < cfg_.decode_threads; ++i) {
        raw_qs_.emplace_back(std::make_unique<queue::SPSCQueue<RawMsg>>(raw_cap));
        evt_qs_.emplace_back(std::make_unique<queue::SPSCQueue<KfkpbEvent>>(evt_cap));
    }

    dec_ths_.reserve(cfg_.decode_threads);
    for (std::size_t i = 0; i < cfg_.decode_threads; ++i) {
        dec_ths_.emplace_back([this, i]{ decodeLoop(i); });
    }

    consumer_th_ = std::thread([this]{ consumerLoop(); });
}

KfkpbClient::~KfkpbClient() {
    stop_.store(true);

    cfg_cv_.notify_all();
    for (auto& cv : raw_cvs_) {
        if (cv) cv->notify_all();
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

void KfkpbClient::subscribeFromTime(std::unordered_map<std::string, KfkpbMsgType> topics, std::int64_t ts_ms) {
    {
        std::lock_guard<std::mutex> lk(cfg_mu_);
        topicTypes_ = std::move(topics);
        seekFromMs_ = ts_ms;
        cfgEpoch_++;
    }
    cfg_cv_.notify_all();
}

void KfkpbClient::drainTo(std::vector<KfkpbEvent>& out) {
    if (evt_qs_.empty()) return;

    const std::size_t qn = evt_qs_.size();
    std::size_t start = drain_rr_.fetch_add(1, std::memory_order_relaxed) % qn;
    std::size_t drained = 0;

    for (std::size_t i = 0; i < qn; ++i) {
        const std::size_t idx = (start + i) % qn;
        auto& q = *evt_qs_[idx];
        KfkpbEvent ev;
        while (q.try_pop(ev)) {
            out.push_back(std::move(ev));
            ++drained;
        }
    }

    if (drained == 0) {
        for (std::size_t i = 0; i < qn; ++i) {
            const std::size_t idx = (start + i) % qn;
            auto& q = *evt_qs_[idx];
            KfkpbEvent ev;
            if (q.try_pop(ev)) {
                out.push_back(std::move(ev));
                break;
            }
        }
    }
}

void KfkpbClient::consumerLoop() {
    std::int64_t appliedEpoch = -1;

    auto apply_subscription_locked = [&](std::unique_lock<std::mutex>& lk) {
        // called with cfg_mu_ held
        if (appliedEpoch == cfgEpoch_) return;

        // If no topics, just unsubscribe (optional behavior)
        if (topicTypes_.empty()) {
            rd_kafka_unsubscribe(rk_);
            appliedEpoch = cfgEpoch_;
            return;
        }

        // build topic list
        rd_kafka_topic_partition_list_t* t =
            rd_kafka_topic_partition_list_new((int)topicTypes_.size());
        for (const auto& [topic, _] : topicTypes_) {
            rd_kafka_topic_partition_list_add(t, topic.c_str(), RD_KAFKA_PARTITION_UA);
        }

        rd_kafka_resp_err_t e = rd_kafka_subscribe(rk_, t);
        rd_kafka_topic_partition_list_destroy(t);

        if (e != RD_KAFKA_RESP_ERR_NO_ERROR) {
            // emit error event
            RawMsg err;
            err.is_error = true;
            err.error_type = KfkpbMsgType::Unknown;
            err.error_reason = std::string("rd_kafka_subscribe failed: ") + rd_kafka_err2str(e);
            pushRaw(std::move(err));
            appliedEpoch = cfgEpoch_;
            return;
        }

        // subscribeFromTime: do seek logic in consumer thread
        if (seekFromMs_) {
            const int64_t ts_ms = *seekFromMs_;

            // Wait for assignment to become available (rebalance)
            rd_kafka_topic_partition_list_t* assn = nullptr;
            bool got = false;
            lk.unlock();

            for (int i = 0; i < 60 && !stop_.load(); ++i) { // up to ~3s
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
                // emit warning event
                RawMsg err;
                err.is_error = true;
                err.error_type = KfkpbMsgType::Unknown;
                err.ts_ms = ts_ms;
                err.error_reason = "subscribeFromTime: assignment not ready, skip seek";
                pushRaw(std::move(err));
                lk.lock();
                appliedEpoch = cfgEpoch_;
                return;
            }

            // offsets_for_times expects .offset = timestamp (ms since epoch)
            for (int i = 0; i < assn->cnt; ++i) {
                assn->elems[i].offset = ts_ms;
            }

            rd_kafka_resp_err_t oe = rd_kafka_offsets_for_times(rk_, assn, 5000);
            if (oe != RD_KAFKA_RESP_ERR_NO_ERROR) {
                rd_kafka_topic_partition_list_destroy(assn);
                RawMsg err;
                err.is_error = true;
                err.error_type = KfkpbMsgType::Unknown;
                err.ts_ms = ts_ms;
                err.error_reason = std::string("offsets_for_times failed: ") + rd_kafka_err2str(oe);
                pushRaw(std::move(err));
                lk.lock();
                appliedEpoch = cfgEpoch_;
                return;
            }

            // Now assn->elems[i].offset contains the resolved offsets (or RD_KAFKA_OFFSET_INVALID)
            // We assign this explicit assignment then seek.
            rd_kafka_resp_err_t ae = rd_kafka_assign(rk_, assn);
            if (ae != RD_KAFKA_RESP_ERR_NO_ERROR) {
                rd_kafka_topic_partition_list_destroy(assn);
                RawMsg err;
                err.is_error = true;
                err.error_type = KfkpbMsgType::Unknown;
                err.ts_ms = ts_ms;
                err.error_reason = std::string("assign after offsets_for_times failed: ") + rd_kafka_err2str(ae);
                pushRaw(std::move(err));
                lk.lock();
                appliedEpoch = cfgEpoch_;
                return;
            }

            // seek each partition
            for (int i = 0; i < assn->cnt; ++i) {
                auto& e = assn->elems[i];
                if (e.offset == RD_KAFKA_OFFSET_INVALID) {
                    // partition has no messages >= ts_ms, skip
                    continue;
                }

                rd_kafka_topic_t* rkt = rd_kafka_topic_new(rk_, e.topic, nullptr);
                if (!rkt) {
                    // emit warning/error if you want
                    continue;
                }

                rd_kafka_resp_err_t se = rd_kafka_seek(rkt, e.partition, e.offset, 5000);
                rd_kafka_topic_destroy(rkt);

                if (se != RD_KAFKA_RESP_ERR_NO_ERROR) {
                    // emit warning but keep going
                    RawMsg err;
                    err.is_error = true;
                    err.error_type = KfkpbMsgType::Unknown;
                    err.topic = e.topic ? e.topic : "";
                    err.ts_ms = ts_ms;
                    err.error_reason = std::string("seek failed: ") + rd_kafka_err2str(se);
                    pushRaw(std::move(err));
                }
            }

            rd_kafka_topic_partition_list_destroy(assn);

            lk.lock();
        }

        appliedEpoch = cfgEpoch_;
    };

    while (!stop_.load()) {
        // Apply subscription changes (only consumer thread touches subscribe/seek)
        {
            std::unique_lock<std::mutex> lk(cfg_mu_);
            if (appliedEpoch != cfgEpoch_) {
                apply_subscription_locked(lk);
            }
        }

        // Poll message
        rd_kafka_message_t* msg = rd_kafka_consumer_poll(rk_, cfg_.poll_ms);
        if (!msg) continue;

        if (msg->err) {
            // You may want to push error events for certain errors; ignore benign timeouts
            rd_kafka_message_destroy(msg);
            continue;
        }

        RawMsg rm;
        rm.topic = msg->rkt ? rd_kafka_topic_name(msg->rkt) : "";
        if (msg->key && msg->key_len > 0) {
            rm.key.assign((const char*)msg->key, (size_t)msg->key_len);
        }
        rm.ts_ms = rd_kafka_message_timestamp(msg, nullptr);

        rm.payload.resize((size_t)msg->len);
        if (msg->len > 0) {
            std::memcpy(rm.payload.data(), msg->payload, (size_t)msg->len);
        }

        rd_kafka_message_destroy(msg);

        // Push to raw queue (backpressure handled in pushRaw)
        pushRaw(std::move(rm));
    }
}

void KfkpbClient::decodeLoop(std::size_t worker_id) {
    while (!stop_.load()) {
        RawMsg m;
        if (!popRaw(worker_id, m)) {
            continue; // either stop or spurious
        }

        if (m.is_error) {
            pushEvent(worker_id, make_error_event(m.error_type, m.topic, m.key, m.ts_ms,
                                                  std::move(m.error_reason),
                                                  std::move(m.payload)));
            continue;
        }

        // Determine message type by topic mapping
        KfkpbMsgType mt = KfkpbMsgType::Unknown;
        {
            std::lock_guard<std::mutex> lk(cfg_mu_);
            auto it = topicTypes_.find(m.topic);
            if (it != topicTypes_.end()) mt = it->second;
        }

        // Unknown topic => emit error event with raw bytes (optional)
        if (mt == KfkpbMsgType::Unknown) {
            pushEvent(worker_id, make_error_event(KfkpbMsgType::Unknown, m.topic, m.key, m.ts_ms,
                                                  "unknown topic mapping (topic not in topics dict)",
                                                  std::move(m.payload)));
            continue;
        }

        // Decode + build table event
        std::string derr;

        try {
            KfkpbEvent::Payload payload;

            switch (mt) {
                case KfkpbMsgType::Ticker: {
                    TickerBatch b;
                    if (!decode_qot_update_ticker(m.payload.data(), m.payload.size(), m.ts_ms, b, derr)) {
                        break;
                    }
                    payload = std::move(b);
                    break;
                }
                case KfkpbMsgType::OrderBook: {
                    OrderBookBatch b;
                    if (!decode_qot_update_orderbook(m.payload.data(), m.payload.size(), m.ts_ms, b, derr)) {
                        break;
                    }
                    payload = std::move(b);
                    break;
                }
                case KfkpbMsgType::BasicQuote: {
                    BasicQuoteBatch b;
                    if (!decode_qot_update_basicquote(m.payload.data(), m.payload.size(), m.ts_ms, b, derr)) {
                        break;
                    }
                    payload = std::move(b);
                    break;
                }
                case KfkpbMsgType::Kline1M: {
                    KL1MinBatch b;
                    if (!decode_qot_update_kl1min(m.payload.data(), m.payload.size(), m.ts_ms, b, derr)) {
                        break;
                    }
                    payload = std::move(b);
                    break;
                }
                default:
                    derr = "unsupported msg type";
                    break;
            }

            if (!derr.empty()) {
                // decode failure: emit error event. You can attach raw payload for debugging.
                pushEvent(worker_id, make_error_event(mt, m.topic, m.key, m.ts_ms,
                                                      derr,
                                                      std::move(m.payload)));
                continue;
            }

            KfkpbEvent ev;
            ev.kind = KfkpbEvent::Kind::Data;
            ev.msg_type = mt;
            ev.topic = m.topic;
            ev.key = m.key;
            ev.ingest_ms = m.ts_ms;
            ev.payload = std::move(payload);
            pushEvent(worker_id, std::move(ev));
        } catch (const std::exception& e) {
            // Exception safety: never crash worker
            pushEvent(worker_id, make_error_event(mt, m.topic, m.key, m.ts_ms,
                                                  std::string("exception: ") + e.what(),
                                                  std::move(m.payload)));
        }
    }
}

void KfkpbClient::notify() {
    char c = 'X';
    ::send(notify_fd_, &c, 1, 0);
}

bool KfkpbClient::popRaw(std::size_t worker_id, RawMsg& out) {
    std::unique_lock<std::mutex> lk(*raw_mus_[worker_id]);
    auto& q = *raw_qs_[worker_id];

    raw_cvs_[worker_id]->wait(lk, [&]{
        return stop_.load() || q.try_pop(out);
    });

    return !stop_.load();
}

void KfkpbClient::pushRaw(RawMsg&& m) {
    const std::size_t worker_id = workerIndexFor(m);
    auto& q = *raw_qs_[worker_id];
    if (!q.try_push(std::move(m))) {
        RawMsg tmp;
        q.try_pop(tmp);
        q.try_push(std::move(m));
    }

    {
        std::lock_guard<std::mutex> lk(*raw_mus_[worker_id]);
    }
    raw_cvs_[worker_id]->notify_one();
}

void KfkpbClient::pushEvent(std::size_t worker_id, KfkpbEvent ev) {
    auto& q = *evt_qs_[worker_id];
    if (!q.try_push(std::move(ev))) {
        KfkpbEvent tmp;
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