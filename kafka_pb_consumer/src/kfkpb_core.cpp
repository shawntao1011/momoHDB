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

KfkpbClient::KfkpbClient(rd_kafka_t* rk, int notify_fd, ThreadCfg cfg)
    : rk_(rk)
    , notify_fd_(notify_fd)
    , cfg_(cfg)
{
    if (!rk_) throw std::runtime_error("null consumer");
    if (notify_fd_ < 0) throw std::runtime_error("bad notify fd");
    if (cfg_.decode_threads == 0) cfg_.decode_threads = 1;

    dec_ths_.reserve(cfg_.decode_threads);
    for (std::size_t i = 0; i < cfg_.decode_threads; ++i) {
        dec_ths_.emplace_back([this]{ decodeLoop(); });
    }

    consumer_th_ = std::thread([this]{ consumerLoop(); });
}

KfkpbClient::~KfkpbClient() {
    stop_.store(true);

    cfg_cv_.notify_all();
    raw_cv_.notify_all();

    if (consumer_th_.joinable()) consumer_th_.join();
    for (auto& t : dec_ths_) if (t.joinable()) t.join();

    if (rk_) {
        rd_kafka_consumer_close(rk_);
        rd_kafka_destroy(rk_);
        rk_ = nullptr;
    }

    {
        std::lock_guard<std::mutex> lk(evt_mu_);
        evt_q_.clear();
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
    std::lock_guard<std::mutex> lk(evt_mu_);
    while (!evt_q_.empty()) {
        out.push_back(std::move(evt_q_.front()));
        evt_q_.pop_front();
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
            pushEvent(make_error_event(KfkpbMsgType::Unknown, "", "", 0,
                                       std::string("rd_kafka_subscribe failed: ") + rd_kafka_err2str(e)));
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
                pushEvent(make_error_event(KfkpbMsgType::Unknown, "", "", ts_ms,
                                           "subscribeFromTime: assignment not ready, skip seek"));
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
                pushEvent(make_error_event(KfkpbMsgType::Unknown, "", "", ts_ms,
                                           std::string("offsets_for_times failed: ") + rd_kafka_err2str(oe)));
                lk.lock();
                appliedEpoch = cfgEpoch_;
                return;
            }

            // Now assn->elems[i].offset contains the resolved offsets (or RD_KAFKA_OFFSET_INVALID)
            // We assign this explicit assignment then seek.
            rd_kafka_resp_err_t ae = rd_kafka_assign(rk_, assn);
            if (ae != RD_KAFKA_RESP_ERR_NO_ERROR) {
                rd_kafka_topic_partition_list_destroy(assn);
                pushEvent(make_error_event(KfkpbMsgType::Unknown, "", "", ts_ms,
                                           std::string("assign after offsets_for_times failed: ") + rd_kafka_err2str(ae)));
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
                    pushEvent(make_error_event(KfkpbMsgType::Unknown,
                                               e.topic ? e.topic : "",
                                               "", ts_ms,
                                               std::string("seek failed: ") + rd_kafka_err2str(se)));
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

void KfkpbClient::decodeLoop() {
    while (!stop_.load()) {
        RawMsg m;
        if (!popRaw(m)) {
            continue; // either stop or spurious
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
            pushEvent(make_error_event(KfkpbMsgType::Unknown, m.topic, m.key, m.ts_ms,
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
                pushEvent(make_error_event(mt, m.topic, m.key, m.ts_ms,
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
            pushEvent(std::move(ev));
        } catch (const std::exception& e) {
            // Exception safety: never crash worker
            pushEvent(make_error_event(mt, m.topic, m.key, m.ts_ms,
                                       std::string("exception: ") + e.what(),
                                       std::move(m.payload)));
        }
    }
}

void KfkpbClient::notify() {
    char c = 'X';
    ::send(notify_fd_, &c, 1, 0);
}

bool KfkpbClient::popRaw(RawMsg& out) {
    std::unique_lock<std::mutex> lk(raw_mu_);
    raw_cv_.wait(lk, [&]{ return stop_.load() || !raw_q_.empty(); });
    if (raw_q_.empty()) return false;
    out = std::move(raw_q_.front());
    raw_q_.pop_front();
    return true;
}

void KfkpbClient::pushRaw(RawMsg&& m) {
    std::unique_lock<std::mutex> lk(raw_mu_);

    if (raw_q_.size() >= cfg_.max_raw_queue) {
        raw_q_.pop_front();
    }
    raw_q_.push_back(std::move(m));
    lk.unlock();
    raw_cv_.notify_one();
}

void KfkpbClient::pushEvent(KfkpbEvent ev) {
    {
        std::lock_guard<std::mutex> lk(evt_mu_);
        if (evt_q_.size() >= cfg_.max_evt_queue) {
            evt_q_.pop_front();
        }
        evt_q_.push_back(std::move(ev));
    }
    notify();
}

