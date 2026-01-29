#include <cstring>
#include "kfkpb_core.hpp"
#include "decoders.hpp"

static inline K kdb_ts_from_ms_epoch(int64_t ms_epoch) {
    if (ms_epoch <= 0) return ktj(-KP, nj);
    constexpr int64_t DAYS_1970_TO_2000 = 10957LL;
    constexpr int64_t NS_PER_DAY = 86400LL * 1000000000LL;
    int64_t ns_epoch = ms_epoch * 1000000LL;
    int64_t ns2000 = ns_epoch - DAYS_1970_TO_2000 * NS_PER_DAY;
    return ktj(-KP, ns2000);
}

static inline K empty_table() {
    return xT(xD(ktn(KS,0), knk(0)));
}

static inline const char* type_to_sym(KfkpbMsgType t) {
    switch (t) {
        case KfkpbMsgType::Ticker: return "ticker";
        case KfkpbMsgType::OrderBook: return "orderbook";
        case KfkpbMsgType::BasicQuote: return "basicquote";
        case KfkpbMsgType::Kline1M: return "kl1min";
        default: return "unknown";
    }
}

static K build_event_dict(const char* etype,
                          const char* type,
                          const std::string& topic,
                          const std::string& key,
                          int64_t ingest_ms,
                          K data,
                          const std::string& reason) {
    K keys = ktn(KS, 7);
    kS(keys)[0]=ss("etype");
    kS(keys)[1]=ss("type");
    kS(keys)[2]=ss("topic");
    kS(keys)[3]=ss("key");
    kS(keys)[4]=ss("ingestTime");
    kS(keys)[5]=ss("data");
    kS(keys)[6]=ss("reason");

    K vals = knk(7);
    kK(vals)[0]=ks((S)etype);
    kK(vals)[1]=ks((S)type);
    kK(vals)[2]=ks((S)topic.c_str());
    kK(vals)[3]=ks((S)key.c_str());
    kK(vals)[4]=kdb_ts_from_ms_epoch(ingest_ms);
    kK(vals)[5]=data;
    kK(vals)[6]=ks((S)reason.c_str());

    return xD(keys, vals);
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
        for (auto& x : evt_q_) r0(x);
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

void KfkpbClient::drainTo(std::vector<K>& out) {
    std::lock_guard<std::mutex> lk(evt_mu_);
    while (!evt_q_.empty()) {
        out.push_back(evt_q_.front());
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
            K ev = build_event_dict("error", "unknown", "", "", 0,
                                    empty_table(),
                                    std::string("rd_kafka_subscribe failed: ") + rd_kafka_err2str(e));
            pushEvent(ev);
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
                K ev = build_event_dict("error", "unknown", "", "", ts_ms,
                                        empty_table(),
                                        "subscribeFromTime: assignment not ready, skip seek");
                pushEvent(ev);
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
                K ev = build_event_dict("error", "unknown", "", "", ts_ms,
                                        empty_table(),
                                        std::string("offsets_for_times failed: ") + rd_kafka_err2str(oe));
                pushEvent(ev);
                lk.lock();
                appliedEpoch = cfgEpoch_;
                return;
            }

            // Now assn->elems[i].offset contains the resolved offsets (or RD_KAFKA_OFFSET_INVALID)
            // We assign this explicit assignment then seek.
            rd_kafka_resp_err_t ae = rd_kafka_assign(rk_, assn);
            if (ae != RD_KAFKA_RESP_ERR_NO_ERROR) {
                rd_kafka_topic_partition_list_destroy(assn);
                K ev = build_event_dict("error", "unknown", "", "", ts_ms,
                                        empty_table(),
                                        std::string("assign after offsets_for_times failed: ") + rd_kafka_err2str(ae));
                pushEvent(ev);
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
                    K ev = build_event_dict("error", "unknown",
                                            e.topic ? e.topic : "",
                                            "", ts_ms,
                                            empty_table(),
                                            std::string("seek failed: ") + rd_kafka_err2str(se));
                    pushEvent(ev);
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
            K raw = ktn(KG, (J)m.payload.size());
            if (!m.payload.empty()) std::memcpy(kG(raw), m.payload.data(), m.payload.size());

            K ev = build_event_dict("error", "unknown", m.topic, m.key, m.ts_ms,
                                    raw,
                                    "unknown topic mapping (topic not in topics dict)");
            pushEvent(ev);
            continue;
        }

        // Decode + build table event
        std::string derr;

        try {
            K data_tbl = nullptr;

            switch (mt) {
                case KfkpbMsgType::Ticker: {
                    TickerBatch b;
                    if (!decode_qot_update_ticker(m.payload.data(), m.payload.size(), m.ts_ms, b, derr)) {
                        break;
                    }
                    // TODO: replace with your real to_table(b, m.ts_ms)
                    // data_tbl = to_table(b, m.ts_ms);
                    data_tbl = empty_table();
                    break;
                }
                case KfkpbMsgType::OrderBook: {
                    OrderBookBatch b;
                    if (!decode_qot_update_orderbook(m.payload.data(), m.payload.size(), m.ts_ms, b, derr)) {
                        break;
                    }
                    // data_tbl = to_table(b, m.ts_ms);
                    data_tbl = empty_table();
                    break;
                }
                case KfkpbMsgType::BasicQuote: {
                    BasicQuoteBatch b;
                    if (!decode_qot_update_basicquote(m.payload.data(), m.payload.size(), m.ts_ms, b, derr)) {
                        break;
                    }
                    // data_tbl = to_table(b, m.ts_ms);
                    data_tbl = empty_table();
                    break;
                }
                case KfkpbMsgType::Kline1M: {
                    KL1MinBatch b;
                    if (!decode_qot_update_kl1min(m.payload.data(), m.payload.size(), m.ts_ms, b, derr)) {
                        break;
                    }
                    // data_tbl = to_table(b, m.ts_ms);
                    data_tbl = empty_table();
                    break;
                }
                default:
                    derr = "unsupported msg type";
                    break;
            }

            if (!derr.empty()) {
                // decode failure: emit error event. You can attach raw payload for debugging.
                K raw = ktn(KG, (J)m.payload.size());
                if (!m.payload.empty()) std::memcpy(kG(raw), m.payload.data(), m.payload.size());

                K ev = build_event_dict("error", type_to_sym(mt), m.topic, m.key, m.ts_ms,
                                        raw,
                                        derr);
                pushEvent(ev);
                continue;
            }

            if (!data_tbl) data_tbl = empty_table();

            K ev = build_event_dict("data", type_to_sym(mt), m.topic, m.key, m.ts_ms,
                                    data_tbl,
                                    "");
            pushEvent(ev);
        } catch (const std::exception& e) {
            // Exception safety: never crash worker
            K raw = ktn(KG, (J)m.payload.size());
            if (!m.payload.empty()) std::memcpy(kG(raw), m.payload.data(), m.payload.size());

            K ev = build_event_dict("error", type_to_sym(mt), m.topic, m.key, m.ts_ms,
                                    raw,
                                    std::string("exception: ") + e.what());
            pushEvent(ev);
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

void KfkpbClient::pushEvent(K ev) {
    {
        std::lock_guard<std::mutex> lk(evt_mu_);
        if (evt_q_.size() >= cfg_.max_evt_queue) {
            r0(evt_q_.front());
            evt_q_.pop_front();
        }
        evt_q_.push_back(ev);
    }
    notify();
}

