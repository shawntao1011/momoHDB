#include "sinks/RedPandaSink.hpp"

#include <chrono>
#include <stdexcept>
#include <utility>

#include <librdkafka/rdkafka.h>

#include "../../../momo_common/include/JsonLogger.hpp"

namespace {

constexpr std::size_t kErrStrSize = 512;

void set_conf_or_throw(rd_kafka_conf_t* conf, const std::string& key, const std::string& value) {
    char errstr[kErrStrSize]{};
    if (rd_kafka_conf_set(conf, key.c_str(), value.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        throw std::runtime_error("rdkafka config error for " + key + ": " + errstr);
    }
}

std::int64_t to_timestamp_ms(std::int64_t ts_ns) {
    if (ts_ns <= 0) return 0;
    return ts_ns / 1'000'000;
}

} // namespace

RedPandaSink::RedPandaSink(std::string name, Options options)
    : name_(std::move(name))
{
    if (options.brokers.empty()) {
        throw std::invalid_argument("RedPandaSink requires non-empty brokers");
    }

    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set_opaque(conf, this);
    rd_kafka_conf_set_dr_msg_cb(conf, &RedPandaSink::delivery_report_cb);

    set_conf_or_throw(conf, "bootstrap.servers", options.brokers);
    set_conf_or_throw(conf, "client.id", options.client_id);
    if (!options.compression.empty()) {
        set_conf_or_throw(conf, "compression.type", options.compression);
    }
    if (!options.acks.empty()) {
        set_conf_or_throw(conf, "acks", options.acks);
    }
    if (options.message_timeout_ms) {
        set_conf_or_throw(conf, "message.timeout.ms", std::to_string(*options.message_timeout_ms));
    }
    for (const auto& [key, value] : options.extra_conf) {
        set_conf_or_throw(conf, key, value);
    }

    char errstr[kErrStrSize]{};
    producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!producer_) {
        rd_kafka_conf_destroy(conf);
        throw std::runtime_error(std::string("rdkafka producer init failed: ") + errstr);
    }

    logger::info("sink", "redpanda_ready",
                 {logger::field("name", name_),
                  logger::field("brokers", options.brokers),
                  logger::field("client_id", options.client_id)});
}

RedPandaSink::~RedPandaSink() {
    if (!producer_) return;
    rd_kafka_flush(producer_, 2000);
    rd_kafka_destroy(producer_);
    producer_ = nullptr;
}

void RedPandaSink::submit(std::shared_ptr<const Envelope> e) {
    if (!producer_ || !e) return;

    const std::int64_t timestamp_ms = to_timestamp_ms(e->ts_ns);
    const auto err = rd_kafka_producev(
        producer_,
        RD_KAFKA_V_TOPIC(e->topic.c_str()),
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        RD_KAFKA_V_TIMESTAMP(timestamp_ms),
        RD_KAFKA_V_KEY(e->key.data(), e->key.size()),
        RD_KAFKA_V_VALUE(const_cast<char*>(e->payload.data()), e->payload.size()),
        RD_KAFKA_V_END);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
            poll(10);
        }
        logger::warn("sink", "redpanda_produce_failed",
                     {logger::field("name", name_),
                      logger::field("topic", e->topic),
                      logger::field("key", e->key),
                      logger::field("error", rd_kafka_err2str(err))});
        return;
    }

    poll(0);
}

void RedPandaSink::flush(int timeout_ms) {
    if (!producer_) return;
    rd_kafka_flush(producer_, timeout_ms);
}

void RedPandaSink::delivery_report_cb(rd_kafka_t*, const rd_kafka_message_t* rkmessage, void* opaque) {
    if (!opaque || !rkmessage) return;
    auto* self = static_cast<RedPandaSink*>(opaque);
    if (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) return;
    self->log_delivery_error(*rkmessage);
}

void RedPandaSink::log_delivery_error(const rd_kafka_message_t& message) const {
    logger::error("sink", "redpanda_delivery_failed",
                  {logger::field("name", name_),
                   logger::field("error", rd_kafka_message_errstr(&message)),
                   logger::num("partition", message.partition),
                   logger::num("offset", message.offset)});
}

void RedPandaSink::poll(int timeout_ms) {
    if (!producer_) return;
    rd_kafka_poll(producer_, timeout_ms);
}