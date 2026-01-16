#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <librdkafka/rdkafka.h>
#include "runtime/Dispatcher.hpp"

class RedPandaSink final : public ISink {
public:
    struct Options {
        std::string brokers;
        std::string client_id{"futu-feedhandler"};
        std::string compression{"lz4"};
        std::string acks{"all"};
        std::optional<int> message_timeout_ms{};
        std::unordered_map<std::string, std::string> extra_conf{};
    };

    RedPandaSink(std::string name, Options options);
    ~RedPandaSink() override;

    void submit(std::shared_ptr<const Envelope> e) override;
    void flush(int timeout_ms) override;

private:
    static void delivery_report_cb(rd_kafka_t* rk, const rd_kafka_message_t* rkmessage, void* opaque);

    void log_delivery_error(const rd_kafka_message_t& message) const;
    void poll(int timeout_ms);

    std::string name_;
    rd_kafka_t* producer_{nullptr};
};