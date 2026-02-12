#pragma once
#include "runtime/Dispatcher.hpp"
#include "JsonLogger.hpp"

class DemoLogSink final : public ISink {
public:
    explicit DemoLogSink(std::string name):name_(name) {}

    void submit(std::shared_ptr<const Envelope> e) override {
        logger::info("sink", "enqueue",
                     {logger::field("name", name_),
                      logger::field("topic", e->topic),
                      logger::field("key", e->key),
                      logger::num("size", e->payload.size()),
                      logger::num("ts_ns", e->ts_ns)});
    }
    void flush(int timeout_ms) override
    {}

private:
    std::string name_;
};
