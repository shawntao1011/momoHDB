#pragma once
#include "runtime/Dispatcher.hpp"
#include "common/JsonLogger.hpp"
#include "runtime/TopicRegistry.hpp"

class DemoLogSink final : public ISink {
public:
    explicit DemoLogSink(std::string name):name_(name) {}

    void submit(std::shared_ptr<const Envelope> e) override {
        logger::info("sink", "enqueue",
                     {logger::field("name", name_),
                      logger::field("topic", runtime::topic_for_msgkind(e->kind)),
                      logger::field("key", e->symbol),
                      logger::num("size", e->payload.size()),
                      logger::num("ingest_time_ns", e->ingest_time_ms)});
    }
    void flush(int timeout_ms) override
    {}

private:
    std::string name_;
};
