#pragma once
#include "runtime/Dispatcher.hpp"

class DemoLogSink final : public ISink {
public:
    explicit DemoLogSink(std::string name):name_(name) {}

    void submit(std::shared_ptr<const Envelope> e) override {
        std::cout << "[" << name_ << "] topic=" << e->topic
                  << " key=" << e->key
                  << " size=" << e->payload.size()
                  << " ts_ns=" << e->ts_ns
                  << "\n";
    }
    void flush(int timeout_ms) override
    {}

private:
    std::string name_;
};
