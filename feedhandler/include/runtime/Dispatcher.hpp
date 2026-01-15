#pragma once
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/Envelope.hpp"
#include "common/SPSCRing.hpp"

class ISink {
public:
    virtual ~ISink() = default;
    virtual void submit(std::shared_ptr<const Envelope> e)  = 0;
    virtual void flush(int timeout_ms) = 0;
};

class QueuedDownstream {
public:
    QueuedDownstream(std::string name, std::unique_ptr<ISink> sink, std::size_t cap_pow2);

    void start();
    void stop();

    void submit(std::shared_ptr<const Envelope> e);
    void flush(int timeout_ms);

private:
    void loop();

    std::string name_;
    std::unique_ptr<ISink> sink_;
    SPSCRing<std::shared_ptr<const Envelope>> q_;

    std::atomic<bool> running_{false};
    std::thread worker_;
};

class Dispatcher {
public:
    explicit Dispatcher(std::vector<std::unique_ptr<QueuedDownstream>> downstreams);
    ~Dispatcher();

    static void submit(void* ctx, Envelope&& e);
    static void flush(void* ctx, int timeout_ms);

private:
    void dispatch(std::shared_ptr<const Envelope> e);
    void flush_all(int  timeout_ms);

    std::vector<std::unique_ptr<QueuedDownstream>> downstreams_;
};
