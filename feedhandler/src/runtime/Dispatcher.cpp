#include "runtime/Dispatcher.hpp"

#include <mutex>

QueuedDownstream::QueuedDownstream(std::string name, std::unique_ptr<ISink> sink)
    : name_(std::move(name))
    , sink_(std::move(sink))
{}

void QueuedDownstream::start() {
    running_.store(true, std::memory_order_release);
    worker_ = std::thread([this]{ this->loop(); });
}
void QueuedDownstream::stop() {
    running_.store(false, std::memory_order_release);
    if (worker_.joinable()) worker_.join();
}

void QueuedDownstream::submit(std::shared_ptr<const Envelope> e) {
    if (!q_.try_push(std::move(e))) {
        std::shared_ptr<const Envelope> dropped;
        (void)q_.try_pop(dropped);
        (void)q_.try_push(std::move(e)); // if capacity is one
    }
}
void QueuedDownstream::flush(int timeout_ms) {
    sink_->flush(timeout_ms);
}

void QueuedDownstream::loop() {
    std::shared_ptr<const Envelope> e;
    while (running_.load(std::memory_order_acquire)) {
        if (q_.try_pop(e)) {
            sink_->submit(std::move(e));
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    // pop msgs remaining in queue
    while (q_.try_pop(e)) {
        sink_->submit(std::move(e));
    }
}

Dispatcher::Dispatcher() = default;
Dispatcher::Dispatcher(std::vector<std::unique_ptr<QueuedDownstream>> downstreams)
    : downstreams_(std::move(downstreams))
{
    for (auto& ds : downstreams_) ds->start();
}

Dispatcher::~Dispatcher() {
    for (auto& ds : downstreams_) {
        ds->stop();
    }
}

void Dispatcher::add_downstream(std::string name, std::unique_ptr<ISink> sink) {
    auto ds = std::make_unique<QueuedDownstream>(std::move(name), std::move(sink));
    {
        std::unique_lock lk(mu_);
        downstreams_.push_back(std::move(ds));
        downstreams_.back()->start();
    }
}

void Dispatcher::submit(void* ctx, Envelope&& e) {
    auto* self = static_cast<Dispatcher*>(ctx);
    auto msg = std::make_shared<Envelope>(std::move(e));
    self->dispatch(std::move(msg));
}
void Dispatcher::flush(void* ctx, int timeout_ms) {
    auto* self = static_cast<Dispatcher*>(ctx);
    self->flush_all(timeout_ms);
}

void Dispatcher::dispatch(std::shared_ptr<const Envelope> e) {
    for (auto& ds : downstreams_) {
        ds->submit(e);
    }
}
void Dispatcher::flush_all(int timeout_ms) {
    for (auto& ds : downstreams_) {
        ds->flush(timeout_ms);
    }
}