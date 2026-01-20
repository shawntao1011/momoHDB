#pragma once
#include <atomic>
#include <cstddef>
#include <stdexcept>
#include <type_traits>
#include <vector>

namespace queue {

enum class OverflowPolicy {
    Block,
    DropOldest,
};

template <class T>
class SPSCRing {
public:
    explicit SPSCRing(std::size_t capacity_pow2)
        : cap_(capacity_pow2)
        , mask_(capacity_pow2 - 1)
        , buf_(capacity_pow2)
    {
        if (cap_ < 2 || (cap_ & mask_) != 0) {
            throw std::runtime_error("SpscRing capacity must be power-of-two >= 2");
        }
    }

    SPSCRing(const SPSCRing&) = delete;
    SPSCRing& operator=(const SPSCRing&) = delete;

    bool try_push(T&& v) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t next = (head + 1) & mask_;

        if (next == tail_.load(std::memory_order_acquire)) {
            return false;
        }

        buf_[head] = std::move(v);
        head_.store(next, std::memory_order_release);
        return true;
    }

    bool try_push(const T& v) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t next = (head + 1) & mask_;

        if (next == tail_.load(std::memory_order_acquire)) {
            return false;
        }

        buf_[head] = v;
        head_.store(next, std::memory_order_release);
        return true;
    }

    bool try_pop(T& out) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_.load(std::memory_order_acquire)) {
            return false; // empty
        }

        out = std::move(buf_[tail]);
        tail_.store((tail + 1) & mask_, std::memory_order_release);
        return true;
    }

    template <class VecT>
    std::size_t pop_many(VecT& out, std::size_t max_n) {
        static_assert(std::is_same_v<typename VecT::value_type, T>,
                      "pop_many out must be vector<T>");

        std::size_t n = 0;
        T tmp;
        while (n < max_n && try_pop(tmp)) {
            out.push_back(std::move(tmp));
            ++n;
        }
        return n;
    }

    std::size_t capacity() const { return cap_; }

private:
    const std::size_t cap_;
    const std::size_t mask_;
    std::vector<T> buf_;

    alignas(64) std::atomic<std::size_t> head_{0};
    alignas(64) std::atomic<std::size_t> tail_{0};
};

} //namespace queue