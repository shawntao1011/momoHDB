#pragma once
#include <stdexcept>
#include <string_view>

namespace cfg {

    enum class OverflowPolicy {
        Block,
        DropOldest,
    };

    inline OverflowPolicy parse_overflow(std::string_view s) {
        if (s == "block")        return OverflowPolicy::Block;
        if (s == "drop_oldest")  return OverflowPolicy::DropOldest;

        throw std::runtime_error(
        "invalid overflow policy: '" + std::string(s) +
        "' (expected: block | drop_oldest | drop_newest)"
        );
    }

} //namespace cfg