#pragma once
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "common/Envelope.hpp"

namespace cfg {

    enum class WarmupMode { Bypass, Eligible };

    // Configuration-driven warmup policy.
    // - Bypass: do not apply warmup gate for this kind.
    // - Eligible: apply warmup gate for this kind.
    class WarmupPolicy {
    public:
        int window_ms{1000};
        std::size_t enable_threshold{2};

        // Message kinds that should bypass warmup gate.
        std::unordered_set<int> bypass_kinds;

        WarmupPolicy() = default;

        WarmupMode mode(MsgKind k) const {
            return bypass_kinds.count(static_cast<int>(k)) ? WarmupMode::Bypass
                                                           : WarmupMode::Eligible;
        }
    };

} // namespace cfg