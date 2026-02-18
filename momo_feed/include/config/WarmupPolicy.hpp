#pragma once
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace cfg {

    enum class WarmupMode { Bypass, Eligible };

    // Configuration-driven warmup policy.
    // - Bypass: do not apply warmup gate for this topic.
    // - Eligible: apply warmup gate for this topic.
    class WarmupPolicy {
    public:
        int window_ms{1000};
        std::size_t enable_threshold{2};

        // Topics that should bypass warmup gate.
        std::unordered_set<int> bypass_kinds;

        WarmupPolicy() = default;

        WarmupMode mode(MsgKind k) const {
            return bypass_kinds.count(static_cast<int>(k)) ? WarmupMode::Bypass
                                                           : WarmupMode::Eligible;
        }
    };

} // namespace cfg