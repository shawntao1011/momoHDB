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
        std::unordered_set<std::string> bypass_topics;

        WarmupPolicy() = default;

        WarmupMode mode(std::string_view topic) const noexcept {
            // Note: topic strings are small; unordered_set lookup is fine.
            if (bypass_topics.find(std::string(topic)) != bypass_topics.end()) {
                return WarmupMode::Bypass;
            }
            return WarmupMode::Eligible;
        }
    };

} // namespace cfg