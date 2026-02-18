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

        struct MsgKindHash {
            std::size_t operator()(MsgKind k) const noexcept {
                return static_cast<std::size_t>(static_cast<int>(k));
            }
        };
        std::unordered_set<MsgKind, MsgKindHash> bypass_kinds;

        WarmupMode mode(MsgKind k) const noexcept {
            return bypass_kinds.contains(k) ? WarmupMode::Bypass : WarmupMode::Eligible;
        }
    };

} // namespace cfg