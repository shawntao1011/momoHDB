#pragma once

#include <cstdint>
#include <string>

#include "Proto/Qot_Common.pb.h"

struct SecurityId {
    Qot_Common::QotMarket market{};
    std::string code;

    bool operator==(const SecurityId& o) const noexcept {
        return market == o.market && code == o.code;
    }
};

struct SecurityIdHash {
    std::size_t operator()(const SecurityId& k) const noexcept {
        std::size_t h1 = std::hash<int>{}(static_cast<int>(k.market));
        std::size_t h2 = std::hash<std::string>{}(k.code);
        return h1 ^ (h2 << 1);
    }
};

// ---------- Config input shape (human-friendly) ----------
struct SecuritySpec {
    std::string code;
    Qot_Common::QotMarket market{};
    std::vector<Qot_Common::SubType> subtypes;
};

struct SubscribeConfig {
    int version;

    std::vector<Qot_Common::SubType> default_subtypes;
    std::vector<SecuritySpec> securities;
};

// ---------- Canonical state (machine-friendly) ----------
using SubMask = std::uint64_t;
using SubState = std::unordered_map<SecurityId, SubMask, SecurityIdHash>;

// Provide a stable mapping: SubType -> bit
// IMPORTANT: don't rely on enum numeric values unless you are 100% sure they are stable/compact.
inline SubMask subtype_bit(Qot_Common::SubType st) noexcept {
    using ST = Qot_Common::SubType;
    switch (st) {
        case ST::SubType_Basic:     return 1ull << 0;
        case ST::SubType_OrderBook: return 1ull << 1;
        case ST::SubType_Ticker:    return 1ull << 2;
        case ST::SubType_KL_1Min:   return 1ull << 3;
        case ST::SubType_RT:        return 1ull << 4;
        case ST::SubType_Broker:    return 1ull << 5;
        default:                    return 0;
    }
}

// Turn config into canonical state:
// - merge duplicates of same (market, code)
// - treat subtypes as set (order independent)
inline SubState canonicalize(const SubscribeConfig& cfg) {
    SubState st;
    st.reserve(cfg.securities.size());

    for (const auto& s : cfg.securities) {
        SecurityId id{ s.market, s.code };
        SubMask m = 0;
        for (auto sub : s.subtypes) m |= subtype_bit(sub);
        st[id] |= m; // union merge
    }
    return st;
}

// Convert mask back to vector<SubType> for API calls
inline void mask_to_vec(SubMask m, std::vector<Qot_Common::SubType>& out) {
    out.clear();
    using ST = Qot_Common::SubType;
    if (m & (1ull << 0)) out.push_back(ST::SubType_Basic);
    if (m & (1ull << 1)) out.push_back(ST::SubType_OrderBook);
    if (m & (1ull << 2)) out.push_back(ST::SubType_Ticker);
    if (m & (1ull << 3)) out.push_back(ST::SubType_KL_1Min);
    if (m & (1ull << 4)) out.push_back(ST::SubType_RT);
    if (m & (1ull << 5)) out.push_back(ST::SubType_Broker);
}

inline bool st_equal(const SubState& a, const SubState& b) {
    if (a.size() != b.size()) return false;
    for (const auto& [id, am] : a) {
        auto it = b.find(id);
        if (it == b.end()) return false;
        if (it->second != am) return false;
    }
    return true;
}