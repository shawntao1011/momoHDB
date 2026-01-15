#pragma once

#include "Subscription.hpp"

struct DiffItem {
    SecurityId id;
    SubMask mask{};
};

struct SubscriptionTools {
    // to subscribe (add)
    std::vector<DiffItem> add_security;
    // to unsubscribe (remove)
    std::vector<DiffItem> remove_security;

    // same security: subtypes modification
    std::vector<DiffItem> add_subtypes; // mask = new & ~old
    std::vector<DiffItem> del_subtypes; // mask = old & ~new
};

inline SubscriptionTools diff_state(const SubState& oldS, const SubState& newS) {
    SubscriptionTools d;

    for (const auto& [id, oldMask] : oldS) {
        auto it = newS.find(id);
        if (it == newS.end()) {
            d.remove_security.push_back({id, oldMask});
            continue;
        }
        const auto newMask = it->second;
        const auto add = newMask & ~oldMask;
        const auto del = oldMask & ~newMask;
        if (add) d.add_subtypes.push_back({id, add});
        if (del) d.del_subtypes.push_back({id, del});
    }

    for (const auto& [id, newMask] : newS) {
        if (oldS.find(id) == oldS.end()) {
            d.add_security.push_back({id, newMask});
        }
    }

    return d;
}