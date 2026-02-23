#pragma once

#include <Proto/Qot_Common.pb.h>

#include <string>
#include <string_view>

namespace momo::utils {

    inline std::string market_code_to_prefix(int32_t market) {
        const std::string name = Qot_Common::QotMarket_Name(
            static_cast<Qot_Common::QotMarket>(market)
        );

        constexpr std::string_view prefix = "QotMarket_";
        constexpr std::string_view suffix = "_Security";

        if (!name.starts_with(prefix) || !name.ends_with(suffix)) {
            return name;
        }

        const auto begin = prefix.size();
        const auto len = name.size() - prefix.size() - suffix.size();
        if (len == 0) {
            return name;
        }

        return name.substr(begin, len);
    }

    inline std::string make_symbol_key(int32_t market, std::string_view code) {
        std::string symbol;
        const std::string prefix = market_code_to_prefix(market);
        symbol.reserve(prefix.size() + 1 + code.size());
        symbol.append(prefix);
        symbol.push_back('.');
        symbol.append(code);
        return symbol;
    }

    inline std::string build_symbol_from_security(const Qot_Common::Security& security) {
        return make_symbol_key(static_cast<int32_t>(security.market()), security.code());
    }

} // namespace momo::utils