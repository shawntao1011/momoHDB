# pragma once
#include "futu_core/Subscription.hpp"
#include <expected>
#include <yaml-cpp/yaml.h>

namespace cfg {

    class SubscriptionLoader {
    public:
        explicit SubscriptionLoader();

        std::expected<SubscribeConfig, std::string> load(const std::string& path) const;

    private:
        std::expected<void, std::string> parse_security(const std::string& symbol, // HK.00700
            Qot_Common::QotMarket& market_out, // HK -> Qot_Common::QotMarket_HK_Security
            std::string& code_out) const; // 00700
        std::expected<void, std::string> parse_market(const std::string& s, Qot_Common::QotMarket& out) const;
        std::expected<void, std::string> parse_code(const std::string& s, std::string& code) const;
        std::expected<void, std::string> parse_subtype(const std::string& s, Qot_Common::SubType& out) const;

        std::expected<void, std::string> parse_security_subtypes(const YAML::Node& node,
            std::vector<Qot_Common::SubType>& subtypes) const;
    };
}// namespace cfg