# pragma once
#include "Subscription.hpp"
#include <yaml-cpp/yaml.h>

class YamlSubscribeConfigLoader {
public:
    explicit YamlSubscribeConfigLoader();

    SubscribeConfig load(const std::string& path) const;

private:
    bool parse_security(const std::string& symbol, // HK.00700
        Qot_Common::QotMarket& market_out, // HK -> Qot_Common::QotMarket_HK_Security
        std::string& code_out) const; // 00700
    bool parse_market(const std::string& s, Qot_Common::QotMarket& out) const;
    bool parse_code(const std::string& s, std::string& code) const;
    bool parse_subtype(const std::string& s, Qot_Common::SubType& out) const;

    bool parse_security_subtypes(const YAML::Node& node,
        std::vector<Qot_Common::SubType>& subtypes) const;
};
