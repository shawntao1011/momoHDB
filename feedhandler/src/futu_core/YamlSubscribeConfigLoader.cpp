# include "futu_core/YamlSubscribeConfigLoader.hpp"

YamlSubscribeConfigLoader::YamlSubscribeConfigLoader()
{}

SubscribeConfig YamlSubscribeConfigLoader::load(const std::string& path) const {
    YAML::Node root;
    try {
        root = YAML::
    }
}

bool YamlSubscribeConfigLoader::parse_security(const std::string& symbol, // HK.00700
        Qot_Common::QotMarket& market_out, // HK -> Qot_Common::QotMarket_HK_Security
        std::string& code_out) {
    auto pos = symbol.find('.');
    if (pos == std::string::npos || pos == 0 || pos + 1>= symbol.size()) return false;

    auto mk = symbol.substr(0, pos);
    auto code = mk.substr(pos + 1);

    Qot_Common::QotMarket market;
    if (!parse_market(mk, market)) return false;

    market_out = market;
    code_out = code;
    return true;
}

bool YamlSubscribeConfigLoader::parse_market(const std::string& s, Qot_Common::QotMarket& out) {
    if (s == "HK") { out = Qot_Common::QotMarket_HK_Security; return true; }
    if (s == "US") { out = Qot_Common::QotMarket_US_Security; return true; }
    if (s == "SH") { out = Qot_Common::QotMarket_CNSH_Security; return true; }
    if (s == "SZ") { out = Qot_Common::QotMarket_CNSZ_Security; return true; }
    return false;
}

bool YamlSubscribeConfigLoader::parse_code(const std::string& s, std::string& code) {
    code = s; return true;
}

bool YamlSubscribeConfigLoader::parse_subtype(const std::string& s, Qot_Common::SubType& out) {
    if (s == "BasicQot") { out = Qot_Common::SubType_Basic; return true; }
    if (s == "OrderBook" ) { out = Qot_Common::SubType_OrderBook; return true; }
    if (s == "Ticker") { out = Qot_Common::SubType_Ticker; return true; }
    if (s == "KL_1M") { out = Qot_Common::SubType_KL_1Min; return true; }
    if (s == "RT") { out = Qot_Common::SubType_RT; return true; }
    if (s == "Broker") { out = Qot_Common::SubType_Broker; return true; }
    return false;
}

bool YamlSubscribeConfigLoader::parse_security_types(const YAML::Node& node,
        std::vector<Qot_Common::SubType>& subtypes) {
    subtypes.clear();
    if (!node) return false;
    if (!node.IsSequence()) return false;

    for (const auto& it : node) {
        if (it.isScalar()) return false;
        Qot_Common::SubType st{};
        auto name = it.as<std::string>;
        if (!parse_subtype(name, st)) return false;
        subtypes.push_back(st);
    }
}
