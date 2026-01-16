#include "../../include/config/YamlSubscribeConfigLoader.hpp"

YamlSubscribeConfigLoader::YamlSubscribeConfigLoader()
{}

std::expected<SubscribeConfig, std::string>   YamlSubscribeConfigLoader::load(const std::string& path) const {
    YAML::Node root;
    try {
        root = YAML::LoadFile(path);
    } catch (const std::exception& ex) {
        return std::unexpected(std::string("failed to load yaml: ") + ex.what());
    }

    SubscribeConfig cfg;

    if (auto v = root["version"]) {
        if (v.IsScalar()) cfg.version = v.as<int>();
    }

    if (auto r = root["refresh_ms"]) {
        if (r.IsScalar()) cfg.refresh_ms = r.as<int>();
    }

    if (auto d = root["defaults"]) {
        if (d.IsMap()) {
            if (auto sts = d["subtypes"]) {
                auto parsed = parse_security_subtypes(sts, cfg.default_subtypes);
                if (!parsed) {
                    return std::unexpected(parsed.error());
                }
            }
        }
    }

    auto secs = root["securities"];
    cfg.securities.reserve((secs.size()));

    for (std::size_t i = 0; i < secs.size(); ++i) {
        const auto item = secs[i];

        auto sym = item["symbol"];

        SecuritySpec spec;
        const std::string symbol = sym.as<std::string>();
        parse_security(symbol, spec.market, spec.code);

        if (auto st = item["subtypes"]) {
            std::string err;
            parse_security_subtypes(st, spec.subtypes);
        } else {
            spec.subtypes = cfg.default_subtypes;
        }

        cfg.securities.push_back(std::move(spec));
    }

    return cfg;
}

std::expected<void, std::string> YamlSubscribeConfigLoader::parse_security(const std::string& symbol, // HK.00700
        Qot_Common::QotMarket& market_out, // HK -> Qot_Common::QotMarket_HK_Security
        std::string& code_out) const {
    auto pos = symbol.find('.');
    if (pos == std::string::npos || pos == 0 || pos + 1>= symbol.size()) {
        return std::unexpected("invalid security symbol: " + symbol);
    }

    auto mk = symbol.substr(0, pos);
    auto code = symbol.substr(pos + 1);

    Qot_Common::QotMarket market;
    auto parsed_market = parse_market(mk, market);
    if (!parsed_market) {
        return std::unexpected(parsed_market.error());
    }

    market_out = market;
    code_out = code;
    return {};
}

std::expected<void, std::string> YamlSubscribeConfigLoader::parse_market(const std::string& s, Qot_Common::QotMarket& out) const {
    if (s == "HK") { out = Qot_Common::QotMarket_HK_Security; return {}; }
    if (s == "US") { out = Qot_Common::QotMarket_US_Security; return {}; }
    if (s == "SH") { out = Qot_Common::QotMarket_CNSH_Security; return {}; }
    if (s == "SZ") { out = Qot_Common::QotMarket_CNSZ_Security; return {}; }
    return std::unexpected("unknown market: " + s);
}

std::expected<void, std::string> YamlSubscribeConfigLoader::parse_code(const std::string& s, std::string& code) const {
    code = s;
    return {};
}

std::expected<void, std::string> YamlSubscribeConfigLoader::parse_subtype(const std::string& s, Qot_Common::SubType& out)  const {
    if (s == "BasicQot") { out = Qot_Common::SubType_Basic; return {}; }
    if (s == "OrderBook" ) { out = Qot_Common::SubType_OrderBook; return {}; }
    if (s == "Ticker") { out = Qot_Common::SubType_Ticker; return {}; }
    if (s == "KL_1Min") { out = Qot_Common::SubType_KL_1Min; return {}; }
    if (s == "RT") { out = Qot_Common::SubType_RT; return {}; }
    if (s == "Broker") { out = Qot_Common::SubType_Broker; return {}; }
    return std::unexpected("unknown subtype: " + s);
}

std::expected<void, std::string> YamlSubscribeConfigLoader::parse_security_subtypes(const YAML::Node& node,
        std::vector<Qot_Common::SubType>& subtypes) const {
    subtypes.clear();
    if (!node) return std::unexpected("subtypes node missing");
    if (!node.IsSequence()) return std::unexpected("subtypes must be sequence");

    for (const auto& it : node) {
        if (!it.IsScalar()) return std::unexpected("subtype entry must be scalar");
        Qot_Common::SubType st{};
        auto name = it.as<std::string>();
        auto parsed = parse_subtype(name, st);
        if (!parsed) {
            return std::unexpected(parsed.error());
        }
        subtypes.push_back(st);
    }

    return {};
}
