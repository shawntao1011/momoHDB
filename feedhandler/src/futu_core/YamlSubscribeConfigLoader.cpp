# include "futu_core/YamlSubscribeConfigLoader.hpp"

YamlSubscribeConfigLoader::YamlSubscribeConfigLoader()
{}

SubscribeConfig YamlSubscribeConfigLoader::load(const std::string& path) const {
    YAML::Node root;
    root = YAML::LoadFile(path);

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
                parse_security_subtypes(sts, cfg.default_subtypes);
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
}

bool YamlSubscribeConfigLoader::parse_security(const std::string& symbol, // HK.00700
        Qot_Common::QotMarket& market_out, // HK -> Qot_Common::QotMarket_HK_Security
        std::string& code_out) const {
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

bool YamlSubscribeConfigLoader::parse_market(const std::string& s, Qot_Common::QotMarket& out) const {
    if (s == "HK") { out = Qot_Common::QotMarket_HK_Security; return true; }
    if (s == "US") { out = Qot_Common::QotMarket_US_Security; return true; }
    if (s == "SH") { out = Qot_Common::QotMarket_CNSH_Security; return true; }
    if (s == "SZ") { out = Qot_Common::QotMarket_CNSZ_Security; return true; }
    return false;
}

bool YamlSubscribeConfigLoader::parse_code(const std::string& s, std::string& code) const {
    code = s; return true;
}

bool YamlSubscribeConfigLoader::parse_subtype(const std::string& s, Qot_Common::SubType& out)  const {
    if (s == "BasicQot") { out = Qot_Common::SubType_Basic; return true; }
    if (s == "OrderBook" ) { out = Qot_Common::SubType_OrderBook; return true; }
    if (s == "Ticker") { out = Qot_Common::SubType_Ticker; return true; }
    if (s == "KL_1M") { out = Qot_Common::SubType_KL_1Min; return true; }
    if (s == "RT") { out = Qot_Common::SubType_RT; return true; }
    if (s == "Broker") { out = Qot_Common::SubType_Broker; return true; }
    return false;
}

bool YamlSubscribeConfigLoader::parse_security_subtypes(const YAML::Node& node,
        std::vector<Qot_Common::SubType>& subtypes) const {
    subtypes.clear();
    if (!node) return false;
    if (!node.IsSequence()) return false;

    for (const auto& it : node) {
        if (it.IsScalar()) return false;
        Qot_Common::SubType st{};
        auto name = it.as<std::string>();
        if (!parse_subtype(name, st)) return false;
        subtypes.push_back(st);
    }

    return true;
}
