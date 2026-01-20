#pragma once
#include <expected>
#include <string>

#include "Config.hpp"
#include "ConfigError.hpp"
#include "common/JsonLogger.hpp"

namespace cfg {

    class ConfigLoader {
    public:
        static std::expected<cfg::AppConfig, ConfigError>
        load(const std::string& path);
    };

} // namespace cfg