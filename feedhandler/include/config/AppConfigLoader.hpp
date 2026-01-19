#pragma once
#include <expected>
#include <string>

#include "AppConfig.hpp"
#include "ConfigError.hpp"

class AppConfigLoader {
public:
    static std::expected<AppConfig, ConfigError>
    load(const std::string& path);
};
