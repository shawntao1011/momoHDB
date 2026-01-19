#pragma once
#include <string>

struct ConfigError {
    std::string file;
    std::string path;
    std::string message;
};

inline std::string to_string(const ConfigError& e) {
    return e.file + ":" + e.path + " -> " + e.message;
}