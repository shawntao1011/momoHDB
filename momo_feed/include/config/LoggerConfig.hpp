#pragma once

#include <cstddef>
#include <string>

#include "OverflowPolicy.hpp"

namespace cfg {

    struct LoggerCfg {
        std::string file_path{"logs/app.jsonl"};
        bool also_console{true};
        std::string level{"Info"};

        std::size_t queue_size{1u << 16};
        std::size_t worker_threads{1};
        OverflowPolicy overflow{OverflowPolicy::DropOldest};

        bool json{true};

        std::string flush_on{"warn"};
    };

} // namespace cfg