#pragma once

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <chrono>
#include <vector>

#include "kafkax/decoder.h"
#include "momo_utils/market_utils.hpp"

namespace momo::decoders {

    inline void set_err(kafkax_decode_out_t* out, const char* msg) {
        if (!out) return;
            out->kind = KAFKAX_DECODE_ERR;
        if (msg) {
            std::snprintf(out->err_msg, sizeof(out->err_msg), "%s", msg);
            out->err_msg[sizeof(out->err_msg) - 1] = '\0';
        } else {
            out->err_msg[0] = '\0';
        }
    }

    inline void set_err(kafkax_decode_out_t* out, const std::string& msg) {
        set_err(out, msg.c_str());
    }

    inline int write_bytes_to_out(const std::vector<std::uint8_t>& bytes, kafkax_decode_out_t* out) {
        if (!out) return -1;
        out->err_msg[0] = '\0';

        if (bytes.size() > out->cap) {
            out->kind = KAFKAX_DECODE_NEED_MORE;
            out->need = bytes.size();
            out->len = 0;
            return 0;
        }

        if (!out->buf || out->cap == 0) {
            out->kind = KAFKAX_DECODE_ERR;
            std::snprintf(out->err_msg, sizeof(out->err_msg), "output buffer is null/empty");
            out->err_msg[sizeof(out->err_msg) - 1] = '\0';
            return -1;
        }

        std::memcpy(out->buf, bytes.data(), bytes.size());
        out->kind = KAFKAX_DECODE_OK;
        out->len = bytes.size();
        out->need = 0;
        return 0;
    }

} // namespace momo::utils
