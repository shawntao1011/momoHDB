#pragma once
#include <string_view>

#include "common/Envelope.hpp"
#include "Proto/Qot_Common.pb.h"

// Central place to map SubType <-> topic names produced by feedhandler.
// Keep topic naming stable and avoid scattering hard-coded strings across modules.
namespace runtime {
    std::string_view topic_for_msgkind(MsgKind kind) noexcept;

    std::string_view topic_for_subtype(Qot_Common::SubType st) noexcept;

} // namespace runtime
