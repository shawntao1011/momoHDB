#include "runtime/TopicRegistry.hpp"

namespace runtime {

    std::string_view topic_for_msgkind(MsgKind kind) noexcept {
        switch (kind) {
            case MsgKind::BasicQuote:   return "futu.basicqot.pb";
            case MsgKind::OrderBook:    return "futu.orderbook.pb";
            case MsgKind::Ticker:       return "futu.ticker.pb";
            case MsgKind::KL1Min:       return "futu.kl1min.pb";
            case MsgKind::RT:           return "futu.rt.pb";
            case MsgKind::Broker:       return "futu.broker.pb";
            default:                    return {};
        }
    }

    std::string_view topic_for_subtype(Qot_Common::SubType st) noexcept {
        using ST = Qot_Common::SubType;
        switch (st) {
            case ST::SubType_Basic:     return "futu.basicqot.pb";
            case ST::SubType_OrderBook: return "futu.orderbook.pb";
            case ST::SubType_Ticker:    return "futu.ticker.pb";
            case ST::SubType_KL_1Min:   return "futu.kl1min.pb";
            case ST::SubType_RT:        return "futu.rt.pb";
            case ST::SubType_Broker:    return "futu.broker.pb";
            default:                    return {};
        }
    }

} // namespace runtime
