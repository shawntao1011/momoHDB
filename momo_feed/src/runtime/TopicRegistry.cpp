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

    MsgKind msgkind_for_yamlcfg(std::string_view tp) noexcept {
        if (tp == "BasicQot") return MsgKind::BasicQuote;
        if (tp == "OrderBook") return MsgKind::OrderBook;
        if (tp == "Ticker") return MsgKind::Ticker;
        if (tp == "KL1Min") return MsgKind::KL1Min;
        if (tp == "RT") return MsgKind::RT;
        if (tp == "Broker") return MsgKind::Broker;
        return MsgKind::Unknown;
    }

    MsgKind msgkind_for_subtype(Qot_Common::SubType st) noexcept {
        using ST = Qot_Common::SubType;
        switch (st) {
            case ST::SubType_Basic:     return MsgKind::BasicQuote;
            case ST::SubType_OrderBook: return MsgKind::OrderBook;
            case ST::SubType_Ticker:    return MsgKind::Ticker;
            case ST::SubType_KL_1Min:   return MsgKind::KL1Min;
            case ST::SubType_RT:        return MsgKind::RT;
            case ST::SubType_Broker:    return MsgKind::Broker;
            default:                    return MsgKind::BasicQuote;
        }
    }

} // namespace runtime
