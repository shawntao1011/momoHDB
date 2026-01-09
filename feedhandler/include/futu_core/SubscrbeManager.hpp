#pragma once
#include <FTAPI.h>
#include <FTSPI.h>
#include <FTAPIChannel_Define.h>
class FutuQuoteSession;

class SubscribeManager {
  public:
    explicit SubscribeManager(FutuQuoteSession &session);

    void Run();

    void OnReply_Sub(Futu::u32_t nSerialNo, const Qot_Sub::Response &stRsp);
    void OnPush_UpdateBasicQot(const Qot_UpdateBasicQot::Response &stRsp);
    void OnPush_UpdateOrderBook(const Qot_UpdateOrderBook::Response &stRsp);
    void OnPush_UpdateTicker(const Qot_UpdateTicker::Response &stRsp);
    void OnPush_UpdateKL(const Qot_UpdateKL::Response &stRsp);
    void OnPush_UpdateRT(const Qot_UpdateRT::Response &stRsp);
    void OnPush_UpdateBroker(const Qot_UpdateBroker::Response &stRsp);

  private:
    void BuildSubscribeRequest(Qot_Sub::Request &request, bool isSub) const;

    FutuQuoteSession &m_session;
    std::vector<std::string> m_securities;
    std::vector<Qot_Common::SubType> m_subTypes{
        Qot_Common::SubType_Basic,  Qot_Common::SubType_OrderBook,
        Qot_Common::SubType_Broker, Qot_Common::SubType_Ticker,
        Qot_Common::SubType_KL_Day, Qot_Common::SubType_RT};
    bool m_subsuc;
};
