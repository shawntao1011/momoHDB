#include "futu_core/SubscrbeManager.hpp"
#include "futu_core/FutuQuoteSession.hpp"
#include <FTAPI.h>
#include <FTSPI.h>
#include <Proto/Qot_Common.pb.h>
#include <Proto/Qot_Sub.pb.h>
#include <Proto/Qot_UpdateBasicQot.pb.h>
#include <chrono>
#include <iostream>
#include <thread>

SubscribeManager::SubscribeManager(FutuQuoteSession &session)
    : m_session(session)
    , m_subsuc(false)
{
    m_securities = {"00700", "00001"};
}

void SubscribeManager::Run()
{
    auto *api = m_session.GetQotApi();
    if (api == nullptr)
    {
         std::cerr << "ERROR: " << __FUNCTION__ << ", retMsg = QotApi not initialized" << std::endl;
        return;   
    }

    Qot_Sub::Request pbSub;
    BuildSubscribeRequest(pbSub, true);
    m_subsuc = false;
    m_session.WaitForReply(api->Sub(pbSub));

    if (m_subsuc)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(20000));

        BuildSubscribeRequest(pbSub, false);
        m_session.WaitForReply(api->Sub(pbSub));
    }
    else
    {
        std::cerr << "ERROR: " << __FUNCTION__ << ", retMsg = Sub failed" << std::endl;
    }       
}

void SubscribeManager::OnReply_Sub(Futu::u32_t nSerialNo, const Qot_Sub::Response &stRsp)
{
    if (stRsp.rettype() == Common::RetType_Succeed)
    {
        m_subsuc = true;
    }
    m_session.NotifyReply();
}
void SubscribeManager::OnPush_UpdateBasicQot(const Qot_UpdateBasicQot::Response &stRsp)
{
    const Qot_UpdateBasicQot::S2C& pbS2C = stRsp.s2c();
    
    if (pbS2C.basicqotlist_size() > 0)
    {
        std::string strCode = pbS2C.basicqotlist(0).security().code();

        std::cout << "OnPush_UpdateBasicQot, (" << strCode << ")" << std::endl;
    }
}
void SubscribeManager::OnPush_UpdateOrderBook(const Qot_UpdateOrderBook::Response &stRsp)
{

}
void SubscribeManager::OnPush_UpdateTicker(const Qot_UpdateTicker::Response &stRsp)
{

}
void SubscribeManager::OnPush_UpdateKL(const Qot_UpdateKL::Response &stRsp)
{

}
void SubscribeManager::OnPush_UpdateRT(const Qot_UpdateRT::Response &stRsp)
{

}
void SubscribeManager::OnPush_UpdateBroker(const Qot_UpdateBroker::Response &stRsp)
{

}

void SubscribeManager::BuildSubscribeRequest(Qot_Sub::Request &request, bool isSub) const
{
    Qot_Sub::C2S* pSubC2S = request.mutable_c2s();
    pSubC2S->clear_securitylist();
    pSubC2S->clear_securitylist();

    for (const auto&sec : m_securities)
    {
        Qot_Common::Security* pSubSec = pSubC2S->add_securitylist();
        pSubSec->set_code(sec);
        pSubSec->set_market(Qot_Common::QotMarket_HK_Security);
    }

    for (const auto subType : m_subTypes)
    {
        pSubC2S->add_subtypelist(subType);
    }

    pSubC2S->set_issuborunsub(isSub);

    pSubC2S->set_issuborunsub(isSub);
}
