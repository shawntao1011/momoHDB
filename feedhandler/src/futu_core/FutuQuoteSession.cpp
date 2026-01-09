#include "futu_core/FutuQuoteSession.hpp"
#include "futu_core/SubscrbeManager.hpp"
#include <cstdint>
#include <iostream>

FutuQuoteSession::FutuQuoteSession()
    : m_pQotApi((Futu::FTAPI::CreateQotApi()))
    , m_bQotInitSuc(false)
    , m_hasReply(false)
    , m_subscribeManager(nullptr)
{
    m_pQotApi->RegisterQotSpi(this);
    m_pQotApi->RegisterConnSpi(this);
}
FutuQuoteSession::~FutuQuoteSession()
{
    UnInitQot();
}

bool FutuQuoteSession::InitQot(const char *szIP, uint16_t nPort)
{
    if (!m_bQotInitSuc && m_pQotApi != nullptr)
    {
        m_pQotApi->InitConnect(szIP, nPort, false);
        WaitForReply(1);
    }
    return m_bQotInitSuc;
}
void FutuQuoteSession::UnInitQot()
{
    if (m_pQotApi != nullptr)
    {
        m_pQotApi->UnregisterQotSpi();
        m_pQotApi->UnregisterConnSpi();
        Futu::FTAPI::ReleaseQotApi(m_pQotApi);
        m_pQotApi = nullptr;
        m_bQotInitSuc = false;
    }
}

void FutuQuoteSession::WaitForReply(int32_t nSerilNo)
{
    if (nSerilNo != 0)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cv.wait(lock, [this]() { return m_hasReply; });
        m_hasReply = false;
    }
}
void FutuQuoteSession::NotifyReply()
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_hasReply = true;
    }
    m_cv.notify_one();
}

Futu::FTAPI_Qot* FutuQuoteSession::GetQotApi() const
{
    return m_pQotApi;
}

void FutuQuoteSession::SetSubscribeManager(SubscribeManager* manager)
{
    m_subscribeManager = manager;
}

void FutuQuoteSession::OnInitConnect(Futu::FTAPI_Conn *pConn,
    Futu::i64_t nErrCode,
    const char *strDesc)
{
    if (pConn != nullptr)
    {
        m_bQotInitSuc = (nErrCode == 0);
    }
    NotifyReply();
}
void FutuQuoteSession::OnDisConnect(Futu::FTAPI_Conn *pConn,
    Futu::i64_t nErrCode)
{}

void FutuQuoteSession::OnReply_Sub(Futu::u32_t nSerialNo, const Qot_Sub::Response &stRsp)
{
    if (m_subscribeManager != nullptr)
    {
        m_subscribeManager->OnReply_Sub(nSerialNo, stRsp);
    }

    NotifyReply();
}

void FutuQuoteSession::OnPush_UpdateBasicQot(const Qot_UpdateBasicQot::Response &stRsp)
{
    if (m_subscribeManager != nullptr)
    {
        m_subscribeManager->OnPush_UpdateBasicQot(stRsp);
    }
}
void FutuQuoteSession::OnPush_UpdateOrderBook(const Qot_UpdateOrderBook::Response &stRsp)
{
    if (m_subscribeManager != nullptr)
    {
        m_subscribeManager->OnPush_UpdateOrderBook(stRsp);
    }
}
void FutuQuoteSession::OnPush_UpdateTicker(const Qot_UpdateTicker::Response &stRsp)
{
    if (m_subscribeManager != nullptr)
    {
        m_subscribeManager->OnPush_UpdateTicker(stRsp);
    }
}
void FutuQuoteSession::OnPush_UpdateKL(const Qot_UpdateKL::Response &stRsp)
{
    if (m_subscribeManager != nullptr)
    {
        m_subscribeManager->OnPush_UpdateKL(stRsp);
    }
}
void FutuQuoteSession::OnPush_UpdateRT(const Qot_UpdateRT::Response &stRsp)
{
    if (m_subscribeManager != nullptr)
    {
        m_subscribeManager->OnPush_UpdateRT(stRsp);
    }
}
void FutuQuoteSession::OnPush_UpdateBroker(const Qot_UpdateBroker::Response &stRsp)
{
    if (m_subscribeManager != nullptr)
    {
        m_subscribeManager->OnPush_UpdateBroker(stRsp);
    }
}

void FutuQuoteSession::Run()
{
    if (!InitQot("127.0.0.1", 11111))
    {
        std::cerr << "ERROR: " << __FUNCTION__ << ", retMsg = InitQot failed" << std::endl;
        return;
    }

    if (m_subscribeManager != nullptr)
    {
        m_subscribeManager->Run();
    }
    else
    {
        std::cerr << "ERROR: " << __FUNCTION__ << ", retMsg = SubscribeManager not set" << std::endl;
    }

    std::cout << "QuotePushDemo End" << std::endl;
}
