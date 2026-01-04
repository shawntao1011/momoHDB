#include "futu_core/session.hpp"
#include <mutex>
#include <string>

using namespace std::chrono;

FutuQuoteSession::FutuQuoteSession(const FutuSessionConfig &cfg)
    : m_config(cfg)
    , m_pQotApi(nullptr)
    , m_running(false)
    , m_stopRequested(false)
    , m_quoteReady(false)
    , m_reconnectNeeded(false)
{};

FutuQuoteSession::~FutuQuoteSession()
{
    Stop();
    CleanupApis();
};

bool FutuQuoteSession::Start()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_running) return true;

    m_stopRequested = false;
    m_reconnectNeeded = false;
    m_quoteReady = false;
    EnsureApis();

    m_running = true;
    m_worker = std::thread(&FutuQuoteSession::Run, this);

    return true;
};

void FutuQuoteSession::Stop()
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_running) return;

        m_stopRequested = true;
        m_cv.notify_all();
    }

    if (m_worker.joinable())
    {
        m_worker.join();
    }

    std::lock_guard<std::mutex> lock(m_mutex);
    m_running = false;
};

bool FutuQuoteSession::WaitUntilReady(std::chrono::milliseconds timeout)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    return m_cv.wait_for(lock, timeout, [this]() {
        return (m_config.enableQuoteConnection ? m_quoteReady : true);
    });
};

bool FutuQuoteSession::QuoteReady() const 
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_quoteReady;
};

std::string FutuQuoteSession::LastQuoteError() const
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_lastQuoteError;
};

void FutuQuoteSession::OnInitConnect(Futu::FTAPI_Conn *pConn, Futu::i64_t nErrCode,
                   const char *strDesc) 
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (pConn == m_pQotApi)
    {
        m_quoteReady = (nErrCode == 0);
        m_lastQuoteError = (nErrCode) ? "" : (strDesc != nullptr ? strDesc : "quote connect failed");
    }
    m_cv.notify_all();
};

void FutuQuoteSession::OnDisConnect(Futu::FTAPI_Conn *pConn, Futu::i64_t nErrCode) 
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (pConn == m_pQotApi)
    {
        m_quoteReady = false;
        m_lastQuoteError = "quote disconnected, err = " + std::to_string(nErrCode);
    }
    m_reconnectNeeded = true;
    m_cv.notify_all();
};

void FutuQuoteSession::Run() 
{
    auto backoff = m_config.initialBackoff;
    while (true) {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_stopRequested) return;
        }
        
        bool needquote = m_config.enableQuoteConnection && !QuoteReady();

        bool attemptFailed = false;

        if (needquote) 
        {
            attemptFailed = !TryConnectQuote();
        }
        
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            if(m_stopRequested) return;

            if(!attemptFailed)
            {
                backoff = m_config.initialBackoff;
            } else {
                backoff = NextBackoff(backoff);
            }

            m_cv.wait_for(lock, backoff, [this]() {
                return m_stopRequested || m_reconnectNeeded;
            });
            m_reconnectNeeded = false;
        }
    };
};

void FutuQuoteSession::EnsureApis() 
{
    if (m_config.enableQuoteConnection && m_pQotApi == nullptr)
    {
        m_pQotApi = Futu::FTAPI::CreateQotApi();
        if (m_pQotApi != nullptr) m_pQotApi->RegisterConnSpi(this);
    }
};

void FutuQuoteSession::CleanupApis() 
{
    if (m_pQotApi != nullptr)
    {
        m_pQotApi->UnregisterConnSpi();
        Futu::FTAPI::ReleaseQotApi(m_pQotApi);
        m_pQotApi = nullptr;
    }
};

bool FutuQuoteSession::TryConnectQuote() 
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_pQotApi == nullptr)
        {
            m_lastQuoteError = "quote api not initialized";
            return false;
        }
        m_quoteReady = false;
        m_pQotApi->InitConnect(m_config.host.c_str(), m_config.port, m_config.enableSsl);
    }
    return WaitForConnectionResult(m_quoteReady, m_lastQuoteError);
};

bool FutuQuoteSession::WaitForConnectionResult(bool &readyFlag, std::string &lastErrorHolder)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    bool notified = m_cv.wait_for(lock, m_config.connectTimeout, [&readyFlag, this]() {
        return readyFlag || m_stopRequested;
    });
    if (!notified || !readyFlag)
    {
        lastErrorHolder = "connect timeout";
        return false;
    }
    return true;
};

std::chrono::milliseconds FutuQuoteSession::NextBackoff(std::chrono::milliseconds current) const
{
    auto doubled = current * 2;
    if (doubled > m_config.maxBackoff) return m_config.maxBackoff;
    return doubled;
};
