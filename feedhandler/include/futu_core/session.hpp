#pragma once
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

#include "Include_FTAPI.h"
#include "FTSPI.h"

struct FutuSessionConfig {
    std::string host{"127.0.0.1"};
    uint16_t port{11111};
    bool enableSsl{false};
    bool enableQuoteConnection{true};
    std::chrono::milliseconds connectTimeout{std::chrono::milliseconds(3000)};
    std::chrono::milliseconds initialBackoff{std::chrono::milliseconds(500)};
    std::chrono::milliseconds maxBackoff{std::chrono::milliseconds(8000)};
};

class FutuQuoteSession : public Futu::FTSPI_Conn {
  public:
    explicit FutuQuoteSession(const FutuSessionConfig &cfg);
    ~FutuQuoteSession();
    
    bool Start();
    void Stop();

    bool WaitUntilReady(std::chrono::milliseconds timeout);
    bool QuoteReady() const;
    
    Futu::FTAPI_Qot* QuoteApi() const { return m_pQotApi; }
    
    std::string LastQuoteError() const;

  protected:
    void OnInitConnect(Futu::FTAPI_Conn* pConn, Futu::i64_t nErrCode, const char* strDesc) override;
    void OnDisConnect(Futu::FTAPI_Conn* pConn, Futu::i64_t nErrCode) override;

  private:
	void Run();
	void EnsureApis();
	void CleanupApis();
	bool TryConnectQuote();
	bool WaitForConnectionResult(bool& readyFlag, std::string& lastErrorHolder);
	std::chrono::milliseconds NextBackoff(std::chrono::milliseconds current) const;

  private:
	FutuSessionConfig m_config;
    Futu::FTAPI_Qot* m_pQotApi;

	std::thread m_worker;
	mutable std::mutex m_mutex;
	std::condition_variable m_cv;
	bool m_running;
	bool m_stopRequested;
	bool m_quoteReady;
	bool m_reconnectNeeded;
	std::string m_lastQuoteError;
};
