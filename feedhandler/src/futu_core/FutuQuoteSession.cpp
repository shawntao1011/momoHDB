#include "futu_core/FutuQuoteSession.hpp"
#include "futu_core/tool.h"
#include <FTAPI.h>

FutuQuoteSession::FutuQuoteSession()
{
    m_pQotApi = Futu::FTAPI::CreateQotApi();
    m_pQotApi->RegisterQotSpi(this);
    m_pQotApi->RegisterConnSpi(this);
    m_bQotInitSuc = false;

    m_pSem = new Semaphore(0);
}
FutuQuoteSession::~FutuQuoteSession()
{
    UnInitQot();
    delete m_pSem;
}

bool FutuQuoteSession::InitQot(const char *szIP, uint16_t nPort)
{
    if (!m_bQotInitSuc && m_pQotApi != nullptr)
    {
        m_pQotApi->InitConnect(szIP, nPort, false);
        m_pSem->wait();
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

void FutuQuoteSession::WaitReply(int32_t nSerilNo)
{
    if (nSerilNo != 0)
    {
        m_pSem->wait();
    }
}
void FutuQuoteSession::PostReply()
{
    m_pSem->post();
}

void FutuQuoteSession::OnInitConnect(FTAPI_Conn *pConn,
    Futu::i64_t nErrCode,
    const char *strDesc)
{
    if (pConn != nullptr)
    {
        m_bQotInitSuc = (nErrCode == 0);
    }
    PostReply();
}
void FutuQuoteSession::OnDisConnect(FTAPI_Conn *pConn,
    Futu::i64_t nErrCode)
{}

void FutuQuoteSession::OnReply_Sub(Futu::u32_t nSerialNo, const Qot_Sub::Response &stRsp)
{
    if (stRsp.rettype() == Common::RetType_Succeed)
    {
        bSubSuc = true;
    }
    PostReply();
}

void FutuQuoteSession::OnPush_UpdateBasicQot(const Qot_UpdateBasicQot::Response &stRsp)
{
	const Qot_UpdateBasicQot::S2C &pbS2C = stRsp.s2c();
	//repeated Qot_Common.BasicQot basicQotList = 1;
	//股票基本行情，这个字段虽然是repeated字段，但是在推送里面只会有一个数据
	if (pbS2C.basicqotlist_size() > 0)
	{
		const Qot_Common::BasicQot &pbBasicQot = pbS2C.basicqotlist(0);
		//required Qot_Common.Security security = 1;
		const Qot_Common::Security &pbSec = pbBasicQot.security();
		//股票Code
		string strCode = pbSec.code();
		//required double highPrice = 6;
		//最高价
		double fHighPrice = pbBasicQot.highprice();

		//required double openPrice = 7;
		//开盘价
		double fOpenPrice = pbBasicQot.openprice();

		//required double lowPrice = 8;
		//最低价
		double fLowPrice = pbBasicQot.lowprice();

		//required double curPrice = 9;
		//最新价
		double fCurPrice = pbBasicQot.curprice();

		//optional int32 secStatus = 21;
		//股票状态
		if (pbBasicQot.has_secstatus())
		{
			Qot_Common::SecurityStatus enStatus = (Qot_Common::SecurityStatus)pbBasicQot.secstatus();
		}

		//......
		cout << "OnPush_UpdateBasicQot, (" << strCode << ", " << fCurPrice << ")" << endl;
	}
}
void FutuQuoteSession::OnPush_UpdateOrderBook(const Qot_UpdateOrderBook::Response &stRsp)
{

}
void FutuQuoteSession::OnPush_UpdateTicker(const Qot_UpdateTicker::Response &stRsp)
{

}
void FutuQuoteSession::OnPush_UpdateKL(const Qot_UpdateKL::Response &stRsp)
{

}
void FutuQuoteSession::OnPush_UpdateRT(const Qot_UpdateRT::Response &stRsp)
{

}
void FutuQuoteSession::OnPush_UpdateBroker(const Qot_UpdateBroker::Response &stRsp)
{

}

void FutuQuoteSession::Run()
{
    std::cout << "Quote Pushing" << std::endl;

    bSubSuc = false;

    InitQot("127.0.0.1", 11111);

    if (m_pQotApi)
	{
		//订阅一批港股的基础报价，摆盘，经纪队列，逐笔，日K，分时的实时推送数据
		//盘中运行会有持续的推送，非盘中时段运行只有一条最近的数据推送
		const int32_t nCodeCount = 3;
		string strCodes[nCodeCount] = { "00700", "00001", "00002" };
		const int32_t nSubTypeCount = 6;
		Qot_Common::SubType enSubTypes[nSubTypeCount] = { Qot_Common::SubType_Basic, Qot_Common::SubType_OrderBook,
			Qot_Common::SubType_Broker, Qot_Common::SubType_Ticker, Qot_Common::SubType_KL_Day, Qot_Common::SubType_RT};

		Qot_Sub::Request pbSub;
		Qot_Sub::C2S *pSubC2S = pbSub.mutable_c2s();
		//repeated Qot_Common.Security securityList = 1;
		//需要订阅的股票		
		for (int32_t i = 0; i < nCodeCount; ++i)
		{
			Qot_Common::Security *pSubSec = pSubC2S->add_securitylist();
			pSubSec->set_code(strCodes[i]);
			pSubSec->set_market(Qot_Common::QotMarket_HK_Security);
		}
		//repeated int32 subTypeList = 2;
		//需要订阅的数据类型
		for (int32_t i = 0; i < nSubTypeCount; ++i)
		{
			pSubC2S->add_subtypelist(enSubTypes[i]);
		}
		//required bool isSubOrUnSub = 3;
		//ture表示订阅,false表示反订阅
		//订阅有额度限制，不需要的行情要反订阅，反订阅后就会返还额度
		pSubC2S->set_issuborunsub(true);
		//optional bool isRegOrUnRegPush = 4;
		//是否注册或反注册该连接上面行情的推送
		//接收推送
		pSubC2S->set_isregorunregpush(true);
		cout << "Sub" << endl;
		WaitReply(m_pQotApi->Sub(pbSub));

		if (bSubSuc)
		{
			//请在OnPush_UpdateXXX回调里面观察数据
			CPSleep(60);

			//反订阅
			pSubC2S = pbSub.mutable_c2s();
			pSubC2S->set_issuborunsub(false);
			pSubC2S->set_isregorunregpush(false);
			WaitReply(m_pQotApi->Sub(pbSub));
		}
		else
		{
			PrintError("Sub failed");
		}
	}

	//关闭行情连接，连接不再使用之后，要关闭，否则占用不必要资源
	//同时OpenD也限制了最多128条连接
	UnInitQot();

	cout << "QuotePushDemo End" << endl << endl;
}
