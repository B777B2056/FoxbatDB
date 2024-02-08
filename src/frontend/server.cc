#include "server.h"
#include "core/db.h"
#include "errors/runtime.h"
#include "flag/flags.h"
#include "log/oplog.h"
#include "log/serverlog.h"
#include "utils/resp.h"
#include <memory>

namespace foxbatdb {
    CMDSession::CMDSession(asio::ip::tcp::socket socket)
        : mSocket_(std::move(socket)) {
        mReadBuffer_.prepare(1024);
    }

    void CMDSession::Start() { DoRead(); }
    Database* CMDSession::CurrentDB() { return mExecutor_.CurrentDB(); }
    void CMDSession::SwitchToTargetDB(std::uint8_t dbIdx) { mExecutor_.SwitchToTargetDB(dbIdx); }
    void CMDSession::AddWatchKey(const std::string& key) { mExecutor_.AddWatchKey(key); }
    void CMDSession::DelWatchKey(const std::string& key) { mExecutor_.DelWatchKey(key); }
    void CMDSession::SetCurrentTxToFail() { mExecutor_.SetCurrentTxToFail(); }

    void CMDSession::WritePublishMsg(const std::string& channel,
                                     const std::string& msg) {
        auto self(shared_from_this());
        DoWrite(utils::BuildPubSubResponse("message", channel, msg));
    }

    void CMDSession::DoRead() {
        auto self(shared_from_this());
        asio::async_read(
                mSocket_, mReadBuffer_, asio::transfer_at_least(1),
                [this, self](std::error_code ec, std::size_t bytesTransferred) {
                    if (!ec) {
                        ProcessMsg(bytesTransferred);
                    } else {
                        ServerLog::GetInstance().Warning("read request from client failed: {}", ec.message());
                    }
                });
    }

    void CMDSession::DoWrite(const std::string& data) {
        auto self(shared_from_this());
        asio::async_write(mSocket_, asio::buffer(data.data(), data.length()),
                          [this, self](std::error_code ec, std::size_t) {
                              if (!ec) {
                                  DoRead();
                              } else {
                                  ServerLog::GetInstance().Warning("write response to client failed: {}", ec.message());
                              }
                          });
    }

    void CMDSession::ProcessMsg(std::size_t bytesTransferred) {
        std::istream is(&mReadBuffer_);
        auto result = mParser_.Run(is, bytesTransferred);
        if (error::ProtocolErrorCode::kContinue == result.ec) {
            DoRead();
            return;
        }

        if (result.ec) {
            DoWrite(utils::BuildResponse(result.ec));
        } else {
            DoWrite(mExecutor_.DoExecOneCmd(weak_from_this(), result));
            if (result.isWriteCmd) {
                OperationLog::GetInstance().AppendCommand(std::move(result.data));
            }
        }
    }

    DBServer::DBServer()
        : mIOContext_{},
          mAcceptor_{mIOContext_, asio::ip::tcp::endpoint(asio::ip::tcp::v4(),
                                                          Flags::GetInstance().port)} {
        this->DoAccept();
    }

    DBServer& DBServer::GetInstance() {
        static DBServer instance;
        return instance;
    }

    void DBServer::Run() { mIOContext_.run(); }

    void DBServer::DoAccept() {
        this->mAcceptor_.async_accept(
                [this](std::error_code ec, asio::ip::tcp::socket socket) {
                    if (!ec) {
                        std::make_shared<CMDSession>(std::move(socket))->Start();
                    } else {
                        ServerLog::GetInstance().Warning("accept connection failed: {}", ec.message());
                    }
                    this->DoAccept();
                });
    }
}// namespace foxbatdb
