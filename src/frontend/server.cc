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

    detail::IOContextPool::IOContextPool() : nextIOContext_{0} {
        std::size_t poolSize = Flags::GetInstance().threadNum;
        if (0 == poolSize)
            throw std::runtime_error("thread num is 0");

        for (std::size_t i = 0; i < poolSize; ++i) {
            auto ioCtx = std::make_shared<asio::io_context>();
            ioContexts_.push_back(ioCtx);
            work_.push_back(asio::make_work_guard(*ioCtx));
        }
    }

    void detail::IOContextPool::Run() {
        std::vector<std::thread> threads;
        threads.reserve(ioContexts_.size());
        for (auto& io_context: ioContexts_)
            threads.emplace_back([&io_context] { io_context->run(); });

        for (auto& thread: threads) {
            if (thread.joinable())
                thread.join();
        }
    }

    void detail::IOContextPool::Stop() {
        for (auto& ioCtx: ioContexts_)
            ioCtx->stop();
    }

    asio::io_context& detail::IOContextPool::GetIOContext() {
        asio::io_context& ioCtx = *ioContexts_[nextIOContext_];
        ++nextIOContext_;
        if (nextIOContext_ == ioContexts_.size())
            nextIOContext_ = 0;
        return ioCtx;
    }

    DBServer::DBServer()
        : ioContextPool_{},
          signals_(ioContextPool_.GetIOContext()),
          acceptor_{ioContextPool_.GetIOContext(), asio::ip::tcp::endpoint(asio::ip::tcp::v4(),
                                                                           Flags::GetInstance().port)} {
        this->DoWaitSignals();
        this->DoAccept();
    }

    DBServer& DBServer::GetInstance() {
        static DBServer instance;
        return instance;
    }

    void DBServer::Run() { ioContextPool_.Run(); }

    void DBServer::DoAccept() {
        this->acceptor_.async_accept(
                ioContextPool_.GetIOContext(),
                [this](std::error_code ec, asio::ip::tcp::socket socket) {
                    if (!acceptor_.is_open()) return;

                    if (!ec) {
                        std::make_shared<CMDSession>(std::move(socket))->Start();
                    } else {
                        ServerLog::GetInstance().Warning("accept connection failed: {}", ec.message());
                    }
                    this->DoAccept();
                });
    }

    void DBServer::DoWaitSignals() {
        signals_.add(SIGINT);
        signals_.add(SIGTERM);
#if defined(SIGQUIT)
        signals_.add(SIGQUIT);
#endif
        signals_.async_wait(
                [this](std::error_code /*ec*/, int /*signo*/) {
                    this->ioContextPool_.Stop();
                });
    }
}// namespace foxbatdb
