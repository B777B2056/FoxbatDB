#pragma once
#include "asio.hpp"
#include "frontend/executor.h"
#include "frontend/parser.h"
#include <list>
#include <memory>

namespace foxbatdb {
    class Database;

    class CMDSession : public std::enable_shared_from_this<CMDSession> {
    public:
        explicit CMDSession(asio::ip::tcp::socket socket);
        CMDSession(const CMDSession&) = delete;
        CMDSession(CMDSession&&) = delete;
        ~CMDSession() = default;

        CMDSession& operator=(const CMDSession&) = delete;
        CMDSession& operator=(CMDSession&&) = delete;

        void Start();

        Database* CurrentDB();
        void SwitchToTargetDB(std::uint8_t dbIdx);

        void AddWatchKey(const std::string& key);
        void DelWatchKey(const std::string& key);
        void SetCurrentTxToFail();

        void WritePublishMsg(const std::string& channel, const std::string& msg);

    private:
        asio::ip::tcp::socket mSocket_;
        asio::streambuf mReadBuffer_;
        RequestParser mParser_;
        CMDExecutor mExecutor_;

        void DoRead();
        void DoWrite(const std::string& data);
        void ProcessMsg(std::size_t bytesTransferred);
    };

    namespace detail {
        class IOContextPool {
        public:
            IOContextPool();
            IOContextPool(const IOContextPool&) = delete;
            IOContextPool& operator=(const IOContextPool&) = delete;

            void Run();
            void Stop();
            asio::io_context& GetIOContext();

        private:
            std::vector<std::shared_ptr<asio::io_context>> ioContexts_;
            std::list<asio::executor_work_guard<asio::io_context::executor_type>> work_;
            std::size_t nextIOContext_;
        };
    }// namespace detail

    class DBServer {
    public:
        DBServer(const DBServer&) = delete;
        DBServer& operator=(const DBServer&) = delete;
        DBServer(DBServer&&) = delete;
        DBServer& operator=(DBServer&&) = delete;
        ~DBServer() = default;

        static DBServer& GetInstance();
        void Run();

    private:
        detail::IOContextPool ioContextPool_;
        asio::signal_set signals_;
        asio::ip::tcp::acceptor acceptor_;

        DBServer();
        void DoAccept();
        void DoWaitSignals();
    };
}// namespace foxbatdb