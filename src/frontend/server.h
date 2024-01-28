#pragma once
#include "asio.hpp"
#include "frontend/executor.h"
#include "frontend/parser.h"

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

    class DBServer {
    public:
        DBServer(const DBServer&) = delete;
        DBServer& operator=(const DBServer&) = delete;
        DBServer(DBServer&&) = default;
        DBServer& operator=(DBServer&&) = default;
        ~DBServer() = default;

        static DBServer& GetInstance();
        void Run();

    private:
        asio::io_context mIOContext_;
        asio::ip::tcp::acceptor mAcceptor_;

        DBServer();
        void DoAccept();
    };
}// namespace foxbatdb