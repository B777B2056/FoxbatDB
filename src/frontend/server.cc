#include "server.h"
#include <algorithm>
#include <cstdint>
#include <memory>
#include "core/db.h"
#include "errors/runtime.h"
#include "flag/flags.h"
#include "log/oplog.h"
#include "log/serverlog.h"
#include "utils/resp.h"

namespace foxbatdb {
CMDSession::CMDSession(asio::ip::tcp::socket socket)
  : mSocket_(std::move(socket)) {
  mReadBuffer_.prepare(1024);
}

void CMDSession::Start() {
  DoRead();
}

Database* CMDSession::CurrentDB() {
  return mExecutor_.CurrentDB();
}

void CMDSession::SwitchToTargetDB(std::uint8_t dbIdx) {
  mExecutor_.SwitchToTargetDB(dbIdx);
}

void CMDSession::AddWatchKey(const std::string& key) {
  mExecutor_.AddWatchKey(key);
}

void CMDSession::DelWatchKey(const std::string& key) {
  mExecutor_.DelWatchKey(key);
}

void CMDSession::SetCurrentTxToFail() {
  mExecutor_.SetCurrentTxToFail();
}

void CMDSession::WritePublishMsg(const std::string& channel,
                                 const std::string& msg) {
  auto self(shared_from_this());
  DoWrite(utils::BuildPubSubResponse(
    std::vector<std::string>{"message", channel, msg}));
}

#ifdef _FOXBATDB_SELF_TEST
std::string CMDSession::DoExecOneCmd(const ParseResult& result) {
  auto self(shared_from_this());
  return mExecutor_.DoExecOneCmd(weak_from_this(), result);
}
#endif

void CMDSession::DoRead() {
  auto self(shared_from_this());
  asio::async_read_until(
      mSocket_, mReadBuffer_, "\r\n",
      [this, self](std::error_code ec, std::size_t length) {
        if (!ec) {
          ProcessMsg(length);
        } else {
          ServerLog::Warnning("read request from client failed: {}", ec.message());
        }
      }
  );
}

void CMDSession::DoWrite(const std::string& data) {
  auto self(shared_from_this());
  asio::async_write(mSocket_, asio::buffer(data.data(), data.length()),
                    [this, self](std::error_code ec, std::size_t) {
                      if (!ec) {
                        DoRead();
                      } else {
                        ServerLog::Warnning("write response to client failed: {}", ec.message());
                      }
                    }
  );
}

void CMDSession::ProcessMsg(std::size_t length) {
  std::istream is(&mReadBuffer_);
  std::string line;
  while (std::getline(is, line) && !line.empty()) {
    std::string_view lineView;
    if (line.back() == '\r')
      lineView = {line.begin(), line.begin() + (line.size() - 1)};
    else
      lineView = {line.begin(), line.end()};
    ParseResult result = mParser_.ParseLine(lineView);
    if (result.hasError || mParser_.IsParseFinished()) {
      DoWrite(mExecutor_.DoExecOneCmd(weak_from_this(), result));
    } else {
      DoRead();
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
          ServerLog::Warnning("accept connection failed: {}", ec.message());
        }
        this->DoAccept();
      }
  );
}
}  // namespace foxbatdb
