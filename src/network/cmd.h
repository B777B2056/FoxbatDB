#pragma once
#include "asio/asio.hpp"
#include "frontend/executor.h"
#include "frontend/parser.h"

namespace foxbatdb {
class Database;

class CMDSession : public std::enable_shared_from_this<CMDSession> {
 public:
  CMDSession(asio::ip::tcp::socket socket);
  CMDSession(const CMDSession&) = delete;
  CMDSession(CMDSession&&) = default;
  ~CMDSession() = default;

  CMDSession& operator=(const CMDSession&) = delete;
  CMDSession& operator=(CMDSession&&) = default;

  void Start();

  Database* CurrentDB();
  std::uint8_t CurrentDBIdx() const;
  void SwitchToTargetDB(std::uint8_t dbIdx);

  void AddWatchKey(const BinaryString& key);
  void DelWatchKey(const BinaryString& key);
  void SetCurrentTxToFail();

  void WritePublishMsg(const BinaryString& channel, const BinaryString& msg);

#ifdef _FOXBATDB_SELF_TEST
  std::string DoExecOneCmd(const ParseResult& result);
#endif

private:
  asio::ip::tcp::socket mSocket_;
  asio::streambuf mReadBuffer_;
  RequestParser mParser_;
  CMDExecutor mExecutor_;

  void DoRead();
  void DoWrite(const std::string& data);
  void ProcessMsg(std::size_t length);
};
}  // namespace foxbatdb