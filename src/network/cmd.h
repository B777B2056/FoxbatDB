#pragma once
#include <tuple>
#include "asio/asio.hpp"
#include "core/transaction.h"
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

  std::tuple<std::uint8_t, Database*> CurrentDB();
  void SwitchToTargetDB(std::uint8_t dbIdx, Database* db);

  void AddWatchKey(const BinaryString& key);
  void DelWatchKey(const BinaryString& key);
  void SetCurrentTxToFail();

  void WritePublishMsg(const BinaryString& channel, const BinaryString& msg);

private:
  asio::ip::tcp::socket mSocket_;
  asio::streambuf mReadBuffer_;
  RequestParser mParser_;
  bool mIsInTxMode_ : 4 = false;
  bool mIsTxFailed_ : 4 = false;
  Transaction mTx_;
  std::vector<BinaryString> mWatchedKeyList_;
  std::uint8_t mDBIdx_; 
  Database* mDB_;

  void DoRead();
  void DoWrite(const std::string& data);

  void ProcessMsg(std::size_t length);
  void DoExecOneCmd(const ParseResult& result);

  void ClearWatchKey();
};
}  // namespace foxbatdb