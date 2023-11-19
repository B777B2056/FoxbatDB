#include "cmd.h"
#include <algorithm>
#include "core/db.h"
#include "errors/runtime.h"
#include "persistence/persistence.h"
#include "utils/resp.h"

namespace foxbatdb {
CMDSession::CMDSession(asio::ip::tcp::socket socket)
  : mSocket_(std::move(socket)) {
  auto& dbm = DatabaseManager::GetInstance();
  mDB_ = dbm.GetDBByIndex(0);
  mReadBuffer_.prepare(1024);
}

void CMDSession::Start() {
  DoRead();
}

std::tuple<std::uint8_t, Database*> CMDSession::CurrentDB() {
  return {mDBIdx_, mDB_};
}

void CMDSession::SwitchToTargetDB(std::uint8_t dbIdx, Database* db) {
  mDBIdx_ = dbIdx;
  mDB_ = db;
}

void CMDSession::AddWatchKey(const BinaryString& key) {
  mWatchedKeyList_.emplace_back(key);
}

void CMDSession::DelWatchKey(const BinaryString& key) {
  mWatchedKeyList_.erase(std::find(mWatchedKeyList_.begin(), mWatchedKeyList_.end(), key));
}

void CMDSession::ClearWatchKey() {
  auto self(shared_from_this());
  for (const auto& key : mWatchedKeyList_) {
    mDB_->DelWatch(key, weak_from_this());
  }

  mWatchedKeyList_.clear();
  mWatchedKeyList_.shrink_to_fit();
}

void CMDSession::SetCurrentTxToFail() {
  if (mIsInTxMode_) {
    mIsTxFailed_ = true;
  }
}

void CMDSession::WritePublishMsg(const BinaryString& channel,
                                 const BinaryString& msg) {
  auto self(shared_from_this());
  DoWrite(utils::BuildPubSubResponse(
    std::vector<std::string>{"message", channel.ToTextString(), msg.ToTextString()}));
}

void CMDSession::DoRead() {
  auto self(shared_from_this());
  asio::async_read_until(
      mSocket_, mReadBuffer_, "\r\n",
      [this, self](std::error_code ec, std::size_t length) {
        if (!ec) {
          ProcessMsg(length);
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
    if (result.hasError) {
      DoWrite(result.errMsg);
    } else {
      if (mParser_.IsParseFinished()) {
        DoExecOneCmd(result);
      } else {
        DoRead();
      }
    }
  }
}

void CMDSession::DoExecOneCmd(const ParseResult& result) {
  auto self(shared_from_this());
  switch (result.txState) {
    case TxState::kBegin:
      mIsInTxMode_ = true;
      DoWrite(utils::BuildResponse("OK"));
      break;
    case TxState::kExec:
      mIsInTxMode_ = false;
      ClearWatchKey();
      if (mIsTxFailed_) {
        mIsTxFailed_ = false;
        mTx_.Discard();
        DoWrite(utils::BuildErrorResponse(error::RuntimeErrorCode::kWatchedKeyModified));
      } else {
        DoWrite(mTx_.Exec(weak_from_this()));
      }
      break;
    case TxState::kDiscard:
      mIsInTxMode_ = false;
      ClearWatchKey();
      mTx_.Discard();
      DoWrite(utils::BuildResponse("OK"));
      break;
    default:
      if (mIsInTxMode_) {
        mTx_.PushCommand(result.data);
        DoWrite(utils::BuildResponse("QUEUED"));
      } else {
        DoWrite(RequestParser::Exec(weak_from_this(), result.data));
      }
      break;
  }
  if (result.isWriteCmd) {
    Persister::GetInstance().AppendCommand(result.cmdText);
  }
}
}  // namespace foxbatdb
