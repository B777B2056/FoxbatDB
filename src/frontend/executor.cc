#include "executor.h"
#include "core/db.h"
#include "errors/runtime.h"
#include "parser.h"
#include "persistence/persistence.h"
#include "utils/resp.h"
#include "network/cmd.h"

namespace foxbatdb {
  CMDExecutor::CMDExecutor()
    : mDBIdx_{0}
    , mDB_{DatabaseManager::GetInstance().GetDBByIndex(0)} {}

  Database* CMDExecutor::CurrentDB() {
    return mDB_;
  }

  std::uint8_t CMDExecutor::CurrentDBIdx() const {
    return mDBIdx_;
  }

  void CMDExecutor::SwitchToTargetDB(std::uint8_t dbIdx) {
    mDBIdx_ = dbIdx;
    mDB_ = DatabaseManager::GetInstance().GetDBByIndex(dbIdx);
  }

  void CMDExecutor::ClearWatchKey(CMDSessionPtr clt) {
    auto pClt = clt.lock();
    if (!pClt)
      return;
    for (const auto& key : mWatchedKeyList_) {
      auto* db = this->CurrentDB();
      db->DelWatch(key, pClt);
    }

    mWatchedKeyList_.clear();
    mWatchedKeyList_.shrink_to_fit();
  }

  void CMDExecutor::AddWatchKey(const BinaryString& key) {
    mWatchedKeyList_.emplace_back(key);
  }

  void CMDExecutor::DelWatchKey(const BinaryString& key) {
    mWatchedKeyList_.erase(std::find(mWatchedKeyList_.begin(), mWatchedKeyList_.end(), key));
  }

  void CMDExecutor::SetCurrentTxToFail() {
    if (mIsInTxMode_) {
      mIsTxFailed_ = true;
    }
  }

  std::string CMDExecutor::DoExecOneCmd(CMDSessionPtr clt, const ParseResult& result) {
    std::string resp = utils::BuildErrorResponse(error::RuntimeErrorCode::kIntervalError);
    auto pClt = clt.lock();
    if (!pClt)
      return resp;

    switch (result.txState) {
      case TxState::kBegin:
        mIsInTxMode_ = true;
        resp = utils::BuildResponse("OK");
        break;
      case TxState::kExec:
        mIsInTxMode_ = false;
        ClearWatchKey(pClt);
        if (mIsTxFailed_) {
          mIsTxFailed_ = false;
          mTx_.Discard();
          resp = utils::BuildErrorResponse(error::RuntimeErrorCode::kWatchedKeyModified);
        } else {
          resp = mTx_.Exec(pClt);
        }
        break;
      case TxState::kDiscard:
        mIsInTxMode_ = false;
        ClearWatchKey(pClt);
        mTx_.Discard();
        resp = utils::BuildResponse("OK");
        break;
      default:
        if (mIsInTxMode_) {
          mTx_.PushCommand(result.data);
          resp = utils::BuildResponse("QUEUED");
        } else {
          resp = RequestParser::Exec(pClt, result.data);
        }
        break;
    }
    if (result.isWriteCmd) {
      Persister::GetInstance().AppendCommand(result.cmdText);
    }
    return resp;
  }
}
