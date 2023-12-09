#pragma once
#include <cstdint>
#include <string>
#include <vector>
#include "core/transaction.h"

namespace foxbatdb {
  class Database;
  class ParseResult;
  class CMDExecutor {
  private:
    std::uint8_t mDBIdx_; 
    Database* mDB_;
    bool mIsInTxMode_ : 4 = false;
    bool mIsTxFailed_ : 4 = false;
    Transaction mTx_;
    std::vector<BinaryString> mWatchedKeyList_;

    void ClearWatchKey(CMDSessionPtr clt);

  public:
    CMDExecutor();
    Database* CurrentDB();
    std::uint8_t CurrentDBIdx() const;
    void SwitchToTargetDB(std::uint8_t dbIdx);
    std::string DoExecOneCmd(CMDSessionPtr clt, const ParseResult& result);
    void AddWatchKey(const BinaryString& key);
    void DelWatchKey(const BinaryString& key);
    void SetCurrentTxToFail();
  };
}