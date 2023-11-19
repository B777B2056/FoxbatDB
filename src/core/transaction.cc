#include "transaction.h"
#include "common/common.h"
#include "frontend/parser.h"
#include "utils/resp.h"
#include "errors/runtime.h"

namespace foxbatdb {
  void Transaction::PushCommand(const Command& cmd) {
    mCmdQueue_.emplace_back(cmd);
  }

  void Transaction::Discard() {
    mCmdQueue_.clear();
    mCmdQueue_.shrink_to_fit();
  }

  std::string Transaction::Exec(CMDSessionPtr weak) {
    std::vector<std::string> resps;
    while (!mCmdQueue_.empty()) {
      // 根据命令和对应参数，执行命令
      auto cmd = mCmdQueue_.front();
      mCmdQueue_.pop_front();
      auto [err, resp] = RequestParser::ExecWithErrorFlag(weak, cmd);
      if (err) {
        Discard();
        return utils::BuildErrorResponse(error::RuntimeErrorCode::kTxError);
      }
      resps.emplace_back(resp);
    }
    return utils::BuildArrayResponseWithFilledItems(resps);
  }
}