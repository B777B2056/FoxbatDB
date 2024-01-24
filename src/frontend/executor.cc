#include "executor.h"
#include "cmdmap.h"
#include "core/db.h"
#include "errors/runtime.h"
#include "frontend/server.h"
#include "parser.h"
#include "utils/resp.h"
#include <algorithm>

namespace foxbatdb {
    CMDExecutor::CMDExecutor()
        : mDB_{DatabaseManager::GetInstance().GetDBByIndex(0)}, mTxState_{TxState::kNoTx} {}

    Database* CMDExecutor::CurrentDB() { return mDB_; }

    void CMDExecutor::SwitchToTargetDB(std::uint8_t dbIdx) {
        mDB_ = DatabaseManager::GetInstance().GetDBByIndex(dbIdx);
    }

    void CMDExecutor::EnableTxMode() {
        mTxState_ = TxState::kAppend;
    }

    void CMDExecutor::CancelTxMode() {
        mTxState_ = TxState::kNoTx;
        mCmdQueue_.clear();
        mCmdQueue_.shrink_to_fit();
        mTxUndo_.clear();
        mTxUndo_.shrink_to_fit();
        ClearWatchKey();
    }

    bool CMDExecutor::SetTxStateByCMD(const Command& cmd) {
        switch (mTxState_) {
            case TxState::kNoTx:
                if (cmd.name == "multi") {
                    mTxState_ = TxState::kBegin;
                    return true;
                } else {
                    return ((cmd.name != "exec") && (cmd.name != "discard"));
                }
            case TxState::kBegin:
                return true;
            case TxState::kAppend:
                if ((cmd.name == "multi") || (cmd.name == "watch") || (cmd.name == "unwatch")) {
                    return false;
                }
                if (cmd.name == "exec") {
                    mTxState_ = TxState::kExec;
                } else if (cmd.name == "discard") {
                    mTxState_ = TxState::kDiscard;
                }
                return true;
            default:
                break;
        }
        return false;
    }

    void CMDExecutor::ClearWatchKey() {
        isTxFailedBefore_ = false;
        for (const auto& key: mWatchedKeyList_) {
            mDB_->DelWatchKeyAndClient(key);
        }
        mWatchedKeyList_.clear();
        mWatchedKeyList_.shrink_to_fit();
    }

    std::string CMDExecutor::ExecTx(std::weak_ptr<CMDSession> weak) {
        if (mCmdQueue_.empty()) {
            CancelTxMode();
            return utils::OK_RESPONSE;
        }

        std::vector<std::string> resps;
        mDB_->InsertTxFlag(TxRuntimeState::kBegin, mCmdQueue_.size());
        while (!mCmdQueue_.empty()) {
            // 根据命令和对应参数，执行命令
            auto cmdInfo = mCmdQueue_.front();
            mCmdQueue_.pop_front();
            if (!cmdInfo.isValidCmd || isTxFailedBefore_) {
                mDB_->InsertTxFlag(TxRuntimeState::kFailed);
                RollbackTx();
                return cmdInfo.errmsg;
            }

            auto [err, resp] = ExecWithErrorFlag(weak, cmdInfo.cmd);
            if (err) {
                mDB_->InsertTxFlag(TxRuntimeState::kFailed);
                RollbackTx();
                return utils::BuildErrorResponse(error::RuntimeErrorCode::kTxError);
            }
            resps.emplace_back(resp);
        }
        mDB_->InsertTxFlag(TxRuntimeState::kFinish);
        CancelTxMode();
        return utils::BuildArrayResponseWithFilledItems(resps);
    }

    void CMDExecutor::AppendUndoLog(const Command& cmd) {
        auto valObj = mDB_->Get(cmd.argv[0]);
        if (valObj.expired()) return;

        mTxUndo_.emplace_back(
                TxUndoInfo{
                        .dbFile = valObj.lock()->GetDataLogFileHandler(),
                        .readPos = valObj.lock()->GetFileOffset(),
                });
    }

    void CMDExecutor::RollbackTx() {
        std::for_each(mTxUndo_.rbegin(), mTxUndo_.rend(),
                      [this](const TxUndoInfo& info) -> void {
                          FileRecord data;
                          if (FileRecord::LoadFromDisk(data, info.dbFile->file, info.readPos))
                              mDB_->StrSetForHistoryData(info.dbFile, info.readPos, data);
                      });
        CancelTxMode();
    }

    std::string CMDExecutor::Exec(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto [_, data] = ExecWithErrorFlag(weak, cmd);
        return data;
    }

    std::tuple<bool, std::string> CMDExecutor::ExecWithErrorFlag(
            std::weak_ptr<CMDSession> weak, const Command& cmd) {
        if (!cmd.call) return {true, ""};
        // 根据命令和对应参数，执行命令
        ProcResult result = (*(cmd.call))(weak, cmd);
        // 根据执行结果构造响应对象
        return {result.hasError, result.data};
    }

    void CMDExecutor::AddWatchKey(const std::string& key) {
        mWatchedKeyList_.emplace_back(key);
    }

    void CMDExecutor::DelWatchKey(const std::string& key) {
        mDB_->DelWatchKeyAndClient(key);
        mWatchedKeyList_.erase(std::find(mWatchedKeyList_.begin(), mWatchedKeyList_.end(), key));
    }

    void CMDExecutor::SetCurrentTxToFail() {
        if (TxState::kNoTx != mTxState_) {
            isTxFailedBefore_ = true;
        }
    }

    std::string CMDExecutor::DoExecOneCmd(std::weak_ptr<CMDSession> weak, const ParseResult& result) {
        std::string resp;
        if (!SetTxStateByCMD(result.data)) {
            resp = utils::BuildErrorResponse(error::RuntimeErrorCode::kInvalidTxCmd);
            RollbackTx();
            return resp;
        }

        switch (mTxState_) {
            case TxState::kNoTx:
                resp = result.ec ? utils::BuildErrorResponse(result.ec) : Exec(weak, result.data);
                break;
            case TxState::kBegin:
                EnableTxMode();
                resp = utils::OK_RESPONSE;
                break;
            case TxState::kAppend:
                if (!mCmdQueue_.empty() && !mCmdQueue_.back().isValidCmd) {
                    resp = mCmdQueue_.back().errmsg;
                } else if (isTxFailedBefore_) {
                    resp = utils::BuildErrorResponse(error::RuntimeErrorCode::kWatchedKeyModified);
                } else {
                    mCmdQueue_.emplace_back(CommandInfo{
                            .cmd = result.data,
                            .isValidCmd = (result.ec == error::ProtocolErrorCode::kSuccess),
                            .errmsg = utils::BuildErrorResponse(result.ec),
                    });
                    if (result.isWriteCmd) {
                        AppendUndoLog(result.data);
                    }
                    resp = utils::QUEUED_RESPONSE;
                }
                break;
            case TxState::kExec:
                resp = ExecTx(weak);
                break;
            case TxState::kDiscard:
                RollbackTx();
                resp = utils::OK_RESPONSE;
                break;
            default:
                break;
        }
        return resp;
    }
}// namespace foxbatdb
