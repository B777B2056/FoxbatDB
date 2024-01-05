#pragma once
#include "cmdmap.h"
#include "log/datalog.h"
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <vector>

namespace foxbatdb {
    struct Command;
    class CMDSession;
    class Database;
    class ParseResult;

    enum class TxState : std::int8_t {
        kNoTx = 0,
        kBegin,
        kAppend,
        kExec,
        kDiscard,
    };

    class CMDExecutor {
    private:
        struct TxUndoInfo {
            DataLogFileObjPtr dbFile;
            std::streampos readPos;
        };

        struct CommandInfo {
            Command cmd;
            bool isValidCmd;
            std::string errmsg;
        };

    private:
        Database* mDB_;
        TxState mTxState_;
        bool isTxFailedBefore_ = false;
        std::deque<CommandInfo> mCmdQueue_;
        std::vector<std::string> mWatchedKeyList_;
        std::deque<TxUndoInfo> mTxUndo_;

        void EnableTxMode();
        void CancelTxMode();

        bool SetTxStateByCMD(const Command& cmd);

        void ClearWatchKey();
        std::string ExecTx(std::weak_ptr<CMDSession> weak);
        void AppendUndoLog(const Command& cmd);
        void RollbackTx();

        static std::string Exec(std::weak_ptr<CMDSession> weak, const Command& cmd);
        static std::tuple<bool, std::string> ExecWithErrorFlag(std::weak_ptr<CMDSession> weak,
                                                               const Command& cmd);

    public:
        CMDExecutor();
        Database* CurrentDB();
        void SwitchToTargetDB(std::uint8_t dbIdx);
        std::string DoExecOneCmd(std::weak_ptr<CMDSession> weak, const ParseResult& result);
        void AddWatchKey(const std::string& key);
        void DelWatchKey(const std::string& key);
        void SetCurrentTxToFail();
    };
}// namespace foxbatdb