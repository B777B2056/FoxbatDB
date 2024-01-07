#include "oplog.h"
#include "flag/flags.h"

namespace foxbatdb {
    OperationLog::OperationLog()
        : mFile_{Flags::GetInstance().operationLogFileName,
                 std::ios::out | std::ios::app | std::ios::binary} {
    }

    OperationLog::~OperationLog() {
        DumpToDisk();
    }

    OperationLog& OperationLog::GetInstance() {
        static OperationLog instance;
        return instance;
    }

    void OperationLog::AppendCommand(std::string&& cmd) {
        if (mCmdBuffer_.IsFull()) {
            WriteAllCommands();// 环形队列满，则所有数据写入os文件缓冲区
        }
        mCmdBuffer_.Enqueue(std::move(cmd));
    }

    void OperationLog::WriteAllCommands() {
        for (std::string cmd; mCmdBuffer_.Dequeue(cmd);) {
            mFile_ << cmd;
        }
    }

    void OperationLog::DumpToDisk() {
        WriteAllCommands();
        mFile_.flush();
    }

    void OperationLog::Init() {}
}// namespace foxbatdb