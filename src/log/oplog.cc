#include "oplog.h"
#include "flag/flags.h"
#include "frontend/cmdmap.h"

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

    static std::vector<std::string> CommandToCMDList(Command&& data) {
        std::vector<std::string> cmdList = {data.name};
        cmdList.reserve(1 + data.argv.size() + data.options.size());
        for (auto&& p: data.argv)
            cmdList.emplace_back(std::move(p));
        for (auto&& opt: data.options) {
            cmdList.emplace_back(opt.name);
            for (auto&& p: opt.argv)
                cmdList.emplace_back(std::move(p));
        }
        return cmdList;
    }

    void OperationLog::AppendCommand(Command&& data) {
        if (mCmdBuffer_.IsFull()) {
            WriteAllCommands();// 环形队列满，则所有数据写入os文件缓冲区
        }

        mCmdBuffer_.Enqueue(CommandToCMDList(std::move(data)));
    }

    void OperationLog::WriteAllCommands() {
        for (std::vector<std::string> cmdList; mCmdBuffer_.Dequeue(cmdList);) {
            mFile_ << "*" << cmdList.size() << "\r\n";
            for (auto&& p: cmdList) {
                mFile_ << "$" << p.size() << "\r\n"
                       << p << "\r\n";
            }
        }
    }

    void OperationLog::DumpToDisk() {
        WriteAllCommands();
        mFile_.flush();
    }

    void OperationLog::Init() {}
}// namespace foxbatdb