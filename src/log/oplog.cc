#include "oplog.h"
#include "flag/flags.h"

namespace foxbatdb {
    OperationLog::OperationLog()
        : mFile_{Flags::GetInstance().operationLogFileName,
                 std::ios::out | std::ios::app | std::ios::binary} {
        mThread_ = std::jthread{
                [this](std::stop_token stoken) {
                    while (!stoken.stop_requested()) {
                        if (mNeedWriteAll_.test()) {
                            this->WriteAllCommand();
                            mNeedWriteAll_.clear();
                            mWriteAllBarrier_.set_value();
                        } else {
                            this->WriteOneCommand();
                        }
                    }
                    this->WriteAllCommand();
                }};
    }

    OperationLog::~OperationLog() {
        DumpToDisk();
        Stop();
    }

    OperationLog& OperationLog::GetInstance() {
        static OperationLog instance;
        return instance;
    }

    void OperationLog::Stop() {
        mThread_.request_stop();
    }

    void OperationLog::AppendCommand(const std::string& cmd) {
        if (mCmdBuffer_.IsFull()) {
            // 环形队列满，则等待子线程完成所有数据写入
            mNeedWriteAll_.test_and_set();
            mWriteAllBarrier_.get_future().wait();
        }
        mCmdBuffer_.Enqueue(cmd);
    }

    void OperationLog::WriteOneCommand() {
        if (std::string cmd; mCmdBuffer_.Dequeue(cmd)) {
            std::unique_lock lock{mFileMutex_};
            mFile_ << cmd;
        }
    }

    void OperationLog::WriteAllCommand() {
        for (std::string cmd; mCmdBuffer_.Dequeue(cmd);) {
            std::unique_lock lock{mFileMutex_};
            mFile_ << cmd;
        }
    }

    void OperationLog::DumpToDisk() {
        std::unique_lock lock{mFileMutex_};
        mFile_.flush();
    }

    void OperationLog::Init() {}
}// namespace foxbatdb