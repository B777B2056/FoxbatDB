#include "persistence.h"
#include "flag/flags.h"

namespace foxbatdb {
  Persister::Persister()
    : mFile_{Flags::GetInstance().logFileName,
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
      }
    };
  }

  Persister::~Persister() {
    FlushToDisk();
    Stop();
  }

  Persister& Persister::GetInstance() {
    static Persister instance;
    return instance;
  }

  void Persister::Stop() {
    mThread_.request_stop();
  }

  void Persister::AppendCommand(const std::string& cmd) {
    if (mCmdBuffer_.IsFull()) {
      // 环形队列满，则等待子线程完成所有数据写入
      mNeedWriteAll_.test_and_set();
      mWriteAllBarrier_.get_future().wait();
    }
    mCmdBuffer_.Enqueue(cmd);
  }

  void Persister::WriteOneCommand() {
    if (std::string cmd; mCmdBuffer_.Dequeue(cmd)) {
      std::unique_lock lock {mFileMutex_};
      mFile_ << cmd;
    }
  }

  void Persister::WriteAllCommand() {
    for (std::string cmd; mCmdBuffer_.Dequeue(cmd); ) {
      std::unique_lock lock{mFileMutex_};
      mFile_ << cmd;
    }
  }

  void Persister::FlushToDisk() {
    std::unique_lock lock{mFileMutex_};
    mFile_.flush();
  }
}