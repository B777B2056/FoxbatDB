#pragma once
#include <atomic>
#include <thread>
#include <fstream>
#include <future>
#include <mutex>
#include <string>
#include "utils/ringbuffer.h"

namespace foxbatdb {
  class Persister {
  private:
    std::jthread mThread_;
    std::ofstream mFile_;
    std::mutex mFileMutex_;
    std::atomic_flag mNeedWriteAll_;
    std::promise<void> mWriteAllBarrier_;
    utils::RingBuffer<std::string> mCmdBuffer_;

    Persister();
    void WriteOneCommand();  // 从缓冲队列里取出一条写命令，刷入os内核文件写缓冲区
    void WriteAllCommand();  // 从缓冲队列里取出所有写命令，刷入os内核文件写缓冲区

  public:
    ~Persister();
    static Persister& GetInstance();
    void Stop();
    void AppendCommand(const std::string& cmd);
    void FlushToDisk();
  };
}