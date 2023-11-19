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
    void WriteOneCommand();  // �ӻ��������ȡ��һ��д���ˢ��os�ں��ļ�д������
    void WriteAllCommand();  // �ӻ��������ȡ������д���ˢ��os�ں��ļ�д������

  public:
    ~Persister();
    static Persister& GetInstance();
    void Stop();
    void AppendCommand(const std::string& cmd);
    void FlushToDisk();
  };
}