#pragma once
#include <fstream>
#include <list>
#include <memory>
#include <mutex>

namespace foxbatdb {
  struct LogFileWrapper {
    std::string name;
    std::fstream file;
  };

  using LogFileObjPtr = std::weak_ptr<LogFileWrapper>;

  class LogFileManager {
  private:
    std::list<std::shared_ptr<LogFileWrapper>> mLogFilePool_;
    std::list<std::shared_ptr<LogFileWrapper>>::iterator mAvailableNode_;

    LogFileManager();
    void PoolExpand();
    bool LoadHistoryTxFromDisk(std::shared_ptr<LogFileWrapper> fileWrapper, std::uint64_t txNum);
    void LoadHistoryRecordsFromDisk();

  public:
    ~LogFileManager() = default;
    static LogFileManager& GetInstance();
    void Init();
    LogFileObjPtr GetAvailableLogFile();
    void Merge();
  };
}