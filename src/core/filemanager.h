#pragma once
#include <fstream>
#include <list>

namespace foxbatdb {
  class LogFileManager {
  private:
    struct LogFileWrapper {
      std::fstream file;
      std::string name;
    };

    std::list<LogFileWrapper> mLogFilePool_;
    std::list<LogFileWrapper>::iterator mAvailableNode_;

    LogFileManager();
    void PoolExpand();
    void LoadHistoryRecordsFromDisk();

  public:
    ~LogFileManager() = default;
    static LogFileManager& GetInstance();
    void Init();
    std::fstream* GetAvailableLogFile();
    void MergeLogFile();
  };
}