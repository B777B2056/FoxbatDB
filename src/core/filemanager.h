#pragma once
#include <fstream>
#include <vector>
#include <string_view>

namespace foxbatdb {
  class LogFileManager {
  private:
    struct LogFileWrapper {
      std::fstream file;
    };

    std::fstream::pos_type mLogFileMaxSize_;
    std::vector<LogFileWrapper> mLogFilePool_;
    std::size_t mAvailableIdx_;

    LogFileManager();
    void PoolExpand();

  public:
    ~LogFileManager();
    static LogFileManager& GetInstance();
    std::fstream* GetAvailableLogFile();
  };
}