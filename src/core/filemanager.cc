#include "filemanager.h"
#include <filesystem>
#include <string>
#include <system_error>
#include "common/common.h"

namespace foxbatdb {
  static constexpr std::string_view CFileNamePrefix = "foxbatdb-";
  static constexpr std::string_view CFileNameSuffix = ".log";

  LogFileManager::LogFileManager() {
    // 若文件夹不存在，则创建文件夹
    if (!std::filesystem::exists(CLogFileDir) ||
        !std::filesystem::is_directory(CLogFileDir)) {
      if (!std::filesystem::create_directory(CLogFileDir)) {
        throw std::runtime_error{"log file directory create failed"};
      }
    }

    // 创建文件
    std::filesystem::current_path(CLogFileDir);
    for (std::size_t i = 0; i < 1; ++i) {
      auto fileName = std::string{CFileNamePrefix} + std::to_string(i) +
                      std::string{CFileNameSuffix};
      std::fstream file{fileName, std::ios::in | std::ios::out | std::ios::binary |
                                 std::ios::app};
      if (!file.is_open()) {
        throw std::runtime_error{"log file create failed"};
      }
      mLogFilePool_.emplace_back(LogFileWrapper{.file = std::move(file)});
    }
    mLogFileMaxSize_ = 1024*1024*1024;
    mLogFilePool_.shrink_to_fit();
    mAvailableIdx_ = 0;
  }

  LogFileManager::~LogFileManager() {

  }

  LogFileManager& LogFileManager::GetInstance() {
    static LogFileManager instance;
    return instance;
  }

  std::fstream* LogFileManager::GetAvailableLogFile() {
    if (mLogFilePool_[mAvailableIdx_].file.tellp() >= mLogFileMaxSize_) {
      ++mAvailableIdx_;
      if (mAvailableIdx_ >= mLogFilePool_.size()) {
        PoolExpand();
      }
    }
    return &mLogFilePool_[mAvailableIdx_].file;
  }

  void LogFileManager::PoolExpand() {
    auto poolSize = mLogFilePool_.size();
    std::filesystem::current_path(CLogFileDir);
    for (std::size_t i = poolSize; i < 1 + poolSize; ++i) {
      auto fileName = std::string{CFileNamePrefix} + std::to_string(i) +
                      std::string{CFileNameSuffix};
      mLogFilePool_.emplace_back(
        std::fstream{fileName, std::ios::binary | std::ios::app}
      );
    }
  }
}