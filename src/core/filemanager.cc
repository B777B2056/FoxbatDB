#include "filemanager.h"
#include <regex>
#include <iterator>
#include <iostream>
#include <filesystem>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>
#include "common/flags.h"
#include "core/db.h"
#include "core/obj.h"

namespace foxbatdb {
  static constexpr std::string_view CFileNamePrefix = "foxbat-";
  static constexpr std::string_view CFileNameSuffix = ".db";

  static std::string BuildLogFileName(const std::string& content) {
    return flags.dbFileDir + "/" + std::string{CFileNamePrefix} +
           content + std::string{CFileNameSuffix};
  }

  static std::string BuildLogFileNameByIdx(std::size_t idx) {
    return BuildLogFileName(std::to_string(idx));
  }

  LogFileManager::LogFileManager() {
    // 加载历史数据
    LoadHistoryRecordsFromDisk();
    if (!mLogFilePool_.empty()) {
      MergeLogFile();  // 合并历史log文件
      return;
    }

    // 若文件夹不存在，则创建文件夹
    if (!std::filesystem::exists(flags.dbFileDir) ||
        !std::filesystem::is_directory(flags.dbFileDir)) {
      if (!std::filesystem::create_directory(flags.dbFileDir)) {
        throw std::runtime_error{"log file directory create failed"};
      }
    }

    // 创建文件
    for (std::size_t i = 0; i < 1; ++i) {
      auto fileName = BuildLogFileNameByIdx(i);
      std::fstream file{fileName, std::ios::in | std::ios::out |
                                      std::ios::binary | std::ios::app};
      if (!file.is_open()) {
        throw std::runtime_error{"log file create failed"};
      }
      mLogFilePool_.emplace_back(
          LogFileWrapper{.file = std::move(file), .name = fileName});
    }
    mAvailableNode_ = mLogFilePool_.begin();
  }

  LogFileManager& LogFileManager::GetInstance() {
    static LogFileManager instance;
    return instance;
  }

  void LogFileManager::Init() {
    return;
  }

  std::fstream* LogFileManager::GetAvailableLogFile() {
    if (std::filesystem::file_size(mAvailableNode_->name) > flags.dbFileMaxSize) {
      if (std::next(mAvailableNode_, 1) == mLogFilePool_.end()) {
        PoolExpand();
      }
      ++mAvailableNode_;
    }
    return &mAvailableNode_->file;
  }

  void LogFileManager::PoolExpand() {
    auto poolSize = mLogFilePool_.size();
    for (std::size_t i = poolSize; i < 1 + poolSize; ++i) {
      auto fileName = BuildLogFileNameByIdx(i);
      std::fstream file{fileName, std::ios::in | std::ios::out |
                                      std::ios::binary | std::ios::app};
      if (!file.is_open())
        continue;

      mLogFilePool_.emplace_back(
          LogFileWrapper{.file = std::move(file), .name = fileName});
    }
  }

  void LogFileManager::LoadHistoryRecordsFromDisk() {
    if (!std::filesystem::exists(flags.dbFileDir))
      return;
    auto& dbm = DatabaseManager::GetInstance();
    std::stringstream ss;
    ss << flags.dbFileDir << "/" << CFileNamePrefix
       << "[[:digit:]]+\\" << CFileNameSuffix;
    std::regex regexpr{ss.str()};
    // 获取目标目录下匹配日志文件格式的所有文件名
    std::vector<std::string> fileNames;
    for (auto& p : std::filesystem::directory_iterator{flags.dbFileDir}) {
      if (!p.exists() ||
          !p.is_regular_file() ||
          !std::regex_match(p.path().string(), regexpr))
        continue;
      fileNames.emplace_back(p.path().string());
      // std::cout << "history file: " << p.path().string() << ", hard link count: " << p.hard_link_count() << std::endl;
    }

    // 按文件名字典序填充文件池
    std::sort(fileNames.begin(), fileNames.end());
    for (const auto& fileName : fileNames) {
        std::fstream file{fileName, std::ios::in | std::ios::out |
                                        std::ios::binary | std::ios::app};
        if (!file.is_open()) continue;
        mLogFilePool_.emplace_back(
            LogFileWrapper{.file = std::move(file), .name = fileName});
    }

    // 依次读文件填充dict
    for (auto& fileWrapper : mLogFilePool_) {
      auto& file = fileWrapper.file;
      while (!file.eof()) {
        FileRecord record;
        auto pos = file.tellg();
        if (!record.LoadFromDisk(file, pos)) break;

        if (record.header.valSize > 0)
          dbm.GetDBByIndex(record.header.dbIdx)
              ->StrSetForHistoryData(file, pos, record);
      }

      file.clear();
    }
    // 设置可用文件位置
    mAvailableNode_ = std::prev(mLogFilePool_.end());
  }

  void LogFileManager::MergeLogFile() {
    // 记录当前活跃的文件索引
    auto currentAvailableNode = mAvailableNode_;

    // 创建merge文件
    auto mergeFileName = BuildLogFileName("merge");
    std::fstream mergeFile{mergeFileName, std::ios::in | std::ios::out |
                                              std::ios::binary |
                                              std::ios::app};
    if (!mergeFile.is_open()) 
      return;

    auto savedMergeFileNode = mLogFilePool_.insert(currentAvailableNode,
        LogFileWrapper{.file = std::move(mergeFile), .name = mergeFileName});
    auto& savedMergeFile = savedMergeFileNode->file;

    // 合并db文件
    auto& dbm = DatabaseManager::GetInstance();
    for (std::size_t i = 0; i < dbm.GetDBListSize(); ++i) {
      auto* db = dbm.GetDBByIndex(i);
      // 遍历DB中所有活跃的key和对应的内存记录
      db->Foreach(
          [&](const BinaryString& key, const ValueObject& valObj) -> void {
            // 不合并当前正可用的db文件
            if (valObj.IsSameLogFile(savedMergeFileNode->file)) 
              return;
            // 读取到内存中
            auto record = valObj.CovertToFileRecord();
            if (!record.has_value()) 
              return;
            // 将活跃的key和记录写入merge文件内后，更新内存哈希表
            db->StrSetForMerge(savedMergeFile, i, key, record->data.value);
        }
      );
    }

    // 删除原先的只读文件
    for (auto it = mLogFilePool_.begin(); it != savedMergeFileNode; ++it) {
      it->file.close();
      std::filesystem::remove(it->name);
    }

    // 缩容file pool，只包含merge文件和当前活跃的db文件
    mLogFilePool_.erase(mLogFilePool_.begin(), savedMergeFileNode);

    // 重命名merge文件
    for (auto it = mLogFilePool_.begin(); it != mLogFilePool_.end(); ++it) {
      auto name = BuildLogFileNameByIdx(std::distance(mLogFilePool_.begin(), it));
      std::filesystem::rename(it->name, name);
      it->name = name;
    }

    // 设置可用文件位置
    mAvailableNode_ = std::prev(mLogFilePool_.end());
  }
}