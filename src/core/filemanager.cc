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
    // ������ʷ����
    LoadHistoryRecordsFromDisk();
    if (!mLogFilePool_.empty()) {
      MergeLogFile();  // �ϲ���ʷlog�ļ�
      return;
    }

    // ���ļ��в����ڣ��򴴽��ļ���
    if (!std::filesystem::exists(flags.dbFileDir) ||
        !std::filesystem::is_directory(flags.dbFileDir)) {
      if (!std::filesystem::create_directory(flags.dbFileDir)) {
        throw std::runtime_error{"log file directory create failed"};
      }
    }

    // �����ļ�
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
    // ��ȡĿ��Ŀ¼��ƥ����־�ļ���ʽ�������ļ���
    std::vector<std::string> fileNames;
    for (auto& p : std::filesystem::directory_iterator{flags.dbFileDir}) {
      if (!p.exists() ||
          !p.is_regular_file() ||
          !std::regex_match(p.path().string(), regexpr))
        continue;
      fileNames.emplace_back(p.path().string());
      // std::cout << "history file: " << p.path().string() << ", hard link count: " << p.hard_link_count() << std::endl;
    }

    // ���ļ����ֵ�������ļ���
    std::sort(fileNames.begin(), fileNames.end());
    for (const auto& fileName : fileNames) {
        std::fstream file{fileName, std::ios::in | std::ios::out |
                                        std::ios::binary | std::ios::app};
        if (!file.is_open()) continue;
        mLogFilePool_.emplace_back(
            LogFileWrapper{.file = std::move(file), .name = fileName});
    }

    // ���ζ��ļ����dict
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
    // ���ÿ����ļ�λ��
    mAvailableNode_ = std::prev(mLogFilePool_.end());
  }

  void LogFileManager::MergeLogFile() {
    // ��¼��ǰ��Ծ���ļ�����
    auto currentAvailableNode = mAvailableNode_;

    // ����merge�ļ�
    auto mergeFileName = BuildLogFileName("merge");
    std::fstream mergeFile{mergeFileName, std::ios::in | std::ios::out |
                                              std::ios::binary |
                                              std::ios::app};
    if (!mergeFile.is_open()) 
      return;

    auto savedMergeFileNode = mLogFilePool_.insert(currentAvailableNode,
        LogFileWrapper{.file = std::move(mergeFile), .name = mergeFileName});
    auto& savedMergeFile = savedMergeFileNode->file;

    // �ϲ�db�ļ�
    auto& dbm = DatabaseManager::GetInstance();
    for (std::size_t i = 0; i < dbm.GetDBListSize(); ++i) {
      auto* db = dbm.GetDBByIndex(i);
      // ����DB�����л�Ծ��key�Ͷ�Ӧ���ڴ��¼
      db->Foreach(
          [&](const BinaryString& key, const ValueObject& valObj) -> void {
            // ���ϲ���ǰ�����õ�db�ļ�
            if (valObj.IsSameLogFile(savedMergeFileNode->file)) 
              return;
            // ��ȡ���ڴ���
            auto record = valObj.CovertToFileRecord();
            if (!record.has_value()) 
              return;
            // ����Ծ��key�ͼ�¼д��merge�ļ��ں󣬸����ڴ��ϣ��
            db->StrSetForMerge(savedMergeFile, i, key, record->data.value);
        }
      );
    }

    // ɾ��ԭ�ȵ�ֻ���ļ�
    for (auto it = mLogFilePool_.begin(); it != savedMergeFileNode; ++it) {
      it->file.close();
      std::filesystem::remove(it->name);
    }

    // ����file pool��ֻ����merge�ļ��͵�ǰ��Ծ��db�ļ�
    mLogFilePool_.erase(mLogFilePool_.begin(), savedMergeFileNode);

    // ������merge�ļ�
    for (auto it = mLogFilePool_.begin(); it != mLogFilePool_.end(); ++it) {
      auto name = BuildLogFileNameByIdx(std::distance(mLogFilePool_.begin(), it));
      std::filesystem::rename(it->name, name);
      it->name = name;
    }

    // ���ÿ����ļ�λ��
    mAvailableNode_ = std::prev(mLogFilePool_.end());
  }
}