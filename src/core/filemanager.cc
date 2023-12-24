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
#include "flag/flags.h"
#include "core/db.h"

namespace foxbatdb {
  static constexpr std::string_view CFileNamePrefix = "foxbat-";
  static constexpr std::string_view CFileNameSuffix = ".db";

  static std::string BuildLogFileName(const std::string& content) {
    return Flags::GetInstance().dbFileDir + "/" + std::string{CFileNamePrefix} +
           content + std::string{CFileNameSuffix};
  }

  static std::string BuildLogFileNameByIdx(std::size_t idx) {
    return BuildLogFileName(std::to_string(idx));
  }

  LogFileManager::LogFileManager() {
    // ������ʷ����
    if (std::filesystem::exists(Flags::GetInstance().dbFileDir)) {
      LoadHistoryRecordsFromDisk();
      if (!mLogFilePool_.empty()) {
        return;
      }
    }

    // ���ļ��в����ڣ��򴴽��ļ���
    if (!std::filesystem::exists(Flags::GetInstance().dbFileDir) ||
        !std::filesystem::is_directory(Flags::GetInstance().dbFileDir)) {
      if (!std::filesystem::create_directory(Flags::GetInstance().dbFileDir)) {
        throw std::runtime_error{"log file directory create failed"};
      }
    }

    // �����ļ�
    for (std::size_t i = 0; i < 1; ++i) {
      auto fileName = BuildLogFileNameByIdx(i);
      std::fstream file{fileName, std::ios::in | std::ios::out |
                                      std::ios::binary | std::ios::app};
      if (!file.is_open()) {
        throw std::runtime_error{ std::string{"log file create failed: "} + ::strerror(errno) };
      }
      mLogFilePool_.emplace_back(
        std::make_shared<LogFileWrapper>(fileName, std::move(file))
      );
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

  LogFileObjPtr LogFileManager::GetAvailableLogFile() {
    if (std::filesystem::file_size((*mAvailableNode_)->name) > Flags::GetInstance().dbFileMaxSize) {
      if (std::next(mAvailableNode_, 1) == mLogFilePool_.end())
        PoolExpand();
    }
    return *mAvailableNode_;
  }

  void LogFileManager::PoolExpand() {
    auto poolSize = mLogFilePool_.size();
    for (std::size_t i = poolSize; i < 1 + poolSize; ++i) {
      auto fileName = BuildLogFileNameByIdx(i);
      std::fstream file{fileName, std::ios::in | std::ios::out |
                                      std::ios::binary | std::ios::app};
      if (!file.is_open()) {
        std::cerr << "DB file open failed: " << ::strerror(errno);
        continue;
      }

      mLogFilePool_.emplace_back(
        std::make_shared<LogFileWrapper>(fileName, std::move(file))
      );
    }

    if (std::next(mAvailableNode_, 1) != mLogFilePool_.end()) {
      ++mAvailableNode_;
    }
  }
  
  bool LogFileManager::LoadHistoryTxFromDisk(std::shared_ptr<LogFileWrapper> fileWrapper, 
                                             std::uint64_t txNum) {
    auto& file = fileWrapper->file;
    auto& dbm = DatabaseManager::GetInstance();

    std::vector<std::pair<std::streampos, FileRecord>> txRecords;
    for (std::uint64_t i = 0; i < txNum; ++i) {
      FileRecord txRecord;
      auto pos = file.tellg();
      if (!FileRecord::LoadFromDisk(txRecord, file, pos)) 
        return false;
      if (TxRuntimeState::kFailed == txRecord.header.txRuntimeState) 
        return true;
      if (TxRuntimeState::kData != txRecord.header.txRuntimeState)
        return false;
      txRecords.emplace_back(pos, txRecord);
    }

    FileRecord txEndFlag;
    if (!FileRecord::LoadFromDisk(txEndFlag, file, file.tellg())) return false;
    if (TxRuntimeState::kFinish != txEndFlag.header.txRuntimeState) {
      return false;
    }

    for (const auto& [pos, record] : txRecords) {
      if (record.header.valSize > 0) {
        dbm.GetDBByIndex(record.header.dbIdx)
          ->StrSetForHistoryData(fileWrapper, pos, record);
      }
    }

    return true;
  }

  void LogFileManager::LoadHistoryRecordsFromDisk() {
    auto& dbm = DatabaseManager::GetInstance();
    std::stringstream ss;
    ss << Flags::GetInstance().dbFileDir << "/" << CFileNamePrefix
       << "[[:digit:]]+\\" << CFileNameSuffix;
    std::regex regexpr{ss.str()};
    // ��ȡĿ��Ŀ¼��ƥ����־�ļ���ʽ�������ļ���
    std::vector<std::string> fileNames;
    for (auto& p : std::filesystem::directory_iterator{Flags::GetInstance().dbFileDir}) {
      if (!p.exists() ||
          !p.is_regular_file() ||
          !p.file_size() ||
          !std::regex_match(p.path().string(), regexpr))
        continue;
      fileNames.emplace_back(p.path().string());
    }

    if (fileNames.empty())
      return;

    // ���ļ����ֵ�������ļ���
    std::sort(fileNames.begin(), fileNames.end());
    for (const auto& fileName : fileNames) {
        std::fstream file{fileName, std::ios::in | std::ios::out |
                                        std::ios::binary | std::ios::app};
        if (!file.is_open()) {
          std::cerr << "DB file open failed: " << ::strerror(errno);
          continue; 
        }
        mLogFilePool_.emplace_back(
          std::make_shared<LogFileWrapper>(fileName, std::move(file))
        );
    }

    // ���ζ��ļ����dict
    for (auto& fileWrapper : mLogFilePool_) {
      auto& file = fileWrapper->file;
      while (!file.eof()) {
        FileRecord record;
        auto pos = file.tellg();
        if (!FileRecord::LoadFromDisk(record, file, pos)) break;

        if (TxRuntimeState::kData == record.header.txRuntimeState) {
          // �ָ���ͨ���ݼ�¼
          if (record.header.valSize > 0) {
            dbm.GetDBByIndex(record.header.dbIdx)
              ->StrSetForHistoryData(fileWrapper, pos, record);
          }
        } else {
          // �ָ������¼
          if (TxRuntimeState::kBegin != record.header.txRuntimeState)
            break;

          if (!LoadHistoryTxFromDisk(fileWrapper, record.header.keySize))
            break;
        }
      }

      file.clear();
    }
    // ���ÿ����ļ�λ��
    mAvailableNode_ = std::prev(mLogFilePool_.end());
  }

  void LogFileManager::Merge() {
    // ����merge�ļ�
    auto mergeFileName = BuildLogFileName("merge");
    std::fstream mergeFile{mergeFileName, std::ios::in | std::ios::out |
                                              std::ios::binary | std::ios::app};
    if (!mergeFile.is_open()) {
      std::cerr << "DB merge file open failed: " << ::strerror(errno);
      return;
    }

    // ��¼��ǰ��Ծ���ļ�����
    auto currentAvailableNode = mAvailableNode_;
    // ���ļ��ز���merge�ļ�
    auto savedMergeFileNode = mLogFilePool_.insert(
      currentAvailableNode,
      std::make_shared<LogFileWrapper>(mergeFileName, std::move(mergeFile))
    );

    // �ϲ�db�ļ�
    auto& dbm = DatabaseManager::GetInstance();
    for (std::size_t i = 0; i < dbm.GetDBListSize(); ++i) {
      auto* db = dbm.GetDBByIndex(i);
      // ����DB�����л�Ծ��key�Ͷ�Ӧ���ڴ��¼
      db->Foreach(
          [db, currentAvailableNode, savedMergeFileNode]
          (const std::string& key, const ValueObject& valObj) -> void {
            // ���ϲ���ǰ�����õ�db�ļ�
            if (valObj.IsSameLogFile(*currentAvailableNode))
              return;
            // ��ȡ���ڴ��У�����Ծ��key�ͼ�¼д��merge�ļ��ں󣬸����ڴ��ϣ��
            if (auto record = valObj.CovertToFileRecord(); record.has_value()) 
              db->StrSetForMerge(*savedMergeFileNode, key, record->data.value);
        }
      );
    }

    // ɾ��ԭ�ȵ�ֻ���ļ�
    for (auto it = mLogFilePool_.begin(); it != savedMergeFileNode; ++it) {
      (*it)->file.close();
      std::filesystem::remove((*it)->name);
    }

    // ����file pool��ֻ����merge�ļ��͵�ǰ��Ծ��db�ļ�
    mLogFilePool_.erase(mLogFilePool_.begin(), savedMergeFileNode);

    // ������merge�ļ�
    for (auto it = mLogFilePool_.begin(); it != mLogFilePool_.end(); ++it) {
      auto name = BuildLogFileNameByIdx(std::distance(mLogFilePool_.begin(), it));
      std::filesystem::rename((*it)->name, name);
      (*it)->name = name;
    }

    // ���ÿ����ļ�λ��
    mAvailableNode_ = std::prev(mLogFilePool_.end());
  }
}