#include "datalog.h"
#include "core/db.h"
#include "flag/flags.h"
#include "serverlog.h"
#include <filesystem>
#include <iterator>
#include <regex>
#include <sstream>
#include <string>
#include <string_view>

namespace foxbatdb {
    static constexpr std::string_view CFileNamePrefix = "foxbat-";
    static constexpr std::string_view CFileNameSuffix = ".db";

    static std::string BuildLogFileName(const std::string& content) {
        return Flags::GetInstance().dbLogFileDir + "/" + std::string{CFileNamePrefix} +
               content + std::string{CFileNameSuffix};
    }

    static std::string BuildLogFileNameByIdx(std::size_t idx) {
        return BuildLogFileName(std::to_string(idx));
    }

    DataLogFileManager::DataLogFileManager() {
        // 加载历史数据
        if (std::filesystem::exists(Flags::GetInstance().dbLogFileDir)) {
            LoadHistoryRecordsFromDisk();
            if (!mLogFilePool_.empty()) {
                return;
            }
        }

        // 若文件夹不存在，则创建文件夹
        if (!std::filesystem::exists(Flags::GetInstance().dbLogFileDir) ||
            !std::filesystem::is_directory(Flags::GetInstance().dbLogFileDir)) {
            if (!std::filesystem::create_directory(Flags::GetInstance().dbLogFileDir)) {
                ServerLog::GetInstance().Fatal("log file directory create failed");
            }
        }

        // 创建文件
        for (std::size_t i = 0; i < 1; ++i) {
            auto fileName = BuildLogFileNameByIdx(i);
            std::fstream file{fileName, std::ios::in | std::ios::out |
                                                std::ios::binary | std::ios::app};
            if (!file.is_open()) {
                ServerLog::GetInstance().Fatal("log file create failed: {}", ::strerror(errno));
            }
            mLogFilePool_.emplace_back(
                    std::make_unique<DataLogFile>(fileName, std::move(file)));
        }
        mWritableFileIter_ = mLogFilePool_.begin();
    }

    DataLogFileManager& DataLogFileManager::GetInstance() {
        static DataLogFileManager instance;
        return instance;
    }

    void DataLogFileManager::Init() {}

    DataLogFile* DataLogFileManager::GetWritableDataFile() {
        if (std::filesystem::file_size((*mWritableFileIter_)->name) > Flags::GetInstance().dbLogFileMaxSize) {
            if (std::next(mWritableFileIter_, 1) == mLogFilePool_.end())
                PoolExpand();
        }
        return mWritableFileIter_->get();
    }

    void DataLogFileManager::PoolExpand() {
        auto poolSize = mLogFilePool_.size();
        for (std::size_t i = poolSize; i < 1 + poolSize; ++i) {
            auto fileName = BuildLogFileNameByIdx(i);
            std::fstream file{fileName, std::ios::in | std::ios::out |
                                                std::ios::binary | std::ios::app};
            if (!file.is_open()) {
                ServerLog::GetInstance().Error("data log file pool expand failed: {}", ::strerror(errno));
                continue;
            }

            mLogFilePool_.emplace_back(
                    std::make_unique<DataLogFile>(fileName, std::move(file)));
        }

        if (std::next(mWritableFileIter_, 1) != mLogFilePool_.end()) {
            ++mWritableFileIter_;
        }
    }

    static bool LoadHistoryTxFromDisk(DataLogFile* fileWrapper, std::uint64_t txNum) {
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

        for (const auto& [pos, record]: txRecords) {
            if (record.header.valSize > 0) {
                dbm.GetDBByIndex(record.header.dbIdx)
                        ->LoadHistoryData(fileWrapper, pos, record);
            }
        }

        return true;
    }

    static bool ValidateDataLogFile(const std::filesystem::directory_entry& dir) {
        std::stringstream ss;
        ss << Flags::GetInstance().dbLogFileDir << "/" << CFileNamePrefix
           << "[[:digit:]]+\\" << CFileNameSuffix;
        std::regex pathValidator{ss.str()};
        if (!dir.exists() ||
            !dir.is_regular_file() ||
            !dir.file_size() ||
            !std::regex_match(dir.path().string(), pathValidator))
            return false;
        return true;
    }

    // 获取目标目录下匹配日志文件格式的所有文件名
    static std::vector<std::string> GetDataLogFileNamesInDirectory() {
        std::vector<std::string> fileNames;
        for (const auto& p: std::filesystem::directory_iterator{Flags::GetInstance().dbLogFileDir}) {
            if (ValidateDataLogFile(p))
                fileNames.emplace_back(p.path().string());
        }
        std::sort(fileNames.begin(), fileNames.end());
        return fileNames;
    }

    // 将历史文件纳入文件池统一管理
    bool DataLogFileManager::FillDataLogFilePoolByHistoryDataFile() {
        // 获取目标目录下匹配日志文件格式的所有文件名
        auto fileNames = GetDataLogFileNamesInDirectory();
        if (fileNames.empty()) return false;

        // 按文件名字典序填充文件池
        for (const auto& fileName: fileNames) {
            std::fstream file{fileName, std::ios::in | std::ios::out |
                                                std::ios::binary | std::ios::app};
            if (!file.is_open()) {
                ServerLog::GetInstance().Error("history data log file open failed: {}", ::strerror(errno));
                continue;
            }
            mLogFilePool_.emplace_back(
                    std::make_unique<DataLogFile>(fileName, std::move(file)));
        }
        return true;
    }

    static void LoadHistoryRecordsFromSingleFile(DataLogFile* fileWrapper) {
        for (auto& file = fileWrapper->file; !file.eof();) {
            auto pos = file.tellg();
            FileRecord record;
            if (!FileRecord::LoadFromDisk(record, file, pos)) break;

            if (TxRuntimeState::kData == record.header.txRuntimeState) {
                // 恢复普通数据记录
                if (record.header.valSize && !record.data.value.empty())
                    DatabaseManager::GetInstance()
                            .GetDBByIndex(record.header.dbIdx)
                            ->LoadHistoryData(fileWrapper, pos, record);
            } else {
                // 恢复事务记录
                if (TxRuntimeState::kBegin != record.header.txRuntimeState)
                    break;

                if (!LoadHistoryTxFromDisk(fileWrapper, record.header.keySize))
                    break;
            }
        }
    }

    void DataLogFileManager::LoadHistoryRecordsFromDisk() {
        // 加载历史数据
        if (!FillDataLogFilePoolByHistoryDataFile())
            return;

        // 依次读文件填充dict
        for (auto& fileWrapper: mLogFilePool_) {
            LoadHistoryRecordsFromSingleFile(fileWrapper.get());
            fileWrapper->file.clear();
        }

        // 设置可用文件位置
        mWritableFileIter_ = std::prev(mLogFilePool_.end());
    }

    static std::unique_ptr<DataLogFile> CreateMergeLogFile() {
        // 创建merge文件
        auto mergeFileName = BuildLogFileName("merge");
        std::fstream mergeFile{mergeFileName, std::ios::in | std::ios::out |
                                                      std::ios::binary | std::ios::app};
        if (!mergeFile.is_open()) {
            ServerLog::GetInstance().Error("merge data log file open failed: {}", ::strerror(errno));
            return nullptr;
        }

        // 新建文件结构
        return std::make_unique<DataLogFile>(mergeFileName, std::move(mergeFile));
    }

    void DataLogFileManager::ModifyDataFilesForMerge(FileIter& writableNode, FilePtr&& mergeLogFile) {
        // 向文件池插入merge文件
        auto mergeFileIter = mLogFilePool_.insert(writableNode, std::move(mergeLogFile));

        // 删除原先的只读文件
        for (auto it = mLogFilePool_.begin(); it != mergeFileIter;) {
            std::filesystem::remove((*it)->name);
            it = mLogFilePool_.erase(it);
        }

        // 重命名merge文件
        for (auto it = mLogFilePool_.begin(); it != mLogFilePool_.end(); ++it) {
            auto name = BuildLogFileNameByIdx(std::distance(mLogFilePool_.begin(), it));
            std::filesystem::rename((*it)->name, name);
            (*it)->name = name;
        }

        // 设置可用文件位置
        mWritableFileIter_ = std::prev(mLogFilePool_.end());
    }

    void DataLogFileManager::Merge() {
        if (mLogFilePool_.size() < 2) return;
        auto writableIterSnapshot = mWritableFileIter_;

        auto& dbm = DatabaseManager::GetInstance();
        if (auto mergeLogFile = CreateMergeLogFile(); mergeLogFile) {                    // 创建merge文件
            dbm.Merge(mergeLogFile.get(), writableIterSnapshot->get());                  // 合并db文件
            this->ModifyDataFilesForMerge(writableIterSnapshot, std::move(mergeLogFile));// 修改磁盘文件组织
        }
    }
}// namespace foxbatdb