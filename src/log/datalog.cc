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
#include <vector>

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
        // ������ʷ����
        if (std::filesystem::exists(Flags::GetInstance().dbLogFileDir)) {
            LoadHistoryRecordsFromDisk();
            if (!mLogFilePool_.empty()) {
                return;
            }
        }

        // ���ļ��в����ڣ��򴴽��ļ���
        if (!std::filesystem::exists(Flags::GetInstance().dbLogFileDir) ||
            !std::filesystem::is_directory(Flags::GetInstance().dbLogFileDir)) {
            if (!std::filesystem::create_directory(Flags::GetInstance().dbLogFileDir)) {
                ServerLog::GetInstance().Fatal("log file directory create failed");
            }
        }

        // �����ļ�
        for (std::size_t i = 0; i < 1; ++i) {
            auto fileName = BuildLogFileNameByIdx(i);
            std::fstream file{fileName, std::ios::in | std::ios::out |
                                                std::ios::binary | std::ios::app};
            if (!file.is_open()) {
                ServerLog::GetInstance().Fatal("log file create failed: {}", ::strerror(errno));
            }
            mLogFilePool_.emplace_back(
                    std::make_unique<DataLogFileWrapper>(fileName, std::move(file)));
        }
        mAvailableNode_ = mLogFilePool_.begin();
    }

    DataLogFileManager& DataLogFileManager::GetInstance() {
        static DataLogFileManager instance;
        return instance;
    }

    void DataLogFileManager::Init() {}

    DataLogFileWrapper* DataLogFileManager::GetAvailableLogFile() {
        std::unique_lock lock{mt};
        if (std::filesystem::file_size((*mAvailableNode_)->name) > Flags::GetInstance().dbLogFileMaxSize) {
            if (std::next(mAvailableNode_, 1) == mLogFilePool_.end())
                PoolExpand();
        }
        return mAvailableNode_->get();
    }

    bool DataLogFileManager::IsRecordInCurrentAvailableLogFile(const RecordObject& record) const {
        return mAvailableNode_->get() == record.GetDataLogFileHandler();
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
                    std::make_unique<DataLogFileWrapper>(fileName, std::move(file)));
        }

        if (std::next(mAvailableNode_, 1) != mLogFilePool_.end()) {
            ++mAvailableNode_;
        }
    }

    bool DataLogFileManager::LoadHistoryTxFromDisk(DataLogFileWrapper* fileWrapper,
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

        for (const auto& [pos, record]: txRecords) {
            if (record.header.valSize > 0) {
                dbm.GetDBByIndex(record.header.dbIdx)
                        ->LoadHistoryData(fileWrapper, pos, record);
            }
        }

        return true;
    }

    void DataLogFileManager::LoadHistoryRecordsFromDisk() {
        auto& dbm = DatabaseManager::GetInstance();
        std::stringstream ss;
        ss << Flags::GetInstance().dbLogFileDir << "/" << CFileNamePrefix
           << "[[:digit:]]+\\" << CFileNameSuffix;
        std::regex regexpr{ss.str()};
        // ��ȡĿ��Ŀ¼��ƥ����־�ļ���ʽ�������ļ���
        std::vector<std::string> fileNames;
        for (auto& p: std::filesystem::directory_iterator{Flags::GetInstance().dbLogFileDir}) {
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
        for (const auto& fileName: fileNames) {
            std::fstream file{fileName, std::ios::in | std::ios::out |
                                                std::ios::binary | std::ios::app};
            if (!file.is_open()) {
                ServerLog::GetInstance().Error("history data log file open failed: {}", ::strerror(errno));
                continue;
            }
            mLogFilePool_.emplace_back(
                    std::make_unique<DataLogFileWrapper>(fileName, std::move(file)));
        }

        // ���ζ��ļ����dict
        for (auto& fileWrapper: mLogFilePool_) {
            auto& file = fileWrapper->file;
            while (!file.eof()) {
                FileRecord record;
                auto pos = file.tellg();
                if (!FileRecord::LoadFromDisk(record, file, pos)) break;

                if (TxRuntimeState::kData == record.header.txRuntimeState) {
                    // �ָ���ͨ���ݼ�¼
                    if (record.header.valSize && !record.data.value.empty()) {
                        dbm.GetDBByIndex(record.header.dbIdx)
                                ->LoadHistoryData(fileWrapper.get(), pos, record);
                    }
                } else {
                    // �ָ������¼
                    if (TxRuntimeState::kBegin != record.header.txRuntimeState)
                        break;

                    if (!LoadHistoryTxFromDisk(fileWrapper.get(), record.header.keySize))
                        break;
                }
            }

            file.clear();
        }
        // ���ÿ����ļ�λ��
        mAvailableNode_ = std::prev(mLogFilePool_.end());
    }

    static std::unique_ptr<DataLogFileWrapper> CreateMergeLogFile() {
        // ����merge�ļ�
        auto mergeFileName = BuildLogFileName("merge");
        std::fstream mergeFile{mergeFileName, std::ios::in | std::ios::out |
                                                      std::ios::binary | std::ios::app};
        if (!mergeFile.is_open()) {
            ServerLog::GetInstance().Error("merge data log file open failed: {}", ::strerror(errno));
            return nullptr;
        }

        // �½��ļ��ṹ
        return std::make_unique<DataLogFileWrapper>(mergeFileName, std::move(mergeFile));
    }

    void DataLogFileManager::ModifyDataFilesForMerge(std::unique_ptr<DataLogFileWrapper>&& mergeLogFile) {
        std::unique_lock lock{mt};
        // ���ļ��ز���merge�ļ�
        auto mergeFileIter = mLogFilePool_.insert(mAvailableNode_, std::move(mergeLogFile));

        // ɾ��ԭ�ȵ�ֻ���ļ�
        for (auto it = mLogFilePool_.begin(); it != mergeFileIter;) {
            std::filesystem::remove((*it)->name);
            it = mLogFilePool_.erase(it);
        }

        // ������merge�ļ�
        for (auto it = mLogFilePool_.begin(); it != mLogFilePool_.end(); ++it) {
            auto name = BuildLogFileNameByIdx(std::distance(mLogFilePool_.begin(), it));
            std::filesystem::rename((*it)->name, name);
            (*it)->name = name;
        }

        // ���ÿ����ļ�λ��
        mAvailableNode_ = std::prev(mLogFilePool_.end());
    }

    void DataLogFileManager::Merge() {
        if (auto mergeLogFile = CreateMergeLogFile(); mergeLogFile) {// ����merge�ļ�
            DatabaseManager::GetInstance().Merge(mergeLogFile.get());// �ϲ�db�ļ�
            this->ModifyDataFilesForMerge(std::move(mergeLogFile));  // �޸Ĵ����ļ���֯
        }
    }
}// namespace foxbatdb