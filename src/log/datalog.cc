#include "datalog.h"
#include "core/db.h"
#include "flag/flags.h"
#include "serverlog.h"
#include "utils/utils.h"
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

    namespace {
#if defined(__cpp_lib_hardware_interference_size)
#include <bit>
#define L1_CACHE_LINE_ALIGNAS alignas(std::hardware_destructive_interference_size)
#elif defined(__x86_64__)
#define L1_CACHE_LINE_ALIGNAS alignas(64)
#elif defined(__aarch64__)
#define L1_CACHE_LINE_ALIGNAS alignas(128)
#else
#define L1_CACHE_LINE_ALIGNAS
#endif

        struct L1_CACHE_LINE_ALIGNAS FileRecordHeader {
            std::uint32_t crc = 0;
            std::uint64_t timestamp = 0;
            RecordState txRuntimeState = RecordState::kData;
            std::uint8_t dbIdx = 0;
            std::uint64_t keySize = 0;
            std::uint64_t valSize = 0;

            bool LoadFromDisk(std::fstream& file, std::streampos pos) {
                if (pos < 0)
                    return false;

                file.seekg(pos, std::ios_base::beg);
                file.read(reinterpret_cast<char*>(&this->crc), sizeof(this->crc));
                file.read(reinterpret_cast<char*>(&this->timestamp), sizeof(this->timestamp));
                file.read(reinterpret_cast<char*>(&this->txRuntimeState), sizeof(this->txRuntimeState));
                file.read(reinterpret_cast<char*>(&this->dbIdx), sizeof(this->dbIdx));
                file.read(reinterpret_cast<char*>(&this->keySize), sizeof(this->keySize));
                file.read(reinterpret_cast<char*>(&this->valSize), sizeof(this->valSize));
                this->TransferEndian();

                if (RecordState::kData == txRuntimeState) {
                    if (!this->ValidateFileRecordHeader())
                        return false;
                } else {
                    if (!this->ValidateTxFlagRecord())
                        return false;
                }
                return true;
            }

            void DumpToDisk(std::fstream& file) {
                this->TransferEndian();
                file.seekp(0, std::fstream::end);
                file.write(reinterpret_cast<const char*>(&this->crc), sizeof(this->crc));
                file.write(reinterpret_cast<const char*>(&this->timestamp), sizeof(this->timestamp));
                file.write(reinterpret_cast<const char*>(&this->txRuntimeState), sizeof(this->txRuntimeState));
                file.write(reinterpret_cast<const char*>(&this->dbIdx), sizeof(this->dbIdx));
                file.write(reinterpret_cast<const char*>(&this->keySize), sizeof(this->keySize));
                file.write(reinterpret_cast<const char*>(&this->valSize), sizeof(this->valSize));
            }

            void SetCRC(const std::string& k, const std::string& v) {
                crc = CalculateCRC32Value(k, v);
            }

            bool CheckCRC(const std::string& k, const std::string& v) const {
                return CalculateCRC32Value(k, v) == crc;
            }

            void TransferEndian() {
                if constexpr (std::endian::native == std::endian::big)
                    return;

                this->crc = utils::ChangeIntegralEndian(this->crc);
                this->timestamp = utils::ChangeIntegralEndian(this->timestamp);
                this->keySize = utils::ChangeIntegralEndian(this->keySize);
                this->valSize = utils::ChangeIntegralEndian(this->valSize);
            }

        private:
            std::uint32_t CalculateCRC32Value(const std::string& k, const std::string& v) const {
                auto crcVal = utils::CRC(reinterpret_cast<const char*>(&timestamp), sizeof(timestamp),
                                         utils::CRC_INIT_VALUE);
                crcVal = utils::CRC(reinterpret_cast<const char*>(&dbIdx), sizeof(dbIdx), crcVal);
                crcVal = utils::CRC(reinterpret_cast<const char*>(&txRuntimeState), sizeof(txRuntimeState), crcVal);
                crcVal = utils::CRC(reinterpret_cast<const char*>(&keySize), sizeof(keySize), crcVal);
                crcVal = utils::CRC(reinterpret_cast<const char*>(&valSize), sizeof(valSize), crcVal);
                crcVal = utils::CRC(k.data(), k.length(), crcVal);
                crcVal = utils::CRC(v.data(), v.length(), crcVal);
                return crcVal ^ utils::CRC_INIT_VALUE;
            }

            bool ValidateFileRecordHeader() const {
                if (!utils::IsValidTimestamp(this->timestamp))
                    return false;

                if (RecordState::kData != this->txRuntimeState)
                    return false;

                if (this->dbIdx > Flags::GetInstance().dbMaxNum)
                    return false;

                if (this->keySize > Flags::GetInstance().keyMaxBytes)
                    return false;

                if (this->valSize > Flags::GetInstance().valMaxBytes)
                    return false;

                return true;
            }

            bool ValidateTxFlagRecord() const {
                if (!utils::IsValidTimestamp(this->timestamp))
                    return false;

                if (RecordState::kData == this->txRuntimeState)
                    return false;

                if (this->dbIdx > Flags::GetInstance().dbMaxNum)
                    return false;

                if ((RecordState::kBegin != this->txRuntimeState) && (0 != this->keySize))
                    return false;

                if ((RecordState::kBegin == this->txRuntimeState) && (0 == this->keySize))
                    return false;

                if (this->valSize != 0)
                    return false;

                return true;
            }
        };

        struct L1_CACHE_LINE_ALIGNAS FileRecordData {
            std::string key;
            std::string value;

            static void LoadFromDisk(FileRecordData& data, std::fstream& file,
                                     std::size_t keySize, std::size_t valSize) {
                if (!keySize || !valSize) return;

                data.key.resize(keySize);
                data.value.resize(valSize);

                file.read(data.key.data(), static_cast<std::streamsize>(keySize));
                file.read(data.value.data(), static_cast<std::streamsize>(valSize));
            }
        };

        struct L1_CACHE_LINE_ALIGNAS FileRecord {
            FileRecordHeader header;
            FileRecordData data;

            static bool LoadFromDisk(FileRecord& record, std::fstream& file, std::streampos pos) {
                if (!record.header.LoadFromDisk(file, pos))
                    return false;

                if (RecordState::kData == record.header.txRuntimeState) {
                    FileRecordData::LoadFromDisk(record.data, file, record.header.keySize, record.header.valSize);
                }

                return record.header.CheckCRC(record.data.key, record.data.value);
            }
        };
    }// namespace

    DataLogFile::DataLogFile(const std::string& fileName)
        : name{fileName},
          file{fileName, std::ios::in | std::ios::out | std::ios::binary | std::ios::app} {
        if (!file.is_open()) {
            throw std::runtime_error{std::strerror(errno)};
        }
    }

    const std::string& DataLogFile::Name() const {
        std::unique_lock l{mt};
        return this->name;
    }

    DataLogFile::OffsetType DataLogFile::GetRowBySequence(Data& data) {
        data.error = true;

        FileRecord record;
        DataLogFile::OffsetType pos = -1;
        {
            std::unique_lock l{mt};
            if (this->file.eof())
                return -1;

            pos = this->file.tellg();
            if (!FileRecord::LoadFromDisk(record, file, pos)) {
                return -1;
            }
        }

        data.error = false;
        data.timestamp = record.header.timestamp;
        data.dbIdx = record.header.dbIdx;
        data.state = RecordState::kData;

        if (RecordState::kBegin == record.header.txRuntimeState)
            data.txNum = record.header.keySize;

        data.key = std::move(record.data.key);
        data.value = std::move(record.data.value);
        return pos;
    }

    DataLogFile::Data DataLogFile::GetDataByOffset(DataLogFile::OffsetType offset) {
        FileRecord record;
        std::unique_lock l{mt};
        if (FileRecord::LoadFromDisk(record, file, offset)) {
            file.seekp(0, std::fstream::end);
            return DataLogFile::Data{
                    .dbIdx = record.header.dbIdx,
                    .state = RecordState::kData,
                    .key = std::move(record.data.key),
                    .value = std::move(record.data.value),
            };
        }
        return DataLogFile::Data{.error = true};
    }

    DataLogFile::OffsetType DataLogFile::DumpToDisk(std::uint8_t dbIdx, const std::string& k, const std::string& v) {
        std::unique_lock l{mt};
        DataLogFile::OffsetType pos = file.tellp();
        FileRecordHeader header{
                .crc = 0,
                .timestamp = utils::GetMicrosecondTimestamp(),
                .txRuntimeState = RecordState::kData,
                .dbIdx = dbIdx,
                .keySize = k.length(),
                .valSize = v.length()};
        header.SetCRC(k, v);
        header.DumpToDisk(file);

        file.write(k.data(), static_cast<std::streamsize>(k.length()));
        file.write(v.data(), static_cast<std::streamsize>(v.length()));
        file.flush();
        return pos;
    }

    void DataLogFile::DumpTxFlagToDisk(std::uint8_t dbIdx, RecordState txFlag, std::size_t txCmdNum) {
        FileRecordHeader header{
                .crc = 0,
                .timestamp = utils::GetMicrosecondTimestamp(),
                .txRuntimeState = txFlag,
                .dbIdx = dbIdx,
                .keySize = (RecordState::kBegin == txFlag) ? txCmdNum : 0,
                .valSize = 0};
        header.SetCRC("", "");

        std::unique_lock l{mt};
        header.DumpToDisk(file);
        file.flush();
    }

    void DataLogFile::Rename(const std::string& newName) {
        std::unique_lock l{mt};
        std::filesystem::rename(this->name, newName);
        this->name = newName;
    }

    void DataLogFile::ClearOSFlag() {
        std::unique_lock l{mt};
        this->file.clear();
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
            try {
                mLogFilePool_.emplace_back(std::make_unique<DataLogFile>(BuildLogFileNameByIdx(i)));
            } catch (const std::runtime_error& e) {
                ServerLog::GetInstance().Fatal("log file create failed: {}", e.what());
            }
        }
        mWritableFileIter_ = mLogFilePool_.begin();
    }

    DataLogFileManager& DataLogFileManager::GetInstance() {
        static DataLogFileManager instance;
        return instance;
    }

    void DataLogFileManager::Init() {}

    DataLogFile* DataLogFileManager::GetWritableDataFile() {
        std::unique_lock l{mt_};
        if (std::filesystem::file_size((*mWritableFileIter_)->Name()) > Flags::GetInstance().dbLogFileMaxSize) {
            if (std::next(mWritableFileIter_, 1) == mLogFilePool_.end())
                PoolExpand();
        }
        return mWritableFileIter_->get();
    }

    void DataLogFileManager::PoolExpand() {
        auto poolSize = mLogFilePool_.size();
        for (std::size_t i = poolSize; i < 1 + poolSize; ++i) {
            try {
                mLogFilePool_.emplace_back(std::make_unique<DataLogFile>(BuildLogFileNameByIdx(i)));
            } catch (const std::runtime_error& e) {
                ServerLog::GetInstance().Error("data log file pool expand failed: {}", e.what());
            }
        }

        if (std::next(mWritableFileIter_, 1) != mLogFilePool_.end()) {
            ++mWritableFileIter_;
        }
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
            try {
                mLogFilePool_.emplace_back(std::make_unique<DataLogFile>(fileName));
            } catch (const std::runtime_error& e) {
                ServerLog::GetInstance().Error("history data log file open failed: {}", e.what());
            }
        }
        return true;
    }

    static bool LoadHistoryTxFromDisk(DataLogFile* fileWrapper, std::uint64_t txNum) {
        auto& dbm = DatabaseManager::GetInstance();

        std::vector<std::pair<DataLogFile::OffsetType, DataLogFile::Data>> txRecords;
        for (std::uint64_t i = 0; i < txNum; ++i) {
            DataLogFile::Data txRecord;
            auto offset = fileWrapper->GetRowBySequence(txRecord);
            if (-1 == offset)
                return false;

            if (RecordState::kFailed == txRecord.state)
                return true;
            if (RecordState::kData != txRecord.state)
                return false;
            txRecords.emplace_back(offset, txRecord);
        }

        DataLogFile::Data txEndFlag;
        if (-1 == fileWrapper->GetRowBySequence(txEndFlag))
            return false;

        if (RecordState::kFinish != txEndFlag.state)
            return false;

        for (const auto& [pos, record]: txRecords) {
            if (!record.value.empty()) {
                dbm.GetDBByIndex(record.dbIdx)
                        ->LoadHistoryData(fileWrapper, pos, record);
            }
        }

        return true;
    }

    static void LoadHistoryRecordsFromSingleFile(DataLogFile* fileWrapper) {
        DataLogFile::Data data;
        auto offset = fileWrapper->GetRowBySequence(data);
        while (-1 != offset) {
            if (RecordState::kData == data.state) {
                // 恢复普通数据记录
                if (!data.value.empty())
                    DatabaseManager::GetInstance()
                            .GetDBByIndex(data.dbIdx)
                            ->LoadHistoryData(fileWrapper, offset, data);
            } else {
                // 恢复事务记录
                if (RecordState::kBegin != data.state)
                    break;

                if (!LoadHistoryTxFromDisk(fileWrapper, data.txNum))
                    break;
            }
            offset = fileWrapper->GetRowBySequence(data);
        }
    }

    void DataLogFileManager::LoadHistoryRecordsFromDisk() {
        // 加载历史数据
        if (!FillDataLogFilePoolByHistoryDataFile())
            return;

        // 依次读文件填充dict
        for (auto& fileWrapper: mLogFilePool_) {
            LoadHistoryRecordsFromSingleFile(fileWrapper.get());
            fileWrapper->ClearOSFlag();
        }

        // 设置可用文件位置
        mWritableFileIter_ = std::prev(mLogFilePool_.end());
    }

    static std::unique_ptr<DataLogFile> CreateMergeLogFile() {
        // 创建merge文件
        try {
            return std::make_unique<DataLogFile>(BuildLogFileName("merge"));
        } catch (const std::runtime_error& e) {
            ServerLog::GetInstance().Error("merge data log file open failed: {}", e.what());
            return nullptr;
        }
    }

    void DataLogFileManager::ModifyDataFilesForMerge(FileIter& writableNode, FilePtr&& mergeLogFile) {
        // 向文件池插入merge文件
        auto mergeFileIter = mLogFilePool_.insert(writableNode, std::move(mergeLogFile));

        // 删除原先的只读文件
        for (auto it = mLogFilePool_.begin(); it != mergeFileIter;) {
            std::filesystem::remove((*it)->Name());
            it = mLogFilePool_.erase(it);
        }

        // 重命名merge文件
        for (auto it = mLogFilePool_.begin(); it != mLogFilePool_.end(); ++it) {
            auto name = BuildLogFileNameByIdx(std::distance(mLogFilePool_.begin(), it));
            (*it)->Rename(name);
        }

        // 设置可用文件位置
        mWritableFileIter_ = std::prev(mLogFilePool_.end());
    }

    void DataLogFileManager::Merge() {
        std::unique_lock l{mt_};
        if (mLogFilePool_.size() < Flags::GetInstance().dbFileMergeThreshold) return;
        auto writableIterSnapshot = mWritableFileIter_;

        auto& dbm = DatabaseManager::GetInstance();
        if (auto mergeLogFile = CreateMergeLogFile(); mergeLogFile) {                    // 创建merge文件
            dbm.Merge(mergeLogFile.get(), writableIterSnapshot->get());                  // 合并db文件
            this->ModifyDataFilesForMerge(writableIterSnapshot, std::move(mergeLogFile));// 修改磁盘文件组织
        }
    }
}// namespace foxbatdb