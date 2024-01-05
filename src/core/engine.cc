#include "engine.h"
#include "errors/runtime.h"
#include "flag/flags.h"
#include "log/serverlog.h"
#include "memory.h"
#include "utils/utils.h"
#include <climits>

namespace foxbatdb {
    static bool ValidateFileRecordHeader(const FileRecordHeader& header) {
        if (!utils::IsValidTimestamp(header.timestamp))
            return false;

        if (TxRuntimeState::kData != header.txRuntimeState)
            return false;

        if (header.dbIdx > Flags::GetInstance().dbMaxNum)
            return false;

        if (header.keySize > Flags::GetInstance().keyMaxBytes)
            return false;

        if (header.valSize > Flags::GetInstance().valMaxBytes)
            return false;

        return true;
    }

    static bool ValidateTxFlagRecord(const FileRecordHeader& header) {
        if (!utils::IsValidTimestamp(header.timestamp))
            return false;

        if (TxRuntimeState::kData == header.txRuntimeState)
            return false;

        if (header.dbIdx > Flags::GetInstance().dbMaxNum)
            return false;

        if ((TxRuntimeState::kBegin != header.txRuntimeState) && (0 != header.keySize))
            return false;

        if ((TxRuntimeState::kBegin == header.txRuntimeState) && (0 == header.keySize))
            return false;

        if (header.valSize != 0)
            return false;

        return true;
    }

    // CRC32算法表
    static std::uint32_t CRC32Table[256];

    // 生成CRC32表
    static void GenerateCRC32Table() {
        for (int i = 0; i < 256; ++i) {
            std::uint32_t crc = i;
            for (int j = 0; j < 8; ++j) {
                crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320 : 0);
            }
            CRC32Table[i] = crc;
        }
    }

    std::uint32_t FileRecord::CalculateCRC32Value() const {
        return header.CalculateCRC32Value(data.key, data.value);
    }

    std::uint32_t FileRecordHeader::CalculateCRC32Value(const std::string& k, const std::string& v) const {
        static bool IsCRC32TableInited = false;
        if (!IsCRC32TableInited) {
            GenerateCRC32Table();
            IsCRC32TableInited = true;
        }

        std::uint32_t crcVal = 0xFFFFFFFF;

        auto calcCRC32Once = [&crcVal](const char* buf, std::size_t size) -> void {
            for (std::size_t i = 0; i < size; ++i) {
                crcVal = (crcVal >> 8) ^ CRC32Table[(crcVal ^ buf[i]) & 0xFF];
            }
        };

        calcCRC32Once(reinterpret_cast<const char*>(&timestamp),
                      sizeof(timestamp));
        calcCRC32Once(reinterpret_cast<const char*>(&dbIdx),
                      sizeof(dbIdx));
        calcCRC32Once(reinterpret_cast<const char*>(&txRuntimeState),
                      sizeof(txRuntimeState));
        calcCRC32Once(reinterpret_cast<const char*>(&keySize),
                      sizeof(keySize));
        calcCRC32Once(reinterpret_cast<const char*>(&valSize),
                      sizeof(valSize));
        calcCRC32Once(k.data(), k.length());
        calcCRC32Once(v.data(), v.length());

        return crcVal ^ 0xFFFFFFFF;
    }

    bool FileRecord::LoadFromDisk(FileRecord& record, std::fstream& file, std::streampos pos) {
        if (pos < 0)
            return {};

        file.seekg(pos, std::ios_base::beg);
        file.read(reinterpret_cast<char*>(&record.header), sizeof(record.header));

        if (TxRuntimeState::kData == record.header.txRuntimeState) {
            if (!ValidateFileRecordHeader(record.header))
                return {};

            record.data.key.resize(record.header.keySize);
            record.data.value.resize(record.header.valSize);

            file.read(record.data.key.data(), record.header.keySize);
            file.read(record.data.value.data(), record.header.valSize);
        } else {
            if (!ValidateTxFlagRecord(record.header))
                return {};
        }

        return record.header.crc == record.CalculateCRC32Value();
    }

    void FileRecord::DumpToDisk(std::fstream& file, std::uint8_t dbIdx) {
        DumpToDisk(file, dbIdx, data.key, data.value);
    }

    void FileRecord::DumpToDisk(std::fstream& file, std::uint8_t dbIdx,
                                const std::string& k, const std::string& v) {
        FileRecordHeader header{
                .crc = 0,
                .timestamp = utils::GetMicrosecondTimestamp(),
                .txRuntimeState = TxRuntimeState::kData,
                .dbIdx = dbIdx,
                .keySize = k.length(),
                .valSize = v.length()};
        header.crc = header.CalculateCRC32Value(k, v);

        file.write(reinterpret_cast<char*>(&header), sizeof(header));
        file.write(k.data(), k.length());
        file.write(v.data(), v.length());
        file.flush();
    }

    void FileRecord::DumpTxFlagToDisk(std::fstream& file, std::uint8_t dbIdx,
                                      TxRuntimeState txFlag, std::size_t txCmdNum) {
        FileRecordHeader header{
                .crc = 0,
                .timestamp = utils::GetMicrosecondTimestamp(),
                .txRuntimeState = txFlag,
                .dbIdx = dbIdx,
                .keySize = (TxRuntimeState::kBegin == txFlag) ? txCmdNum : 0,
                .valSize = 0};
        header.crc = header.CalculateCRC32Value("", "");
    }

    const std::chrono::milliseconds RecordObject::INVALID_EXPIRE_TIME{ULLONG_MAX};

    RecordObject::RecordObject(std::uint8_t dbIdx, DataLogFileObjPtr file,
                             std::streampos pos, std::chrono::milliseconds ms,
                             std::chrono::time_point<std::chrono::steady_clock> createdTime)
        : dbIdx{dbIdx}, logFilePtr{file}, pos{pos}, expirationTimeMs{ms}, createdTime{createdTime} {}

    std::shared_ptr<RecordObject> RecordObject::New(std::uint8_t dbIdx,
                                                  const std::string& k,
                                                  const std::string& v) {
        return RecordObject::New(dbIdx, k, v, INVALID_EXPIRE_TIME);
    }

    std::shared_ptr<RecordObject> RecordObject::New(std::uint8_t dbIdx,
                                                  const std::string& k,
                                                  const std::string& v,
                                                  std::chrono::milliseconds ms) {
        auto& logFileManager = DataLogFileManager::GetInstance();
        std::shared_ptr<RecordObject> obj{new RecordObject(dbIdx, logFileManager.GetAvailableLogFile(), -1, ms)};
        obj->UpdateValue(k, v);
        return obj;
    }

    std::shared_ptr<RecordObject> RecordObject::NewForMerge(DataLogFileObjPtr file,
                                                          std::uint8_t dbIdx,
                                                          const std::string& k,
                                                          const std::string& v) {
        std::shared_ptr<RecordObject> obj{new RecordObject(dbIdx, file)};
        obj->UpdateValue(k, v);
        return obj;
    }

    std::shared_ptr<RecordObject> RecordObject::NewForHistory(
            DataLogFileObjPtr file, std::streampos pos, std::uint8_t dbIdx, std::uint64_t microSecondTimestamp) {
        auto createdTime = utils::MicrosecondTimestampCovertToTimePoint(microSecondTimestamp);
        return std::shared_ptr<RecordObject>{new RecordObject(dbIdx, file, pos, INVALID_EXPIRE_TIME, createdTime)};
    }

    std::optional<std::string> RecordObject::GetValue() const {
        auto record = CovertToFileRecord();
        if (record.has_value()) {
            return record->data.value;
        } else {
            return {};
        }
    }

    void RecordObject::UpdateValue(const std::string& k, const std::string& v) {
        auto logFile = logFilePtr.lock();
        // 当前record文件读指针位置为写入数据之前的写指针位置
        logFile->file.sync();
        logFile->file.clear();
        this->pos = logFile->file.tellp();
        // 刷入日志文件（磁盘）
        FileRecord::DumpToDisk(logFile->file, this->dbIdx, k, v);
    }

    void RecordObject::DeleteValue(const std::string& k) {
        UpdateValue(k, "");
    }

    DataLogFileObjPtr RecordObject::GetLogFileHandler() const {
        return this->logFilePtr;
    }

    std::optional<FileRecord> RecordObject::CovertToFileRecord() const {
        FileRecord record;
        if (FileRecord::LoadFromDisk(record, logFilePtr.lock()->file, pos)) {
            return record;
        } else {
            return {};
        }
    }

    bool RecordObject::IsSameLogFile(DataLogFileObjPtr targetFilePtr) const {
        auto targetFile = targetFilePtr.lock();
        auto logFile = logFilePtr.lock();
        if (!targetFile || !logFile) {
            return false;
        }
        return &targetFile->file == &logFile->file;
    }

    std::fstream::pos_type RecordObject::GetFileOffset() const {
        return pos;
    }

    void RecordObject::SetExpiration(std::chrono::seconds sec) {
        SetExpiration(std::chrono::duration_cast<std::chrono::milliseconds>(sec));
    }

    void RecordObject::SetExpiration(std::chrono::milliseconds ms) {
        expirationTimeMs = ms;
    }

    std::chrono::milliseconds RecordObject::GetExpiration() const {
        return expirationTimeMs;
    }

    bool RecordObject::IsExpired() const {
        if (expirationTimeMs == std::chrono::milliseconds{ULLONG_MAX}) {
            return false;
        }
        auto exTime = createdTime + expirationTimeMs;
        return std::chrono::steady_clock::now() >= exTime;
    }

    StorageEngine::StorageEngine(std::uint8_t dbIdx, MaxMemoryStrategy* maxMemoryStrategy)
        : mDBIdx_{dbIdx}, mMaxMemoryStrategy_{maxMemoryStrategy} {
        assert(nullptr != mMaxMemoryStrategy_);
    }

    StorageEngine::StorageEngine(StorageEngine&& rhs) noexcept
        : mDBIdx_{rhs.mDBIdx_}, mMaxMemoryStrategy_{rhs.mMaxMemoryStrategy_}, mHATTrieTree_{std::move(rhs.mHATTrieTree_)} {
        rhs.mMaxMemoryStrategy_ = nullptr;
    }

    StorageEngine& StorageEngine::operator=(StorageEngine&& rhs) noexcept {
        if (this != &rhs) {
            mDBIdx_ = rhs.mDBIdx_;
            mMaxMemoryStrategy_ = rhs.mMaxMemoryStrategy_;
            mHATTrieTree_ = std::move(rhs.mHATTrieTree_);
            rhs.mMaxMemoryStrategy_ = nullptr;
        }
        return *this;
    }

    void StorageEngine::ReleaseMemory() {
        mMaxMemoryStrategy_->ReleaseKey(this);
    }

    bool StorageEngine::HaveMemoryAvailable() const {
        return mMaxMemoryStrategy_->HaveMemoryAvailable();
    }

    void StorageEngine::InsertTxFlag(TxRuntimeState txFlag, std::size_t txCmdNum) const {
        if (TxRuntimeState::kBegin != txFlag)
            assert(0 == txCmdNum);
        else
            assert(0 != txCmdNum);
        auto& logFileManager = DataLogFileManager::GetInstance();
        auto& targetLogFile = logFileManager.GetAvailableLogFile().lock()->file;
        FileRecord::DumpTxFlagToDisk(targetLogFile, mDBIdx_, txFlag, txCmdNum);
    }

    std::error_code StorageEngine::Put(const std::string& key, const std::string& val) {
        std::error_code ec;
        this->Put(ec, key, val);
        return ec;
    }

    std::weak_ptr<RecordObject> StorageEngine::Put(std::error_code& ec,
                                                  const std::string& key, const std::string& val) {
        ec = error::RuntimeErrorCode::kSuccess;
        auto valObj = RecordObject::New(mDBIdx_, key, val);
        if (!valObj) {
            ServerLog::Error("memory allocate failed");
            ec = error::RuntimeErrorCode::kMemoryOut;
            return {};
        }

        mHATTrieTree_[key] = valObj;
        mMaxMemoryStrategy_->UpdateStateForWriteOp(key);
        return valObj;
    }

    std::error_code StorageEngine::PutForMerge(DataLogFileObjPtr file,
                                               const std::string& key, const std::string& val) {
        auto valObj = RecordObject::NewForMerge(file, mDBIdx_, key, val);
        if (!valObj) {
            ServerLog::Error("memory allocate failed");
            return error::RuntimeErrorCode::kMemoryOut;
        }

        mHATTrieTree_[key] = valObj;
        return error::RuntimeErrorCode::kSuccess;
    }

    std::error_code StorageEngine::PutForHistoryData(DataLogFileObjPtr file, std::streampos pos,
                                                     std::uint64_t microSecondTimestamp,
                                                     const std::string& key) {
        auto valObj = RecordObject::NewForHistory(file, pos, mDBIdx_, microSecondTimestamp);
        if (!valObj) {
            return error::RuntimeErrorCode::kMemoryOut;
        }

        mHATTrieTree_[key] = valObj;
        return error::RuntimeErrorCode::kSuccess;
    }

    bool StorageEngine::Contains(const std::string& key) const {
        return mHATTrieTree_.count_ks(key.data(), key.length()) > 0;
    }

    std::string StorageEngine::Get(std::error_code& ec, const std::string& key) {
        auto valObj = this->Get(key);
        if (!valObj.lock()) {
            ec = error::RuntimeErrorCode::kKeyNotFound;
            return {};
        }

        if (auto val = valObj.lock()->GetValue(); val.has_value()) {
            ec = error::RuntimeErrorCode::kSuccess;
            return *val;
        } else {
            ec = error::RuntimeErrorCode::kIntervalError;
            return {};
        }
    }

    std::weak_ptr<RecordObject> StorageEngine::Get(const std::string& key) {
        if (!mHATTrieTree_.count(key)) {
            return {};
        }

        auto valObj = mHATTrieTree_.at(key);
        if (valObj->IsExpired()) {
            this->Del(key);
            return {};
        }

        if (!valObj->GetValue().has_value()) {
            return {};
        }
        mMaxMemoryStrategy_->UpdateStateForReadOp(key);
        return valObj;
    }

    std::error_code StorageEngine::Del(const std::string& key) {
        if (!mHATTrieTree_.count_ks(key.data(), key.length())) {
            return error::RuntimeErrorCode::kKeyNotFound;
        }

        auto valObj = mHATTrieTree_.at_ks(key.data(), key.length());
        valObj->DeleteValue(key);
        mHATTrieTree_.erase_ks(key.data(), key.length());
        return error::RuntimeErrorCode::kSuccess;
    }

    std::vector<std::string> StorageEngine::PrefixSearch(const std::string& prefix) const {
        std::vector<std::string> ret;
        auto prefixRange = mHATTrieTree_.equal_prefix_range({prefix.data(), prefix.length()});
        for (auto it = prefixRange.first; it != prefixRange.second; ++it) {
            if (auto val = it.value()->GetValue(); val.has_value()) {
                mMaxMemoryStrategy_->UpdateStateForReadOp(it.key());
                ret.emplace_back(*val);
            }
        }
        return ret;
    }

    void StorageEngine::Foreach(ForeachCallback&& callback) {
        for (auto it = mHATTrieTree_.cbegin(); it != mHATTrieTree_.cend(); ++it) {
            callback(it.key(), *(it.value()));
        }
    }
}// namespace foxbatdb