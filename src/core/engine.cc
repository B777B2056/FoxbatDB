#include "engine.h"
#include "errors/runtime.h"
#include "flag/flags.h"
#include "log/serverlog.h"
#include "memory.h"
#include "utils/utils.h"

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

    std::uint32_t FileRecordHeader::CalculateCRC32Value(const std::string& k, const std::string& v) const {
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

    bool FileRecordHeader::LoadFromDisk(FileRecordHeader& header, std::fstream& file, std::streampos pos) {
        if (pos < 0)
            return false;

        file.seekg(pos, std::ios_base::beg);
        file.read(reinterpret_cast<char*>(&header), sizeof(header));

        if (TxRuntimeState::kData == header.txRuntimeState) {
            if (!ValidateFileRecordHeader(header))
                return false;
        } else {
            if (!ValidateTxFlagRecord(header))
                return false;
        }
        return true;
    }

    void FileRecordHeader::SetCRC(const std::string& k, const std::string& v) {
        crc = CalculateCRC32Value(k, v);
    }

    bool FileRecordHeader::CheckCRC(const std::string& k, const std::string& v) const {
        return crc == CalculateCRC32Value(k, v);
    }

    void FileRecordData::LoadFromDisk(FileRecordData& data, std::fstream& file,
                                      std::size_t keySize, std::size_t valSize) {
        if (!keySize || !valSize) return;

        data.key.resize(keySize);
        data.value.resize(valSize);

        file.read(data.key.data(), static_cast<std::streamsize>(keySize));
        file.read(data.value.data(), static_cast<std::streamsize>(valSize));
    }

    bool FileRecord::LoadFromDisk(FileRecord& record, std::fstream& file, std::streampos pos) {
        if (!FileRecordHeader::LoadFromDisk(record.header, file, pos))
            return false;

        if (TxRuntimeState::kData == record.header.txRuntimeState)
            FileRecordData::LoadFromDisk(record.data, file, record.header.keySize, record.header.valSize);

        return record.header.CheckCRC(record.data.key, record.data.value);
    }

    void FileRecord::DumpRecordToDisk(std::fstream& file, std::uint8_t dbIdx,
                                      const std::string& k, const std::string& v) {
        FileRecordHeader header{
                .crc = 0,
                .timestamp = utils::GetMicrosecondTimestamp(),
                .txRuntimeState = TxRuntimeState::kData,
                .dbIdx = dbIdx,
                .keySize = k.length(),
                .valSize = v.length()};
        header.SetCRC(k, v);

        file.write(reinterpret_cast<char*>(&header), sizeof(header));
        file.write(k.data(), static_cast<std::streamsize>(k.length()));
        file.write(v.data(), static_cast<std::streamsize>(v.length()));
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
        header.SetCRC("", "");

        file.write(reinterpret_cast<char*>(&header), sizeof(header));
        file.flush();
    }

    RecordObject::RecordObject(const RecordMetaObject& opt)
        : RecordMetaObject(opt) {}

    std::shared_ptr<RecordObject> RecordObject::New(const RecordMetaObject& opt,
                                                    const std::string& k, const std::string& v) {
        std::shared_ptr<RecordObject> obj{new RecordObject(opt)};
        if (!k.empty() && !v.empty())
            obj->UpdateValue(k, v);
        return obj;
    }

    std::string RecordObject::GetValue() const {
        if (FileRecord record; CovertToFileRecord(record)) {
            return record.data.value;
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
        FileRecord::DumpRecordToDisk(logFile->file, this->dbIdx, k, v);
    }

    void RecordObject::MarkAsDeleted(const std::string& k) {
        UpdateValue(k, "");
    }

    DataLogFileObjPtr RecordObject::GetDataLogFileHandler() const {
        return this->logFilePtr;
    }

    bool RecordObject::CovertToFileRecord(FileRecord& record) const {
        return FileRecord::LoadFromDisk(record, logFilePtr.lock()->file, pos);
    }

    bool RecordObject::IsInTargetDataLogFile(DataLogFileObjPtr targetFilePtr) const {
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

    std::weak_ptr<RecordObject> StorageEngine::Put(std::error_code& ec,
                                                   const std::string& key, const std::string& val) {
        ec = error::RuntimeErrorCode::kSuccess;
        RecordMetaObject opt{.dbIdx = mDBIdx_};
        auto valObj = RecordObject::New(opt, key, val);
        if (!valObj) {
            ServerLog::Error("memory allocate failed");
            ec = error::RuntimeErrorCode::kMemoryOut;
            return {};
        }

        mHATTrieTree_[key] = valObj;
        mMaxMemoryStrategy_->UpdateStateForWriteOp(key);
        return valObj;
    }

    std::error_code StorageEngine::InnerPut(const StorageEngine::InnerPutOption& opt,
                                            const std::string& key, const std::string& val) {
        RecordMetaObject meta{.dbIdx = mDBIdx_, .logFilePtr = opt.logFilePtr};
        if (opt.pos != -1) meta.pos = opt.pos;
        if (opt.microSecondTimestamp != 0)
            meta.createdTime = utils::MicrosecondTimestampCovertToTimePoint(opt.microSecondTimestamp);

        auto valObj = RecordObject::New(meta, key, val);
        if (!valObj) {
            ServerLog::Error("memory allocate failed");
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

        if (auto val = valObj.lock()->GetValue(); !val.empty()) {
            ec = error::RuntimeErrorCode::kSuccess;
            return val;
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
            Del(key);
            return {};
        }

        if (valObj->GetValue().empty()) {
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
        valObj->MarkAsDeleted(key);
        mHATTrieTree_.erase_ks(key.data(), key.length());
        return error::RuntimeErrorCode::kSuccess;
    }

    std::vector<std::string> StorageEngine::PrefixSearch(const std::string& prefix) const {
        std::vector<std::string> ret;
        auto prefixRange = mHATTrieTree_.equal_prefix_range({prefix.data(), prefix.length()});
        for (auto it = prefixRange.first; it != prefixRange.second; ++it) {
            if (auto val = it.value()->GetValue(); !val.empty()) {
                mMaxMemoryStrategy_->UpdateStateForReadOp(it.key());
                ret.emplace_back(val);
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