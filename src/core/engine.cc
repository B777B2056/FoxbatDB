#include "engine.h"
#include "errors/runtime.h"
#include "flag/flags.h"
#include "log/serverlog.h"
#include "memory.h"
#include "utils/utils.h"
#include <bit>

namespace foxbatdb {
    bool FileRecordHeader::ValidateFileRecordHeader() const {
        if (!utils::IsValidTimestamp(this->timestamp))
            return false;

        if (TxRuntimeState::kData != this->txRuntimeState)
            return false;

        if (this->dbIdx > Flags::GetInstance().dbMaxNum)
            return false;

        if (this->keySize > Flags::GetInstance().keyMaxBytes)
            return false;

        if (this->valSize > Flags::GetInstance().valMaxBytes)
            return false;

        return true;
    }

    bool FileRecordHeader::ValidateTxFlagRecord() const {
        if (!utils::IsValidTimestamp(this->timestamp))
            return false;

        if (TxRuntimeState::kData == this->txRuntimeState)
            return false;

        if (this->dbIdx > Flags::GetInstance().dbMaxNum)
            return false;

        if ((TxRuntimeState::kBegin != this->txRuntimeState) && (0 != this->keySize))
            return false;

        if ((TxRuntimeState::kBegin == this->txRuntimeState) && (0 == this->keySize))
            return false;

        if (this->valSize != 0)
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

    bool FileRecordHeader::LoadFromDisk(std::fstream& file, std::streampos pos) {
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

        if (TxRuntimeState::kData == txRuntimeState) {
            if (!this->ValidateFileRecordHeader())
                return false;
        } else {
            if (!this->ValidateTxFlagRecord())
                return false;
        }
        return true;
    }

    void FileRecordHeader::DumpToDisk(std::fstream& file) {
        this->TransferEndian();
        file.write(reinterpret_cast<const char*>(&this->crc), sizeof(this->crc));
        file.write(reinterpret_cast<const char*>(&this->timestamp), sizeof(this->timestamp));
        file.write(reinterpret_cast<const char*>(&this->txRuntimeState), sizeof(this->txRuntimeState));
        file.write(reinterpret_cast<const char*>(&this->dbIdx), sizeof(this->dbIdx));
        file.write(reinterpret_cast<const char*>(&this->keySize), sizeof(this->keySize));
        file.write(reinterpret_cast<const char*>(&this->valSize), sizeof(this->valSize));
    }

    void FileRecordHeader::SetCRC(const std::string& k, const std::string& v) {
        crc = CalculateCRC32Value(k, v);
    }

    bool FileRecordHeader::CheckCRC(const std::string& k, const std::string& v) const {
        return CalculateCRC32Value(k, v) == crc;
    }

    void FileRecordHeader::TransferEndian() {
        if constexpr (std::endian::native == std::endian::big)
            return;

        this->crc = utils::ChangeIntegralEndian(this->crc);
        this->timestamp = utils::ChangeIntegralEndian(this->timestamp);
        this->keySize = utils::ChangeIntegralEndian(this->keySize);
        this->valSize = utils::ChangeIntegralEndian(this->valSize);
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
        if (!record.header.LoadFromDisk(file, pos))
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
        header.DumpToDisk(file);

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
        header.DumpToDisk(file);
        file.flush();
    }

    RecordObject::RecordObject(Meta&& opt) : meta{std::move(opt)} {}

    std::shared_ptr<RecordObject> RecordObject::New(Meta&& opt,
                                                    const std::string& k, const std::string& v) {
        std::shared_ptr<RecordObject> obj{new RecordObject(std::move(opt))};
        if (!k.empty() && !v.empty())
            obj->UpdateValue(k, v);
        return obj;
    }

    std::string RecordObject::GetValue() const {
        if (FileRecord record; ConvertToFileRecord(record)) {
            return record.data.value;
        } else {
            return {};
        }
    }

    void RecordObject::UpdateValue(const std::string& k, const std::string& v) {
        auto logFile = meta.logFilePtr.lock();
        // 当前record文件读指针位置为写入数据之前的写指针位置
        logFile->file.sync();
        meta.pos = logFile->file.tellp();
        // 刷入日志文件（磁盘）
        FileRecord::DumpRecordToDisk(logFile->file, meta.dbIdx, k, v);
    }

    void RecordObject::MarkAsDeleted(const std::string& k) {
        UpdateValue(k, "");
    }

    DataLogFileObjPtr RecordObject::GetDataLogFileHandler() const {
        return meta.logFilePtr;
    }

    bool RecordObject::ConvertToFileRecord(FileRecord& record) const {
        return FileRecord::LoadFromDisk(record, meta.logFilePtr.lock()->file, meta.pos);
    }

    bool RecordObject::IsInTargetDataLogFile(DataLogFileObjPtr targetFilePtr) const {
        auto targetFile = targetFilePtr.lock();
        auto logFile = meta.logFilePtr.lock();
        if (!targetFile || !logFile) {
            return false;
        }
        return &targetFile->file == &logFile->file;
    }

    std::fstream::pos_type RecordObject::GetFileOffset() const {
        return meta.pos;
    }

    void RecordObject::SetExpiration(std::chrono::seconds sec) {
        SetExpiration(std::chrono::duration_cast<std::chrono::milliseconds>(sec));
    }

    void RecordObject::SetExpiration(std::chrono::milliseconds ms) {
        meta.expirationTimeMs = ms;
    }

    std::chrono::milliseconds RecordObject::GetExpiration() const {
        return meta.expirationTimeMs;
    }

    bool RecordObject::IsExpired() const {
        if (meta.expirationTimeMs == std::chrono::milliseconds{ULLONG_MAX}) {
            return false;
        }
        auto exTime = meta.createdTime + meta.expirationTimeMs;
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
        RecordObject::Meta opt{.dbIdx = mDBIdx_};
        auto valObj = RecordObject::New(std::move(opt), key, val);
        if (!valObj) {
            ServerLog::GetInstance().Error("memory allocate failed");
            ec = error::RuntimeErrorCode::kMemoryOut;
            return {};
        }

        mHATTrieTree_[key] = valObj;
        mMaxMemoryStrategy_->UpdateStateForWriteOp(key);
        return valObj;
    }

    std::error_code StorageEngine::InnerPut(const StorageEngine::InnerPutOption& opt,
                                            const std::string& key, const std::string& val) {
        RecordObject::Meta meta{.dbIdx = mDBIdx_, .logFilePtr = opt.logFilePtr};
        if (opt.pos != -1) meta.pos = opt.pos;
        if (opt.microSecondTimestamp != 0)
            meta.createdTime = utils::MicrosecondTimestampConvertToTimePoint(opt.microSecondTimestamp);

        auto valObj = RecordObject::New(std::move(meta), key, val);
        if (!valObj) {
            ServerLog::GetInstance().Error("memory allocate failed");
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

        ec = error::RuntimeErrorCode::kSuccess;
        return valObj.lock()->GetValue();
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