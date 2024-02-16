#include "engine.h"
#include "errors/runtime.h"
#include "flag/flags.h"
#include "log/serverlog.h"
#include "memory.h"
#include "utils/utils.h"

namespace foxbatdb {
    RecordObject::RecordObject() : meta{RecordObjectMeta{.logFilePtr = {}}} {}

    void RecordObject::SetMeta(const RecordObjectMeta& m) {
        this->meta = m;
    }

    RecordObjectMeta RecordObject::GetMeta() const {
        return this->meta;
    }

    std::string RecordObject::GetValue() const {
        auto data = meta.logFilePtr->GetDataByOffset(meta.pos);
        if (data.error) return {};
        return data.value;
    }

    void RecordObject::DumpToDisk(const std::string& k, const std::string& v) {
        if (k.empty() || v.empty()) return;
        meta.pos = meta.logFilePtr->DumpToDisk(meta.dbIdx, k, v);
    }

    void RecordObject::MarkAsDeleted(const std::string& k) {
        DumpToDisk(k, "");
    }

    const DataLogFile* RecordObject::GetDataLogFileHandler() const {
        return meta.logFilePtr;
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

    MemoryIndex::MemoryIndex(std::uint8_t dbIdx) : mDBIdx_{dbIdx} {}

    MemoryIndex::MemoryIndex(MemoryIndex&& rhs) noexcept
        : mDBIdx_{rhs.mDBIdx_}, mHATTrieTree_{std::move(rhs.mHATTrieTree_)} {}

    MemoryIndex& MemoryIndex::operator=(MemoryIndex&& rhs) noexcept {
        if (this != &rhs) {
            mDBIdx_ = rhs.mDBIdx_;
            mHATTrieTree_ = std::move(rhs.mHATTrieTree_);
        }
        return *this;
    }

    void MemoryIndex::InsertTxFlag(RecordState txFlag, std::size_t txCmdNum) const {
        if (RecordState::kBegin != txFlag)
            assert(0 == txCmdNum);
        else
            assert(0 != txCmdNum);
        auto& logFileManager = DataLogFileManager::GetInstance();
        auto* targetLogFile = logFileManager.GetWritableDataFile();
        targetLogFile->DumpTxFlagToDisk(mDBIdx_, txFlag, txCmdNum);
    }

    void MemoryIndex::Put(const std::string& key, std::shared_ptr<RecordObject> valObj) {
        std::unique_lock l{mt_};
        mHATTrieTree_[key] = std::move(valObj);
    }

    std::error_code MemoryIndex::PutHistoryData(const std::string& key, const HistoryDataInfo& info) {
        RecordObjectMeta meta{
                .dbIdx = mDBIdx_,
                .logFilePtr = info.logFilePtr,
                .pos = info.pos,
                .createdTime = utils::MicrosecondTimestampConvertToTimePoint(info.microSecondTimestamp)};

        auto valObj = RecordObjectPool::GetInstance().Acquire(meta);
        if (!valObj) [[unlikely]] {
            ServerLog::GetInstance().Error("memory allocate failed");
            return error::RuntimeErrorCode::kMemoryOut;
        }

        {
            std::unique_lock l{mt_};
            mHATTrieTree_[key] = valObj;
        }
        return error::RuntimeErrorCode::kSuccess;
    }

    bool MemoryIndex::Contains(const std::string& key) const {
        std::unique_lock l{mt_};
        return mHATTrieTree_.count(key) > 0;
    }

    std::string MemoryIndex::Get(std::error_code& ec, const std::string& key) {
        auto valObj = this->Get(key);
        if (valObj.expired()) {
            ec = error::RuntimeErrorCode::kKeyNotFound;
            return {};
        }

        ec = error::RuntimeErrorCode::kSuccess;
        return valObj.lock()->GetValue();
    }

    std::weak_ptr<RecordObject> MemoryIndex::Get(const std::string& key) {
        std::unique_lock l{mt_};
        if (!mHATTrieTree_.count(key)) {
            return {};
        }

        auto valObj = mHATTrieTree_.at(key);
        if (valObj->IsExpired()) {
            valObj->MarkAsDeleted(key);
            mHATTrieTree_.erase(key);
            return {};
        }
        return valObj;
    }

    std::error_code MemoryIndex::Del(const std::string& key) {
        std::unique_lock l{mt_};
        if (!mHATTrieTree_.count(key)) {
            return error::RuntimeErrorCode::kKeyNotFound;
        }

        auto valObj = mHATTrieTree_.at(key);
        valObj->MarkAsDeleted(key);
        mHATTrieTree_.erase(key);
        return error::RuntimeErrorCode::kSuccess;
    }

    std::vector<std::pair<std::string, std::string>> MemoryIndex::PrefixSearch(const std::string& prefix) const {
        std::vector<std::pair<std::string, std::string>> ret;

        std::unique_lock l{mt_};
        auto prefixRange = mHATTrieTree_.equal_prefix_range({prefix.data(), prefix.length()});
        for (auto it = prefixRange.first; it != prefixRange.second; ++it) {
            if (auto val = it.value()->GetValue(); !val.empty()) {
                ret.emplace_back(it.key(), val);
            }
        }
        return ret;
    }

    void MemoryIndex::Merge(DataLogFile* targetFile, const DataLogFile* writableFile) {
        std::unique_lock l{mt_};
        std::vector<std::string> expiredKeyList;
        for (auto it = mHATTrieTree_.cbegin(); it != mHATTrieTree_.cend(); ++it) {
            const auto& key = it.key();
            const auto& valObj = it.value();

            if (valObj->IsExpired()) {
                expiredKeyList.emplace_back(key);
                continue;
            }

            // 不合并当前正可用的db文件
            if (writableFile == valObj->GetDataLogFileHandler())
                continue;
            // 将活跃的key和记录写入merge文件内后，再更新内存索引
            if (auto val = valObj->GetValue(); !val.empty()) {
                RecordObjectMeta meta{.dbIdx = mDBIdx_, .logFilePtr = targetFile};
                valObj->SetMeta(meta);
                valObj->DumpToDisk(key, val);
            }
        }

        for (auto&& key: expiredKeyList)
            mHATTrieTree_.erase(key);
    }
}// namespace foxbatdb