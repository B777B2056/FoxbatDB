#pragma once
#include "log/datalog.h"
#include "tsl/htrie_map.h"
#include <chrono>
#include <cstdint>
#include <new>
#include <optional>
#include <vector>

namespace foxbatdb {
    constexpr static std::chrono::milliseconds INVALID_EXPIRE_TIME{ULLONG_MAX};

    struct RecordObjectMeta {
        std::uint8_t dbIdx = 0;
        DataLogFile* logFilePtr = DataLogFileManager::GetInstance().GetWritableDataFile();
        std::streampos pos = -1;
        std::chrono::milliseconds expirationTimeMs = INVALID_EXPIRE_TIME;
        std::chrono::time_point<std::chrono::steady_clock> createdTime = std::chrono::steady_clock::now();
    };

    class RecordObject {
    private:
        RecordObjectMeta meta;

    public:
        RecordObject();

        void SetMeta(const RecordObjectMeta& m);
        RecordObjectMeta GetMeta() const;

        void DumpToDisk(const std::string& k, const std::string& v);

        [[nodiscard]] std::string GetValue() const;
        void MarkAsDeleted(const std::string& k);

        [[nodiscard]] const DataLogFile* GetDataLogFileHandler() const;

        void SetExpiration(std::chrono::seconds sec);
        void SetExpiration(std::chrono::milliseconds ms);
        [[nodiscard]] std::chrono::milliseconds GetExpiration() const;
        [[nodiscard]] bool IsExpired() const;
    };

    class MemoryIndex {
    private:
        std::uint8_t mDBIdx_;
        tsl::htrie_map<char, std::shared_ptr<RecordObject>> mHATTrieTree_;

    public:
        struct HistoryDataInfo {
            DataLogFile* logFilePtr = nullptr;
            std::streampos pos = -1;
            std::uint64_t microSecondTimestamp = 0;
        };

    public:
        explicit MemoryIndex(std::uint8_t dbIdx);
        MemoryIndex(const MemoryIndex&) = delete;
        MemoryIndex& operator=(const MemoryIndex&) = delete;
        MemoryIndex(MemoryIndex&& rhs) noexcept;
        MemoryIndex& operator=(MemoryIndex&& rhs) noexcept;
        ~MemoryIndex() = default;

        void InsertTxFlag(RecordState txFlag, std::size_t txCmdNum = 0) const;

        void Put(const std::string& key, std::shared_ptr<RecordObject> valObj);
        std::error_code PutHistoryData(const std::string& key, const HistoryDataInfo& info);

        [[nodiscard]] bool Contains(const std::string& key) const;

        std::string Get(std::error_code& ec, const std::string& key);
        std::weak_ptr<RecordObject> Get(const std::string& key);

        std::error_code Del(const std::string& key);
        std::vector<std::pair<std::string, std::string>> PrefixSearch(const std::string& prefix) const;

        void Merge(DataLogFile* targetFile, const DataLogFile* writableFile);
    };
}// namespace foxbatdb