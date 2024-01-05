#pragma once
#include "log/datalog.h"
#include "tsl/htrie_map.h"
#include <chrono>
#include <cstdint>
#include <optional>
#include <vector>

namespace foxbatdb {
    enum class TxRuntimeState : std::int8_t {
        kData = 0,
        kFailed,
        kBegin,
        kFinish
    };

#pragma pack(push, 1)
    struct FileRecordHeader {
        std::uint32_t crc;
        std::uint64_t timestamp;
        TxRuntimeState txRuntimeState;
        std::uint8_t dbIdx;
        std::uint64_t keySize;
        std::uint64_t valSize;

        [[nodiscard]] std::uint32_t CalculateCRC32Value(const std::string& k, const std::string& v) const;
    };
#pragma pack(pop)

    struct FileRecordData {
        std::string key;
        std::string value;
    };

    struct FileRecord {
        FileRecordHeader header{};
        FileRecordData data;

        [[nodiscard]] std::uint32_t CalculateCRC32Value() const;
        static bool LoadFromDisk(FileRecord& record, std::fstream& file, std::streampos pos);
        void DumpToDisk(std::fstream& file, std::uint8_t dbIdx);
        static void DumpToDisk(std::fstream& file, std::uint8_t dbIdx, const std::string& k,
                               const std::string& v);
        static void DumpTxFlagToDisk(std::fstream& file, std::uint8_t dbIdx,
                                     TxRuntimeState txFlag, std::size_t txCmdNum = 0);
    };

    class RecordObject {
    private:
        std::uint8_t dbIdx;
        mutable DataLogFileObjPtr logFilePtr;
        std::fstream::pos_type pos{-1};
        std::chrono::milliseconds expirationTimeMs;
        std::chrono::time_point<std::chrono::steady_clock> createdTime;

        const static std::chrono::milliseconds INVALID_EXPIRE_TIME;

        RecordObject(std::uint8_t dbIdx, DataLogFileObjPtr file, std::streampos pos = -1,
                    std::chrono::milliseconds ms = INVALID_EXPIRE_TIME,
                    std::chrono::time_point<std::chrono::steady_clock> createdTime = std::chrono::steady_clock::now());

        void UpdateValue(const std::string& k, const std::string& v);

    public:
        ~RecordObject() = default;

        static std::shared_ptr<RecordObject> New(std::uint8_t dbIdx,
                                                const std::string& k,
                                                const std::string& v);
        static std::shared_ptr<RecordObject> New(std::uint8_t dbIdx,
                                                const std::string& k,
                                                const std::string& v,
                                                std::chrono::milliseconds ms);
        static std::shared_ptr<RecordObject> NewForMerge(DataLogFileObjPtr file,
                                                        std::uint8_t dbIdx,
                                                        const std::string& k,
                                                        const std::string& v);
        static std::shared_ptr<RecordObject> NewForHistory(DataLogFileObjPtr file,
                                                          std::streampos pos, std::uint8_t dbIdx, std::uint64_t microSecondTimestamp);

        std::optional<std::string> GetValue() const;
        void DeleteValue(const std::string& k);

        DataLogFileObjPtr GetLogFileHandler() const;
        std::optional<FileRecord> CovertToFileRecord() const;

        bool IsSameLogFile(DataLogFileObjPtr targetFilePtr) const;
        std::fstream::pos_type GetFileOffset() const;

        void SetExpiration(std::chrono::seconds sec);
        void SetExpiration(std::chrono::milliseconds ms);
        std::chrono::milliseconds GetExpiration() const;
        bool IsExpired() const;
    };

    class MaxMemoryStrategy;

    class StorageEngine {
    private:
        using ValueObjectPtr = std::shared_ptr<RecordObject>;

        std::uint8_t mDBIdx_;
        MaxMemoryStrategy* mMaxMemoryStrategy_;
        tsl::htrie_map<char, ValueObjectPtr> mHATTrieTree_;

    public:
        using ForeachCallback = std::function<void(const std::string&, const RecordObject&)>;

        StorageEngine(std::uint8_t dbIdx, MaxMemoryStrategy* maxMemoryStrategy);
        StorageEngine(const StorageEngine&) = delete;
        StorageEngine& operator=(const StorageEngine&) = delete;
        StorageEngine(StorageEngine&& rhs) noexcept;
        StorageEngine& operator=(StorageEngine&& rhs) noexcept;
        ~StorageEngine() = default;

        void ReleaseMemory();
        [[nodiscard]] bool HaveMemoryAvailable() const;

        void InsertTxFlag(TxRuntimeState txFlag, std::size_t txCmdNum = 0) const;

        std::error_code Put(const std::string& key, const std::string& val);
        std::weak_ptr<RecordObject> Put(std::error_code& ec, const std::string& key, const std::string& val);

        std::error_code PutForMerge(DataLogFileObjPtr file, const std::string& key, const std::string& val);
        std::error_code PutForHistoryData(DataLogFileObjPtr file, std::streampos pos,
                                          std::uint64_t microSecondTimestamp, const std::string& key);

        [[nodiscard]] bool Contains(const std::string& key) const;

        std::string Get(std::error_code& ec, const std::string& key);
        std::weak_ptr<RecordObject> Get(const std::string& key);

        std::error_code Del(const std::string& key);
        [[nodiscard]] std::vector<std::string> PrefixSearch(const std::string& prefix) const;
        void Foreach(ForeachCallback&& callback);
    };
}// namespace foxbatdb