#pragma once
#include "log/datalog.h"
#include "tsl/htrie_map.h"
#include <chrono>
#include <cstdint>
#include <new>
#include <optional>
#include <vector>

namespace foxbatdb {
    enum class TxRuntimeState : std::int8_t {
        kData = 0,
        kFailed,
        kBegin,
        kFinish
    };

#if defined(__cpp_lib_hardware_interference_size)
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
        TxRuntimeState txRuntimeState = TxRuntimeState::kData;
        std::uint8_t dbIdx = 0;
        std::uint64_t keySize = 0;
        std::uint64_t valSize = 0;

        bool LoadFromDisk(std::fstream& file, std::streampos pos);
        void DumpToDisk(std::fstream& file);
        void SetCRC(const std::string& k, const std::string& v);
        [[nodiscard]] bool CheckCRC(const std::string& k, const std::string& v) const;
        void TransferEndian();

    private:
        [[nodiscard]] std::uint32_t CalculateCRC32Value(const std::string& k, const std::string& v) const;
        [[nodiscard]] bool ValidateFileRecordHeader() const;
        [[nodiscard]] bool ValidateTxFlagRecord() const;
    };

    struct FileRecordData {
        std::string key;
        std::string value;

        static void LoadFromDisk(FileRecordData& data, std::fstream& file,
                                 std::size_t keySize, std::size_t valSize);
    };

    struct FileRecord {
        FileRecordHeader header;
        FileRecordData data;

        static bool LoadFromDisk(FileRecord& record, std::fstream& file, std::streampos pos);
        static void DumpRecordToDisk(std::fstream& file, std::uint8_t dbIdx,
                                     const std::string& k, const std::string& v);
        static void DumpTxFlagToDisk(std::fstream& file, std::uint8_t dbIdx,
                                     TxRuntimeState txFlag, std::size_t txCmdNum = 0);
    };

    struct L1_CACHE_LINE_ALIGNAS RecordObjectMeta {
        std::uint8_t dbIdx = 0;
        DataLogFileObjPtr logFilePtr = DataLogFileManager::GetInstance().GetAvailableLogFile();
        std::streampos pos = -1;
        std::chrono::milliseconds expirationTimeMs = INVALID_EXPIRE_TIME;
        std::chrono::time_point<std::chrono::steady_clock> createdTime = std::chrono::steady_clock::now();

    private:
        constexpr const static std::chrono::milliseconds INVALID_EXPIRE_TIME{ULLONG_MAX};
    };

    class L1_CACHE_LINE_ALIGNAS RecordObject {
    private:
        RecordObjectMeta meta;
        bool ConvertToFileRecord(FileRecord& record) const;

    public:
        RecordObject();

        void SetMeta(RecordObjectMeta&& m);
        void UpdateValue(const std::string& k, const std::string& v);

        [[nodiscard]] std::string GetValue() const;
        void MarkAsDeleted(const std::string& k);

        [[nodiscard]] DataLogFileObjPtr GetDataLogFileHandler() const;
        [[nodiscard]] bool IsInTargetDataLogFile(DataLogFileObjPtr targetFilePtr) const;
        [[nodiscard]] std::fstream::pos_type GetFileOffset() const;

        void SetExpiration(std::chrono::seconds sec);
        void SetExpiration(std::chrono::milliseconds ms);
        [[nodiscard]] std::chrono::milliseconds GetExpiration() const;
        [[nodiscard]] bool IsExpired() const;
    };

#undef LI_CACHE_LINE_ALIGNAS

    class MaxMemoryStrategy;

    class StorageEngine {
    private:
        using RecordObjectPtr = std::shared_ptr<RecordObject>;

        std::uint8_t mDBIdx_;
        MaxMemoryStrategy* mMaxMemoryStrategy_;
        tsl::htrie_map<char, RecordObjectPtr> mHATTrieTree_;

    public:
        using ForeachCallback = std::function<void(const std::string&, const RecordObject&)>;

        struct InnerPutOption {
            DataLogFileObjPtr logFilePtr{};
            std::streampos pos = -1;
            std::uint64_t microSecondTimestamp = 0;
        };

    public:
        StorageEngine(std::uint8_t dbIdx, MaxMemoryStrategy* maxMemoryStrategy);
        StorageEngine(const StorageEngine&) = delete;
        StorageEngine& operator=(const StorageEngine&) = delete;
        StorageEngine(StorageEngine&& rhs) noexcept;
        StorageEngine& operator=(StorageEngine&& rhs) noexcept;
        ~StorageEngine() = default;

        void ReleaseMemory();
        [[nodiscard]] bool HaveMemoryAvailable() const;

        void InsertTxFlag(TxRuntimeState txFlag, std::size_t txCmdNum = 0) const;

        std::weak_ptr<RecordObject> Put(std::error_code& ec, const std::string& key, const std::string& val);
        std::error_code InnerPut(const InnerPutOption& opt,
                                 const std::string& key, const std::string& val);

        [[nodiscard]] bool Contains(const std::string& key) const;

        std::string Get(std::error_code& ec, const std::string& key);
        std::weak_ptr<RecordObject> Get(const std::string& key);

        std::error_code Del(const std::string& key);
        [[nodiscard]] std::vector<std::string> PrefixSearch(const std::string& prefix) const;
        void Foreach(ForeachCallback&& callback);
    };
}// namespace foxbatdb