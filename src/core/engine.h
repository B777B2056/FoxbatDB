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

    constexpr static std::chrono::milliseconds INVALID_EXPIRE_TIME{ULLONG_MAX};

    struct L1_CACHE_LINE_ALIGNAS RecordObjectMeta {
        std::uint8_t dbIdx = 0;
        DataLogFile* logFilePtr = DataLogFileManager::GetInstance().GetWritableDataFile();
        std::streampos pos = -1;
        std::chrono::milliseconds expirationTimeMs = INVALID_EXPIRE_TIME;
        std::chrono::time_point<std::chrono::steady_clock> createdTime = std::chrono::steady_clock::now();
    };

    class L1_CACHE_LINE_ALIGNAS RecordObject {
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

#undef LI_CACHE_LINE_ALIGNAS

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

        void InsertTxFlag(TxRuntimeState txFlag, std::size_t txCmdNum = 0) const;

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