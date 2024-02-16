#pragma once
#include <fstream>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace foxbatdb {
    enum class RecordState : std::int8_t {
        kData = 0,
        kFailed,
        kBegin,
        kFinish
    };

    class DataLogFile {
    public:
        using OffsetType = std::fstream::pos_type;

        struct Data {
            bool error = false;
            std::uint64_t timestamp = 0;
            std::uint8_t dbIdx = 0;
            RecordState state = RecordState::kData;
            std::uint16_t txNum = 0;
            std::string key;
            std::string value;
        };

    public:
        explicit DataLogFile(const std::string& fileName);

        const std::string& Name() const;

        OffsetType GetRowBySequence(Data& data);
        Data GetDataByOffset(OffsetType offset);
        OffsetType DumpToDisk(std::uint8_t dbIdx, const std::string& k, const std::string& v);
        void DumpTxFlagToDisk(std::uint8_t dbIdx, RecordState txFlag, std::size_t txCmdNum = 0);

        void Rename(const std::string& newName);
        void ClearOSFlag();

    private:
        std::string name;
        std::fstream file;
    };

    class DataLogFileManager {
    private:
        using FilePtr = std::unique_ptr<DataLogFile>;
        using FileIter = std::list<FilePtr>::iterator;

    private:
        std::list<FilePtr> mLogFilePool_;
        std::list<FilePtr>::iterator mWritableFileIter_;

        DataLogFileManager();
        void PoolExpand();
        bool FillDataLogFilePoolByHistoryDataFile();
        void LoadHistoryRecordsFromDisk();
        void ModifyDataFilesForMerge(FileIter& writableNode, FilePtr&& mergeLogFile);

    public:
        DataLogFileManager(const DataLogFileManager&) = delete;
        DataLogFileManager& operator=(const DataLogFileManager&) = delete;
        ~DataLogFileManager() = default;
        static DataLogFileManager& GetInstance();
        void Init();
        DataLogFile* GetWritableDataFile();
        void Merge();
    };
}// namespace foxbatdb