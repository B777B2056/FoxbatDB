#pragma once
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>

namespace foxbatdb {
    struct DataLogFileWrapper {
        std::string name;
        std::fstream file;
    };

    class RecordObject;

    class DataLogFileManager {
    private:
        mutable std::mutex mt;
        std::list<std::unique_ptr<DataLogFileWrapper>> mLogFilePool_;
        std::list<std::unique_ptr<DataLogFileWrapper>>::iterator mAvailableNode_;

        DataLogFileManager();
        void PoolExpand();
        static bool LoadHistoryTxFromDisk(DataLogFileWrapper* fileWrapper, std::uint64_t txNum);
        void LoadHistoryRecordsFromDisk();
        void ModifyDataFilesForMerge(std::unique_ptr<DataLogFileWrapper>&& mergeLogFile);

    public:
        DataLogFileManager(const DataLogFileManager&) = delete;
        DataLogFileManager& operator=(const DataLogFileManager&) = delete;
        ~DataLogFileManager() = default;
        static DataLogFileManager& GetInstance();
        void Init();
        DataLogFileWrapper* GetAvailableLogFile();
        bool IsRecordInCurrentAvailableLogFile(const RecordObject& record) const;
        void Merge();
    };
}// namespace foxbatdb