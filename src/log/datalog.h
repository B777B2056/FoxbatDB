#pragma once
#include <fstream>
#include <list>
#include <memory>
#include <mutex>

namespace foxbatdb {
    struct DataLogFileWrapper {
        std::string name;
        std::fstream file;
    };

    class DataLogFileManager {
    private:
        std::list<std::unique_ptr<DataLogFileWrapper>> mLogFilePool_;
        std::list<std::unique_ptr<DataLogFileWrapper>>::iterator mAvailableNode_;

        DataLogFileManager();
        void PoolExpand();
        static bool LoadHistoryTxFromDisk(DataLogFileWrapper* fileWrapper, std::uint64_t txNum);
        void LoadHistoryRecordsFromDisk();

    public:
        DataLogFileManager(const DataLogFileManager&) = delete;
        DataLogFileManager& operator=(const DataLogFileManager&) = delete;
        ~DataLogFileManager() = default;
        static DataLogFileManager& GetInstance();
        void Init();
        DataLogFileWrapper* GetAvailableLogFile();
        void Merge();
    };
}// namespace foxbatdb