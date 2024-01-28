#pragma once
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <vector>

namespace foxbatdb {
    struct DataLogFile {
        std::string name;
        std::fstream file;
    };

    class RecordObject;

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