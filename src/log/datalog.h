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

  using DataLogFileObjPtr = std::weak_ptr<DataLogFileWrapper>;

  class DataLogFileManager {
  private:
    std::list<std::shared_ptr<DataLogFileWrapper>> mLogFilePool_;
    std::list<std::shared_ptr<DataLogFileWrapper>>::iterator mAvailableNode_;

    DataLogFileManager();
    void PoolExpand();
    bool LoadHistoryTxFromDisk(std::shared_ptr<DataLogFileWrapper> fileWrapper, std::uint64_t txNum);
    void LoadHistoryRecordsFromDisk();

  public:
    ~DataLogFileManager() = default;
    static DataLogFileManager& GetInstance();
    void Init();
    DataLogFileObjPtr GetAvailableLogFile();
    void Merge();
  };
}