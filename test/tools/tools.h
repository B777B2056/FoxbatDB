#pragma once
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "network/cmd.h"

std::shared_ptr<foxbatdb::CMDSession> GetMockCMDSession();
foxbatdb::ParseResult BuildCMD(const std::string& cmdName, const std::vector<std::string>& argv);
foxbatdb::ParseResult BuildCMD(const std::string& cmdName, const std::vector<std::string>& argv, 
                               const std::vector<foxbatdb::CommandOption>& options);

class TestDataset {
private:
  std::size_t mDatasetSize_;
  std::size_t mSingleStringMaxLength_;
  std::fstream mDataFile_;

#pragma pack(push, 1)
  struct RecordHeader {
    std::uint64_t keySize;
    std::uint64_t valSize;
  };
#pragma pack(pop)

  static std::string GenRandomString(std::size_t length);
  void WriteToFile(const RecordHeader& header, const std::string& key, const std::string& val);
  void ReadFromFile(std::string& key, std::string& val);

public:
  TestDataset(std::size_t datasetSize, std::size_t singleStringMaxLength);
  TestDataset(const TestDataset&) = delete;
  TestDataset& operator=(const TestDataset&) = delete;
  ~TestDataset();
  std::size_t Size() const;
  void Foreach(std::function<void(const std::string&,const std::string&)> cb);
};