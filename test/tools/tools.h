#pragma once
#include <fstream>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include "frontend/server.h"

std::shared_ptr<foxbatdb::CMDSession> GetMockCMDSession();
std::string GenRandomString(std::size_t length);
foxbatdb::ParseResult BuildCMD(const std::string& cmdName, const std::vector<std::string>& argv);
foxbatdb::ParseResult BuildCMD(const std::string& cmdName, const std::vector<std::string>& argv, 
                               const std::vector<foxbatdb::CommandOption>& options);

class TestDataset {
public:
#pragma pack(push, 1)
  struct RecordHeader {
    std::uint64_t keySize;
    std::uint64_t valSize;
  };
#pragma pack(pop)

private:
  std::size_t mDatasetSize_;
  std::size_t mSingleStringMaxLength_;
  std::fstream mDataFile_;
  std::unordered_map<std::string, std::fstream::pos_type> mRecordPosMap_;

  void WriteToFile(const RecordHeader& header, const std::string& key, const std::string& val);
  void ReadFromFile(std::string& key, std::string& val);

public:
  TestDataset(std::size_t datasetSize, std::size_t singleStringMaxLength, std::function<std::string()> keyGenerator);
  TestDataset(const TestDataset&) = delete;
  TestDataset& operator=(const TestDataset&) = delete;
  ~TestDataset();
  std::size_t Size() const;
  void Foreach(std::function<void(const std::string&,const std::string&)> cb);
};