#include <algorithm>
#include <chrono>
#include <iostream>
#include <unordered_map>
#include <string>
#include <thread>
#include <random>
#include <gtest/gtest.h>
#include "core/db.h"
#include "frontend/cmdmap.h"
#include "frontend/parser.h"
#include "network/cmd.h"

using namespace foxbatdb;

std::unordered_map<std::string, std::string> testData;

std::shared_ptr<CMDSession> GetMockCMDSession() {
  static asio::io_context ioContext;
  return std::make_shared<CMDSession>(asio::ip::tcp::socket{ioContext});
}

ParseResult BuildCMD(const std::string& cmdName, const std::vector<std::string>& argv) {
  ParseResult cmd {
    .hasError=false,
    .isWriteCmd=MainCommandMap.at(cmdName).isWriteCmd,
    .txState=TxState::kInvalid,
    .data = Command{
      .name=cmdName,
      .call=MainCommandMap.at(cmdName).call,
    }
  };
  for (const auto& param : argv) {
    cmd.data.argv.emplace_back(BinaryString{param});
  }
  return cmd;
}

std::string GenRandomString(std::size_t length) {
  auto randchar = []() -> char {
      const char charset[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
      const size_t max_index = (sizeof(charset) - 1);
      return charset[std::rand()%max_index];
  };
  std::string str(length,0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

void FillTestData() {
  static constexpr std::size_t singleStringMaxLength = 128;
  static constexpr std::size_t testDataSize = 64;
  while (testData.size() < testDataSize) {
    std::size_t keyLength = 1 + std::rand()%singleStringMaxLength;
    std::size_t valLength = 1 + std::rand()%singleStringMaxLength;
    testData[GenRandomString(keyLength)] = GenRandomString(valLength);
  }
}

TEST(SetGetTest, WithNoOption) {
  if (testData.empty())
    FillTestData();
  std::cout << "test data number: " << testData.size() << std::endl;
  auto cmdSession = GetMockCMDSession();
  // 注入kv存储引擎
  for (const auto& [key, val] : testData) {
    ParseResult cmd = BuildCMD("set", {key, val});
    auto resp = cmdSession->DoExecOneCmd(cmd);
    EXPECT_EQ(resp, utils::BuildResponse("OK"));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  // 从kv存储引擎读取数据
  for (const auto& [key, val] : testData) {
    ParseResult cmd = BuildCMD("get", {key});
    auto resp = cmdSession->DoExecOneCmd(cmd);
    EXPECT_EQ(resp, utils::BuildResponse(val));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
}