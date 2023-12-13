#include <algorithm>
#include <chrono>
#include <string>
#include <thread>
#include <random>
#include <gtest/gtest.h>
#include "common/common.h"
#include "errors/runtime.h"
#include "utils/resp.h"
#include "tools/tools.h"

using namespace foxbatdb;
using namespace std::chrono_literals;
using CMDServerPtr = std::shared_ptr<foxbatdb::CMDSession>;

static void InsertIntoDBWithNoOption(CMDServerPtr cmdSession, const std::string& key, const std::string& val) {
  static auto expectResp = utils::BuildResponse("OK");
  auto cmd = ::BuildCMD("set", {key, val});
  EXPECT_EQ(cmdSession->DoExecOneCmd(cmd), expectResp);
}

static void InsertIntoDBWithExOption(CMDServerPtr cmdSession, const std::string& key, const std::string& val, 
                                     std::uint64_t timeoutMS) {
  static auto expectResp = utils::BuildResponse("OK");
  foxbatdb::CommandOption opt{
    .name="ex",
    .type=CmdOptionType::kEX,
  };
  opt.argv.emplace_back(std::to_string(timeoutMS));
  auto cmd = ::BuildCMD("set", {key, val}, {opt});
  EXPECT_EQ(cmdSession->DoExecOneCmd(cmd), expectResp);
}

static void ReadAndTestFromDB(CMDServerPtr cmdSession, const std::string& key, const std::string& val) {
  auto cmd = ::BuildCMD("get", {key});
  EXPECT_EQ(cmdSession->DoExecOneCmd(cmd), utils::BuildResponse(val)) << "key: " << key;
}

static void ReadAndTestNotExistFromDB(CMDServerPtr cmdSession, const std::string& key) {
  auto cmd = ::BuildCMD("get", {key});
  auto ec = error::make_error_code(error::RuntimeErrorCode::kKeyNotFound);
  EXPECT_EQ(cmdSession->DoExecOneCmd(cmd), utils::BuildErrorResponse(ec)) << "key: " << key;
}

TEST(SetGetTest, WithNoOption) {
  TestDataset dataset{1024, 64};
  auto cmdSession = ::GetMockCMDSession();
  // 注入kv存储引擎
  dataset.Foreach(
    [cmdSession](const std::string& key, const std::string& val)->void {
      InsertIntoDBWithNoOption(cmdSession, key, val);
    }
  );
  // 从kv存储引擎读取并测试数据
  dataset.Foreach(
    [cmdSession](const std::string& key, const std::string& val)->void {
        ReadAndTestFromDB(cmdSession, key, val);
      }
  );   
}

TEST(SetGetTest, WithExOptionNotTimeout) {
  TestDataset dataset{1024, 64};
  auto cmdSession = ::GetMockCMDSession();
  auto exTime = 1000min; // 超时时间为1000分钟
  // 注入kv存储引擎
  int i = 0;
  dataset.Foreach(
    [cmdSession, &i, exTime](const std::string& key, const std::string& val)->void {
      if (i % 2 == 0)
        InsertIntoDBWithExOption(cmdSession, key, val, exTime.count());  
      else
        InsertIntoDBWithNoOption(cmdSession, key, val);
      ++i;
    }
  );
  // 从kv存储引擎读取并测试数据
  dataset.Foreach(
    [cmdSession](const std::string& key, const std::string& val)->void {
      ReadAndTestFromDB(cmdSession, key, val);
    }
  );        
}

TEST(SetGetTest, WithExOptionTimeout) {
  TestDataset dataset{1024, 64};
  auto cmdSession = ::GetMockCMDSession();
  auto exTime = 1min; // 超时时间为1分钟
  // 注入kv存储引擎
  int i = 0;
  dataset.Foreach(
    [cmdSession, &i, exTime](const std::string& key, const std::string& val)->void {
      if (i % 2 == 0)
        InsertIntoDBWithExOption(cmdSession, key, val, exTime.count());  
      else
        InsertIntoDBWithNoOption(cmdSession, key, val);
      ++i;
    }
  );
  // 等待key超时
  std::this_thread::sleep_for(1min);
  // 从kv存储引擎读取并测试数据
  i = 0;
  dataset.Foreach(
    [cmdSession, &i](const std::string& key, const std::string& val)->void {
        if (i % 2 == 0)
          ReadAndTestNotExistFromDB(cmdSession, key);
        else
          ReadAndTestFromDB(cmdSession, key, val);
        ++i;
    }
  );     
}