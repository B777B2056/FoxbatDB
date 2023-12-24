#include <gtest/gtest.h>
#include "flag/flags.h"
#include "log/datalog.h"
#include "utils/resp.h"
#include "tools/tools.h"

using namespace foxbatdb;
using CMDServerPtr = std::shared_ptr<foxbatdb::CMDSession>;

constexpr static std::size_t KEY_LENGTH = 128;
constexpr static std::size_t VAL_LENGTH = 128;

static std::string DuplicatedKeyGenerator() {
  static int count = 0;
  static std::string repeatedString;

  ++count;
  if (count % 4 != 0) {
    repeatedString = GenRandomString(KEY_LENGTH);
  }
  return repeatedString;
}

static void InsertIntoDBWithNoOption(CMDServerPtr cmdSession, const std::string& key, const std::string& val) {
  static auto expectResp = utils::BuildResponse("OK");
  auto cmd = ::BuildCMD("set", {key, val});
  EXPECT_EQ(cmdSession->DoExecOneCmd(cmd), expectResp);
}

static void ReadAndTestFromDB(CMDServerPtr cmdSession, const std::string& key, const std::string& val) {
  auto cmd = ::BuildCMD("get", {key});
  EXPECT_EQ(cmdSession->DoExecOneCmd(cmd), utils::BuildResponse(val)) << "key: " << key;
}

TEST(MergeTest, Merge) {
  constexpr static std::size_t dbFileNum = 999;
  constexpr static std::size_t datasetRecordNum = 1024 * 1024;
  constexpr static std::size_t recordSize = sizeof(TestDataset::RecordHeader) + KEY_LENGTH + VAL_LENGTH;

  auto dbFileMaxSizeBefore = Flags::GetInstance().dbLogFileMaxSize;
  Flags::GetInstance().dbLogFileMaxSize = (datasetRecordNum * recordSize) / dbFileNum;

  TestDataset dataset{datasetRecordNum, VAL_LENGTH, DuplicatedKeyGenerator};
  auto cmdSession = ::GetMockCMDSession();
  // 注入kv存储引擎
  dataset.Foreach(
    [cmdSession](const std::string& key, const std::string& val)->void {
      InsertIntoDBWithNoOption(cmdSession, key, val);
    }
  );
  // 合并文件
  DataLogFileManager::GetInstance().Merge();
  // 从kv存储引擎读取并测试数据
  dataset.Foreach(
    [cmdSession](const std::string& key, const std::string& val)->void {
        ReadAndTestFromDB(cmdSession, key, val);
      }
  );   
  // 恢复dbFileSize设置
  Flags::GetInstance().dbLogFileMaxSize = dbFileMaxSizeBefore;
}