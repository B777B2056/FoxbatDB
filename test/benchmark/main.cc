#include <benchmark/benchmark.h>
#include "tools/tools.h"
#include "errors/runtime.h"
#include "utils/utils.h"
#include "utils/resp.h"
#include "tools/tools.h"

static std::string flagConfPath = "/mnt/e/jr/FoxbatDB/config/flag.toml";

using namespace foxbatdb;
using CMDServerPtr = std::shared_ptr<foxbatdb::CMDSession>;

static std::string TimestampKeyGenerator() {
  return std::to_string(utils::GetMicrosecondTimestamp());
}

static void InsertIntoDBWithNoOption(CMDServerPtr cmdSession, const std::string& key, const std::string& val) {
  static auto expectResp = utils::BuildResponse("OK");
  auto cmd = ::BuildCMD("set", {key, val});
  cmdSession->DoExecOneCmd(cmd);
}

static void ReadAndTestFromDB(CMDServerPtr cmdSession, const std::string& key, const std::string& val) {
  auto cmd = ::BuildCMD("get", {key});
  cmdSession->DoExecOneCmd(cmd);
}

static TestDataset dataset{1024, 1024, TimestampKeyGenerator};

static void SetBenchmark(benchmark::State&) {
  // 注入kv存储引擎
  auto cmdSession = ::GetMockCMDSession();
  dataset.Foreach(
    [cmdSession](const std::string& key, const std::string& val)->void {
      InsertIntoDBWithNoOption(cmdSession, key, val);
    }
  );  
}

static void GetBenchmark(benchmark::State&) {
  // 从kv存储引擎读取并测试数据
  auto cmdSession = ::GetMockCMDSession();
  dataset.Foreach(
    [cmdSession](const std::string& key, const std::string& val)->void {
        ReadAndTestFromDB(cmdSession, key, val);
      }
  );   
}

int main(int argc, char **argv) {
  InitComponents(flagConfPath);
  BENCHMARK(SetBenchmark);
  BENCHMARK(GetBenchmark);
  ::benchmark::Initialize(&argc, argv);
  return ::benchmark::RunSpecifiedBenchmarks();
}