#include "errors/runtime.h"
#include "tools/tools.h"
#include "utils/resp.h"
#include "utils/utils.h"
#include <benchmark/benchmark.h>

static std::string flagConfPath = "/mnt/e/jr/FoxbatDB/config/flag.toml";

using namespace foxbatdb;
using CMDServerPtr = std::shared_ptr<foxbatdb::CMDSession>;

static auto key = ::GenRandomString(512);
static auto value = ::GenRandomString(512);

static void SetBenchmark(benchmark::State&) {
    // 注入一条数据到kv存储引擎
    auto cmdSession = ::GetMockCMDSession();
    cmdSession->DoExecOneCmd(::BuildCMD("set", {key, value}));
}

static void GetBenchmark(benchmark::State&) {
    // 从kv存储引擎读取一条数据
    auto cmdSession = ::GetMockCMDSession();
    cmdSession->DoExecOneCmd(::BuildCMD("get", {key}));
}

int main(int argc, char** argv) {
    InitComponents(flagConfPath);
    BENCHMARK(SetBenchmark);
    BENCHMARK(GetBenchmark);
    ::benchmark::Initialize(&argc, argv);
    return ::benchmark::RunSpecifiedBenchmarks();
}