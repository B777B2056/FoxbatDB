#include "errors/runtime.h"
#include "tools/tools.h"
#include <benchmark/benchmark.h>

static std::string flagConfPath = "/mnt/e/jr/FoxbatDB/config/flag.toml";

using namespace foxbatdb;
using CMDServerPtr = std::shared_ptr<foxbatdb::CMDSession>;

static auto key = ::GenRandomString(512);
static auto value = ::GenRandomString(512);

static void Set(benchmark::State& state) {
    for (auto _: state) {
        // 注入一条数据到kv存储引擎
        auto cmdSession = ::GetMockCMDSession();
        cmdSession->DoExecOneCmd(::BuildCMD("set", {key, value}));
    }
}

static void Get(benchmark::State& state) {
    for (auto _: state) {
        // 从kv存储引擎读取一条数据
        auto cmdSession = ::GetMockCMDSession();
        cmdSession->DoExecOneCmd(::BuildCMD("get", {key}));
    }
}

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    InitComponents(flagConfPath);
    ::benchmark::RegisterBenchmark("Set", &Set);
    ::benchmark::RegisterBenchmark("Get", &Get);
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    RemoveRelatedFiles();
    return 0;
}
