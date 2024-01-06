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
    // ע��һ�����ݵ�kv�洢����
    auto cmdSession = ::GetMockCMDSession();
    cmdSession->DoExecOneCmd(::BuildCMD("set", {key, value}));
}

static void GetBenchmark(benchmark::State&) {
    // ��kv�洢�����ȡһ������
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