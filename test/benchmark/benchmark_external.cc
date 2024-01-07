#include "asio.hpp"
#include "flag/flags.h"
#include "tools/tools.h"
#include <benchmark/benchmark.h>
#include <sstream>

static std::string flagConfPath = "/mnt/e/jr/FoxbatDB/config/flag.toml";

using namespace foxbatdb;
using CMDServerPtr = std::shared_ptr<foxbatdb::CMDSession>;

static auto key = ::GenRandomString(512);
static auto value = ::GenRandomString(512);

class MockClient {
private:
    asio::io_context ioCtx;
    asio::ip::tcp::socket socket;
    asio::ip::tcp::endpoint end_point;

    MockClient()
        : ioCtx{}, socket{ioCtx},
          end_point{asio::ip::address::from_string("127.0.0.1"), Flags::GetInstance().port} {
        socket.connect(end_point);
    }

    static std::string BuildCMD(const std::string& cmdName, const std::vector<std::string>& argv) {
        std::stringstream ss;
        ss << "*" << argv.size() + 1 << "\r\n";
        ss << "$" << cmdName.size() << "\r\n";
        ss << cmdName << "\r\n";
        for (const auto& p: argv) {
            ss << "$" << p.size() << "\r\n";
            ss << p << "\r\n";
        }
        return ss.str();
    }

public:
    MockClient(const MockClient&) = delete;
    MockClient& operator=(const MockClient&) = delete;
    ~MockClient() = default;

    static MockClient& GetInstance() {
        static MockClient clt;
        return clt;
    }

    void Init() {}

    void SendCMD(const std::string& cmdName, const std::vector<std::string>& argv) {
        asio::write(socket, asio::buffer(MockClient::BuildCMD(cmdName, argv)));
    }
};

static void Set(benchmark::State& state) {
    for (auto _: state) {
        // 注入一条数据到kv存储引擎
        MockClient::GetInstance().SendCMD("set", {key, value});
    }
}

static void Get(benchmark::State& state) {
    for (auto _: state) {
        // 从kv存储引擎读取一条数据
        MockClient::GetInstance().SendCMD("get", {key});
    }
}

void Init() {
    foxbatdb::Flags::GetInstance().Init(flagConfPath);
    MockClient::GetInstance().Init();
}

/* 注意：必须先运行FoxbatDB进程 */
int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    Init();
    ::benchmark::RegisterBenchmark("Set", &Set);
    ::benchmark::RegisterBenchmark("Get", &Get);
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}