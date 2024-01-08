#include "asio.hpp"
#include "flag/flags.h"
#include "tools/tools.h"
#include <benchmark/benchmark.h>
#include <cassert>
#include <sstream>
#include <string_view>

static std::string flagConfPath = "/mnt/e/jr/FoxbatDB/config/flag.toml";

using namespace foxbatdb;
using CMDServerPtr = std::shared_ptr<foxbatdb::CMDSession>;

static auto key = ::GenRandomString(512);
static auto value = ::GenRandomString(512);

class MockClient {
private:
    char buf[1024];
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

    void TestResponse(const std::string& expectResp) {
        std::size_t bytes = asio::read(socket,
                                       asio::buffer(buf),
                                       asio::transfer_at_least(3));
        assert(std::string_view(buf, buf + bytes) == utils::BuildResponse(expectResp));
    }
};

static void Set(benchmark::State& state) {
    for (auto _: state) {
        // ע��һ�����ݵ�kv�洢����
        MockClient::GetInstance().SendCMD("set", {key, value});
        MockClient::GetInstance().TestResponse("OK");
    }
}

static void Get(benchmark::State& state) {
    for (auto _: state) {
        // ��kv�洢�����ȡһ������
        MockClient::GetInstance().SendCMD("get", {key});
        MockClient::GetInstance().TestResponse(value);
    }
}

void Init() {
    foxbatdb::Flags::GetInstance().Init(flagConfPath);
    MockClient::GetInstance().Init();
}

/* ע�⣺����������FoxbatDB���� */
int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    Init();
    ::benchmark::RegisterBenchmark("Set", &Set);
    ::benchmark::RegisterBenchmark("Get", &Get);
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}