#include "asio.hpp"
#include <benchmark/benchmark.h>
#include <sstream>

#define KEY_SIZE 512
#define VAL_SIZE 512

static std::string GenRandomString(std::size_t length) {
    auto randchar = []() -> char {
        const char charset[] =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[std::rand() % max_index];
    };
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str;
}

static auto key = ::GenRandomString(KEY_SIZE);
static auto value = ::GenRandomString(VAL_SIZE);

class MockClient {
private:
    char buf[1024];
    asio::io_context ioCtx;
    asio::ip::tcp::socket socket;
    asio::ip::tcp::endpoint end_point;

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

    static std::string BuildResponse(const std::string& content) {
        std::string resp;
        resp += "+";
        resp += content;
        resp += "\r\n";
        return resp;
    }

public:
    MockClient(const char* ip, std::uint16_t port)
        : ioCtx{}, socket{ioCtx},
          end_point{asio::ip::address::from_string(ip), port} {
        socket.connect(end_point);
    }

    MockClient(const MockClient&) = delete;
    MockClient& operator=(const MockClient&) = delete;
    ~MockClient() = default;

    void SendCMD(const std::string& cmdName, const std::vector<std::string>& argv) {
        asio::write(socket, asio::buffer(MockClient::BuildCMD(cmdName, argv)));
    }

    void TestResponse(const std::string& expectResp) {
        asio::read(socket,
                   asio::buffer(buf),
                   asio::transfer_at_least(3));
    }
};

MockClient foxbatdbClt{"127.0.0.1", 7698};
MockClient redisClt{"127.0.0.1", 6379};

static void FoxbatDBSet(benchmark::State& state) {
    for (auto _: state) {
        // 注入一条数据到kv存储引擎
        foxbatdbClt.SendCMD("set", {key, value});
        foxbatdbClt.TestResponse("OK");
    }
}

static void FoxbatDBGet(benchmark::State& state) {
    for (auto _: state) {
        // 从kv存储引擎读取一条数据
        foxbatdbClt.SendCMD("get", {key});
        foxbatdbClt.TestResponse(value);
    }
}

static void RedisSet(benchmark::State& state) {
    for (auto _: state) {
        // 注入一条数据到kv存储引擎
        redisClt.SendCMD("set", {key, value});
        redisClt.TestResponse("OK");
    }
}

static void RedisGet(benchmark::State& state) {
    for (auto _: state) {
        // 从kv存储引擎读取一条数据
        redisClt.SendCMD("get", {key});
        redisClt.TestResponse(value);
    }
}

/* 注意：必须先运行FoxbatDB进程和Redis服务 */
int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    ::benchmark::RegisterBenchmark("FoxbatDBSet", &FoxbatDBSet);
    ::benchmark::RegisterBenchmark("FoxbatDBGet", &FoxbatDBGet);
    ::benchmark::RegisterBenchmark("RedisSet", &RedisSet);
    ::benchmark::RegisterBenchmark("RedisGet", &RedisGet);
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}