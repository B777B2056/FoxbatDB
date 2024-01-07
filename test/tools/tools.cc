#include "tools/tools.h"
#include "core/db.h"
#include "cron/cron.h"
#include "errors/protocol.h"
#include "flag/flags.h"
#include "frontend/cmdmap.h"
#include "frontend/parser.h"
#include "log/datalog.h"
#include "log/oplog.h"
#include "log/serverlog.h"
#include <filesystem>
#include <random>
#include <sstream>
#include <system_error>

void OutOfMemoryHandler() {
    // 所有DB进入不允许写入状态，只响应非写入命令
    foxbatdb::DatabaseManager::GetInstance().SetNonWrite();
}

void MemoryAllocRetryFunc() {
    auto& dbm = foxbatdb::DatabaseManager::GetInstance();
    if (dbm.HaveMemoryAvailable()) {
        dbm.ScanDBForReleaseMemory();
    } else {
        OutOfMemoryHandler();
    }
}

void InitComponents(const std::string& flagConfPath) {
    std::set_new_handler(MemoryAllocRetryFunc);
    foxbatdb::Flags::GetInstance().Init(flagConfPath);
    foxbatdb::ServerLog::GetInstance().Init();
    foxbatdb::OperationLog::GetInstance().Init();
    foxbatdb::DatabaseManager::GetInstance().Init();
    foxbatdb::DataLogFileManager::GetInstance().Init();
    foxbatdb::CronJobManager::GetInstance().Init();
}

std::shared_ptr<foxbatdb::CMDSession> GetMockCMDSession() {
    static asio::io_context ioContext;
    return std::make_shared<foxbatdb::CMDSession>(asio::ip::tcp::socket{ioContext});
}

std::string GenRandomString(std::size_t length) {
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

foxbatdb::ParseResult BuildCMD(const std::string& cmdName, const std::vector<std::string>& argv) {
    bool isCmdExist = foxbatdb::MainCommandMap.contains(cmdName);
    if (!isCmdExist) {
        return foxbatdb::ParseResult{
                .ec = foxbatdb::error::ProtocolErrorCode::kCommandNotFound,
                .isWriteCmd = false,
                .data = {},
        };
    }
    foxbatdb::ParseResult cmd{
            .ec = foxbatdb::error::ProtocolErrorCode::kSuccess,
            .isWriteCmd = foxbatdb::MainCommandMap.at(cmdName).isWriteCmd,
            .data = foxbatdb::Command{
                    .name = cmdName,
                    .call = foxbatdb::MainCommandMap.at(cmdName).call,
            }};
    for (const auto& param: argv) {
        cmd.data.argv.emplace_back(param);
    }
    return cmd;
}

foxbatdb::ParseResult BuildCMD(const std::string& cmdName, const std::vector<std::string>& argv,
                               const std::vector<foxbatdb::CommandOption>& options) {
    foxbatdb::ParseResult cmd{
            .ec = foxbatdb::error::ProtocolErrorCode::kSuccess,
            .isWriteCmd = foxbatdb::MainCommandMap.at(cmdName).isWriteCmd,
            .data = foxbatdb::Command{
                    .name = cmdName,
                    .call = foxbatdb::MainCommandMap.at(cmdName).call,
                    .options = options,
            }};
    for (const auto& param: argv) {
        cmd.data.argv.emplace_back(param);
    }
    return cmd;
}

static std::random_device rd;
static std::mt19937 gen(rd());
static std::uniform_int_distribution<> dis(0, 15);
static std::uniform_int_distribution<> dis2(8, 11);

std::string GenerateUUID() {
    std::stringstream ss;
    int i;
    ss << std::hex;
    for (i = 0; i < 8; i++) {
        ss << dis(gen);
    }
    ss << "-";
    for (i = 0; i < 4; i++) {
        ss << dis(gen);
    }
    ss << "-4";
    for (i = 0; i < 3; i++) {
        ss << dis(gen);
    }
    ss << "-";
    ss << dis2(gen);
    for (i = 0; i < 3; i++) {
        ss << dis(gen);
    }
    ss << "-";
    for (i = 0; i < 12; i++) {
        ss << dis(gen);
    };
    return ss.str();
}

void RemoveRelatedFiles() {
    std::filesystem::remove_all(foxbatdb::Flags::GetInstance().dbLogFileDir);
    std::filesystem::remove(foxbatdb::Flags::GetInstance().serverLogPath);
    std::filesystem::remove(foxbatdb::Flags::GetInstance().operationLogFileName);
}

static constexpr const char* TempDatasetFile = "tmp.data";

TestDataset::TestDataset(std::size_t datasetSize, std::size_t singleStringMaxLength, std::function<std::string()> keyGenerator)
    : mDatasetSize_{datasetSize}, mSingleStringMaxLength_{singleStringMaxLength}, mDataFile_{TempDatasetFile, std::ios::in | std::ios::out | std::ios::binary | std::ios::trunc} {
    if (!mDataFile_.is_open()) {
        throw std::runtime_error{std::string{"temp data file open failed: "} + ::strerror(errno)};
    }
    for (std::size_t i = 0; i < mDatasetSize_; ++i) {
        auto key = keyGenerator();
        TestDataset::RecordHeader header{
                .keySize = key.size(),
                .valSize = 1 + std::rand() % mSingleStringMaxLength_};
        WriteToFile(header, key, GenRandomString(header.valSize));
    }
    mDataFile_.seekg(0);
}

TestDataset::~TestDataset() {
    std::filesystem::remove(TempDatasetFile);
}

std::size_t TestDataset::Size() const {
    return mDatasetSize_;
}

void TestDataset::Foreach(std::function<void(const std::string&, const std::string&)> cb) {
    mDataFile_.clear();
    mDataFile_.seekg(0);

    for (const auto& [_, pos]: mRecordPosMap_) {
        std::string key, val;
        mDataFile_.seekg(pos);
        ReadFromFile(key, val);
        cb(key, val);
    }
}

void TestDataset::WriteToFile(const TestDataset::RecordHeader& header,
                              const std::string& key, const std::string& val) {
    mRecordPosMap_[key] = mDataFile_.tellp();
    mDataFile_.write(reinterpret_cast<const char*>(&header), sizeof(header));
    mDataFile_.write(key.c_str(), key.size());
    mDataFile_.write(val.c_str(), val.size());
    mDataFile_.flush();
}

void TestDataset::ReadFromFile(std::string& key, std::string& val) {
    TestDataset::RecordHeader header;
    mDataFile_.read(reinterpret_cast<char*>(&header), sizeof(header));

    if ((header.keySize > mSingleStringMaxLength_) || (header.valSize > mSingleStringMaxLength_)) {
        throw std::runtime_error("key or val size too long");
    }

    key.resize(header.keySize);
    val.resize(header.valSize);

    mDataFile_.read(key.data(), key.size());
    mDataFile_.read(val.data(), val.size());
}