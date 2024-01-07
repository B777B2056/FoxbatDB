#include "flags.h"
#include "toml.hpp"
#include <system_error>
#include <unordered_map>

namespace foxbatdb {
    Flags& Flags::GetInstance() {
        static Flags instance;
        return instance;
    }

    void Flags::Init(const std::string& tomlFilePath) {
        LoadFromConf(tomlFilePath);
        Preprocess();
    }

    void Flags::LoadFromConf(const std::string& tomlFilePath) {
        auto tbl = toml::parse_file(tomlFilePath);

        this->port = tbl["startup"]["listenPort"].value<std::uint16_t>().value();
        this->dbMaxNum = tbl["startup"]["databaseNumber"].value<std::uint8_t>().value();
        this->serverLogPath = tbl["startup"]["serverLogPath"].value<std::string>().value();
        this->serverLogMaxFileSize = tbl["startup"]["serverLogMaxFileSizeMB"].value<std::uint64_t>().value();
        this->serverLogMaxFileNumber = tbl["startup"]["serverLogMaxFileNumber"].value<std::uint64_t>().value();
        this->serverLogFlushPeriodSec = tbl["startup"]["serverLogFlushPeriodSec"].value<std::int64_t>().value();

        this->operationLogWriteCronJobPeriodMs = tbl["aof"]["aofCronJobPeriodMs"].value<std::int64_t>().value();
        this->operationLogFileName = tbl["aof"]["aofLogFilePath"].value<std::string>().value();

        this->dbLogFileDir = tbl["dbfile"]["dbFileDirectory"].value<std::string>().value();
        this->dbLogFileMaxSize = tbl["dbfile"]["dbFileMaxSizeMB"].value<std::uint64_t>().value();

        this->keyMaxBytes = tbl["keyval"]["keyMaxBytes"].value<std::uint32_t>().value();
        this->valMaxBytes = tbl["keyval"]["valueMaxBytes"].value<std::uint32_t>().value();

        {
            static const std::unordered_map<std::string, MaxMemoryPolicyEnum> maxMemoryPolicyMap{
                    {"noeviction", MaxMemoryPolicyEnum::eNoeviction},
                    {"allkeys-lru", MaxMemoryPolicyEnum::eLRU}};

            auto maxMemoryPolicyStr = tbl["memory"]["maxmemory-policy"].value<std::string>().value();
            if (!maxMemoryPolicyMap.contains(maxMemoryPolicyStr))
                throw std::runtime_error{"invalid maxmemory-policy config"};

            this->maxMemoryPolicy = maxMemoryPolicyMap.at(maxMemoryPolicyStr);
        }
    }

    void Flags::Preprocess() {
        serverLogMaxFileSize = serverLogMaxFileSize * 1024 * 1024;

        if (dbLogFileDir.back() == '/') {
            dbLogFileDir.pop_back();
        }
        dbLogFileMaxSize = dbLogFileMaxSize * 1024 * 1024;
    }
}// namespace foxbatdb