#pragma once
#include <cstdint>
#include <string>

namespace foxbatdb {
    enum class MaxMemoryPolicyEnum : std::uint8_t {
        eNoeviction = 1,
        eLRU
    };

    struct Flags {
        std::uint16_t port;
        std::string serverLogPath;
        std::uint64_t serverLogMaxFileSize;
        std::uint64_t serverLogMaxFileNumber;
        std::int64_t serverLogFlushPeriodSec;
        std::int64_t operationLogWriteCronJobPeriodMs;
        std::string operationLogFileName;
        std::string dbLogFileDir;
        std::uint8_t dbMaxNum;
        std::uint64_t dbLogFileMaxSize;
        std::uint32_t keyMaxBytes;
        std::uint32_t valMaxBytes;
        MaxMemoryPolicyEnum maxMemoryPolicy;

        Flags(const Flags&) = delete;
        Flags& operator=(const Flags&) = delete;
        ~Flags() = default;
        static Flags& GetInstance();
        void Init(const std::string& tomlFilePath);

    private:
        Flags() = default;
        void LoadFromConf(const std::string& tomlFilePath);
        void Preprocess();
    };
}// namespace foxbatdb