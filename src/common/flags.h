#pragma once
#include <cstdint>
#include <string>

namespace foxbatdb {
  struct Flags {
    std::uint16_t port;
    std::int64_t logWriteCronJobPeriodMs;
    std::string logFileName;
    std::string dbFileDir;
    std::uint8_t dbMaxNum;
    std::uint64_t dbFileMaxRecordNum;
    std::uint32_t keyMaxBytes;
    std::uint32_t valMaxBytes;
  };

  extern Flags flags;
}