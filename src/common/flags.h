#pragma once
#include <cstdint>
#include <string>

namespace foxbatdb {
  struct Flags {
    std::uint16_t port;
    std::int64_t logWriteCronJobPeriodMs;
    std::string logFileName;
  };

  extern Flags flags;
}