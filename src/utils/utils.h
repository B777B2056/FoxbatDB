#pragma once
#include <cstdint>
#include <chrono>

namespace foxbatdb {
  namespace utils {
    std::uint64_t GetMicrosecondTimestamp();
    std::chrono::steady_clock::time_point MicrosecondTimestampCovertToTimePoint(std::uint64_t timestamp);
    bool IsValidTimestamp(std::uint64_t timestamp);
  }
}