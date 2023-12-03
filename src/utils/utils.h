#pragma once
#include <cstdint>
#include <chrono>

namespace foxbatdb {
  namespace utils {
    std::uint64_t GetMillisecondTimestamp();
    std::chrono::steady_clock::time_point TimestampCovertToTimePoint(std::uint64_t timestamp);
    bool IsValidTimestamp(std::uint64_t timestamp);
  }
}