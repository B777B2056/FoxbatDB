#pragma once
#include <cstdint>

namespace foxbatdb {
  namespace utils {
    std::uint64_t GetMillisecondTimestamp();
    bool IsValidTimestamp(std::uint64_t timestamp);
  }
}