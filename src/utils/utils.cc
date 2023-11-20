#include "utils.h"
#include <chrono>
#include <cstddef>

namespace foxbatdb {
namespace utils {
  std::uint64_t GetMillisecondTimestamp() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch()).count();
  }

  bool IsValidTimestamp(std::uint64_t timestamp) {
    if (timestamp > 0xFFFFFFFFFFFFFFFF) {
      return false;
    }
    return timestamp <= utils::GetMillisecondTimestamp();
  }
}
}  // namespace foxbatdb