#include "utils.h"
#include <chrono>

namespace foxbatdb::utils {
    std::uint64_t GetMicrosecondTimestamp() {
        return std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                .count();
    }

    std::chrono::steady_clock::time_point MicrosecondTimestampCovertToTimePoint(
            std::uint64_t timestamp) {
        return std::chrono::steady_clock::time_point{
                std::chrono::microseconds{timestamp}};
    }

    bool IsValidTimestamp(std::uint64_t timestamp) {
        if (timestamp > 0xFFFFFFFFFFFFFFFF) {
            return false;
        }
        return timestamp <= utils::GetMicrosecondTimestamp();
    }
}// namespace foxbatdb::utils
