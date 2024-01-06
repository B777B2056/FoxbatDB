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

    constexpr unsigned long long operator"" _MB(unsigned long long m) {
        return m * 1024 * 1024;
    }

    // CRC32算法表
    static std::uint32_t CRC32Table[256];

    // 生成CRC32表
    static void InitCRC32Table() {
        static bool IsCRC32TableInited = false;

        for (int i = 0; i < 256; ++i) {
            std::uint32_t crc = i;
            for (int j = 0; j < 8; ++j) {
                crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320 : 0);
            }
            CRC32Table[i] = crc;
        }

        if (!IsCRC32TableInited) {
            IsCRC32TableInited = true;
        }
    }

    std::uint32_t CRC(const char* buf, std::size_t size, std::uint32_t lastCRC) {
        InitCRC32Table();
        std::uint32_t crcVal = lastCRC;
        for (std::size_t i = 0; i < size; ++i) {
            crcVal = (crcVal >> 8) ^ CRC32Table[(crcVal ^ buf[i]) & 0xFF];
        }
        return crcVal;
    }
}// namespace foxbatdb::utils
