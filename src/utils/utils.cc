#include "utils.h"
#include <chrono>
#include <cstddef>
#include "core/obj.h"

namespace foxbatdb {
namespace utils {
  std::uint64_t GetMillisecondTimestamp() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
             std::chrono::system_clock::now().time_since_epoch()).count();
  }

  // CRC32算法的表
  static std::uint32_t CRC32Table[256];

  // 生成CRC32表
  static void GenerateCRC32Table() {
    for (int i = 0; i < 256; ++i) {
      std::uint32_t crc = i;
      for (int j = 0; j < 8; ++j) {
        crc = (crc >> 1) ^ ((crc & 1) ? 0xEDB88320 : 0);
      }
      CRC32Table[i] = crc;
    }
  }

  std::uint32_t FillCRC32Value(const FileRecordHeader& header, std::string_view k,
                      std::string_view v) {
    static bool IsCRC32TableInited = false;
    if (!IsCRC32TableInited) {
      GenerateCRC32Table();
      IsCRC32TableInited = true;
    }

    std::uint32_t crc = 0xFFFFFFFF;

    auto calcCRC32Once = [&crc](const char* data, std::size_t size) -> void {
      for (std::size_t i = 0; i < size; ++i) {
        crc = (crc >> 8) ^ CRC32Table[(crc ^ data[i]) & 0xFF];
      }
    };

    calcCRC32Once(reinterpret_cast<const char*>(&header.dbIdx),
                  sizeof(header.dbIdx));
    calcCRC32Once(reinterpret_cast<const char*>(&header.timestamp),
                  sizeof(header.timestamp));
    calcCRC32Once(reinterpret_cast<const char*>(&header.keySize),
                  sizeof(header.keySize));
    calcCRC32Once(reinterpret_cast<const char*>(&header.valSize),
                  sizeof(header.valSize));
    calcCRC32Once(k.data(), k.size());
    calcCRC32Once(v.data(), v.size());

    return crc ^ 0xFFFFFFFF;
  }
}
}  // namespace foxbatdb