#pragma once
#include <cstdint>
#include <string_view>

namespace foxbatdb {
  struct FileRecordHeader;

  namespace utils {
    std::uint64_t GetMillisecondTimestamp();
    std::uint32_t FillCRC32Value(const FileRecordHeader& header, std::string_view k, std::string_view v);
  }
}