#pragma once
#include <any>
#include <cstdint>
#include <chrono>
#include <fstream>
#include <variant>
#include <optional>
#include <memory>
#include <string_view>
#include "data_structure/binstr.h"

namespace foxbatdb {
#pragma pack(push, 1)
  struct FileRecordHeader {
    std::uint32_t crc;
    std::uint64_t timestamp;
    std::uint8_t dbIdx;
    std::uint64_t keySize;
    std::uint64_t valSize;
  };
#pragma pack(pop)

  struct FileRecordData {
    BinaryString key;
    BinaryString value;
  };

  struct FileRecord {
    FileRecordHeader header;
    FileRecordData data;

    std::uint32_t CalculateCRC32Value() const;
    bool LoadFromDisk(std::fstream& file);
    void DumpToDisk(std::fstream& file);
  };

  class ValueObject {
   private:
     std::uint8_t dbIdx;
     std::fstream* logFilePtr;
     std::fstream::pos_type pos{-1};
     std::chrono::milliseconds expirationTimeMs;
     std::chrono::time_point<std::chrono::steady_clock> createdTime;

     ValueObject() = default;
     void UpdateValue(const BinaryString& k, const BinaryString& v);

    public:
     ~ValueObject() = default;

     static std::shared_ptr<ValueObject> New(std::uint8_t dbIdx,
                                             const BinaryString& k,
                                             const BinaryString& v);
     static std::shared_ptr<ValueObject> New(std::uint8_t dbIdx,
                                             const BinaryString& k,
                                             const BinaryString& v,
                                             std::chrono::milliseconds ms);
     
     std::optional<BinaryString> GetValue();
     void DeleteValue(const BinaryString& k);

     void SetExpiration(std::chrono::seconds sec);
     void SetExpiration(std::chrono::milliseconds ms);
     std::chrono::milliseconds GetExpiration() const;
     bool IsExpired() const;
  };
}  // namespace foxbatdb