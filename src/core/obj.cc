#include "obj.h"
#include <climits>
#include "common/flags.h"
#include "filemanager.h"
#include "utils/utils.h"

namespace foxbatdb {
  static bool ValidateFileRecordHeader(const FileRecordHeader& header) { 
    if (!utils::IsValidTimestamp(header.timestamp))
      return false;

    if (header.dbIdx > flags.dbMaxNum)
      return false;

    if (header.keySize > flags.keyMaxBytes)
      return false;

    if (header.valSize > flags.valMaxBytes)
      return false;

    return true;
  }

  // CRC32算法表
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

  std::uint32_t FileRecord::CalculateCRC32Value() const {
    static bool IsCRC32TableInited = false;
    if (!IsCRC32TableInited) {
      GenerateCRC32Table();
      IsCRC32TableInited = true;
    }

    std::uint32_t crc = 0xFFFFFFFF;

    auto calcCRC32Once = [&crc](const char* buf, std::size_t size) -> void {
      for (std::size_t i = 0; i < size; ++i) {
        crc = (crc >> 8) ^ CRC32Table[(crc ^ buf[i]) & 0xFF];
      }
    };

    calcCRC32Once(reinterpret_cast<const char*>(&header.dbIdx),
                  sizeof(header.dbIdx));
    calcCRC32Once(reinterpret_cast<const char*>(&header.timestamp),
                  sizeof(header.timestamp));
    calcCRC32Once(reinterpret_cast<const char*>(&header.dbIdx),
                  sizeof(header.dbIdx));
    calcCRC32Once(reinterpret_cast<const char*>(&header.keySize),
                  sizeof(header.keySize));
    calcCRC32Once(reinterpret_cast<const char*>(&header.valSize),
                  sizeof(header.valSize));
    calcCRC32Once(data.key.ToCString(), header.keySize);
    calcCRC32Once(data.value.ToCString(), header.valSize);

    return crc ^ 0xFFFFFFFF;
  }

  bool FileRecord::LoadFromDisk(std::fstream& file) {
    file.read(reinterpret_cast<char*>(&header), sizeof(header));

    if (!ValidateFileRecordHeader(header))
      return false;

    data.key.Resize(header.keySize);
    data.value.Resize(header.valSize);

    file.read(data.key.ToCString(), header.keySize);
    file.read(data.value.ToCString(), header.valSize);

    return (header.crc == CalculateCRC32Value());
  }

  void FileRecord::DumpToDisk(std::fstream& file) {
    header.timestamp = utils::GetMillisecondTimestamp();
    header.keySize = data.key.Length();
    header.valSize = data.value.Length();
    header.crc = CalculateCRC32Value();

    file.write(reinterpret_cast<char*>(&header), sizeof(header));
    file.write(data.key.ToCString(), data.key.Length());
    file.write(data.value.ToCString(), data.value.Length());
    file.flush();
  }

  std::shared_ptr<ValueObject> ValueObject::New(std::uint8_t dbIdx,
                                                const BinaryString& k,
                                                const BinaryString& v) {
    return ValueObject::New(dbIdx, k, v, std::chrono::milliseconds{ULLONG_MAX});
  }

  std::shared_ptr<ValueObject> ValueObject::New(std::uint8_t dbIdx,
                                                const BinaryString& k,
                                                const BinaryString& v,
                                                std::chrono::milliseconds ms) {
    std::shared_ptr<ValueObject> obj{new ValueObject()};
    obj->dbIdx = dbIdx;
    obj->expirationTimeMs = ms;
    obj->createdTime = std::chrono::steady_clock::now();
    obj->UpdateValue(k, v);
    return obj;
  }

  std::optional<BinaryString> ValueObject::GetValue() {
    if (-1 == pos)  
      return {};
    
    logFilePtr->seekg(pos);
    FileRecord record;
    if (record.LoadFromDisk(*logFilePtr)) {
      return record.data.value;
    } else {
      return {};
    }
  }

  void ValueObject::UpdateValue(const BinaryString& k, const BinaryString& v) {
    this->logFilePtr = LogFileManager::GetInstance().GetAvailableLogFile();
    // 当前record文件读指针位置为写入数据之前的写指针位置
    this->pos = this->logFilePtr->tellp();  
    // 刷入日志文件（磁盘）
    FileRecord record;
    record.header.dbIdx = dbIdx;
    record.data.key = k;
    record.data.value = v;
    record.DumpToDisk(*logFilePtr);
  }

  void ValueObject::DeleteValue(const BinaryString& k) {
    UpdateValue(k, {});
  }

  void ValueObject::SetExpiration(std::chrono::seconds sec) {
    SetExpiration(std::chrono::duration_cast<std::chrono::milliseconds>(sec));
  }

  void ValueObject::SetExpiration(std::chrono::milliseconds ms) {
    expirationTimeMs = ms;
  }

  std::chrono::milliseconds ValueObject::GetExpiration() const {
    return expirationTimeMs;
  }

  bool ValueObject::IsExpired() const {
    auto exTime = createdTime + expirationTimeMs;
    return std::chrono::steady_clock::now() <= exTime;
  }
}