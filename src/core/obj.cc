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
    return CalculateCRC32Value(data.key, data.value);
  }

  std::uint32_t FileRecord::CalculateCRC32Value(const BinaryString& k, const BinaryString& v) const {
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

    calcCRC32Once(reinterpret_cast<const char*>(&header.timestamp),
                  sizeof(header.timestamp));
    calcCRC32Once(reinterpret_cast<const char*>(&header.dbIdx),
                  sizeof(header.dbIdx));
    calcCRC32Once(reinterpret_cast<const char*>(&header.keySize),
                  sizeof(header.keySize));
    calcCRC32Once(reinterpret_cast<const char*>(&header.valSize),
                  sizeof(header.valSize));
    calcCRC32Once(k.ToCString(), k.Length());
    calcCRC32Once(v.ToCString(), v.Length());

    return crc ^ 0xFFFFFFFF;
  }

  bool FileRecord::LoadFromDisk(std::fstream& file, std::streampos pos) {
    if (pos < 0)
      return false;

    file.seekg(pos);
    file.read(reinterpret_cast<char*>(&header), sizeof(header));

    if (!ValidateFileRecordHeader(header))
      return false;

    data.key.Resize(header.keySize);
    data.value.Resize(header.valSize);
    
    file.read(data.key.ToCString(), header.keySize);
    file.read(data.value.ToCString(), header.valSize);

    return (header.crc == CalculateCRC32Value());
  }

  void FileRecord::DumpToDisk(std::fstream& file, std::uint8_t dbIdx) { 
    DumpToDisk(file, dbIdx, data.key, data.value);
  }

  void FileRecord::DumpToDisk(std::fstream& file, std::uint8_t dbIdx, 
                              const BinaryString& k, const BinaryString& v) {
    header.dbIdx = dbIdx;
    header.timestamp = utils::GetMillisecondTimestamp();
    header.keySize = k.Length();
    header.valSize = v.Length();
    header.crc = CalculateCRC32Value(k, v);

    file.write(reinterpret_cast<char*>(&header), sizeof(header));
    file.write(k.ToCString(), k.Length());
    file.write(v.ToCString(), v.Length());
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
    obj->logFilePtr = LogFileManager::GetInstance().GetAvailableLogFile();
    obj->UpdateValue(k, v);
    return obj;
  }

  std::shared_ptr<ValueObject> ValueObject::NewForMerge(std::fstream& file,
                                                        std::uint8_t dbIdx,
                                                        const BinaryString& k,
                                                        const BinaryString& v) {
    std::shared_ptr<ValueObject> obj{new ValueObject()};
    obj->dbIdx = dbIdx;
    obj->expirationTimeMs = std::chrono::milliseconds{ULLONG_MAX};
    obj->createdTime = std::chrono::steady_clock::now();
    obj->logFilePtr = &file;
    obj->UpdateValue(k, v);
    return obj;
  }

  std::shared_ptr<ValueObject> ValueObject::NewForHistory(
      std::fstream& file, std::streampos pos, const FileRecord& record) {
    std::shared_ptr<ValueObject> obj{new ValueObject()};
    obj->dbIdx = record.header.dbIdx;
    obj->expirationTimeMs = std::chrono::milliseconds{ULLONG_MAX};
    obj->createdTime = utils::TimestampCovertToTimePoint(record.header.timestamp);
    obj->logFilePtr = &file;
    obj->pos = pos;
    return obj;
  }

  std::optional<BinaryString> ValueObject::GetValue() const {
    auto record = CovertToFileRecord();
    if (record.has_value()) {
      return record->data.value;
    } else {
      return {};
    }
  }

  void ValueObject::UpdateValue(const BinaryString& k, const BinaryString& v) {
    // 当前record文件读指针位置为写入数据之前的写指针位置
    this->pos = this->logFilePtr->tellp();  
    // 刷入日志文件（磁盘）
    FileRecord{}.DumpToDisk(*logFilePtr, this->dbIdx, k, v);
  }

  void ValueObject::DeleteValue(const BinaryString& k) {
    UpdateValue(k, {});
  }

  const std::fstream* ValueObject::GetLogFileHandler() const {
    return this->logFilePtr;
  }

  std::optional<FileRecord> ValueObject::CovertToFileRecord() const {
    FileRecord record;
    if (record.LoadFromDisk(*logFilePtr, pos)) {
      return record;
    } else {
      return {};
    }
  }

  bool ValueObject::IsSameLogFile(const std::fstream& targetFile) const {
    return &targetFile == logFilePtr;
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
    if (expirationTimeMs == std::chrono::milliseconds{ULLONG_MAX}) {
      return false;
    }
    auto exTime = createdTime + expirationTimeMs;
    return std::chrono::steady_clock::now() <= exTime;
  }
}