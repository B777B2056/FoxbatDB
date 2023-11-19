#include "obj.h"
#include <climits>
#include "filemanager.h"
#include "utils/utils.h"

namespace foxbatdb {
  bool FileRecord::LoadFromDisk(std::fstream& file) {
    file.read(reinterpret_cast<char*>(&header), sizeof(header));

    char* keyBuf = new char[header.keySize];
    file.read(keyBuf, header.keySize);

    char* valBuf = new char[header.valSize];
    file.read(valBuf, header.valSize);

    std::uint32_t fileCRC =
        utils::FillCRC32Value(header, std::string_view{keyBuf, header.keySize},
                              std::string_view{valBuf, header.valSize});

    bool isValidRecord = (header.crc == fileCRC);
    if (isValidRecord) {
      data.key = std::string_view{keyBuf, header.keySize};
      data.value = std::string_view{valBuf, header.valSize};
    }
    delete[] keyBuf;
    delete[] valBuf;
    return isValidRecord;
  }

  void FileRecord::DumpToDisk(std::fstream& file) {
    header.crc = 0;
    header.timestamp = utils::GetMillisecondTimestamp();
    header.keySize = data.key.Length();
    header.valSize = data.value.Length();

    header.crc = utils::FillCRC32Value(
        header, std::string_view{data.key.ToCString(), data.key.Length()},
        std::string_view{data.value.ToCString(), data.value.Length()});

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