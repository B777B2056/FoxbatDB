#pragma once
#include <cstddef>
#include <deque>
#include <functional>
#include <string>
#include <vector>
#include <tuple>
#include <memory>
#include "common/common.h"
#include "data_structure/dict.h"
#include "pubsub.h"
#include "utils/resp.h"

namespace foxbatdb {
  class Database;
  class ValueObject;
  class MaxMemoryPolicyAdapter;

  struct ProcResult {
    bool hasError;
    std::string data;
  };

  ProcResult MakeProcResult(std::error_code err);
  ProcResult MakePubSubProcResult(const std::vector<std::string>& data);

  template <typename T>
    requires(!std::is_same_v<T, std::error_code>)
  ProcResult MakeProcResult(const T& data) {
    return ProcResult{.hasError = false, .data = utils::BuildResponse<T>(data)};
  }

  ProcResult OKResp();

  ProcResult CommandDB(CMDSessionPtr weak, const Command& cmd);
  ProcResult InfoDB(CMDSessionPtr weak, const Command& cmd);
  ProcResult ServerDB(CMDSessionPtr weak, const Command& cmd);
  ProcResult SwitchDB(CMDSessionPtr weak, const Command& cmd);
  ProcResult Load(CMDSessionPtr weak, const Command& cmd);
  ProcResult StrSet(CMDSessionPtr weak, const Command& cmd);
  ProcResult StrGet(CMDSessionPtr weak, const Command& cmd);
  ProcResult Del(CMDSessionPtr weak, const Command& cmd);
  ProcResult Watch(CMDSessionPtr weak, const Command& cmd);
  ProcResult UnWatch(CMDSessionPtr weak, const Command& cmd);
  ProcResult PublishWithChannel(CMDSessionPtr weak, const Command& cmd);
  ProcResult SubscribeWithChannel(CMDSessionPtr weak, const Command& cmd);
  ProcResult UnSubscribeWithChannel(CMDSessionPtr weak, const Command& cmd);
  ProcResult Merge(CMDSessionPtr weak, const Command& cmd);

  class DatabaseManager {
   private:
    bool mIsNonWrite_;
    std::vector<Database> mDBList_;
    PubSubWithChannel mPubSubChannel_;

    DatabaseManager();

   public:
    ~DatabaseManager();
    static DatabaseManager& GetInstance();
    void Init();
    bool LoadRecordsFromLogFile(const std::string& path);
    bool HaveMemoryAvailable() const;
    void ScanDBForReleaseMemory();
    void SetNonWrite();
    void CancelNonWrite();
    bool IsInReadonlyMode() const;
    std::size_t GetDBListSize() const;
    Database* GetDBByIndex(std::size_t idx);

    void SubscribeWithChannel(const BinaryString& channel, CMDSessionPtr weak);
    void UnSubscribeWithChannel(const BinaryString& channel,
                                CMDSessionPtr weak);
    std::int32_t PublishWithChannel(const BinaryString& channel,
                                    const BinaryString& msg);
  };

  class Database {
  private:
    StorageImpl mStorage_;
    MaxMemoryPolicyAdapter* mEngine_;
    WatchedKeyMap mWatchedMap_;

    std::tuple<std::error_code, std::optional<BinaryString>> StrSetWithOption(
      const BinaryString& key, ValueObject& obj, const CommandOption& opt);

    void NotifyWatchedClientSession(const BinaryString& key);
        
  public:
    Database();
    ~Database();

    void ReleaseMemory();
    bool HaveMemoryAvailable() const;

    void Foreach(ForeachCallback callback);

    void StrSetForHistoryData(std::fstream& file, std::streampos pos,
                              const FileRecord& record);
    std::tuple<std::error_code, std::optional<BinaryString>> StrSet(
        std::uint8_t dbIdx,
        const BinaryString& key, const BinaryString& val,
        const std::vector<CommandOption>& opts = {});
    void StrSetForMerge(std::fstream& mergeFile, std::uint8_t dbIdx,
                        const BinaryString& key, const BinaryString& val);
    std::optional<BinaryString> StrGet(const BinaryString& key);
    std::error_code Del(const BinaryString& key);
    void AddWatch(const BinaryString& key, CMDSessionPtr clt);
    void DelWatch(const BinaryString& key, CMDSessionPtr clt);
  };
}