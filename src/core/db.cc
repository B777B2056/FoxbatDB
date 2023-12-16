#include "db.h"
#include <filesystem>
#include <iostream>
#include "common/flags.h"
#include "errors/protocol.h"
#include "errors/runtime.h"
#include "filemanager.h"
#include "obj.h"
#include "memory.h"
#include "network/cmd.h"
#include "utils/resp.h"

namespace foxbatdb {
  ProcResult MakeProcResult(std::error_code err) {
    return ProcResult{.hasError = true, .data = utils::BuildErrorResponse(err)};
  }

  ProcResult MakePubSubProcResult(const std::vector<std::string>& data) {
    return ProcResult{.hasError = false, .data = utils::BuildPubSubResponse(data)};
  }

  ProcResult OKResp() {
    return MakeProcResult("OK");
  }

  ProcResult CommandDB(CMDSessionPtr weak, const Command& cmd) {
    return OKResp();
  }

  ProcResult InfoDB(CMDSessionPtr weak, const Command& cmd) {
    return OKResp();
  }

  ProcResult ServerDB(CMDSessionPtr weak, const Command& cmd) {
    return OKResp();
  }

  ProcResult SwitchDB(CMDSessionPtr weak, const Command& cmd) {
    auto idx = cmd.argv.back().ToInteger<std::size_t>();
    if (!idx.has_value()) {
      return MakeProcResult(error::ProtocolErrorCode::kSyntax);
    }

    if (*idx >= DatabaseManager::GetInstance().GetDBListSize()) {
      return MakeProcResult(error::RuntimeErrorCode::kDBIdxOutOfRange);
    }
    if (auto clt = weak.lock(); clt) {
      clt->SwitchToTargetDB(*idx);
      return OKResp();
    } else {
      return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
    }
  }

  ProcResult Load(CMDSessionPtr weak, const Command& cmd) {
    if (DatabaseManager::GetInstance().IsInReadonlyMode()) {
      return MakeProcResult(error::RuntimeErrorCode::kMemoryOut);
    }

    int cnt = 0;
    auto& dbm = DatabaseManager::GetInstance();
    for (const auto& path : cmd.argv) {
      if (dbm.LoadRecordsFromLogFile(path.ToTextString())) {
        ++cnt;
      }
    }

    return MakeProcResult(cnt);
  }

  ProcResult StrSet(CMDSessionPtr weak, const Command& cmd) {
    if (DatabaseManager::GetInstance().IsInReadonlyMode()) {
      return MakeProcResult(error::RuntimeErrorCode::kMemoryOut);
    }

    auto& key = cmd.argv[0];
    auto& val = cmd.argv[1];

    auto clt = weak.lock();
    if (!clt) {
      return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
    }

    auto* db = clt->CurrentDB();
    auto [err, data] = db->StrSet(clt->CurrentDBIdx(), key, val, cmd.options);
    if (err) {
      return MakeProcResult(err);
    } else if (data.has_value()) {
      return MakeProcResult(*data);
    } else {
      return OKResp();
    }
  }

  ProcResult StrGet(CMDSessionPtr weak, const Command& cmd) {
    auto clt = weak.lock();
    if (!clt) {
      return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
    }

    auto& key = cmd.argv[0];
    auto* db = clt->CurrentDB();
    auto val = db->StrGet(key);
    if (!val.has_value() || val->IsEmpty()) {
      return MakeProcResult(error::RuntimeErrorCode::kKeyNotFound);
    }

    return MakeProcResult(*val);
  }

  ProcResult Del(CMDSessionPtr weak, const Command& cmd) {
    auto clt = weak.lock();
    if (!clt) {
      return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
    }

    int cnt = 0;
    auto* db = clt->CurrentDB();
    for (const auto& key : cmd.argv) {
      if (!db->Del(key))
        ++cnt;
    }
    return MakeProcResult(cnt);
  }

  ProcResult Watch(CMDSessionPtr weak, const Command& cmd) {
    auto clt = weak.lock();
    if (!clt) {
      return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
    }

    auto* db = clt->CurrentDB();
    db->AddWatch(cmd.argv[0], weak);
    return OKResp();
  }

  ProcResult UnWatch(CMDSessionPtr weak, const Command& cmd) {
    auto clt = weak.lock();
    if (!clt) {
      return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
    }

    auto* db = clt->CurrentDB();
    db->DelWatch(cmd.argv[0], weak);
    return OKResp();
  }

  ProcResult PublishWithChannel(CMDSessionPtr weak, const Command& cmd) {
    auto clt = weak.lock();
    if (!clt) {
      return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
    }

    auto& dbm = DatabaseManager::GetInstance();
    auto cnt = dbm.PublishWithChannel(cmd.argv[0], cmd.argv[1]);

    return MakeProcResult(cnt);
  }

  ProcResult SubscribeWithChannel(CMDSessionPtr weak, const Command& cmd) {
    auto clt = weak.lock();
    if (!clt) {
      return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
    }

    std::vector<std::string> result;

    auto& dbm = DatabaseManager::GetInstance();
    for (std::size_t i = 0; i < cmd.argv.size(); ++i) {
      dbm.SubscribeWithChannel(cmd.argv[i], weak);
      result.emplace_back(cmd.name);
      result.emplace_back(cmd.argv[i].ToTextString());
      result.emplace_back(std::to_string(i+1));
    }

    return MakePubSubProcResult(result);
  }

  ProcResult UnSubscribeWithChannel(CMDSessionPtr weak, const Command& cmd) {
    auto clt = weak.lock();
    if (!clt) {
      return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
    }

    auto& dbm = DatabaseManager::GetInstance();
    for (const auto& channel : cmd.argv)
      dbm.UnSubscribeWithChannel(channel, weak);
    return OKResp();
  }

  ProcResult Merge(CMDSessionPtr weak, const Command& cmd) {
    auto clt = weak.lock();
    if (!clt) {
      return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
    }

    auto& fm = LogFileManager::GetInstance();
    fm.Merge();
    return OKResp();
  }

  DatabaseManager::DatabaseManager()
      : mIsNonWrite_{false},
        mDBList_{Flags::GetInstance().dbMaxNum} {}

  DatabaseManager::~DatabaseManager() {}

  DatabaseManager& DatabaseManager::GetInstance() {
    static DatabaseManager inst;
    return inst;
  }

  void DatabaseManager::Init() {

  }

  bool DatabaseManager::LoadRecordsFromLogFile(const std::string& path) {
    if (!std::filesystem::exists(path)) {
      return false;
    }
    std::fstream file{path, std::ios_base::in | std::ios::binary};
    if (!file.is_open()) {
      std::cerr << "DB file open failed: " << ::strerror(errno);
      return false;
    }

    while (!file.eof()) {
      FileRecord record;
      if (!record.LoadFromDisk(file, file.tellg())) continue;

      Database* db = GetDBByIndex(record.header.dbIdx);
      BinaryString& key = record.data.key;
      BinaryString& val = record.data.value;

      if (val.Length())
        db->StrSet(record.header.dbIdx, key, val);
      else
        db->Del(key);
    }
    return true;
  }

  bool DatabaseManager::HaveMemoryAvailable() const {
    for (auto& db : mDBList_) {
      if (db.HaveMemoryAvailable()) {
        return true;
      }
    }
    return false;
  }

  void DatabaseManager::ScanDBForReleaseMemory() {
    for (auto& db : mDBList_) {
      if (db.HaveMemoryAvailable()) {
        db.ReleaseMemory();
      }
    }
  }

  void DatabaseManager::SetNonWrite() {
    mIsNonWrite_ = true;
  }

  void DatabaseManager::CancelNonWrite() {
    mIsNonWrite_ = false;
  }

  bool DatabaseManager::IsInReadonlyMode() const {
    return mIsNonWrite_;
  }

  std::size_t DatabaseManager::GetDBListSize() const {
    return mDBList_.size();
  }

  Database* DatabaseManager::GetDBByIndex(std::size_t idx) {
    return &mDBList_[idx];
  }

  void DatabaseManager::SubscribeWithChannel(const BinaryString& channel,
    CMDSessionPtr weak) {
    mPubSubChannel_.Subscribe(channel, weak);
  }

  void DatabaseManager::UnSubscribeWithChannel(const BinaryString& channel,
                                               CMDSessionPtr weak) {
    mPubSubChannel_.UnSubscribe(channel, weak);
  }

  std::int32_t DatabaseManager::PublishWithChannel(const BinaryString& channel,
    const BinaryString& msg) {
    return mPubSubChannel_.Publish(channel, msg);
  }

  Database::Database() : mEngine_{new LRUAdapter()} {
    mEngine_->SetStorage(&mStorage_);
  }

  Database::~Database() {
    delete mEngine_;
  }

  void Database::ReleaseMemory() {
    mEngine_->RemoveItem();
  }

  bool Database::HaveMemoryAvailable() const {
    return mEngine_->IsEmpty();
  }

  void Database::Foreach(ForeachCallback callback) { 
    mEngine_->Foreach(callback);
  }

  void Database::NotifyWatchedClientSession(const BinaryString& key) {
    if (mWatchedMap_.Contains(key)) {
      for (const auto& weak : *(mWatchedMap_.GetRef(key))) {
        if (auto clt = weak.lock(); clt) {
          clt->SetCurrentTxToFail();
        }
      }
    }
  }
  void Database::StrSetForHistoryData(std::fstream& file, std::streampos pos,
                                      const FileRecord& record) {
    auto obj = ValueObject::NewForHistory(file, pos, record);
    if (!obj) {
      return;
    }
    mEngine_->Put(record.data.key, obj);
  }

  std::tuple<std::error_code, std::optional<BinaryString>> Database::StrSet(
      std::uint8_t dbIdx, 
      const BinaryString& key, const BinaryString& val,
      const std::vector<CommandOption>& opts) {
    auto obj = ValueObject::New(dbIdx, key, val);
    if (!obj) {
      // TODO：内存耗尽
      return std::make_tuple(error::RuntimeErrorCode::kMemoryOut,
                              std::nullopt);
    }
    
    std::optional<BinaryString> data = std::nullopt;
    for (const auto& opt : opts) {
      auto [err, payload] = StrSetWithOption(key, *obj, opt);
      if (err) {
        return std::make_tuple(err, std::nullopt);
      }
      if (payload.has_value()) {
        data = payload;
      }
    }
    NotifyWatchedClientSession(key);
    mEngine_->Put(key, obj);
    return std::make_tuple(error::ProtocolErrorCode::kSuccess, data);
  }

  void Database::StrSetForMerge(std::fstream& mergeFile, std::uint8_t dbIdx,
                                const BinaryString& key,
                                const BinaryString& val) {
    auto obj = ValueObject::NewForMerge(mergeFile, dbIdx, key, val);
    if (!obj) {
      // TODO：内存耗尽
      return;
    }
    mEngine_->Put(key, obj);
  }

  std::tuple<std::error_code, std::optional<BinaryString>>
  Database::StrSetWithOption(const BinaryString& key, ValueObject& obj,
                             const CommandOption& opt) {
    std::error_code err = error::RuntimeErrorCode::kSuccess;
    std::optional<BinaryString> data = std::nullopt;
    switch (opt.type) {
      case CmdOptionType::kEX: {
        auto sec = opt.argv.front().ToInteger<std::int64_t>();
        if (!sec.has_value()) {
          err = error::ProtocolErrorCode::kSyntax;
        } else {
          obj.SetExpiration(std::chrono::seconds{*sec});
        }
      } break;
      case CmdOptionType::kPX: {
        auto ms = opt.argv.front().ToInteger<std::int64_t>();
        if (!ms.has_value()) {
          err = error::ProtocolErrorCode::kSyntax;
        } else {
          obj.SetExpiration(std::chrono::milliseconds{*ms});
        }
      } break;
      case CmdOptionType::kNX:
        if (mEngine_->Contains(key)) {
          err = error::RuntimeErrorCode::kKeyAlreadyExist;
        }
        break;
      case CmdOptionType::kXX:
        if (!mEngine_->Contains(key)) {
          err = error::RuntimeErrorCode::kKeyNotFound;
        }
        break;
      case CmdOptionType::kKEEPTTL:
        // 保留设置前指定键的生存时间
        if (mEngine_->Contains(key)) {
          auto oldObj = mEngine_->Get(key);
          obj.SetExpiration(oldObj->GetExpiration());
        }
        break;
      case CmdOptionType::kGET:
        // 返回 key 存储的值，如果 key 不存在返回空
        if (mEngine_->Contains(key)) {
          data = StrGet(key);
        } else {
          data = {"nil"};
        }
        break;
      default:
        break;
    }
    return std::make_tuple(err, data);
  }

  std::optional<BinaryString> Database::StrGet(const BinaryString& key) {
    auto obj = mEngine_->Get(key);
    if (!obj) {
      return {};
    }
    if (obj->IsExpired()) {
      obj->DeleteValue(key);
      Del(key);
      return {};
    }
    return obj->GetValue();
  }

  std::error_code Database::Del(const BinaryString& key) {
    NotifyWatchedClientSession(key);
    mEngine_->Get(key)->DeleteValue(key);
    mEngine_->Del(key);
    return error::RuntimeErrorCode::kSuccess;
  }

  void Database::AddWatch(const BinaryString& key, CMDSessionPtr clt) {
    if (!mEngine_->Contains(key))
      return;

    auto cltPtr = clt.lock();
    if (!cltPtr) 
      return;
    cltPtr->AddWatchKey(key);

    if (!mWatchedMap_.Contains(key)) {
      mWatchedMap_.Add(key, {});
    }
    mWatchedMap_.GetRef(key)->emplace_back(clt);
  }

  void Database::DelWatch(const BinaryString& key, CMDSessionPtr clt) {
    if (!mEngine_->Contains(key))
      return;

    auto cltPtr = clt.lock();
    if (!cltPtr)
      return;
    cltPtr->DelWatchKey(key);
    mWatchedMap_.Del(key);
  }
}