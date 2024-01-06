#include "db.h"
#include "errors/protocol.h"
#include "errors/runtime.h"
#include "flag/flags.h"
#include "frontend/cmdmap.h"
#include "frontend/server.h"
#include "log/serverlog.h"
#include "memory.h"
#include "utils/utils.h"
#include <algorithm>
#include <filesystem>

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

    ProcResult CommandDB(std::weak_ptr<CMDSession>, const Command&) {
        return OKResp();
    }

    ProcResult InfoDB(std::weak_ptr<CMDSession>, const Command&) {
        return OKResp();
    }

    ProcResult ServerDB(std::weak_ptr<CMDSession>, const Command&) {
        return OKResp();
    }

    ProcResult SwitchDB(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto idx = utils::ToInteger<std::size_t>(cmd.argv.back());
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

    ProcResult Load(std::weak_ptr<CMDSession>, const Command& cmd) {
        if (DatabaseManager::GetInstance().IsInReadonlyMode()) {
            return MakeProcResult(error::RuntimeErrorCode::kMemoryOut);
        }

        int cnt = 0;
        auto& dbm = DatabaseManager::GetInstance();
        for (const auto& path: cmd.argv) {
            if (dbm.LoadRecordsFromLogFile(path)) {
                ++cnt;
            }
        }

        return MakeProcResult(cnt);
    }

    ProcResult StrSet(std::weak_ptr<CMDSession> weak, const Command& cmd) {
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
        auto [err, data] = db->StrSet(key, val, cmd.options);
        if (err) {
            return MakeProcResult(err);
        } else if (data.has_value()) {
            return MakeProcResult(*data);
        } else {
            return OKResp();
        }
    }

    ProcResult StrGet(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto& key = cmd.argv[0];
        auto* db = clt->CurrentDB();
        auto val = db->StrGet(key);
        if (!val.has_value() || val->empty()) {
            return MakeProcResult(error::RuntimeErrorCode::kKeyNotFound);
        }

        return MakeProcResult(*val);
    }

    ProcResult Del(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        int cnt = 0;
        auto* db = clt->CurrentDB();
        for (const auto& key: cmd.argv) {
            if (!db->Del(key))
                ++cnt;
        }
        return MakeProcResult(cnt);
    }

    ProcResult Watch(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto* db = clt->CurrentDB();
        db->AddWatchKeyWithClient(cmd.argv[0], weak);
        return OKResp();
    }

    ProcResult UnWatch(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        clt->DelWatchKey(cmd.argv[0]);
        return OKResp();
    }

    ProcResult PublishWithChannel(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto& dbm = DatabaseManager::GetInstance();
        auto cnt = dbm.PublishWithChannel(cmd.argv[0], cmd.argv[1]);

        return MakeProcResult(cnt);
    }

    ProcResult SubscribeWithChannel(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        std::vector<std::string> result;

        auto& dbm = DatabaseManager::GetInstance();
        for (std::size_t i = 0; i < cmd.argv.size(); ++i) {
            dbm.SubscribeWithChannel(cmd.argv[i], weak);
            result.emplace_back(cmd.name);
            result.emplace_back(cmd.argv[i]);
            result.emplace_back(std::to_string(i + 1));
        }

        return MakePubSubProcResult(result);
    }

    ProcResult UnSubscribeWithChannel(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto& dbm = DatabaseManager::GetInstance();
        for (const auto& channel: cmd.argv)
            dbm.UnSubscribeWithChannel(channel, weak);
        return OKResp();
    }

    ProcResult Merge(std::weak_ptr<CMDSession> weak, const Command&) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto& fm = DataLogFileManager::GetInstance();
        fm.Merge();
        return OKResp();
    }

    DatabaseManager::DatabaseManager()
        : mIsNonWrite_{false}, mMaxMemoryStrategy_{new LRUStrategy()} {
        for (std::uint8_t i = 0; i < Flags::GetInstance().dbMaxNum; ++i) {
            Database db{i, mMaxMemoryStrategy_};
            mDBList_.push_back(std::move(db));
        }
    }

    DatabaseManager::~DatabaseManager() { delete mMaxMemoryStrategy_; }

    DatabaseManager& DatabaseManager::GetInstance() {
        static DatabaseManager inst;
        return inst;
    }

    void DatabaseManager::Init() {}

    bool DatabaseManager::LoadRecordsFromLogFile(const std::string& path) {
        if (!std::filesystem::exists(path)) {
            return false;
        }
        std::fstream file{path, std::ios_base::in | std::ios::binary};
        if (!file.is_open()) {
            ServerLog::Error("data log file open failed: {}", ::strerror(errno));
            return false;
        }

        while (!file.eof()) {
            FileRecord record;
            if (!FileRecord::LoadFromDisk(record, file, file.tellg())) continue;

            Database* db = GetDBByIndex(record.header.dbIdx);
            auto& key = record.data.key;
            auto& val = record.data.value;

            if (!val.empty())
                db->StrSet(key, val);
            else {
                if (auto ec = db->Del(key); ec) {
                    ServerLog::Warnning("load history key [] failed: []", key, ec.message());
                }
            }
        }
        return true;
    }

    bool DatabaseManager::HaveMemoryAvailable() const {
        return std::any_of(mDBList_.begin(), mDBList_.end(),
                           [](const Database& db) -> bool { return db.HaveMemoryAvailable(); });
    }

    void DatabaseManager::ScanDBForReleaseMemory() {
        for (auto& db: mDBList_) {
            if (db.HaveMemoryAvailable()) {
                db.ReleaseMemory();
                CancelNonWrite();
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

    void DatabaseManager::SubscribeWithChannel(const std::string& channel,
                                               std::weak_ptr<CMDSession> weak) {
        mPubSubChannel_.Subscribe(channel, weak);
    }

    void DatabaseManager::UnSubscribeWithChannel(const std::string& channel,
                                                 std::weak_ptr<CMDSession> weak) {
        mPubSubChannel_.UnSubscribe(channel, weak);
    }

    std::int32_t DatabaseManager::PublishWithChannel(const std::string& channel,
                                                     const std::string& msg) {
        return mPubSubChannel_.Publish(channel, msg);
    }

    Database::Database(std::uint8_t dbIdx, MaxMemoryStrategy* maxMemoryStrategy)
        : mEngine_{dbIdx, maxMemoryStrategy} {}

    void Database::ReleaseMemory() {
        mEngine_.ReleaseMemory();
    }

    bool Database::HaveMemoryAvailable() const {
        return mEngine_.HaveMemoryAvailable();
    }

    void Database::Foreach(StorageEngine::ForeachCallback&& callback) {
        mEngine_.Foreach(std::move(callback));
    }

    void Database::InsertTxFlag(TxRuntimeState txFlag, std::size_t txCmdNum) {
        mEngine_.InsertTxFlag(txFlag, txCmdNum);
    }

    void Database::NotifyWatchedClientSession(const std::string& key) {
        if (mWatchedMap_.contains(key)) {
            for (const auto& weak: mWatchedMap_.at(key)) {
                if (auto clt = weak.lock(); clt) {
                    clt->SetCurrentTxToFail();
                }
            }
        }
    }
    void Database::StrSetForHistoryData(DataLogFileObjPtr file, std::streampos pos,
                                        const FileRecord& record) {
        StorageEngine::PutOption opt{
                .file = file,
                .pos = pos,
                .microSecondTimestamp = record.header.timestamp};
        auto ec = mEngine_.InnerPut(opt, record.data.key, "");
        if (ec) {
            ServerLog::Warnning("load history data failed: []", ec.message());
        }
    }

    std::tuple<std::error_code, std::optional<std::string>> Database::StrSet(
            const std::string& key, const std::string& val,
            const std::vector<CommandOption>& opts) {
        std::error_code ec;
        auto obj = mEngine_.Put(ec, key, val);
        if (ec) {
            return std::make_tuple(ec, std::nullopt);
        }

        std::optional<std::string> data = std::nullopt;
        for (const auto& opt: opts) {
            auto [err, payload] = StrSetWithOption(key, *(obj.lock()), opt);
            if (err) {
                return std::make_tuple(err, std::nullopt);
            }
            if (payload.has_value()) {
                data = payload;
            }
        }
        NotifyWatchedClientSession(key);
        return std::make_tuple(error::ProtocolErrorCode::kSuccess, data);
    }

    void Database::StrSetForMerge(DataLogFileObjPtr mergeFile,
                                  const std::string& key, const std::string& val) {
        StorageEngine::PutOption opt{.file = mergeFile};
        auto ec = mEngine_.InnerPut(opt, key, val);
        if (ec) {
            ServerLog::Error("merge file insert key [] failed: []", key, ec.message());
        }
    }

    std::tuple<std::error_code, std::optional<std::string>>
    Database::StrSetWithOption(const std::string& key, RecordObject& obj,
                               const CommandOption& opt) {
        std::error_code err = error::RuntimeErrorCode::kSuccess;
        std::optional<std::string> data = std::nullopt;
        switch (opt.type) {
            case CmdOptionType::kEX: {
                auto sec = utils::ToInteger<std::int64_t>(opt.argv.front());
                if (!sec.has_value()) {
                    err = error::ProtocolErrorCode::kSyntax;
                } else {
                    obj.SetExpiration(std::chrono::seconds{*sec});
                }
            } break;
            case CmdOptionType::kPX: {
                auto ms = utils::ToInteger<std::int64_t>(opt.argv.front());
                if (!ms.has_value()) {
                    err = error::ProtocolErrorCode::kSyntax;
                } else {
                    obj.SetExpiration(std::chrono::milliseconds{*ms});
                }
            } break;
            case CmdOptionType::kNX:
                if (mEngine_.Contains(key)) {
                    err = error::RuntimeErrorCode::kKeyAlreadyExist;
                }
                break;
            case CmdOptionType::kXX:
                if (!mEngine_.Contains(key)) {
                    err = error::RuntimeErrorCode::kKeyNotFound;
                }
                break;
            case CmdOptionType::kKEEPTTL:
                // 保留设置前指定键的生存时间
                if (mEngine_.Contains(key)) {
                    auto oldObj = mEngine_.Get(key);
                    obj.SetExpiration(oldObj.lock()->GetExpiration());
                }
                break;
            case CmdOptionType::kGET:
                // 返回 key 存储的值，如果 key 不存在返回空
                if (mEngine_.Contains(key)) {
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

    std::optional<std::string> Database::StrGet(const std::string& key) {
        std::error_code ec;
        auto val = mEngine_.Get(ec, key);
        if (ec) {
            return {};
        }
        return val;
    }

    std::error_code Database::Del(const std::string& key) {
        NotifyWatchedClientSession(key);
        return mEngine_.Del(key);
    }

    std::weak_ptr<RecordObject> Database::Get(const std::string& key) {
        return mEngine_.Get(key);
    }

    void Database::AddWatchKeyWithClient(const std::string& key, std::weak_ptr<CMDSession> clt) {
        if (!mEngine_.Contains(key))
            return;

        auto cltPtr = clt.lock();
        if (!cltPtr)
            return;
        cltPtr->AddWatchKey(key);
        mWatchedMap_[key].emplace_back(clt);
    }

    void Database::DelWatchKeyAndClient(const std::string& key) {
        if (!mEngine_.Contains(key))
            return;
        mWatchedMap_.erase(key);
    }
}// namespace foxbatdb