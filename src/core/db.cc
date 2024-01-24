#include "db.h"
#include "errors/protocol.h"
#include "errors/runtime.h"
#include "flag/flags.h"
#include "frontend/server.h"
#include "log/serverlog.h"
#include "memory.h"
#include "utils/utils.h"
#include <algorithm>
#include <filesystem>

namespace foxbatdb {
    DatabaseManager::DatabaseManager()
        : mIsNonWrite_{false}, mMaxMemoryStrategy_{nullptr} {
        switch (Flags::GetInstance().maxMemoryPolicy) {
            case MaxMemoryPolicyEnum::eLRU:
                mMaxMemoryStrategy_ = new LRUStrategy();
                break;
            case MaxMemoryPolicyEnum::eNoeviction:
            default:
                mMaxMemoryStrategy_ = new NoevictionStrategy();
                break;
        }

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
            ServerLog::GetInstance().Error("data log file open failed: {}", ::strerror(errno));
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
                    ServerLog::GetInstance().Warning("load history key [] failed: []", key, ec.message());
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
        : mIndex_{dbIdx, maxMemoryStrategy} {}

    void Database::ReleaseMemory() {
        mIndex_.ReleaseMemory();
    }

    bool Database::HaveMemoryAvailable() const {
        return mIndex_.HaveMemoryAvailable();
    }

    void Database::Foreach(MemoryIndex::ForeachCallback&& callback) {
        mIndex_.Foreach(std::move(callback));
    }

    void Database::InsertTxFlag(TxRuntimeState txFlag, std::size_t txCmdNum) {
        mIndex_.InsertTxFlag(txFlag, txCmdNum);
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

    void Database::StrSetForHistoryData(DataLogFileWrapper* file, std::streampos pos,
                                        const FileRecord& record) {
        MemoryIndex::InnerPutOption opt{
                .logFilePtr = file,
                .pos = pos,
                .microSecondTimestamp = record.header.timestamp};
        auto ec = mIndex_.InnerPut(opt, record.data.key, "");
        if (ec) {
            ServerLog::GetInstance().Warning("load history data failed: []", ec.message());
        }
    }

    std::tuple<std::error_code, std::optional<std::string>> Database::StrSet(
            const std::string& key, const std::string& val,
            const std::vector<CommandOption>& opts) {
        std::error_code ec;
        auto obj = mIndex_.Put(ec, key, val);
        if (ec) {
            return std::make_tuple(ec, std::nullopt);
        }

        std::optional<std::string> data = std::nullopt;
        for (const auto& opt: opts) {
            auto [err, payload] = StrSetWithOption(key, *obj.lock(), opt);
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

    void Database::StrSetForMerge(DataLogFileWrapper* mergeFile,
                                  const std::string& key, const std::string& val) {
        auto ec = mIndex_.InnerPut(MemoryIndex::InnerPutOption{.logFilePtr = mergeFile}, key, val);
        if (ec) {
            ServerLog::GetInstance().Error("merge file insert key [] failed: []", key, ec.message());
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
                if (mIndex_.Contains(key)) {
                    err = error::RuntimeErrorCode::kKeyAlreadyExist;
                }
                break;
            case CmdOptionType::kXX:
                if (!mIndex_.Contains(key)) {
                    err = error::RuntimeErrorCode::kKeyNotFound;
                }
                break;
            case CmdOptionType::kKEEPTTL:
                // 保留设置前指定键的生存时间
                if (mIndex_.Contains(key)) {
                    auto oldObj = mIndex_.Get(key);
                    if (!oldObj.expired())
                        obj.SetExpiration(oldObj.lock()->GetExpiration());
                }
                break;
            case CmdOptionType::kGET:
                // 返回 key 存储的值，如果 key 不存在返回空
                if (mIndex_.Contains(key)) {
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
        auto val = mIndex_.Get(ec, key);
        if (ec) {
            return {};
        }
        return val;
    }

    std::error_code Database::Del(const std::string& key) {
        NotifyWatchedClientSession(key);
        return mIndex_.Del(key);
    }

    std::weak_ptr<RecordObject> Database::Get(const std::string& key) {
        return mIndex_.Get(key);
    }

    void Database::AddWatchKeyWithClient(const std::string& key, std::weak_ptr<CMDSession> clt) {
        if (!mIndex_.Contains(key))
            return;

        auto cltPtr = clt.lock();
        if (!cltPtr)
            return;
        cltPtr->AddWatchKey(key);
        mWatchedMap_[key].emplace_back(clt);
    }

    void Database::DelWatchKeyAndClient(const std::string& key) {
        if (!mIndex_.Contains(key))
            return;
        mWatchedMap_.erase(key);
    }

    std::vector<std::string> Database::PrefixSearch(const std::string& prefix) const {
        return mIndex_.PrefixSearch(prefix);
    }
}// namespace foxbatdb