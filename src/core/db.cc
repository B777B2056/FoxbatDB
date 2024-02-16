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

    void DatabaseManager::SetNonWrite() { mIsNonWrite_ = true; }
    void DatabaseManager::CancelNonWrite() { mIsNonWrite_ = false; }
    bool DatabaseManager::IsInReadonlyMode() const { return mIsNonWrite_; }
    std::size_t DatabaseManager::GetDBListSize() const { return mDBList_.size(); }
    Database* DatabaseManager::GetDBByIndex(std::size_t idx) { return &mDBList_[idx]; }

    void DatabaseManager::SubscribeWithChannel(const std::string& channel, std::weak_ptr<CMDSession> weak) {
        mPubSubChannel_.Subscribe(channel, weak);
    }

    void DatabaseManager::UnSubscribeWithChannel(const std::string& channel, std::weak_ptr<CMDSession> weak) {
        mPubSubChannel_.UnSubscribe(channel, weak);
    }

    std::int32_t DatabaseManager::PublishWithChannel(const std::string& channel, const std::string& msg) {
        return mPubSubChannel_.Publish(channel, msg);
    }

    void DatabaseManager::Merge(DataLogFile* targetFile, const DataLogFile* writableFile) {
        // 遍历DB中所有活跃的key和对应的内存记录
        for (auto& db: mDBList_) {
            db.Merge(targetFile, writableFile);
        }
    }

    Database::Database(std::uint8_t dbIdx, MaxMemoryStrategy* maxMemoryStrategy)
        : mDBIdx_{dbIdx},
          mIndex_{dbIdx},
          mMaxMemoryStrategy_{maxMemoryStrategy} { assert(nullptr != mMaxMemoryStrategy_); }

    void Database::ReleaseMemory() {
        mMaxMemoryStrategy_->ReleaseKey(&mIndex_);
    }

    bool Database::HaveMemoryAvailable() const {
        return mMaxMemoryStrategy_->HaveMemoryAvailable();
    }

    std::shared_ptr<RecordObject> Database::GetRecordSnapshot(const std::string& key) {
        auto srcValObj = this->Get(key);
        if (srcValObj.expired()) return nullptr;

        auto valObj = RecordObjectPool::GetInstance().Acquire(srcValObj.lock()->GetMeta());
        if (!valObj) [[unlikely]] {
            ServerLog::GetInstance().Error("memory allocate failed");
            return nullptr;
        }

        return valObj;
    }

    void Database::RecoverRecordWithSnapshot(const std::string& key, std::shared_ptr<RecordObject> snapshot) {
        if (!snapshot) return;

        snapshot->DumpToDisk(key, snapshot->GetValue());
        mIndex_.Put(key, snapshot);
    }

    void Database::InsertTxFlag(RecordState txFlag, std::size_t txCmdNum) {
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

    void Database::LoadHistoryData(DataLogFile* file, std::streampos pos,
                                   const DataLogFile::Data& record) {
        MemoryIndex::HistoryDataInfo opt{
                .logFilePtr = file,
                .pos = pos,
                .microSecondTimestamp = record.timestamp};
        auto ec = mIndex_.PutHistoryData(record.key, opt);
        if (ec) {
            ServerLog::GetInstance().Warning("load history data failed: []", ec.message());
        }
    }

    std::tuple<std::error_code, std::optional<std::string>> Database::StrSet(
            const std::string& key, const std::string& val,
            const std::vector<CommandOption>& opts) {
        std::error_code ec;
        if ((key.size() > Flags::GetInstance().keyMaxBytes) ||
            (val.size() > Flags::GetInstance().valMaxBytes)) {
            ec = error::RuntimeErrorCode::kKeyValTooLong;
            return std::make_tuple(ec, std::nullopt);
        }

        RecordObjectMeta meta{.dbIdx = mDBIdx_};
        auto valObj = RecordObjectPool::GetInstance().Acquire(meta);
        if (!valObj) [[unlikely]] {
            ServerLog::GetInstance().Error("memory allocate failed");
            ec = error::RuntimeErrorCode::kMemoryOut;
            return std::make_tuple(ec, std::nullopt);
        }

        std::optional<std::string> data = std::nullopt;
        for (const auto& opt: opts) {
            auto [err, payload] = StrSetWithOption(key, *valObj, opt);
            if (err) {
                return std::make_tuple(err, std::nullopt);
            }
            if (payload.has_value()) {
                data = payload;
            }
        }

        valObj->DumpToDisk(key, val);
        mIndex_.Put(key, valObj);

        mMaxMemoryStrategy_->UpdateStateForWriteOp(key);
        NotifyWatchedClientSession(key);
        return std::make_tuple(error::ProtocolErrorCode::kSuccess, data);
    }

    std::tuple<std::error_code, std::optional<std::string>>
    Database::StrSetWithOption(const std::string& key, RecordObject& obj,
                               const CommandOption& opt) {
        std::error_code err = error::RuntimeErrorCode::kSuccess;
        std::optional<std::string> data = std::nullopt;
        switch (opt.type) {
            case CmdOptionType::kEX: {
                auto sec = utils::ToNumber<std::int64_t>(opt.argv.front());
                if (!sec.has_value()) {
                    err = error::ProtocolErrorCode::kSyntax;
                } else {
                    obj.SetExpiration(std::chrono::seconds{*sec});
                }
            } break;
            case CmdOptionType::kPX: {
                auto ms = utils::ToNumber<std::int64_t>(opt.argv.front());
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
                    err = error::RuntimeErrorCode::kKeyNotFound;
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

        mMaxMemoryStrategy_->UpdateStateForReadOp(key);
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

    std::vector<std::pair<std::string, std::string>> Database::PrefixSearch(const std::string& prefix) const {
        auto list = mIndex_.PrefixSearch(prefix);
        for (const auto& [key, _]: list)
            mMaxMemoryStrategy_->UpdateStateForReadOp(key);
        return list;
    }

    void Database::Merge(DataLogFile* targetFile, const DataLogFile* writableFile) {
        mIndex_.Merge(targetFile, writableFile);
    }

    std::string Database::StrGetRange(const std::string& key, std::int64_t start, std::int64_t end) {
        auto ptr = this->Get(key);
        if (ptr.expired()) return "";

        std::size_t startPos, endPos;
        auto val = ptr.lock()->GetValue();
        if (start < 0)
            startPos = val.size() - static_cast<std::size_t>(std::abs(start));
        else
            startPos = static_cast<std::size_t>(std::abs(start));

        if (end < 0) {
            endPos = 1 + val.size() - static_cast<std::size_t>(std::abs(end));
        } else {
            endPos = 1 + static_cast<std::size_t>(std::abs(end));
        }

        if (endPos > val.size())
            endPos = val.size();

        if ((startPos > endPos) || (startPos >= val.size()))
            return "";

        return val.substr(startPos, endPos - startPos);
    }
}// namespace foxbatdb