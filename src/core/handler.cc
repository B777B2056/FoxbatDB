#include "handler.h"
#include "db.h"
#include "errors/protocol.h"
#include "errors/runtime.h"
#include "frontend/cmdmap.h"
#include "frontend/server.h"
#include "utils/resp.h"
#include "utils/utils.h"

namespace foxbatdb {
    namespace {
        template<typename T>
        ProcResult MakeProcResult(const T& data) {
            return ProcResult{.hasError = false, .data = utils::BuildResponse<T>(data)};
        }

        ProcResult NullResp() {
            return ProcResult{.hasError = false, .data = {utils::NULL_RESPONSE}};
        }

        ProcResult NilResp() {
            return ProcResult{.hasError = false, .data = {utils::NIL_RESPONSE}};
        }

        ProcResult OKResp() {
            return ProcResult{.hasError = false, .data = {utils::OK_RESPONSE}};
        }

    }// namespace

    ProcResult SwitchDB(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto idx = utils::ToNumber<std::size_t>(cmd.argv.back());
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

    ProcResult Hello(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        if (weak.expired()) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto protocol = utils::ToNumber<std::uint8_t>(cmd.argv[0]);
        if (!protocol.has_value() || (*protocol != 3)) {
            return MakeProcResult(error::ProtocolErrorCode::kNoProto);
        }

        return ProcResult{.hasError = false, .data = utils::HELLO_RESPONSE};
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

    ProcResult Move(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto dbIdx = utils::ToNumber<std::size_t>(cmd.argv[1]);
        if (!dbIdx.has_value())
            return MakeProcResult(error::RuntimeErrorCode::kInvalidValueType);

        auto* db = clt->CurrentDB();
        const auto& key = cmd.argv[0];
        auto val = db->StrGet(key);
        if (val.has_value()) {
            auto& dbm = DatabaseManager::GetInstance();
            if (*dbIdx < dbm.GetDBListSize()) {
                dbm.GetDBByIndex(*dbIdx)->StrSet(key, *val);
                db->Del(key);
                return MakeProcResult(1);
            }
        }
        return ProcResult{.hasError = true, .data = utils::BuildResponse(0)};
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

        std::string result;
        auto& dbm = DatabaseManager::GetInstance();
        for (std::size_t i = 0; i < cmd.argv.size(); ++i) {
            dbm.SubscribeWithChannel(cmd.argv[i], weak);
            result += utils::BuildPubSubResponse(cmd.name, cmd.argv[i], i + 1);
        }

        return ProcResult{.hasError = false, .data = result};
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

    ProcResult StrGet(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto& key = cmd.argv[0];
        auto* db = clt->CurrentDB();
        auto val = db->StrGet(key);
        if (!val.has_value() || val->empty()) {
            return NilResp();
        }

        return MakeProcResult(*val);
    }

    ProcResult Exists(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto& key = cmd.argv[0];
        auto* db = clt->CurrentDB();
        auto val = db->StrGet(key);
        if (!val.has_value() || val->empty()) {
            return MakeProcResult(0);
        }

        return MakeProcResult(1);
    }

    ProcResult StrGetRange(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto& key = cmd.argv[0];
        auto start = utils::ToNumber<std::int64_t>(cmd.argv[1]);
        auto end = utils::ToNumber<std::int64_t>(cmd.argv[2]);
        if (!start.has_value() || !end.has_value()) {
            return MakeProcResult(error::ProtocolErrorCode::kSyntax);
        }

        auto* db = clt->CurrentDB();
        return MakeProcResult(db->StrGetRange(key, *start, *end));
    }

    ProcResult StrMultiGet(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        std::vector<std::string> ret;
        for (const auto& key: cmd.argv) {
            auto val = clt->CurrentDB()->StrGet(key);
            if (!val.has_value() || val->empty()) {
                ret.emplace_back(utils::NIL_RESPONSE);
            } else {
                ret.emplace_back(utils::BuildResponse(*val));
            }
        }
        return MakeProcResult(ret);
    }

    ProcResult StrLength(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        std::size_t len = 0;
        auto val = clt->CurrentDB()->StrGet(cmd.argv[0]);
        if (val.has_value()) {
            len = val->size();
        }
        return MakeProcResult(len);
    }

    ProcResult Prefix(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto* db = clt->CurrentDB();
        auto kvList = db->PrefixSearch(cmd.argv[0]);

        std::vector<std::string> ret;
        ret.reserve(kvList.size() * 2);

        std::for_each(kvList.begin(), kvList.end(),
                      [&ret](const std::pair<std::string, std::string>& item) {
                          ret.emplace_back(utils::BuildResponse(item.first));
                          ret.emplace_back(utils::BuildResponse(item.second));
                      });
        return MakeProcResult(ret);
    }

    std::pair<std::int8_t, std::chrono::milliseconds> GetMillSecondTTL(std::weak_ptr<CMDSession> weak,
                                                                       const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return {-3, {}};
        }

        auto& key = cmd.argv[0];
        auto* db = clt->CurrentDB();
        auto ptr = db->Get(key);
        if (ptr.expired())
            return {-2, {}};

        auto ttlMs = ptr.lock()->GetExpiration();
        if (std::chrono::milliseconds{ULLONG_MAX} == ttlMs) {
            return {-1, {}};
        }
        return {0, ttlMs};
    }

    ProcResult TTL(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto [flag, ttlMs] = GetMillSecondTTL(weak, cmd);
        if (-3 == flag) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        if (0 != flag)
            return MakeProcResult(flag);
        return MakeProcResult(std::chrono::duration_cast<std::chrono::seconds>(ttlMs).count());
    }

    ProcResult PTTL(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto [flag, ttlMs] = GetMillSecondTTL(weak, cmd);
        if (-3 == flag) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        if (0 != flag)
            return MakeProcResult(flag);
        return MakeProcResult(ttlMs.count());
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
            if (error::RuntimeErrorCode::kIntervalError == err)
                return MakeProcResult(err);
            else
                return NullResp();
        } else if (data.has_value()) {
            return MakeProcResult(*data);
        } else {
            return OKResp();
        }
    }

    ProcResult StrMultiSet(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        if (DatabaseManager::GetInstance().IsInReadonlyMode()) {
            return MakeProcResult(error::RuntimeErrorCode::kMemoryOut);
        }

        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        if (cmd.argv.size() % 2 != 0) {
            return MakeProcResult(error::ProtocolErrorCode::kArgNumbers);
        }

        for (std::size_t i = 0; i < cmd.argv.size(); i += 2) {
            const auto& key = cmd.argv.at(i);
            const auto& val = cmd.argv.at(i + 1);
            clt->CurrentDB()->StrSet(key, val, cmd.options);
        }

        return OKResp();
    }

    ProcResult StrAppend(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        if (DatabaseManager::GetInstance().IsInReadonlyMode()) {
            return MakeProcResult(error::RuntimeErrorCode::kMemoryOut);
        }

        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto* db = clt->CurrentDB();
        const auto& key = cmd.argv[0];
        auto val = db->StrGet(key);
        db->StrSet(key, val.has_value() ? (*val + cmd.argv[1]) : cmd.argv[1]);

        return OKResp();
    }

    ProcResult Del(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        ProcResult ret;
        int cnt = 0;
        auto* db = clt->CurrentDB();
        for (const auto& key: cmd.argv) {
            auto ec = db->Del(key);
            if (ec)
                ret.hasError = true;
            else
                ++cnt;
        }

        ret.data = utils::BuildResponse(cnt);
        return ret;
    }

    ProcResult Rename(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        const auto& oldKey = cmd.argv[0];
        const auto& newKey = cmd.argv[1];

        auto* db = clt->CurrentDB();
        auto val = db->StrGet(oldKey);
        if (!val.has_value())
            return MakeProcResult(error::RuntimeErrorCode::kKeyNotFound);

        auto [ec, _] = db->StrSet(newKey, *val);
        if (ec) return MakeProcResult(ec);

        if (oldKey != newKey) {
            if (auto ec = db->Del(oldKey); ec) {
                return MakeProcResult(ec);
            }
        }

        return OKResp();
    }

    template<typename T>
        requires utils::Number<T>
    std::tuple<std::error_code, T> NumberOperateHelper(Database* db, const std::string& key, const std::string& offsetStr) {
        auto offset = utils::ToNumber<T>(offsetStr);
        if (!offset.has_value()) {
            return {error::RuntimeErrorCode::kInvalidValueType, {}};
        }

        T ret;
        if (auto valObj = db->Get(key); valObj.expired()) {
            auto [ec, _] = db->StrSet(key, offsetStr);
            if (ec) return {ec, {}};
            ret = *offset;
        } else {
            auto num = utils::ToNumber<T>(valObj.lock()->GetValue());
            if (!num.has_value()) {
                return {error::RuntimeErrorCode::kIntervalError, {}};
            }
            auto [ec, _] = db->StrSet(key, std::to_string(*num + *offset));
            if (ec) return {ec, {}};
            ret = *num + *offset;
        }

        return {error::RuntimeErrorCode::kSuccess, ret};
    }

    ProcResult Incr(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        const auto& key = cmd.argv[0];
        auto* db = clt->CurrentDB();
        auto [ec, _] = NumberOperateHelper<std::int64_t>(db, key, "1");
        if (ec)
            return MakeProcResult(ec);
        return OKResp();
    }

    ProcResult Decr(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        const auto& key = cmd.argv[0];
        auto* db = clt->CurrentDB();
        auto [ec, _] = NumberOperateHelper<std::int64_t>(db, key, "-1");
        if (ec)
            return MakeProcResult(ec);
        return OKResp();
    }

    ProcResult IncrBy(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        const auto& key = cmd.argv[0];
        auto* db = clt->CurrentDB();
        auto [ec, _] = NumberOperateHelper<std::int64_t>(db, key, cmd.argv[1]);
        if (ec)
            return MakeProcResult(ec);
        return OKResp();
    }

    ProcResult DecrBy(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        const auto& key = cmd.argv[0];
        auto* db = clt->CurrentDB();

        auto offset = cmd.argv[1];
        if ('-' != offset.front())
            offset = std::string{"-"} + offset;

        auto [ec, _] = NumberOperateHelper<std::int64_t>(db, key, offset);
        if (ec)
            return MakeProcResult(ec);
        return OKResp();
    }

    ProcResult IncrByFloat(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        const auto& key = cmd.argv[0];
        auto* db = clt->CurrentDB();
        auto [ec, ret] = NumberOperateHelper<double>(db, key, cmd.argv[1]);
        if (ec)
            return MakeProcResult(ec);
        return MakeProcResult(ret);
    }
}// namespace foxbatdb
