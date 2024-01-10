#include "handler.h"
#include "db.h"
#include "errors/protocol.h"
#include "errors/runtime.h"
#include "frontend/cmdmap.h"
#include "frontend/server.h"
#include "utils/utils.h"

namespace foxbatdb {
    ProcResult detail::MakeProcResult(std::error_code err) {
        return ProcResult{.hasError = true, .data = utils::BuildErrorResponse(err)};
    }

    ProcResult detail::MakePubSubProcResult(const std::vector<std::string>& data) {
        return ProcResult{.hasError = false, .data = utils::BuildPubSubResponse(data)};
    }

    static ProcResult OKResp() {
        return ProcResult{.hasError = false, .data = utils::OK_RESPONSE};
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
            return detail::MakeProcResult(error::ProtocolErrorCode::kSyntax);
        }

        if (*idx >= DatabaseManager::GetInstance().GetDBListSize()) {
            return detail::MakeProcResult(error::RuntimeErrorCode::kDBIdxOutOfRange);
        }
        if (auto clt = weak.lock(); clt) {
            clt->SwitchToTargetDB(*idx);
            return OKResp();
        } else {
            return detail::MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }
    }

    ProcResult Load(std::weak_ptr<CMDSession>, const Command& cmd) {
        if (DatabaseManager::GetInstance().IsInReadonlyMode()) {
            return detail::MakeProcResult(error::RuntimeErrorCode::kMemoryOut);
        }

        int cnt = 0;
        auto& dbm = DatabaseManager::GetInstance();
        for (const auto& path: cmd.argv) {
            if (dbm.LoadRecordsFromLogFile(path)) {
                ++cnt;
            }
        }

        return detail::MakeProcResult(cnt);
    }

    ProcResult StrSet(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        if (DatabaseManager::GetInstance().IsInReadonlyMode()) {
            return detail::MakeProcResult(error::RuntimeErrorCode::kMemoryOut);
        }

        auto& key = cmd.argv[0];
        auto& val = cmd.argv[1];

        auto clt = weak.lock();
        if (!clt) {
            return detail::MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto* db = clt->CurrentDB();
        auto [err, data] = db->StrSet(key, val, cmd.options);
        if (err) {
            return detail::MakeProcResult(err);
        } else if (data.has_value()) {
            return detail::MakeProcResult(*data);
        } else {
            return OKResp();
        }
    }

    ProcResult StrGet(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return detail::MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto& key = cmd.argv[0];
        auto* db = clt->CurrentDB();
        auto val = db->StrGet(key);
        if (!val.has_value() || val->empty()) {
            return detail::MakeProcResult(error::RuntimeErrorCode::kKeyNotFound);
        }

        return detail::MakeProcResult(*val);
    }

    ProcResult Del(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return detail::MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        int cnt = 0;
        auto* db = clt->CurrentDB();
        for (const auto& key: cmd.argv) {
            if (!db->Del(key))
                ++cnt;
        }
        return detail::MakeProcResult(cnt);
    }

    ProcResult Watch(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return detail::MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto* db = clt->CurrentDB();
        db->AddWatchKeyWithClient(cmd.argv[0], weak);
        return OKResp();
    }

    ProcResult UnWatch(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return detail::MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        clt->DelWatchKey(cmd.argv[0]);
        return OKResp();
    }

    ProcResult PublishWithChannel(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return detail::MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto& dbm = DatabaseManager::GetInstance();
        auto cnt = dbm.PublishWithChannel(cmd.argv[0], cmd.argv[1]);

        return detail::MakeProcResult(cnt);
    }

    ProcResult SubscribeWithChannel(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return detail::MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        std::vector<std::string> result;

        auto& dbm = DatabaseManager::GetInstance();
        for (std::size_t i = 0; i < cmd.argv.size(); ++i) {
            dbm.SubscribeWithChannel(cmd.argv[i], weak);
            result.emplace_back(cmd.name);
            result.emplace_back(cmd.argv[i]);
            result.emplace_back(std::to_string(i + 1));
        }

        return detail::MakePubSubProcResult(result);
    }

    ProcResult UnSubscribeWithChannel(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return detail::MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto& dbm = DatabaseManager::GetInstance();
        for (const auto& channel: cmd.argv)
            dbm.UnSubscribeWithChannel(channel, weak);
        return OKResp();
    }

    ProcResult Merge(std::weak_ptr<CMDSession> weak, const Command&) {
        auto clt = weak.lock();
        if (!clt) {
            return detail::MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto& fm = DataLogFileManager::GetInstance();
        fm.Merge();
        return OKResp();
    }

    ProcResult Prefix(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return detail::MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto* db = clt->CurrentDB();
        return detail::MakeProcResult(
                utils::BuildArrayResponseWithFilledItems(
                        db->PrefixSearch(cmd.argv[0])));
    }
}// namespace foxbatdb
