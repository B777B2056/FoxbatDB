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
        ProcResult MakeProcResult(std::error_code err) {
            return ProcResult{.hasError = true, .data = {utils::BuildErrorResponse(err)}};
        }

        template<typename T>
            requires(!std::is_same_v<T, std::error_code>)
        ProcResult MakeProcResult(const T& data) {
            return ProcResult{.hasError = false, .data = utils::BuildResponse<T>(data)};
        }

        ProcResult MakeNullProcResult() {
            return ProcResult{.hasError = false, .data = {utils::NULL_RESPONSE}};
        }

        ProcResult MakeNilProcResult() {
            return ProcResult{.hasError = false, .data = {utils::NIL_RESPONSE}};
        }

        ProcResult OKResp() {
            return ProcResult{.hasError = false, .data = {utils::OK_RESPONSE}};
        }

    }// namespace

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

    ProcResult Hello(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        if (weak.expired()) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto protocol = utils::ToInteger<std::uint8_t>(cmd.argv[0]);
        if (!protocol.has_value() || (*protocol != 3)) {
            return MakeProcResult(error::ProtocolErrorCode::kNoProto);
        }

        return ProcResult{.hasError = false, .data = utils::HELLO_RESPONSE};
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
                return MakeNullProcResult();
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
            return MakeNilProcResult();
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

    ProcResult Merge(std::weak_ptr<CMDSession> weak, const Command&) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto& fm = DataLogFileManager::GetInstance();
        fm.Merge();
        return OKResp();
    }

    ProcResult Prefix(std::weak_ptr<CMDSession> weak, const Command& cmd) {
        auto clt = weak.lock();
        if (!clt) {
            return MakeProcResult(error::RuntimeErrorCode::kIntervalError);
        }

        auto* db = clt->CurrentDB();
        auto kvList = db->PrefixSearch(cmd.argv[0]);

        std::vector<std::string> ret{kvList.size() * 2};
        std::for_each(kvList.begin(), kvList.end(),
                      [&ret](const std::pair<std::string, std::string>& item) {
                          ret.emplace_back(item.first);
                          ret.emplace_back(item.second);
                      });
        return MakeProcResult(utils::BuildArrayResponseWithFilledItems(ret));
    }
}// namespace foxbatdb
