#pragma once
#include "utils/resp.h"
#include <memory>
#include <string>

namespace foxbatdb {
    class CMDSession;
    struct Command;

    struct ProcResult {
        bool hasError;
        std::string data;
    };

    namespace detail {
        ProcResult MakeProcResult(std::error_code err);
        ProcResult MakePubSubProcResult(const std::vector<std::string>& data);

        template<typename T>
            requires(!std::is_same_v<T, std::error_code>)
        ProcResult MakeProcResult(const T& data) {
            return ProcResult{.hasError = false, .data = utils::BuildResponse<T>(data)};
        }
    }// namespace detail

    ProcResult CommandDB(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult InfoDB(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult ServerDB(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult SwitchDB(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult Load(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult StrSet(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult StrGet(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult Del(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult Watch(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult UnWatch(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult PublishWithChannel(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult SubscribeWithChannel(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult UnSubscribeWithChannel(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult Merge(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult Prefix(std::weak_ptr<CMDSession> weak, const Command& cmd);
}// namespace foxbatdb
