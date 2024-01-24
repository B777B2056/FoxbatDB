#pragma once
#include <memory>
#include <string>

namespace foxbatdb {
    class CMDSession;
    struct Command;

    struct ProcResult {
        bool hasError;
        std::string data;
    };

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
