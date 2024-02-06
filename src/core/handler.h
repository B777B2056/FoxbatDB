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

    /* DB */
    ProcResult SwitchDB(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult Hello(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult Merge(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult Move(std::weak_ptr<CMDSession> weak, const Command& cmd);

    /* 事务 */
    ProcResult Watch(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult UnWatch(std::weak_ptr<CMDSession> weak, const Command& cmd);

    /* Pub-Sub */
    ProcResult PublishWithChannel(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult SubscribeWithChannel(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult UnSubscribeWithChannel(std::weak_ptr<CMDSession> weak, const Command& cmd);

    /* Key-Value */
    // 查询
    ProcResult StrGet(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult Exists(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult StrGetRange(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult StrMultiGet(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult StrLength(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult Prefix(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult TTL(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult PTTL(std::weak_ptr<CMDSession> weak, const Command& cmd);

    // 写入
    ProcResult StrSet(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult StrMultiSet(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult StrAppend(std::weak_ptr<CMDSession> weak, const Command& cmd);

    // 删除
    ProcResult Del(std::weak_ptr<CMDSession> weak, const Command& cmd);

    // 更新
    ProcResult Rename(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult Incr(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult Decr(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult IncrBy(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult DecrBy(std::weak_ptr<CMDSession> weak, const Command& cmd);
    ProcResult IncrByFloat(std::weak_ptr<CMDSession> weak, const Command& cmd);
}// namespace foxbatdb
