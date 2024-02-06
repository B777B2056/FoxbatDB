#pragma once
#include "core/handler.h"
#include <climits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace foxbatdb {
    enum class CmdOptionType : std::uint8_t {
        kEX = 1,
        kPX,
        kNX,
        kXX,
        kKEEPTTL,
        kGET
    };

    struct CommandOption {
        std::string name;
        CmdOptionType type;
        std::vector<std::string> argv;
    };

    struct Command;
    class CMDSession;
    using CmdProcFunc = ProcResult (*)(std::weak_ptr<CMDSession>, const Command&);

    struct Command {
        std::string name;
        CmdProcFunc call;
        std::vector<std::string> argv;
        std::vector<CommandOption> options;

        [[nodiscard]] std::error_code Validate() const;
    };

    namespace detail {
        constexpr static std::uint8_t MAX_COMMAND_PARAM_NUMBER = -1;
        constexpr static std::uint8_t MAX_COMMAND_NAME_LENGTH = 32;

        struct MainCommandWrapper {
            CmdProcFunc call;
            bool isWriteCmd;
            std::uint8_t minArgc;
            std::uint8_t maxArgc;
        };

        struct CommandOptionWrapper {
            CmdOptionType type;
            std::unordered_set<std::string> matchedMainCommand;
            std::vector<CmdOptionType> exclusiveOpts;
            std::uint8_t minArgc;
            std::uint8_t maxArgc;
        };
    }// namespace detail

    const std::unordered_map<std::string, detail::MainCommandWrapper>
            MainCommandMap{
                    {"select",detail::MainCommandWrapper{.call = &SwitchDB,.isWriteCmd = true,.minArgc = 1,.maxArgc = 1}},
                    {"hello",detail::MainCommandWrapper{.call = &Hello,.isWriteCmd = false,.minArgc = 1,.maxArgc = 1}},
                    {"merge",detail::MainCommandWrapper{.call = &Merge,.isWriteCmd = false,.minArgc = 0,.maxArgc = 0}},
                    {"move",detail::MainCommandWrapper{.call = &Move,.isWriteCmd = true,.minArgc = 2,.maxArgc = 2}},

                    {"multi",detail::MainCommandWrapper{.call = nullptr,.isWriteCmd = false,.minArgc = 0,.maxArgc = 0}},
                    {"discard",detail::MainCommandWrapper{.call = nullptr,.isWriteCmd = false,.minArgc = 0,.maxArgc = 0}},
                    {"exec",detail::MainCommandWrapper{.call = nullptr,.isWriteCmd = false,.minArgc = 0,.maxArgc = 0}},
                    {"watch",detail::MainCommandWrapper{.call = &Watch,.isWriteCmd = false,.minArgc = 1,.maxArgc = 1}},
                    {"unwatch",detail::MainCommandWrapper{.call = &UnWatch,.isWriteCmd = false,.minArgc = 1,.maxArgc = 1}},

                    {"publish",detail::MainCommandWrapper{.call = &PublishWithChannel,.isWriteCmd = false,.minArgc = 2,.maxArgc = 2}},
                    {"subscribe",detail::MainCommandWrapper{.call = &SubscribeWithChannel,.isWriteCmd = false,.minArgc = 1,.maxArgc = detail::MAX_COMMAND_PARAM_NUMBER}},
                    {"unsubscribe",detail::MainCommandWrapper{.call = &UnSubscribeWithChannel,.isWriteCmd = false,.minArgc = 1,.maxArgc = detail::MAX_COMMAND_PARAM_NUMBER}},

                    {"get",detail::MainCommandWrapper{.call = &StrGet,.isWriteCmd = false,.minArgc = 1,.maxArgc = 1}},
                    {"exists",detail::MainCommandWrapper{.call = &Exists,.isWriteCmd = false,.minArgc = 1,.maxArgc = 1}},
                    {"getrange",detail::MainCommandWrapper{.call = &StrGetRange,.isWriteCmd = false,.minArgc = 3,.maxArgc = 3}},
                    {"mget",detail::MainCommandWrapper{.call = &StrMultiGet,.isWriteCmd = false,.minArgc = 1,.maxArgc = detail::MAX_COMMAND_PARAM_NUMBER}},
                    {"strlen",detail::MainCommandWrapper{.call = &StrLength,.isWriteCmd = false,.minArgc = 1,.maxArgc = 1}},
                    {"prefix",detail::MainCommandWrapper{.call = &Prefix,.isWriteCmd = false,.minArgc = 1,.maxArgc = 1}},
                    {"ttl",detail::MainCommandWrapper{.call = &TTL,.isWriteCmd = false,.minArgc = 1,.maxArgc = 1}},
                    {"pttl",detail::MainCommandWrapper{.call = &PTTL,.isWriteCmd = false,.minArgc = 1,.maxArgc = 1}},

                    {"set",detail::MainCommandWrapper{.call = &StrSet,.isWriteCmd = true,.minArgc = 2,.maxArgc = 2}},
                    {"mset",detail::MainCommandWrapper{.call = &StrMultiSet,.isWriteCmd = true,.minArgc = 2,.maxArgc = detail::MAX_COMMAND_PARAM_NUMBER}},
                    {"append",detail::MainCommandWrapper{.call = &StrAppend,.isWriteCmd = true,.minArgc = 2,.maxArgc = 2}},

                    {"del",detail::MainCommandWrapper{.call = &Del,.isWriteCmd = true,.minArgc = 1,.maxArgc = detail::MAX_COMMAND_PARAM_NUMBER}},

                    {"rename",detail::MainCommandWrapper{.call = &Rename,.isWriteCmd = true,.minArgc = 2,.maxArgc = 2}},
                    {"incr",detail::MainCommandWrapper{.call = &Incr,.isWriteCmd = true,.minArgc = 1,.maxArgc = 1}},
                    {"decr",detail::MainCommandWrapper{.call = &Decr,.isWriteCmd = true,.minArgc = 1,.maxArgc = 1}},
                    {"incrby",detail::MainCommandWrapper{.call = &IncrBy,.isWriteCmd = true,.minArgc = 2,.maxArgc = 2}},
                    {"decrby",detail::MainCommandWrapper{.call = &DecrBy,.isWriteCmd = true,.minArgc = 2,.maxArgc = 2}},
                    {"incrbyfloat",detail::MainCommandWrapper{.call = &IncrByFloat,.isWriteCmd = true,.minArgc = 2,.maxArgc = 2}},
            };

    const std::unordered_map<std::string, detail::CommandOptionWrapper>
            CommandOptionMap{
                    {"ex",detail::CommandOptionWrapper{.type = CmdOptionType::kEX,.matchedMainCommand = {"set", "mset"},.exclusiveOpts = {CmdOptionType::kPX, CmdOptionType::kKEEPTTL},.minArgc = 1,.maxArgc = 1}},
                    {"px",detail::CommandOptionWrapper{.type = CmdOptionType::kPX,.matchedMainCommand = {"set", "mset"},.exclusiveOpts = {CmdOptionType::kEX, CmdOptionType::kKEEPTTL},.minArgc = 1,.maxArgc = 1}},
                    {"nx",detail::CommandOptionWrapper{.type = CmdOptionType::kNX,.matchedMainCommand = {"set", "mset"},.exclusiveOpts = {CmdOptionType::kXX},.minArgc = 0,.maxArgc = 0}},
                    {"xx",detail::CommandOptionWrapper{.type = CmdOptionType::kXX,.matchedMainCommand = {"set", "mset"},.exclusiveOpts = {CmdOptionType::kNX},.minArgc = 0,.maxArgc = 0}},
                    {"keepttl",detail::CommandOptionWrapper{.type = CmdOptionType::kKEEPTTL,.matchedMainCommand = {"set", "mset"},.exclusiveOpts = {CmdOptionType::kEX, CmdOptionType::kPX},.minArgc = 0,.maxArgc = 0}},
                    {"get",detail::CommandOptionWrapper{.type = CmdOptionType::kGET,.matchedMainCommand = {"set", "mset"},.minArgc = 0,.maxArgc = 0}},
            };
}// namespace foxbatdb