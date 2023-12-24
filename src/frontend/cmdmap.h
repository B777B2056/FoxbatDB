#pragma once
#include <climits>
#include <unordered_set>
#include <unordered_map>
#include "core/db.h"

namespace foxbatdb {
  enum class CmdOptionType : std::uint8_t { 
    kEX = 1, 
    kPX, 
    kNX, 
    kXX, 
    kKEEPTTL, 
    kGET, 
    kTODO 
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
  };

  namespace detail {
    struct MainCommandWrapper {
      CmdProcFunc call;
      bool isWriteCmd;
      std::uint8_t minArgc : 4;
      std::uint8_t maxArgc : 4;
    };

    struct CommandOptionWrapper {
      CmdOptionType type;
      std::unordered_set<std::string> matchedMainCommand;
      std::vector<CmdOptionType> exclusiveOpts;
      std::uint8_t minArgc : 4;
      std::uint8_t maxArgc : 4;
    };
  }

  const std::unordered_map<std::string, detail::MainCommandWrapper>
      MainCommandMap
  {
    {"select",
      detail::MainCommandWrapper{
          .call = &SwitchDB,
          .isWriteCmd = true,
          .minArgc = 1,
          .maxArgc = 1,
      }},

    {"set",
      detail::MainCommandWrapper{
          .call = &StrSet,
          .isWriteCmd = true,
          .minArgc = 2,
          .maxArgc = 2,
      }},

    {"get",
      detail::MainCommandWrapper{
          .call = &StrGet,
          .isWriteCmd = false,
          .minArgc = 1,
          .maxArgc = 1,
      }},

    {"del",
      detail::MainCommandWrapper{
          .call = &Del,
          .isWriteCmd = true,
          .minArgc = 1,
          .maxArgc = 15,
      }},

    {"multi",
       detail::MainCommandWrapper{
         .call = nullptr,
         .isWriteCmd = false,
         .minArgc = 0,
         .maxArgc = 0,
       }},

    {"discard",
       detail::MainCommandWrapper{
         .call = nullptr,
         .isWriteCmd = false,
         .minArgc = 0,
         .maxArgc = 0,
       }},

    {"exec",
       detail::MainCommandWrapper{
         .call = nullptr,
         .isWriteCmd = false,
         .minArgc = 0,
         .maxArgc = 0,
       }},

    {"watch",
       detail::MainCommandWrapper{
         .call = &Watch,
         .isWriteCmd = false,
         .minArgc = 1,
         .maxArgc = 1,
       }},

    {"unwatch",
       detail::MainCommandWrapper{
         .call = &UnWatch,
         .isWriteCmd = false,
         .minArgc = 1,
         .maxArgc = 1,
       }},

    {"publish",
       detail::MainCommandWrapper{
         .call = &PublishWithChannel,
         .isWriteCmd = false,
         .minArgc = 2,
         .maxArgc = 2,
       }},

    {"subscribe",
       detail::MainCommandWrapper{
         .call = &SubscribeWithChannel,
         .isWriteCmd = false,
         .minArgc = 1,
         .maxArgc = 15,
       }},

    {"unsubscribe",
       detail::MainCommandWrapper{
         .call = &UnSubscribeWithChannel,
         .isWriteCmd = false,
         .minArgc = 1,
         .maxArgc = 15,
       }},

    {"load",
        detail::MainCommandWrapper{
            .call = &Load,
            .isWriteCmd = false,
            .minArgc = 1,
            .maxArgc = 15,
        }},

    {"merge",
        detail::MainCommandWrapper{
            .call = &Merge,
            .isWriteCmd = false,
            .minArgc = 0,
            .maxArgc = 0,
        }},

    {"command",
      detail::MainCommandWrapper{
          .call = &CommandDB,
          .isWriteCmd = false,
          .minArgc = 0,
          .maxArgc = 0,
      }},

    {"info",
      detail::MainCommandWrapper{
          .call = &InfoDB,
          .isWriteCmd = false,
          .minArgc = 0,
          .maxArgc = 0,
      }},

    {"server",
      detail::MainCommandWrapper{
          .call = &ServerDB,
          .isWriteCmd = false,
          .minArgc = 0,
          .maxArgc = 0,
      }},
  };

  const std::unordered_map<std::string, detail::CommandOptionWrapper>
      CommandOptionMap{
          {"ex", 
            detail::CommandOptionWrapper{.type = CmdOptionType::kEX,
                                         .matchedMainCommand = {"set"},
                                         .exclusiveOpts = {CmdOptionType::kPX, CmdOptionType::kKEEPTTL},
                                         .minArgc = 1,
                                         .maxArgc = 1}},

          {"px", 
            detail::CommandOptionWrapper{.type = CmdOptionType::kPX,
                                         .matchedMainCommand = {"set"},
                                         .exclusiveOpts = {CmdOptionType::kEX, CmdOptionType::kKEEPTTL},
                                         .minArgc = 1,
                                         .maxArgc = 1}},

          {"nx", 
            detail::CommandOptionWrapper{.type = CmdOptionType::kNX,
                                         .matchedMainCommand = {"set"},
                                         .exclusiveOpts = {CmdOptionType::kXX},
                                         .minArgc = 0,
                                         .maxArgc = 0}},

          {"xx", 
            detail::CommandOptionWrapper{.type = CmdOptionType::kXX,
                                         .matchedMainCommand = {"set"},
                                         .exclusiveOpts = {CmdOptionType::kNX},
                                         .minArgc = 0,
                                         .maxArgc = 0}},

          {"keepttl", 
            detail::CommandOptionWrapper{.type = CmdOptionType::kKEEPTTL,
                                         .matchedMainCommand = {"set"},
                                         .exclusiveOpts = {CmdOptionType::kEX, CmdOptionType::kPX},
                                         .minArgc = 0,
                                         .maxArgc = 0}},

          {"get",
           detail::CommandOptionWrapper{.type = CmdOptionType::kGET,
                                        .matchedMainCommand = {"set"},
                                        .minArgc = 0,
                                        .maxArgc = 0}},

          {"docs",
           detail::CommandOptionWrapper{.type = CmdOptionType::kTODO,
                                        .matchedMainCommand = {"command"},
                                        .minArgc = 0,
                                        .maxArgc = 0}},
               
          {"info",
           detail::CommandOptionWrapper{.type = CmdOptionType::kTODO,
                                        .matchedMainCommand = {"command"},
                                        .minArgc = 0,
                                        .maxArgc = 0}},
      };
  }