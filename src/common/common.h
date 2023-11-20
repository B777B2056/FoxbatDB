#pragma once
#include <string>
#include <vector>
#include <list>
#include <utility>
#include <memory>
#include "data_structure/binstr.h"
#include "data_structure/dict.h"

namespace foxbatdb {
  class Database;
  class DatabaseManager;

  enum class CmdOptionType : std::uint8_t { kEX = 1, kPX, kNX, kXX, kKEEPTTL, kGET, kTODO };

  struct ProcResult;

  class Command;
  class CommandOption;
  class CMDSession;
  using CMDSessionPtr = std::weak_ptr<CMDSession>;

  using CmdProcFunc = ProcResult (*)(CMDSessionPtr, const Command&);

  struct CommandOption {
    std::string name;
    CmdOptionType type;
    std::vector<BinaryString> argv;
  };

  struct Command {
    std::string name;
    CmdProcFunc call;
    std::vector<BinaryString> argv;
    std::vector<CommandOption> options;
  };

  class ValueObject;
  using StorageImpl = Dict<std::shared_ptr<ValueObject>>;

  using WatchedMap = Dict<std::vector<CMDSessionPtr>>;

  enum class TxState : std::uint8_t {
    kInvalid = 0,
    kBegin,
    kExec,
    kDiscard,
  };

  using PubSubChannelMap = Dict<std::list<CMDSessionPtr>>;
}