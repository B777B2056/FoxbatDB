#pragma once
#include <deque>
#include <tuple>
#include <vector>
#include <string>
#include "common/common.h"

namespace foxbatdb {
  class Command;

  class Transaction {
  private:
    std::deque<Command> mCmdQueue_;

  public:
    void PushCommand(const Command& cmd);
    void Discard();
    std::string Exec(CMDSessionPtr weak);
  };
}