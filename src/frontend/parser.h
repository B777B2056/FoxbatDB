﻿#pragma once
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>
#include "cmdmap.h"
#include "errors/protocol.h"

namespace foxbatdb {
  struct ParseResult {
    bool hasError : 2;
    bool isWriteCmd : 2;
    std::string errMsg;
    Command data;
    std::string cmdText;
  };

  class RequestParser {
  private:
    enum class ProtocolParseState : std::uint8_t {
      kParamNum = 1,
      kParamLen,
      kParamContent,
    };

    enum class CommandParseState : std::uint8_t {
      kMainCommand = 1,
      kMainArgv,
      kCommandOption,
    };

  private:
    ProtocolParseState mProtocolState_ : 4;
    CommandParseState mCommandState_ : 4;
    std::uint8_t mParamCnt_;
    std::uint8_t mParamNum_;
    std::uint16_t mCurParamLen_;
    ParseResult mResult_;
    std::error_code mParseError_, mSyntaxError_;

    void ParseCommand(std::string_view param);
    void ValidateCommand();
    void HandleParseError(std::error_code err);
    void HandleSyntaxError(std::error_code err);
    ParseResult BuildParseResult();

  public:
    RequestParser();
    bool IsParseFinished() const;
    ParseResult ParseLine(std::string_view curLine);                                                                  
  };
}