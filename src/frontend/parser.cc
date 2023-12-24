#include "parser.h"
#include <algorithm>
#include <cctype>
#include <charconv>
#include <cmath>
#include <unordered_map>
#include "core/db.h"
#include "utils/resp.h"

namespace foxbatdb {
  static std::string ToLowerString(std::string_view str) {
  std::string ret{str.begin(), str.end()};
    std::transform(str.begin(), str.end(), ret.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return ret;
  }

  RequestParser::RequestParser()
    : mProtocolState_{ProtocolParseState::kParamNum}
    , mCommandState_{CommandParseState::kMainCommand}
    , mParamCnt_{0}
    , mParamNum_{0}
    , mCurParamLen_{0}
    , mResult_{.hasError=false}
    , mParseError_{error::ProtocolErrorCode::kSuccess}
    , mSyntaxError_{error::ProtocolErrorCode::kSuccess} {

  }

  bool RequestParser::IsParseFinished() const {
    return mParamCnt_ == mParamNum_;
  }

  /*
    协议格式
    *<参数数量>CRLF
    $<参数1的字节长度>CRLF
    <参数1的数据>CRLF
    ...
    $<参数N的字节长度>CRLF
    <参数N的数据>CRLF
   */
  ParseResult RequestParser::ParseLine(
      std::string_view curLine) {
    switch (mProtocolState_) {
      case ProtocolParseState::kParamNum: {
        if ('*' != curLine[0]) {
          HandleParseError(error::ProtocolErrorCode::kRequestFormat);
          break;
        }
        auto [_, ec] =
            std::from_chars(curLine.data() + 1,
                            curLine.data() + curLine.length(), mParamNum_);
        if (ec != std::errc()) {
          HandleParseError(error::ProtocolErrorCode::kRequestFormat);
        } else {
          mProtocolState_ = ProtocolParseState::kParamLen;
        }
      } break;
      case ProtocolParseState::kParamLen: {
        if ('$' != curLine[0]) {
          HandleParseError(error::ProtocolErrorCode::kRequestFormat);
          break;
        }
        auto [_, ec] = std::from_chars(curLine.data() + 1,
                                        curLine.data() + curLine.length(),
                                        mCurParamLen_);
        if (ec != std::errc()) {
          HandleParseError(error::ProtocolErrorCode::kRequestFormat);
        } else {
          mProtocolState_ = ProtocolParseState::kParamContent;
        }
      } break;
      case ProtocolParseState::kParamContent: {
        ParseCommand(curLine);
        ++mParamCnt_;
        if (!IsParseFinished()) {
          mProtocolState_ = ProtocolParseState::kParamLen;
        } else {
          ValidateCommand();  // 校验命令语法正确性
        }
      } break;
    }

    if (mResult_.isWriteCmd) {
      mResult_.cmdText += curLine;
      mResult_.cmdText += "\r\n";
    }
    return BuildParseResult();
  }

  void RequestParser::ParseCommand(std::string_view param) {
    switch (mCommandState_) {
      case CommandParseState::kMainCommand:
        if (auto cmdStr = ToLowerString(param);
            MainCommandMap.contains(cmdStr)) {
          // 保存主命令信息到解析结果
          auto& info = MainCommandMap.at(cmdStr);
          mResult_.data.name = cmdStr;
          mResult_.data.call = info.call;
          mResult_.isWriteCmd = info.isWriteCmd;
          // 状态转移
          mCommandState_ = CommandParseState::kMainArgv;
        } else {
          HandleSyntaxError(error::ProtocolErrorCode::kCommandNotFound);
        }
        break;
      case CommandParseState::kMainArgv:
      {
        auto cmdOptStr = ToLowerString(param);
        if (!CommandOptionMap.contains(cmdOptStr)) {
          // 非命令选项关键字，作为主命令参数保存到解析结果
          mResult_.data.argv.emplace_back(param);
        } else {
          // 命令选项关键字，作为命令选项保存到解析结果，并转移下一状态至命令选项解析状态
          auto optWrapper = CommandOptionMap.at(cmdOptStr);
          if (!optWrapper.matchedMainCommand.contains(mResult_.data.name)) {
            HandleSyntaxError(error::ProtocolErrorCode::kCommandNotFound);
          } else {
            // 保存命令选项到解析结果
            mResult_.data.options.push_back(CommandOption{.name=cmdOptStr, .type=optWrapper.type});
            // 状态转移
            mCommandState_ = CommandParseState::kCommandOption;
          }
        }
      }
        break;
      case CommandParseState::kCommandOption:
      {
        auto cmdOptStr = ToLowerString(param);
        if (!CommandOptionMap.contains(cmdOptStr)) {
          // 非命令选项关键字，作为命令选项参数保存到解析结果
          mResult_.data.options.back().argv.emplace_back(param);
        } else {
          // 命令选项关键字，作为命令选项保存到解析结果，并转移下一状态至命令选项解析状态
          auto optWrapper = CommandOptionMap.at(cmdOptStr);
          if (!optWrapper.matchedMainCommand.contains(mResult_.data.name)) {
            HandleSyntaxError(error::ProtocolErrorCode::kCommandNotFound);
          } else {
            // 保存命令选项到解析结果
            mResult_.data.options.push_back(CommandOption{.name=cmdOptStr, .type=optWrapper.type});
          }
        }
      }
        break;
      default:
        break;
    }
  }

  void RequestParser::ValidateCommand() {
    if (mSyntaxError_)  return;
    // 校验主命令参数数量是否正确
    auto& mainCmdWrapper = MainCommandMap.at(mResult_.data.name);
    if ((mResult_.data.argv.size() < mainCmdWrapper.minArgc) ||
        (mResult_.data.argv.size() > mainCmdWrapper.maxArgc)) {
      HandleSyntaxError(error::ProtocolErrorCode::kArgNumbers);
      return;
    }
    // 校验主命令选项参数
    auto& opts = mResult_.data.options;
    for (const auto& opt : opts) {
      auto& cmdOptWrapper = CommandOptionMap.at(opt.name);
      // 校验主命令选项参数是否存在互斥
      for (auto optType : cmdOptWrapper.exclusiveOpts) {
        if (opts.end() != std::find_if(opts.begin(), opts.end(), 
          [optType](const CommandOption& o){return o.type == optType;})) {
          HandleSyntaxError(error::ProtocolErrorCode::kOptionExclusive);
          return;
        }
      }
      // 校验主命令选项参数数量是否正确
      if ((opt.argv.size() < cmdOptWrapper.minArgc) ||
        (opt.argv.size() > cmdOptWrapper.maxArgc)) {
        HandleSyntaxError(error::ProtocolErrorCode::kArgNumbers);
        return;
      }
    }
  }

  void RequestParser::HandleParseError(std::error_code err) {
    mParseError_ = err;
  }

  void RequestParser::HandleSyntaxError(std::error_code err) {
    mSyntaxError_ = err;
  }

  ParseResult RequestParser::BuildParseResult() {
    ParseResult ret = mResult_;
    if (!mParseError_) {
      if (IsParseFinished()) {
        if (mSyntaxError_) {
          ret.hasError = true;
          ret.errMsg = utils::BuildErrorResponse(mSyntaxError_);
        }
        *this = {};  // 解析结束，重置状态
      }
    } else {
      ret.hasError = true;
      ret.errMsg = utils::BuildErrorResponse(mParseError_);
      *this = {};  // 解析结束，重置状态
    }

    return ret;
  }
}