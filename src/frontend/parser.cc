#include "parser.h"
#include "utils/resp.h"
#include <cctype>
#include <unordered_map>

namespace foxbatdb {
    namespace detail {
        static constexpr std::uint8_t MAX_PARAM_COUNT = -1;

        void ParamCountStartState::react(RequestParser& fsm) {
            if ('*' == fsm.GetCurrentInput()) {
                fsm.Transit<ParamCountState>();
            } else {
                fsm.Transit<ErrorState>();
            }
        }

        void ParamCountState::react(RequestParser& fsm) {
            char input = fsm.GetCurrentInput();
            if ('\r' == input) {
                auto cnt = std::stoul(cntStr);
                if (cnt > MAX_PARAM_COUNT) {
                    fsm.Transit<ErrorState>();
                } else {
                    fsm.SetParamCnt(cnt);
                    fsm.Transit<ParamCountEndState>();
                }
            } else if ((input < '0') || (input > '9')) {
                fsm.Transit<ErrorState>();
            } else {
                cntStr += input;
            }
        }

        void ParamCountState::exit() {
            cntStr = "";
        }

        void ParamCountEndState::react(RequestParser& fsm) {
            if ('\n' != fsm.GetCurrentInput()) {
                fsm.Transit<ErrorState>();
            } else {
                fsm.Transit<ParamLengthStartState>();
            }
        }

        void ErrorState::react(RequestParser& fsm) {
        }

        void ParamLengthStartState::react(RequestParser& fsm) {
            if ('$' == fsm.GetCurrentInput()) {
                fsm.Transit<ParamLengthState>();
            } else {
                fsm.Transit<ErrorState>();
            }
        }

        void ParamLengthState::react(RequestParser& fsm) {
            char input = fsm.GetCurrentInput();
            if ('\r' == input) {
                fsm.SetNextParamLength(std::stoull(lengthStr));
                fsm.Transit<ParamLengthEndState>();
            } else if ((input < '0') || (input > '9')) {
                fsm.Transit<ErrorState>();
            } else {
                lengthStr += input;
            }
        }

        void ParamLengthState::exit() {
            lengthStr = "";
        }

        void ParamLengthEndState::react(RequestParser& fsm) {
            if ('\n' != fsm.GetCurrentInput()) {
                fsm.Transit<ErrorState>();
            } else {
                fsm.Transit<ParamContentState>();
            }
        }

        void ParamContentState::react(RequestParser& fsm) {
            char input = fsm.GetCurrentInput();
            if ('\r' != input) {
                content += input;
            } else {
                if (content.size() > fsm.GetNextParamLength())
                    fsm.Transit<ErrorState>();
                else {
                    fsm.AppendParamContent(std::move(content));
                    fsm.Transit<ParamContentEndState>();
                }
            }
        }

        void ParamContentState::exit() {
            content = "";
        }

        void ParamContentEndState::react(RequestParser& fsm) {
            if ('\n' != fsm.GetCurrentInput()) {
                fsm.Transit<ErrorState>();
            } else {
                if (fsm.GetResult().paramList.size() == fsm.GetResult().paramCnt) {
                    fsm.SetFinishFlag();
                    fsm.Transit<ParamCountStartState>();
                } else
                    fsm.Transit<ParamLengthStartState>();
            }
        }

        Result::operator ParseResult() {
            if (paramList.empty()) {
                return {.ec = error::ProtocolErrorCode::kRequestFormat};
            }

            const auto& mainCMDName = paramList.front();
            if (!MainCommandMap.contains(mainCMDName)) {
                return {.ec = error::ProtocolErrorCode::kCommandNotFound};
            }

            const auto& mainCMDInfo = MainCommandMap.at(mainCMDName);

            ParseResult ret{
                    .isWriteCmd = mainCMDInfo.isWriteCmd,
                    .data = Command{
                            .name = mainCMDName,
                            .call = mainCMDInfo.call,
                    }};

            BuildCommandText(ret.cmdText);
            BuildCommandData(ret.data);
            ret.ec = ret.data.Validate();
            return ret;
        }

        void Result::BuildCommandText(std::string& cmdText) {
            std::vector<std::string> filledParamList{paramList.size(), "$"};
            for (std::size_t i = 0; i < paramList.size(); ++i) {
                filledParamList[i] += (std::to_string(paramList[i].size()) + "\r\n" + paramList[i] + "\r\n");
            }
            cmdText = utils::BuildArrayResponseWithFilledItems(filledParamList);
        }

        void Result::BuildCommandData(Command& data) {
            std::size_t i;
            for (i = 1; i < paramList.size(); ++i) {
                auto&& param = paramList.at(i);
                if (CommandOptionMap.contains(param)) {
                    break;
                }
                data.argv.emplace_back(std::move(param));
            }

            for (; i < paramList.size(); ++i) {
                auto&& param = paramList.at(i);
                if (CommandOptionMap.contains(param)) {
                    const auto& cmdOptInfo = CommandOptionMap.at(param);
                    data.options.emplace_back(std::move(param), cmdOptInfo.type);
                } else {
                    data.options.back().argv.emplace_back(std::move(param));
                }
            }
        }
    }// namespace detail

    RequestParser::RequestParser() : input{}, finished{false}, nextParamLength{0} {
        mCurrentState_ = &std::get<detail::ParamCountStartState>(mStates_);
    }

    void RequestParser::SetCurrentInput(char ch) {
        this->input = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }

    void RequestParser::SetParamCnt(std::uint16_t cnt) {
        this->result.paramCnt = cnt;
    }

    void RequestParser::AppendParamContent(std::string&& content) {
        this->result.paramList.emplace_back(std::move(content));
    }

    const detail::Result& RequestParser::GetResult() const {
        return this->result;
    }

    void RequestParser::Reset() {
        finished = false;
        mCurrentState_ = &std::get<detail::ParamCountStartState>(mStates_);
        result.paramCnt = 0;
        result.paramList.clear();
    }

    void RequestParser::RunOnce() {
        mCurrentState_->react(*this);
    }

    char RequestParser::GetCurrentInput() const {
        return this->input;
    }

    void RequestParser::SetFinishFlag() {
        finished = true;
    }

    bool RequestParser::CheckError() const {
        return dynamic_cast<detail::ErrorState*>(mCurrentState_) != nullptr;
    }

    void RequestParser::SetNextParamLength(std::size_t length) {
        this->nextParamLength = length;
    }

    std::size_t RequestParser::GetNextParamLength() const {
        return this->nextParamLength;
    }
}// namespace foxbatdb