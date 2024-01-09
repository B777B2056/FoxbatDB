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

        static void Tolower(std::string& str) {
            for (char& ch: str) {
                ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            }
        }

        bool Result::TestMainCommandAndTolower(std::string& str) {
            if (str.size() > MAX_COMMAND_NAME_LENGTH)
                return false;

            auto tmp = str;
            detail::Tolower(str);
            if (MainCommandMap.contains(tmp)) {
                str = std::move(tmp);
                return true;
            }
            return false;
        }

        bool Result::TestMainCommandOptionAndTolower(std::string& str) {
            if (str.size() > MAX_COMMAND_NAME_LENGTH)
                return false;

            auto tmp = str;
            detail::Tolower(str);
            if (CommandOptionMap.contains(tmp)) {
                str = std::move(tmp);
                return true;
            }
            return false;
        }

        void Result::ConvertToParseResult(ParseResult& ret) {
            if (paramList.empty()) {
                ret.ec = error::ProtocolErrorCode::kRequestFormat;
                return;
            }

            if (!Result::TestMainCommandAndTolower(paramList.front())) {
                ret.ec = error::ProtocolErrorCode::kCommandNotFound;
                return;
            }

            const auto& mainCMDName = paramList.front();
            const auto& mainCMDInfo = MainCommandMap.at(mainCMDName);

            ret.isWriteCmd = mainCMDInfo.isWriteCmd;
            ret.data.name = mainCMDName;
            ret.data.call = mainCMDInfo.call;

            BuildCommandData(ret.data);
            ret.ec = ret.data.Validate();
        }

        void Result::BuildCommandData(Command& data) {
            std::size_t i;
            for (i = 1; i < paramList.size(); ++i) {
                if (TestMainCommandOptionAndTolower(paramList.at(i)))
                    break;
                data.argv.emplace_back(std::move(paramList.at(i)));
            }

            for (; i < paramList.size(); ++i) {
                if (TestMainCommandOptionAndTolower(paramList.at(i))) {
                    const auto& cmdOptInfo = CommandOptionMap.at(paramList.at(i));
                    data.options.emplace_back(std::move(paramList.at(i)), cmdOptInfo.type);
                } else {
                    data.options.back().argv.emplace_back(std::move(paramList.at(i)));
                }
            }
        }
    }// namespace detail

    RequestParser::RequestParser() : input{}, finished{false}, nextParamLength{0} {
        mCurrentState_ = &std::get<detail::ParamCountStartState>(mStates_);
    }

    void RequestParser::SetCurrentInput(char ch) {
        this->input = ch;
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

    ParseResult RequestParser::Run(std::istream& is, std::size_t bytesTransferred) {
        char ch;
        for (std::size_t i = 0; i < bytesTransferred; ++i) {
            if (!is)
                break;

            is.get(ch);
            this->SetCurrentInput(ch);
            this->RunOnce();

            if (this->CheckError())
                return {.ec = error::ProtocolErrorCode::kRequestFormat};
        }

        ParseResult ret{.ec = error::ProtocolErrorCode::kContinue};
        if (finished) {
            result.ConvertToParseResult(ret);
            this->Reset();
        }
        return ret;
    }
}// namespace foxbatdb