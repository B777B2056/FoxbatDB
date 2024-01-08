#pragma once
#include "cmdmap.h"
#include "errors/protocol.h"
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace foxbatdb {
    class RequestParser;

    struct ParseResult {
        std::error_code ec;
        bool isWriteCmd;
        Command data;
        std::string cmdText;
    };

    namespace detail {
        struct RESPRequestParseState {
            virtual void react(RequestParser& fsm) = 0;
            virtual void exit() {}
        };

        struct ErrorState : public RESPRequestParseState {
            void react(RequestParser& fsm) override;
        };

        struct ParamCountStartState : public RESPRequestParseState {
            void react(RequestParser& fsm) override;
        };

        struct ParamCountState : public RESPRequestParseState {
            void react(RequestParser& fsm) override;
            void exit() override;

        private:
            std::string cntStr;
        };

        struct ParamCountEndState : public RESPRequestParseState {
            void react(RequestParser& fsm) override;
        };

        struct ParamLengthStartState : public RESPRequestParseState {
            void react(RequestParser& fsm) override;
        };

        struct ParamLengthState : public RESPRequestParseState {
            void react(RequestParser& fsm) override;
            void exit() override;

        private:
            std::string lengthStr;
        };

        struct ParamLengthEndState : public RESPRequestParseState {
            void react(RequestParser& fsm) override;
        };

        struct ParamContentState : public RESPRequestParseState {
            void react(RequestParser& fsm) override;
            void exit() override;

        private:
            std::string content;
        };

        struct ParamContentEndState : public RESPRequestParseState {
            void react(RequestParser& fsm) override;
        };

        struct Result {
            std::uint16_t paramCnt = 0;
            std::vector<std::string> paramList;

            static bool TestMainCommandAndTolower(std::string& str);
            static bool TestMainCommandOptionAndTolower(std::string& str);

            void ConvertToParseResult(ParseResult& ret);
            void BuildCommandText(std::string& cmdText);
            void BuildCommandData(Command& data);
        };
    }// namespace detail

    class RequestParser {
    private:
        char input;
        bool finished;
        detail::Result result;
        std::size_t nextParamLength;
        std::tuple<detail::ParamCountStartState, detail::ParamCountState, detail::ParamCountEndState,
                   detail::ParamLengthStartState, detail::ParamLengthState, detail::ParamLengthEndState,
                   detail::ParamContentState, detail::ParamContentEndState, detail::ErrorState>
                mStates_;
        detail::RESPRequestParseState* mCurrentState_;

        void RunOnce();
        void SetCurrentInput(char ch);
        [[nodiscard]] bool CheckError() const;

    public:
        RequestParser();

        [[nodiscard]] char GetCurrentInput() const;
        void SetFinishFlag();
        void SetParamCnt(std::uint16_t cnt);
        void SetNextParamLength(std::size_t length);
        [[nodiscard]] std::size_t GetNextParamLength() const;
        void AppendParamContent(std::string&& content);
        [[nodiscard]] const detail::Result& GetResult() const;
        void Reset();

        ParseResult Run(std::istream& is, std::size_t bytesTransferred);

        template<class S>
        void Transit() {
            this->mCurrentState_->exit();
            this->mCurrentState_ = &std::get<S>(mStates_);
        }
    };
}// namespace foxbatdb