#include "protocol.h"

namespace foxbatdb::error {
    std::string ProtocolErrorCategory::message(int c) const {
        switch (static_cast<ProtocolErrorCode>(c)) {
            case ProtocolErrorCode::kSuccess:
                return "Success";

            case ProtocolErrorCode::kContinue:
                return "Continue";

            case ProtocolErrorCode::kArgNumbers:
                return "Argument number error";

            case ProtocolErrorCode::kCommandNotFound:
                return "Command not found";

            case ProtocolErrorCode::kSyntax:
                return "Syntax error";

            case ProtocolErrorCode::kRequestFormat:
                return "Request format error";

            case ProtocolErrorCode::kOptionExclusive:
                return "Input option has another exclusive option(s)";

            default:
                return "Wrong Protocol Error Code";
        }
    }

    const char* ProtocolErrorCategory::name() const noexcept {
        return "Protocol Error Category";
    }

    std::error_code make_error_code(ProtocolErrorCode code) {
        return {
                static_cast<int>(code),
                ProtocolErrorCategory::get(),
        };
    }
}// namespace foxbatdb::error
