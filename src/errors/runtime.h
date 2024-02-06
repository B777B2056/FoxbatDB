#pragma once
#include <system_error>

namespace foxbatdb::error {
    enum class RuntimeErrorCode {
        kSuccess = 0,
        kIntervalError,
        kDBIdxOutOfRange,
        kKeyAlreadyExist,
        kKeyNotFound,
        kKeyValTooLong,
        kMemoryOut,
        kAlreadyInTx,
        kNotInTx,
        kTxError,
        kWatchedKeyModified,
        kInvalidTxCmd,
        kInvalidValueType
    };

    class RuntimeErrorCategory : public std::error_category {
    public:
        RuntimeErrorCategory() = default;

        [[nodiscard]] std::string message(int c) const override;
        [[nodiscard]] const char* name() const noexcept override;

    public:
        static const std::error_category& get() {
            const static RuntimeErrorCategory sCategory;
            return sCategory;
        }
    };

    std::error_code make_error_code(RuntimeErrorCode code);
}// namespace foxbatdb::error


namespace std {
    template<>
    struct is_error_code_enum<foxbatdb::error::RuntimeErrorCode> : true_type {};
}// namespace std