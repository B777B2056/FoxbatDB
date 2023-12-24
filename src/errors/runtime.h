#pragma once
#include <system_error>

namespace foxbatdb {
namespace error {
enum class RuntimeErrorCode {
  kSuccess = 0,
  kIntervalError,
  kDBIdxOutOfRange,
  kKeyAlreadyExist,
  kKeyNotFound,
  kMemoryOut,
  kAlreadyInTx,
  kNotInTx,
  kTxError,
  kWatchedKeyModified,
  kInvalidTxCmd
};

class RuntimeErrorCategory : public std::error_category {
 public:
  RuntimeErrorCategory() = default;

  std::string message(int c) const override;
  const char* name() const noexcept override;

 public:
  static const std::error_category& get() {
    const static RuntimeErrorCategory sCategory;
    return sCategory;
  }
};

std::error_code make_error_code(RuntimeErrorCode code);
}  // namespace error
}  // namespace foxbatdb

namespace std {
template <>
struct is_error_code_enum<foxbatdb::error::RuntimeErrorCode> : true_type {};
}  // namespace std