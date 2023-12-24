#include "runtime.h"

namespace foxbatdb {
namespace error {
std::string RuntimeErrorCategory::message(int c) const {
  switch (static_cast<RuntimeErrorCode>(c)) {
    case RuntimeErrorCode::kSuccess:
      return "Success";

    case RuntimeErrorCode::kIntervalError:
      return "Interval server error";

    case RuntimeErrorCode::kDBIdxOutOfRange:
      return "DB index is out of range";

    case RuntimeErrorCode::kKeyAlreadyExist:
      return "Key Already exist";

    case RuntimeErrorCode::kKeyNotFound:
      return "Key not found";

    case RuntimeErrorCode::kMemoryOut:
      return "Memory used out";

    case RuntimeErrorCode::kAlreadyInTx:
      return "Already in tx mode";

    case RuntimeErrorCode::kNotInTx:
      return "Not in tx mode";

    case RuntimeErrorCode::kTxError:
      return "Tx exec error";

    case RuntimeErrorCode::kWatchedKeyModified:
      return "Watched key has been modified by others";

    case RuntimeErrorCode::kInvalidTxCmd:
      return "Invalid tx command";

    default:
      return "Wrong Runtime Error Code";
  }
}

const char* RuntimeErrorCategory::name() const noexcept {
  return "Runtime Error Category";
}

std::error_code make_error_code(RuntimeErrorCode code) {
  return {
      static_cast<int>(code),
      RuntimeErrorCategory::get(),
  };
}
}  // namespace error
}  // namespace foxbatdb