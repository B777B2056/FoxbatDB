#include "net.h"

namespace foxbatdb {
namespace error {
std::string NetErrorCategory::message(int c) const {
  switch (static_cast<NetErrorCode>(c)) {
    case NetErrorCode::kSuccess:
      return "Success";

    case NetErrorCode::kTODO:
      return "Error TODO";

    default:
      return "Wrong Net Error Code";
  }
}

const char* NetErrorCategory::name() const noexcept {
  return "Network Error Category";
}

std::error_code make_error_code(NetErrorCode code) {
  return {
      static_cast<int>(code),
      NetErrorCategory::get(),
  };
}
}  // namespace error
}  // namespace foxbatdb