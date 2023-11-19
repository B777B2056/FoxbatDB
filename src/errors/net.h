#pragma once
#include <system_error>

namespace foxbatdb {
namespace error {
enum class NetErrorCode {
  kSuccess = 0,
  kTODO,  // TODO
};

class NetErrorCategory : public std::error_category {
 public:
  NetErrorCategory() = default;

  std::string message(int c) const override;
  const char* name() const noexcept override;

 public:
  static const std::error_category& get() {
    const static NetErrorCategory sCategory;
    return sCategory;
  }
};

std::error_code make_error_code(NetErrorCode code);
}  // namespace error
}  // namespace foxbatdb

namespace std {
template <>
struct is_error_code_enum<foxbatdb::error::NetErrorCode> : true_type {};
}  // namespace std