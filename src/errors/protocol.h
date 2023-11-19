#pragma once
#include <system_error>

namespace foxbatdb {
  namespace error {
    enum class ProtocolErrorCode {
      kSuccess = 0,
      kArgNumbers,
      kCommandNotFound,
      kSyntax,
      kRequestFormat,
    };

    class ProtocolErrorCategory : public std::error_category {
     public:
      ProtocolErrorCategory() = default;

      std::string message(int c) const override;
      const char* name() const noexcept override;

     public:
      static const std::error_category& get() {
        const static ProtocolErrorCategory sCategory;
        return sCategory;
      }
    };

    std::error_code make_error_code(ProtocolErrorCode code);
  }  // namespace error
}

namespace std {
template <>
struct is_error_code_enum<foxbatdb::error::ProtocolErrorCode> : true_type {};
}  // namespace std