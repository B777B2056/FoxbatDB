#include "resp.h"

namespace foxbatdb {
  namespace detail {
    static void BuildResponseHelper(std::string& resp, const std::string& content) {
      resp += content;
      resp += "\r\n";
    }

    static void BuildResponseHelper(std::string& resp, char prefix,
                                    const std::string& content) {
      resp += prefix;
      resp += content;
      resp += "\r\n";
    }

    void BuildSimpleErrorResp(std::string& resp, std::error_code err) {
      BuildResponseHelper(resp, '-', err.message());
    }

    void BuildBulkErrorResp(std::string& resp, std::error_code err) {
      BuildResponseHelper(resp, '!', std::to_string(err.message().length()));
      BuildResponseHelper(resp, err.message());
    }

    void BuildSimpleStringResp(std::string& resp, const char* data) {
      BuildResponseHelper(resp, '+', std::string{data});
    }

    void BuildSimpleStringResp(std::string& resp, const std::string& data) {
      BuildResponseHelper(resp, '+', data);
    }

    void BuildSimpleStringResp(std::string& resp, const BinaryString& data) {
      BuildResponseHelper(resp, '+', data.ToTextString());
    }

    void BuildBulkStringResp(std::string& resp, const BinaryString& data) {
      BuildResponseHelper(resp, '!', std::to_string(data.Length()));
      BuildResponseHelper(resp, data.ToTextString());
    }

    void BuildIntegerResp(std::string& resp, int val) {
      BuildResponseHelper(resp, ':', std::to_string(val));
    }

    void BuildBooleanResp(std::string& resp, bool val) {
      BuildResponseHelper(resp, '#', val ? "t" : "f");
    }

    void BuildDoubleResp(std::string& resp, double val) {
      std::string content;
      if (std::isnan(val)) {
        content += "nan";
      } else if (std::isinf(val)) {
        if (std::signbit(val)) {
          content += "-";
        }
        content += "inf";
      } else {
        content += std::to_string(val);
      }

      BuildResponseHelper(resp, ',', content);
    }

    /*
      void BuildBigNumberResp(std::string& resp, bool isNegative,
                                               const std::string& bignum) {
        BuildResponseHelper(resp, '(', isNegative ? std::string{"-"}+bignum : bignum);
      }
      */

    void BuildArrayResp(std::string& resp,
                               const std::vector<std::string>& params) {
      BuildResponseHelper(resp, '*', std::to_string(params.size()));
      for (const auto& param : params) {
        resp += param;
      }
    }

    void BuildNullResp(std::string& resp) {
      BuildResponseHelper(resp, '_', "");
    }

    void BuildSetResp(std::string& resp,
                             const std::vector<std::string>& elements) {
      BuildResponseHelper(resp, '~', std::to_string(elements.size()));
      for (const auto& e : elements) {
        resp += e;
      }
    }

    void BuildMapResp(
      std::string& resp,
      const std::vector<std::pair<std::string, std::string>>& data) {
      BuildResponseHelper(resp, '%', std::to_string(data.size()));
      for (const auto& [key, val] : data) {
        resp += key;
        resp += val;
      }
    }

    void BuildPushesResp(std::string& resp,
                                const std::vector<std::string>& outOfBandData) {
      BuildResponseHelper(resp, '>', std::to_string(outOfBandData.size()));
      for (const auto& e : outOfBandData) {
        resp += e;
      }
    }
  }

  namespace utils {
    std::string BuildArrayResponseWithFilledItems(
      const std::vector<std::string>& list) {
        std::string resp;
        detail::BuildArrayResp(resp, list);
        return resp;
    }

    std::string BuildErrorResponse(std::error_code err) {
        return detail::ResponseBuilder<decltype(err)>{}(err);
    }
  }
}