#pragma once
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace foxbatdb {
    namespace detail {
        void BuildSimpleErrorResp(std::string& resp, std::error_code err);
        void BuildBulkErrorResp(std::string& resp, std::error_code err);
        void BuildSimpleStringResp(std::string& resp, const char* data);
        void BuildSimpleStringResp(std::string& resp, const std::string& data);
        void BuildBulkStringResp(std::string& resp, const std::string& data);
        void BuildIntegerResp(std::string& resp, int val);
        void BuildBooleanResp(std::string& resp, bool val);
        void BuildDoubleResp(std::string& resp, double val);

        /*
        void BuildBigNumberResp(std::string& resp, bool isNegative,
                                const std::string& bignum);
        */

        void BuildNilResp(std::string& resp);
        void BuildNullResp(std::string& resp);
        void BuildArrayResp(std::string& resp,
                            const std::vector<std::string>& params);

        void BuildSetResp(std::string& resp, const std::vector<std::string>& elements);
        void BuildMapResp(
                std::string& resp,
                const std::vector<std::pair<std::string, std::string>>& data);
        void BuildPushesResp(std::string& resp,
                             const std::vector<std::string>& outOfBandData);

        // 基本类型RESP
        template<typename T>
        struct ResponseBuilder {
            std::string operator()(const T& data) {
                std::string resp;
                if constexpr (std::is_same_v<bool, T>) {
                    BuildBooleanResp(resp, data);
                } else if constexpr (std::is_same_v<float, T> || std::is_same_v<double, T>) {
                    BuildDoubleResp(resp, data);
                } else if constexpr (std::is_integral_v<T>) {
                    BuildIntegerResp(resp, data);
                } else if constexpr (std::is_bounded_array_v<T>) {
                    BuildSimpleStringResp(resp, data);
                } else if constexpr (std::is_same_v<std::string, T>) {
                    BuildSimpleStringResp(resp, data);
                } else if constexpr (std::is_void_v<T>) {
                    BuildNullResp(resp);
                } else if constexpr (std::is_convertible_v<T, std::error_code>) {
                    BuildSimpleErrorResp(resp, data);
                } else if constexpr (std::is_same_v<std::vector<std::string>, T>) {
                    BuildArrayResp(resp, data);
                } else {
                    static_assert(data.NOT_EXISTS_METHOD());
                }
                return resp;
            }
        };

        // Helper
        template<typename T, typename... Args>
        void BuildPubSubResponseHelper(std::vector<std::string>& list, const T& param) {
            list.emplace_back(detail::ResponseBuilder<T>{}(param));
        }

        template<typename T, typename... Args>
        void BuildPubSubResponseHelper(std::vector<std::string>& list, const T& param, const Args&... args) {
            list.emplace_back(detail::ResponseBuilder<T>{}(param));
            BuildPubSubResponseHelper(list, args...);
        }
    }// namespace detail

    namespace utils {
        template<typename T>
        std::string BuildResponse(const T& data) {
            return detail::ResponseBuilder<T>{}(data);
        }

        template<typename... Args>
        std::string BuildPubSubResponse(const Args&... args) {
            std::string resp;
            std::vector<std::string> list;
            detail::BuildPubSubResponseHelper(list, args...);
            detail::BuildPushesResp(resp, list);
            return resp;
        }

        const static std::string OK_RESPONSE = BuildResponse("OK");
        const static std::string NIL_RESPONSE = []() -> std::string {
            std::string resp;
            detail::BuildNilResp(resp);
            return resp;
        }();
        const static std::string NULL_RESPONSE = []() -> std::string {
            std::string resp;
            detail::BuildNullResp(resp);
            return resp;
        }();
        const static std::string QUEUED_RESPONSE = BuildResponse("QUEUED");
        const static std::string HELLO_RESPONSE = []() -> std::string {
            std::string resp;
            detail::BuildMapResp(resp, {{BuildResponse("server"), BuildResponse("foxbatdb")},
                                        {BuildResponse("version"), BuildResponse("1.0.0")},
                                        {BuildResponse("proto"), BuildResponse(3)},
                                        {BuildResponse("mode"), BuildResponse("standalone")}});
            return resp;
        }();
    }// namespace utils
}// namespace foxbatdb