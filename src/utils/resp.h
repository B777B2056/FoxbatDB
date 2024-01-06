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

        void BuildNullResp(std::string& resp);
        void BuildArrayResp(std::string& resp,
                            const std::vector<std::string>& params);

        void BuildSetResp(std::string& resp, const std::vector<std::string>& elements);
        void BuildMapResp(
                std::string& resp,
                const std::vector<std::pair<std::string, std::string>>& data);
        void BuildPushesResp(std::string& resp,
                             const std::vector<std::string>& outOfBandData);

        // ��������RESP
        template<typename T>
        struct ResponseBuilder {
            std::string operator()(const T& data) {
                std::string resp;
                if constexpr (std::is_same_v<int, T>) {
                    BuildIntegerResp(resp, data);
                } else if constexpr (std::is_same_v<double, T>) {
                    BuildDoubleResp(resp, data);
                } else if constexpr (std::is_same_v<bool, T>) {
                    BuildBooleanResp(resp, data);
                } else if constexpr (std::is_bounded_array_v<T>) {
                    BuildSimpleStringResp(resp, data);
                } else if constexpr (std::is_same_v<std::string, T>) {
                    BuildSimpleStringResp(resp, data);
                } else if constexpr (std::is_void_v<T>) {
                    BuildNullResp(resp);
                } else if constexpr (std::is_convertible_v<T, std::error_code>) {
                    BuildSimpleErrorResp(resp, data);
                }
                return resp;
            }
        };

        // ��������RESP
        template<typename U>
        struct ResponseBuilder<std::vector<U>> {
            std::string operator()(const std::vector<U>& data) {
                std::string resp;
                std::vector<std::string> list;
                for (const U& param: data) {
                    list.emplace_back(ResponseBuilder<U>{}(param));
                }
                BuildArrayResp(resp, list);
                return resp;
            }
        };

        // Set����RESP

        // Map����RESP

        // Pushes����RESP
    }// namespace detail

    namespace utils {
        template<typename T>
        std::string BuildResponse(const T& data) {
            return detail::ResponseBuilder<T>{}(data);
        }

        std::string BuildArrayResponseWithFilledItems(const std::vector<std::string>& list);
        std::string BuildErrorResponse(std::error_code err);

        template<typename T>
        std::string BuildPubSubResponse(const std::vector<T>& data) {
            std::string resp;
            std::vector<std::string> list;
            for (const T& param: data) {
                list.emplace_back(detail::ResponseBuilder<T>{}(param));
            }
            detail::BuildPushesResp(resp, list);
            return resp;
        }
    }// namespace utils
}// namespace foxbatdb