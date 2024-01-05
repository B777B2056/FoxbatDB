#pragma once
#include <cctype>
#include <charconv>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <optional>
#include <string>

namespace foxbatdb::utils {
    std::uint64_t GetMicrosecondTimestamp();
    std::chrono::steady_clock::time_point MicrosecondTimestampCovertToTimePoint(std::uint64_t timestamp);
    bool IsValidTimestamp(std::uint64_t timestamp);

    constexpr unsigned long long operator"" _MB(unsigned long long m) {
        return m * 1024 * 1024;
    }

    template<std::integral T>
    std::optional<T> ToInteger(const std::string& data) {
        T ret;
        auto [_, ec] = std::from_chars<T>(data.data(), data.data() + data.size(), ret);
        if (ec != std::errc()) {
            return std::nullopt;
        } else {
            return ret;
        }
    }

    template<std::floating_point T>
    std::optional<T> ToFloat(const std::string& data) {
        T ret;
        auto [_, ec] = std::from_chars<T>(
                data.data(), data.data() + data.size(), ret);
        if (ec != std::errc()) {
            return std::nullopt;
        } else {
            return ret;
        }
    }
}// namespace foxbatdb::utils
