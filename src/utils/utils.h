#pragma once
#include <cctype>
#include <charconv>
#include <chrono>
#include <concepts>
#include <cstdint>
#include <fstream>
#include <optional>
#include <string>

namespace foxbatdb::utils {
    std::uint64_t GetMicrosecondTimestamp();
    std::chrono::steady_clock::time_point MicrosecondTimestampConvertToTimePoint(std::uint64_t timestamp);
    bool IsValidTimestamp(std::uint64_t timestamp);

    constexpr unsigned long long operator"" _MB(unsigned long long m);

    static constexpr std::uint32_t CRC_INIT_VALUE = 0xFFFFFFFF;
    std::uint32_t CRC(const char* buf, std::size_t size, std::uint32_t lastCRC = CRC_INIT_VALUE);

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

#if defined(__clang__) || defined(__GNUC__) || defined(__GNUG__)
    template<std::integral T>
    T ChangeIntegralEndian(T data) {
        static_assert((2 == sizeof(data)) || (4 == sizeof(data)) || (8 == sizeof(data)), "integral length not support");

        if constexpr (2 == sizeof(data)) {
            return static_cast<T>(__builtin_bswap16(static_cast<std::uint16_t>(data)));
        } else if constexpr (4 == sizeof(data)) {
            return static_cast<T>(__builtin_bswap32(static_cast<std::uint32_t>(data)));
        } else {
            return static_cast<T>(__builtin_bswap64(static_cast<std::uint64_t>(data)));
        }
    }
#elif defined(_MSC_VER)
#include <stdlib.h>

    template<std::integral T>
    T ChangeIntegralEndian(T data) {
        static_assert((2 == sizeof(data)) || (4 == sizeof(data)) || (8 == sizeof(data)), "integral length not support");

        if constexpr (2 == sizeof(data)) {
            return static_cast<T>(_byteswap_ushort(static_cast<unsigned short>(data)));
        } else if constexpr (4 == sizeof(data)) {
            return static_cast<T>(_byteswap_ulong(static_cast<unsigned long>(data)));
        } else {
            return static_cast<T>(_byteswap_uint64(static_cast<unsigned __int64>(data)));
        }
    }
#else
    namespace detail {
        template<std::integral T>
        T ByteSwap(T data) {
            auto* ptr = reinterpret_cast<uint8_t*>(&data);
            for (size_t i = 0, j = sizeof(data) - 1; i < j; ++i, --j) {
                std::swap(ptr[i], ptr[j]);
            }
            return data;
        }
    }// namespace detail

    template<std::integral T>
    T ChangeIntegralEndian(T data) {
        static_assert((2 == sizeof(data)) || (4 == sizeof(data)) || (8 == sizeof(data)), "integral length not support");

        if constexpr (2 == sizeof(data)) {
            return static_cast<T>(detail::ByteSwap(static_cast<std::uint16_t>(data)));
        } else if constexpr (4 == sizeof(data)) {
            return static_cast<T>(detail::ByteSwap(static_cast<std::uint32_t>(data)));
        } else {
            return static_cast<T>(detail::ByteSwap(static_cast<std::uint64_t>(data)));
        }
    }
#endif
}// namespace foxbatdb::utils
