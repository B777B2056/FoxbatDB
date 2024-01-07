#pragma once
#include "spdlog/spdlog.h"
#include <mutex>

namespace foxbatdb {
    class ServerLog {
    private:
        mutable std::mutex mutex_;
        std::shared_ptr<spdlog::logger> mLogger_;
        ServerLog();

    public:
        static ServerLog& GetInstance();
        ServerLog(const ServerLog&) = delete;
        ServerLog& operator=(const ServerLog&) = delete;
        ServerLog(ServerLog&&) = default;
        ServerLog& operator=(ServerLog&&) = default;
        ~ServerLog();

        void DumpToDisk();

        template<typename... Args>
        void Debug(spdlog::format_string_t<Args...> fmt, Args&&... args) {
            std::unique_lock lock{mutex_};
            mLogger_->debug(fmt, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void Info(spdlog::format_string_t<Args...> fmt, Args&&... args) {
            std::unique_lock lock{mutex_};
            mLogger_->info(fmt, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void Warnning(spdlog::format_string_t<Args...> fmt, Args&&... args) {
            std::unique_lock lock{mutex_};
            mLogger_->warn(fmt, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void Error(spdlog::format_string_t<Args...> fmt, Args&&... args) {
            std::unique_lock lock{mutex_};
            mLogger_->error(fmt, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void Fatal(spdlog::format_string_t<Args...> fmt, Args&&... args) {
            std::unique_lock lock{mutex_};
            mLogger_->critical(fmt, std::forward<Args>(args)...);
        }
    };
}// namespace foxbatdb