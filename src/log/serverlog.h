#pragma once
#include "spdlog/spdlog.h"

namespace foxbatdb {
    class ServerLog {
    private:
        std::shared_ptr<spdlog::logger> mLogger_;
        ServerLog();

        static ServerLog& GetInstance();

    public:
        ServerLog(const ServerLog&) = delete;
        ServerLog& operator=(const ServerLog&) = delete;
        ServerLog(ServerLog&&) = default;
        ServerLog& operator=(ServerLog&&) = default;
        ~ServerLog();

        static void DumpToDisk();

        template<typename... Args>
        static void Debug(spdlog::format_string_t<Args...> fmt, Args&&... args) {
            ServerLog::GetInstance().mLogger_->debug(fmt, std::forward<Args>(args)...);
        }

        template<typename... Args>
        static void Info(spdlog::format_string_t<Args...> fmt, Args&&... args) {
            ServerLog::GetInstance().mLogger_->info(fmt, std::forward<Args>(args)...);
        }

        template<typename... Args>
        static void Warnning(spdlog::format_string_t<Args...> fmt, Args&&... args) {
            ServerLog::GetInstance().mLogger_->warn(fmt, std::forward<Args>(args)...);
        }

        template<typename... Args>
        static void Error(spdlog::format_string_t<Args...> fmt, Args&&... args) {
            ServerLog::GetInstance().mLogger_->error(fmt, std::forward<Args>(args)...);
        }

        template<typename... Args>
        static void Fatal(spdlog::format_string_t<Args...> fmt, Args&&... args) {
            ServerLog::GetInstance().mLogger_->critical(fmt, std::forward<Args>(args)...);
        }
    };
}// namespace foxbatdb