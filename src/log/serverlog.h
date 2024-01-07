#pragma once
#include "spdlog/spdlog.h"

namespace foxbatdb {
    class ServerLog {
    private:
        std::shared_ptr<spdlog::logger> mLogger_;
        ServerLog();

    public:
        static ServerLog& GetInstance();
        ServerLog(const ServerLog&) = delete;
        ServerLog& operator=(const ServerLog&) = delete;
        ServerLog(ServerLog&&) = default;
        ServerLog& operator=(ServerLog&&) = default;
        ~ServerLog();

        void Init();
        void DumpToDisk();

        template<typename... Args>
        void Info(spdlog::format_string_t<Args...> fmt, Args&&... args) {
            mLogger_->info(fmt, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void Warning(spdlog::format_string_t<Args...> fmt, Args&&... args) {
            mLogger_->warn(fmt, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void Error(spdlog::format_string_t<Args...> fmt, Args&&... args) {
            mLogger_->error(fmt, std::forward<Args>(args)...);
        }

        template<typename... Args>
        void Fatal(spdlog::format_string_t<Args...> fmt, Args&&... args) {
            mLogger_->critical(fmt, std::forward<Args>(args)...);
        }
    };
}// namespace foxbatdb