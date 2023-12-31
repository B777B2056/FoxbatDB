#include "serverlog.h"
#include "flag/flags.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include <filesystem>

namespace foxbatdb {
    ServerLog::ServerLog() {
        auto& flag = Flags::GetInstance();
        auto serverLogFileDir = std::filesystem::path{flag.serverLogPath}.parent_path();
        if (!std::filesystem::exists(serverLogFileDir)) {
            std::filesystem::create_directories(serverLogFileDir);
        }
        mLogger_ = spdlog::rotating_logger_mt("foxbatdb",
                                              flag.serverLogPath,
                                              flag.serverLogMaxFileSize,
                                              flag.serverLogMaxFileNumber);
        mLogger_->set_level(spdlog::level::info);
        mLogger_->flush_on(spdlog::level::warn);
        spdlog::flush_every(std::chrono::seconds{Flags::GetInstance().serverLogFlushPeriodSec});
    }

    ServerLog::~ServerLog() {
        spdlog::drop_all();
    }

    ServerLog& ServerLog::GetInstance() {
        static ServerLog instance;
        return instance;
    }

    void ServerLog::Init() {}
}// namespace foxbatdb