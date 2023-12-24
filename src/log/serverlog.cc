#include "serverlog.h"
#include <filesystem>
#include "spdlog/sinks/rotating_file_sink.h"
#include "flag/flags.h"

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
  }

  ServerLog::~ServerLog() {
    spdlog::drop_all();
  }

  ServerLog& ServerLog::GetInstance() {
    static ServerLog instance;
    return instance;
  }

  void ServerLog::DumpToDisk() {
    ServerLog::GetInstance().mLogger_->flush();
  }
}