#include "flags.h"
#include "toml++/toml.hpp"

namespace foxbatdb {
  Flags& Flags::GetInstance() {
    static Flags instance;
    return instance;
  }

  void Flags::Init(const std::string& tomlFilePath) {
    LoadFromConf(tomlFilePath);
    Preprocess();
  }

  void Flags::LoadFromConf(const std::string& tomlFilePath) {
    auto tbl = toml::parse_file(tomlFilePath);

    this->port = tbl["startup"]["listenPort"].value<std::uint16_t>().value();
    this->dbMaxNum = tbl["startup"]["databaseNumber"].value<std::uint8_t>().value();

    this->logWriteCronJobPeriodMs = tbl["aof"]["aofCronJobPeriodMs"].value<std::int64_t>().value();
    this->logFileName = tbl["aof"]["aofLogFilePath"].value<std::string>().value();

    this->dbFileDir = tbl["dbfile"]["dbFileDirectory"].value<std::string>().value();
    this->dbFileMaxSize = tbl["dbfile"]["dbFileMaxSizeMB"].value<std::uint64_t>().value();

    this->keyMaxBytes = tbl["keyval"]["keyMaxBytes"].value<std::uint32_t>().value();
    this->valMaxBytes = tbl["keyval"]["valueMaxBytes"].value<std::uint32_t>().value();
  }

  void Flags::Preprocess() {
    if (dbFileDir.back() == '/') {
      dbFileDir.pop_back();
    } 
    dbFileMaxSize = dbFileMaxSize * 1024 * 1024;
  }
}