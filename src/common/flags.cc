#include "flags.h"
#include "cmdline/cmdline.h"
#include "common/common.h"

namespace foxbatdb {
  Flags flags;

  static void PreprocessFlags() {
    if (flags.dbFileDir.back() == '/') {
      flags.dbFileDir.pop_back();
    } 
  }

  void ParseFlags(int argc, char** argv) {
    cmdline::parser parser;
    parser.add<std::uint16_t>("port", 'p', "port number", false, 7698,
                              cmdline::range(1, 65535));
    parser.add<std::int64_t>("log-write-cron-job-period-ms", 't',
                             "log write cron job period ms", false, 3000,
                             cmdline::range(0, 10 * 60 * 1000));
    parser.add<std::string>("log-file-name", 'f', "log file name", false, "foxbat.log");
    parser.add<std::string>("db-file-dir", 'd', "db file dir", false, "db");
    parser.add<std::uint8_t>("db-max-num", 'n', "db max num", false, 16, cmdline::range(1, 64));
    parser.add<std::uint64_t>("db-file-maxsize-mb", 'r', "db file maxsize mb", false, 512_MB);
    parser.add<std::uint32_t>("key-max-bytes", 'k', "key max bytes", false, 1024);
    parser.add<std::uint32_t>("val-max-bytes", 'v', "val max bytes", false, 1024);
    parser.parse_check(argc, argv);

    flags.port = parser.get<std::uint16_t>("port");
    flags.logWriteCronJobPeriodMs =
        parser.get<std::int64_t>("log-write-cron-job-period-ms");
    flags.logFileName = parser.get<std::string>("log-file-name");
    flags.dbFileDir = parser.get<std::string>("db-file-dir");
    flags.dbMaxNum = parser.get<std::uint8_t>("db-max-num");
    flags.dbFileMaxSize =
        parser.get<std::uint64_t>("db-file-maxsize-mb");
    flags.keyMaxBytes = parser.get<std::uint32_t>("key-max-bytes");
    flags.valMaxBytes = parser.get<std::uint32_t>("val-max-bytes");

    PreprocessFlags();
  }
}