#include <new>
#include "cmdline/cmdline.h"
#include "core/db.h"
#include "cron/cron.h"
#include "common/flags.h"
#include "network/access.h"

void InitFlags(int argc, char** argv) {
  cmdline::parser parser;
  parser.add<std::uint16_t>("port", 'p', "port number", false, 7698,
                            cmdline::range(1, 65535));
  parser.add<std::int64_t>("log-write-cron-job-period-ms", 't',
                           "log write cron job period ms", false, 3000,
                            cmdline::range(0, 10 * 60 * 1000));
  parser.add<std::string>("log-file-name", 'f', "log file name", false,
                          "foxbat.log");
  parser.add<std::string>("db-file-dir", 'd', "db file dir", false,
                          "db");
  parser.add<std::uint8_t>("db-max-num", 'n', "db max num", false, 16,
                            cmdline::range(1, 64));
  parser.add<std::uint64_t>("db-file-max-record-num", 'r',
                            "db file max record num", false, 1024,
                            cmdline::range(1, 1024 * 1024));
  parser.add<std::uint32_t>("key-max-bytes", 'k', "key max bytes", false, 1024,
                            cmdline::range(1, 1024 * 1024));
  parser.add<std::uint32_t>("val-max-bytes", 'v', "val max bytes", false, 1024,
                            cmdline::range(1, 1024 * 1024));
  parser.parse_check(argc, argv);

  foxbatdb::flags.port = parser.get<std::uint16_t>("port");
  foxbatdb::flags.logWriteCronJobPeriodMs = parser.get<std::int64_t>("log-write-cron-job-period-ms");
  foxbatdb::flags.logFileName = parser.get<std::string>("log-file-name");
  foxbatdb::flags.dbFileDir = parser.get<std::string>("db-file-dir");
  foxbatdb::flags.dbMaxNum = parser.get<std::uint8_t>("db-max-num");
  foxbatdb::flags.dbFileMaxRecordNum = parser.get<std::uint64_t>("db-file-max-record-num");
  foxbatdb::flags.keyMaxBytes = parser.get<std::uint32_t>("key-max-bytes");
  foxbatdb::flags.valMaxBytes = parser.get<std::uint32_t>("val-max-bytes");
}

void OutOfMemoryHandler() {
  // 所有DB进入不允许写入状态，只响应非写入命令
  foxbatdb::DatabaseManager::GetInstance().SetNonWrite();
}

void MemoryAllocRetryFunc() {
  auto& dbm = foxbatdb::DatabaseManager::GetInstance();
  if (dbm.HaveMemoryAvailable()) {
    dbm.ScanDBForReleaseMemory();
  } else {
    OutOfMemoryHandler();
  }
}

void InitMemoryAllocateHook() {
  std::set_new_handler(MemoryAllocRetryFunc);
}

void InitCronJobs() {
  foxbatdb::CronJobManager::GetInstance().Init();
}

void InitComponents() {
  InitMemoryAllocateHook();
  InitCronJobs();
}

int main(int argc, char** argv) {
  // 初始化运行参数
  InitFlags(argc, argv);
  // 初始化各组件
  InitComponents();
  // kv引擎已准备好，启动服务
  foxbatdb::DBServer{}.Run();
  return 0;
}
