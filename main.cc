#include <new>
#include "cmdline.h"
#include "core/db.h"
#include "cron/cron.h"
#include "flag/flags.h"
#include "log/datalog.h"
#include "frontend/server.h"

static std::string flagConfPath;

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

void ParseArgs(int argc, char** argv) {
  cmdline::parser parser;
  parser.add<std::string>("flag-conf-path", 'f', "flag conf path", false, "config/flag.toml");
  parser.parse_check(argc, argv);
  flagConfPath = parser.get<std::string>("flag-conf-path");
}

void InitComponents() {
  std::set_new_handler(MemoryAllocRetryFunc);
  foxbatdb::Flags::GetInstance().Init(flagConfPath);
  foxbatdb::DatabaseManager::GetInstance().Init();
  foxbatdb::DataLogFileManager::GetInstance().Init();
  foxbatdb::CronJobManager::GetInstance().Init();
}

int main(int argc, char** argv) {
  ParseArgs(argc, argv);
  // 初始化各组件
  InitComponents();
  // kv引擎已准备好，启动服务
  foxbatdb::DBServer::GetInstance().Run();
  return 0;
}
