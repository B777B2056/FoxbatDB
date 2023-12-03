#include <new>
#include "core/db.h"
#include "core/filemanager.h"
#include "cron/cron.h"
#include "common/flags.h"
#include "network/access.h"

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

void InitComponents(int argc, char** argv) {
  foxbatdb::ParseFlags(argc, argv); // 初始化运行参数
  std::set_new_handler(MemoryAllocRetryFunc);
  foxbatdb::DatabaseManager::GetInstance().Init();
  foxbatdb::LogFileManager::GetInstance().Init();
  foxbatdb::CronJobManager::GetInstance().Init();
}

int main(int argc, char** argv) {
  // 初始化各组件
  InitComponents(argc, argv);
  // kv引擎已准备好，启动服务
  foxbatdb::DBServer{}.Run();
  return 0;
}
