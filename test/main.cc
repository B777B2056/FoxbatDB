#include <gtest/gtest.h>
#include <new>
#include "core/db.h"
#include "log/datalog.h"
#include "cron/cron.h"
#include "flag/flags.h"
#include "frontend/server.h"

using namespace foxbatdb;

static std::string flagConfPath = "/mnt/e/jr/FoxbatDB/config/flag.toml";

void OutOfMemoryHandler() {
  // 所有DB进入不允许写入状态，只响应非写入命令
  DatabaseManager::GetInstance().SetNonWrite();
}

void MemoryAllocRetryFunc() {
  auto& dbm = DatabaseManager::GetInstance();
  if (dbm.HaveMemoryAvailable()) {
    dbm.ScanDBForReleaseMemory();
  } else {
    OutOfMemoryHandler();
  }
}

void InitComponents() {
  std::set_new_handler(MemoryAllocRetryFunc);
  foxbatdb::Flags::GetInstance().Init(flagConfPath);
  foxbatdb::DatabaseManager::GetInstance().Init();
  foxbatdb::DataLogFileManager::GetInstance().Init();
  foxbatdb::CronJobManager::GetInstance().Init();
}

int main(int argc, char **argv) {
  InitComponents();
  testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = "TxTest.*";
  return RUN_ALL_TESTS();
}