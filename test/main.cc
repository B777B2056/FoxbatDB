#include <gtest/gtest.h>
#include <new>
#include "core/db.h"
#include "core/filemanager.h"
#include "cron/cron.h"
#include "common/flags.h"
#include "network/access.h"

using namespace foxbatdb;

static std::string flagConfPath = "/mnt/e/jr/FoxbatDB/config/flag.toml";

void OutOfMemoryHandler() {
  // ����DB���벻����д��״̬��ֻ��Ӧ��д������
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
  foxbatdb::LogFileManager::GetInstance().Init();
  foxbatdb::CronJobManager::GetInstance().Init();
}

int main(int argc, char **argv) {
  InitComponents();
  testing::InitGoogleTest(&argc, argv);
  ::testing::GTEST_FLAG(filter) = "MergeTest.*";
  return RUN_ALL_TESTS();
}