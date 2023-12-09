#include <gtest/gtest.h>
#include <new>
#include "core/db.h"
#include "core/filemanager.h"
#include "cron/cron.h"
#include "common/flags.h"
#include "network/access.h"

using namespace foxbatdb;

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

void InitComponents(int argc, char** argv) {
  ParseFlags(argc, argv);  // ��ʼ�����в���
  std::set_new_handler(MemoryAllocRetryFunc);
  DatabaseManager::GetInstance().Init();
  LogFileManager::GetInstance().Init();
  CronJobManager::GetInstance().Init();
}

int main(int argc, char **argv) {
  InitComponents(1, &argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}