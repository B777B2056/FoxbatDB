#include "cron.h"
#include "common/flags.h"
#include "persistence/persistence.h"

namespace foxbatdb {
  CronJobManager::CronJobManager()
    : mIOContext_{},
      mLogFlushTimer_{mIOContext_} {
    AddJobs();
    mCronThread_ = std::thread{[this] { mIOContext_.run(); }};
    Start();
  }

  CronJobManager::~CronJobManager() {
    mLogFlushTimer_.Stop();
    if (mCronThread_.joinable()) {
      mCronThread_.join();
    }
  }

  CronJobManager& CronJobManager::GetInstance() {
    static CronJobManager instance;
    return instance;
  }

  void CronJobManager::AddJobs() {
    mLogFlushTimer_.SetTimeoutHandler(
      [this](const std::error_code& e) { 
        Persister::GetInstance().FlushToDisk();
      }
    );
  }

  void CronJobManager::Start() {
    mLogFlushTimer_.Start(
      std::chrono::milliseconds{foxbatdb::flags.logWriteCronJobPeriodMs});
  }

  void CronJobManager::Init() {
    return;
  }
}