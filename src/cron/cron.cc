#include "cron.h"
#include <iostream>
#include "flag/flags.h"
#include "persistence/persistence.h"

namespace foxbatdb {
  CronJobManager::CronJobManager()
    : mIOContext_{}
    , mAOFFlushTimer_{mIOContext_} {
    AddJobs();
    mCronThread_ = std::thread{[this] { mIOContext_.run(); }};
    Start();
  }

  CronJobManager::~CronJobManager() {
    mAOFFlushTimer_.Stop();
    if (mCronThread_.joinable()) {
      mCronThread_.join();
    }
  }

  CronJobManager& CronJobManager::GetInstance() {
    static CronJobManager instance;
    return instance;
  }

  void CronJobManager::AddJobs() {
    mAOFFlushTimer_.SetTimeoutHandler(
      [this]()->void {
        Persister::GetInstance().FlushToDisk();
      }
    );
  }

  void CronJobManager::Start() {
    mAOFFlushTimer_.Start(
      std::chrono::milliseconds{Flags::GetInstance().logWriteCronJobPeriodMs}
    );
  }

  void CronJobManager::Init() {
    return;
  }
}