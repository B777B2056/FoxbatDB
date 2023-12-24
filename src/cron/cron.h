#pragma once
#include <thread>
#include "utils/timer.h"

namespace foxbatdb {
  class CronJobManager {
  private:
    asio::io_context mIOContext_;
    std::thread mCronThread_;
    utils::RepeatedTimer mAOFFlushTimer_;

    CronJobManager();
    void AddJobs();
    void Start();

  public:
    ~CronJobManager();
    static CronJobManager& GetInstance();
    void Init();
  };
}