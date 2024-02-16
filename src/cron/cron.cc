#include "cron.h"
#include "flag/flags.h"
#include "log/datalog.h"
#include "log/oplog.h"

namespace foxbatdb {
    namespace detail {
        RepeatedTimer::RepeatedTimer(asio::io_context& ioContext,
                                     TimeoutHandler timeoutCallback)
            : mTimer_(ioContext),
              mTimeoutCallback_(std::move(timeoutCallback)) {}

        void RepeatedTimer::SetTimeoutHandler(TimeoutHandler timeoutCallback) {
            mTimeoutCallback_ = std::move(timeoutCallback);
        }

        void RepeatedTimer::Start(std::chrono::milliseconds timeoutMS) {
            Reset(timeoutMS);
        }

        void RepeatedTimer::Stop() {
            mIsNeedStop_ = true;
        }

        void RepeatedTimer::Reset(std::chrono::milliseconds timeoutMS) {
            mTimer_.expires_from_now(timeoutMS);
            mTimer_.async_wait([this, timeoutMS](const asio::error_code& ec) {
                if (mIsNeedStop_) return;
                if (!ec && mTimeoutCallback_)
                    mTimeoutCallback_();
                this->Reset(timeoutMS);
            });
        }
    }// namespace detail

    CronJobManager::CronJobManager()
        : mIOContext_{}, mOperationLogDumpTimer_{mIOContext_}, mDataLogFileMergeTimer_{mIOContext_} {
        mWait_ = std::async(
                std::launch::async,
                [this]() -> void {
                    this->AddJobs();
                    this->Start();
                    this->mIOContext_.run();
                });
    }

    CronJobManager::~CronJobManager() {
        mOperationLogDumpTimer_.Stop();
        mWait_.wait();
    }

    CronJobManager& CronJobManager::GetInstance() {
        static CronJobManager instance;
        return instance;
    }

    void CronJobManager::AddJobs() {
        mOperationLogDumpTimer_.SetTimeoutHandler(
                []() -> void {
                    OperationLog::GetInstance().DumpToDisk();
                });
        mDataLogFileMergeTimer_.SetTimeoutHandler(
                []() -> void {
                    DataLogFileManager::GetInstance().Merge();
                });
    }

    void CronJobManager::Start() {
        mOperationLogDumpTimer_.Start(
                std::chrono::milliseconds{Flags::GetInstance().operationLogWriteCronJobPeriodMs});
        mDataLogFileMergeTimer_.Start(
                std::chrono::milliseconds{Flags::GetInstance().dbFileMergeCronJobPeriodMs});
    }

    void CronJobManager::Init() {}
}// namespace foxbatdb