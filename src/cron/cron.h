#pragma once
#include "asio.hpp"
#include <atomic>
#include <chrono>
#include <functional>
#include <future>

namespace foxbatdb {
    namespace detail {
        class RepeatedTimer final {
        public:
            using TimeoutHandler = std::function<void()>;
            explicit RepeatedTimer(asio::io_context& ioContext, TimeoutHandler timeoutCallback = {});

            void SetTimeoutHandler(TimeoutHandler timeoutCallback);
            void Start(std::chrono::milliseconds timeoutMS);
            void Stop();

        private:
            void Reset(std::chrono::milliseconds timeoutMS);

        private:
            std::atomic_bool mIsNeedStop_ = false;
            asio::steady_timer mTimer_;
            TimeoutHandler mTimeoutCallback_;
        };
    }// namespace detail

    class CronJobManager {
    private:
        asio::io_context mIOContext_;
        std::future<void> mWait_;
        detail::RepeatedTimer mServerLogDumpTimer_;
        detail::RepeatedTimer mOperationLogDumpTimer_;

        CronJobManager();
        void AddJobs();
        void Start();

    public:
        CronJobManager(const CronJobManager&) = delete;
        CronJobManager& operator=(const CronJobManager&) = delete;
        ~CronJobManager();
        static CronJobManager& GetInstance();
        void Init();
    };
}// namespace foxbatdb