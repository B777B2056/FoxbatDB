#pragma once
#include <atomic>
#include <chrono>
#include <functional>
#include "asio/asio.hpp"

namespace foxbatdb {
  namespace utils {
    class RepeatedTimer final {
     public:
      using TimeoutHandler = std::function<void()>;

      RepeatedTimer(asio::io_context& io_service);
      RepeatedTimer(asio::io_context& io_service, TimeoutHandler timeout_handler);

      void SetTimeoutHandler(TimeoutHandler timeout_handler);
      void Start(std::chrono::milliseconds timeout_ms);
      void Stop();
      void Reset(std::chrono::milliseconds timeout_ms);

     private:
      void DoSetExpired(std::chrono::milliseconds timeout_ms);

     private:
      asio::io_context& io_service_;
      asio::steady_timer timer_;
      std::atomic<bool> is_running_;
      TimeoutHandler timeout_handler_;
    };
  }
}