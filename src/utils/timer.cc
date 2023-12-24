#include "timer.h"

namespace foxbatdb {
  namespace utils {
    RepeatedTimer::RepeatedTimer(asio::io_context& io_service)
      : io_service_(io_service), timer_(io_service_), is_running_{false} {

    }

    RepeatedTimer::RepeatedTimer(asio::io_context& io_service,
                                 TimeoutHandler timeout_handler)
      : io_service_(io_service),
        timer_(io_service_),
        is_running_{false},
        timeout_handler_(std::move(timeout_handler)) {}

    void RepeatedTimer::SetTimeoutHandler(TimeoutHandler timeout_handler) {
      timeout_handler_ = std::move(timeout_handler);
    }
    
    void RepeatedTimer::Start(std::chrono::milliseconds timeout_ms) {
      Reset(timeout_ms);
    }

    void RepeatedTimer::Stop() {
      is_running_.store(false);
    }

    void RepeatedTimer::Reset(std::chrono::milliseconds timeout_ms) {
      is_running_.store(true);
      DoSetExpired(timeout_ms);
    }

    void RepeatedTimer::DoSetExpired(std::chrono::milliseconds timeout_ms) {
      if (!is_running_.load()) {
        return;
      }

      timer_.expires_from_now(timeout_ms);
      timer_.async_wait([this, timeout_ms](const asio::error_code& ec) {
        if (ec.value() == asio::error::operation_aborted ||
            !is_running_.load()) {
          return;
        }
        if (!ec)
          timeout_handler_();
        this->DoSetExpired(timeout_ms);
      });
    }
  }
}