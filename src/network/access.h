#pragma once
#include <cstdint>
#include <memory>
#include "cmd.h"
#include "flag/flags.h"
#include "asio/asio.hpp"

namespace foxbatdb {

template <typename Session>
class TCPServer {
 public:
  TCPServer()
      : mIOContext_{},
        mAcceptor_{mIOContext_, asio::ip::tcp::endpoint(asio::ip::tcp::v4(),
                                                        Flags::GetInstance().port)} {
    this->DoAccept();
  }

  TCPServer(const TCPServer&) = delete;
  TCPServer(TCPServer&&) = default;
  ~TCPServer() = default;

  TCPServer& operator=(const TCPServer&) = delete;
  TCPServer& operator=(TCPServer&&) = default;

  void Run() { this->mIOContext_.run(); }

 private:
  asio::io_context mIOContext_;
  asio::ip::tcp::acceptor mAcceptor_;

  void DoAccept() {
    this->mAcceptor_.async_accept(
        [this](std::error_code ec, asio::ip::tcp::socket socket) {
          if (!ec) {
            std::make_shared<Session>(std::move(socket))->Start();
          }
          this->DoAccept();
        }
    );
  }
};

using DBServer = TCPServer<CMDSession>;

}  // namespace foxbatdb