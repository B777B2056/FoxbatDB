#pragma once
#include <cstdint>
#include "common/common.h"

namespace foxbatdb {
  class PubSubWithChannel {
  private:
    PubSubChannelMap mChannelMap_;

  public:
    void Subscribe(const BinaryString& channel, CMDSessionPtr weak);
    void UnSubscribe(const BinaryString& channel, CMDSessionPtr weak);
    std::int32_t Publish(const BinaryString& channel, const BinaryString& msg);
  };
}