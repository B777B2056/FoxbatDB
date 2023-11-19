#include "pubsub.h"
#include <algorithm>
#include "network/cmd.h"

namespace foxbatdb {
  void PubSubWithChannel::Subscribe(const BinaryString& channel,
                                    CMDSessionPtr weak) {
    if (!weak.lock()) 
      return;

    if (!mChannelMap_.Contains(channel))
      mChannelMap_.Add(channel, {});

    mChannelMap_.GetRef(channel)->emplace_back(weak);
  }

  void PubSubWithChannel::UnSubscribe(const BinaryString& channel,
                                      CMDSessionPtr weak) {
    if (!weak.lock())
      return;

    if (mChannelMap_.Contains(channel)) {
      auto* list = mChannelMap_.GetRef(channel);
      for (auto it = list->begin(); it != list->end(); ) {
        if (weak.lock() == it->lock()) {
          it = list->erase(it);
        } else {
          ++it;
        }
      }
      if (list->empty()) {
        mChannelMap_.Del(channel);
      }
    }
  }

  std::int32_t PubSubWithChannel::Publish(const BinaryString& channel,
                                          const BinaryString& msg) {
    if (!mChannelMap_.Contains(channel))
      mChannelMap_.Add(channel, {});

    std::int32_t cltNum = 0;
    auto* cltList = mChannelMap_.GetRef(channel);
    std::for_each(cltList->begin(), cltList->end(),
      [&channel, &msg, &cltNum](CMDSessionPtr weak) -> void { 
        auto clt = weak.lock();
        if (!clt) return;
        clt->WritePublishMsg(channel, msg);
        ++cltNum;
      }
    );
    return cltNum;
  }
}