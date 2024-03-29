#include "pubsub.h"
#include "frontend/server.h"
#include <algorithm>

namespace foxbatdb {
    void PubSubWithChannel::Subscribe(const std::string& channel, std::weak_ptr<CMDSession> weak) {
        if (!weak.lock())
            return;

        std::unique_lock l{mt_};
        mChannelMap_[channel].emplace_back(weak);
    }

    void PubSubWithChannel::UnSubscribe(const std::string& channel, std::weak_ptr<CMDSession> weak) {
        if (!weak.lock())
            return;

        std::unique_lock l{mt_};
        if (mChannelMap_.contains(channel)) {
            auto& list = mChannelMap_[channel];
            for (auto it = list.begin(); it != list.end();) {
                if (weak.lock() == it->lock()) {
                    it = list.erase(it);
                } else {
                    ++it;
                }
            }
            if (list.empty()) {
                mChannelMap_.erase(channel);
            }
        }
    }

    std::int32_t PubSubWithChannel::Publish(const std::string& channel, const std::string& msg) {
        std::int32_t cltNum = 0;
        std::unique_lock l{mt_};
        auto& cltList = mChannelMap_[channel];
        std::for_each(cltList.begin(), cltList.end(),
                      [&channel, &msg, &cltNum](std::weak_ptr<CMDSession> weak) -> void {
                          auto clt = weak.lock();
                          if (!clt) return;
                          clt->WritePublishMsg(channel, msg);
                          ++cltNum;
                      });
        return cltNum;
    }
}// namespace foxbatdb