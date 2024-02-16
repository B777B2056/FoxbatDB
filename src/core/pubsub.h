#pragma once
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace foxbatdb {
    class CMDSession;
    class PubSubWithChannel {
    private:
        mutable std::mutex mt_;
        std::unordered_map<std::string, std::list<std::weak_ptr<CMDSession>>> mChannelMap_;

    public:
        void Subscribe(const std::string& channel, std::weak_ptr<CMDSession> weak);
        void UnSubscribe(const std::string& channel, std::weak_ptr<CMDSession> weak);
        std::int32_t Publish(const std::string& channel, const std::string& msg);
    };
}// namespace foxbatdb