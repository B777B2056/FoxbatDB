#include "memory.h"
#include "engine.h"

namespace foxbatdb {
    void NoevictionStrategy::UpdateStateForReadOp(const std::string&) {}
    void NoevictionStrategy::UpdateStateForWriteOp(const std::string&) {}
    bool NoevictionStrategy::ReleaseKey(StorageEngine*) { return false; }
    bool NoevictionStrategy::HaveMemoryAvailable() const { return false; }

    void LRUStrategy::Update(const std::string& key) const {
        if (queryMap.contains(key)) {
            lruList.splice(lruList.end(), lruList,
                           queryMap.at(key));
        } else {
            lruList.push_back(key);
            queryMap[key] = std::prev(lruList.end());
        }
    }

    void LRUStrategy::UpdateStateForReadOp(const std::string& key) {
        Update(key);
    }

    void LRUStrategy::UpdateStateForWriteOp(const std::string& key) {
        Update(key);
    }

    bool LRUStrategy::ReleaseKey(StorageEngine* engine) {
        auto& key = lruList.back();
        if (engine->Del(key)) {
            return false;
        }
        lruList.pop_back();
        queryMap.erase(key);
        return true;
    }

    bool LRUStrategy::HaveMemoryAvailable() const {
        if (lruList.empty() || queryMap.empty())
            return false;
        return true;
    }
}// namespace foxbatdb