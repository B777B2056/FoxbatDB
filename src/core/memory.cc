#include "memory.h"
#include "engine.h"

namespace foxbatdb {
  void NoevictionStrategy::UpdateStateForReadOp(const std::string&) {}
  void NoevictionStrategy::UpdateStateForWriteOp(const std::string&) {}
  void NoevictionStrategy::ReleaseKey(StorageEngine*) {}
  bool NoevictionStrategy::HaveMemoryAvailable() const { return false; }

  void LRUStrategy::Update(const std::string& key) const {
    if (queryMap.contains(key)) {
      lruList.splice(std::prev(lruList.end()), lruList,
                     queryMap.at(key));  // TODO£º´æÒÉ
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

  void LRUStrategy::ReleaseKey(StorageEngine* engine) {
    auto& key = lruList.back();
    engine->Del(key);
    lruList.pop_back();
    queryMap.erase(key);
  }

  bool LRUStrategy::HaveMemoryAvailable() const { 
    if (lruList.empty() || queryMap.empty())
      return false;
    return true;
  }
}