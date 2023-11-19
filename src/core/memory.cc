#include "memory.h"
#include "obj.h"

namespace foxbatdb {
  MaxMemoryPolicyAdapter::MaxMemoryPolicyAdapter() : storage{nullptr} {

  }

  MaxMemoryPolicyAdapter::MaxMemoryPolicyAdapter(StorageImpl* dict) {
    SetStorage(dict);
  }

  void MaxMemoryPolicyAdapter::SetStorage(StorageImpl* dict) {
    storage = dict;
  }

  void NoevictionAdapter::RemoveItem() {
    return;
  }

  bool NoevictionAdapter::IsEmpty() const {
    return true;
  }

  void NoevictionAdapter::Put(const BinaryString& key,
                              std::shared_ptr<ValueObject> val) {
    storage->Add(key, val);
  }

  void NoevictionAdapter::Del(const BinaryString& key) {
    storage->Del(key);
  }

  bool NoevictionAdapter::Contains(const BinaryString& key) const {
    return storage->Contains(key);
  }

  std::shared_ptr<ValueObject> NoevictionAdapter::Get(
      const BinaryString& key) const {
    auto val = storage->GetValue(key);
    if (val.has_value()) {
      return *val;
    } else {
      return nullptr;
    }
  }

  LRUAdapter::LRUAdapter() : MaxMemoryPolicyAdapter() {}

  void LRUAdapter::Update(const BinaryString& key) const {
    if (queryMap.contains(key)) {
      lruList.splice(std::prev(lruList.end()), lruList,
                     queryMap.at(key));  // TODO£º´æÒÉ
    } else {
      lruList.push_back(key);
      queryMap[key] = std::prev(lruList.end());
    }
  }

  void LRUAdapter::RemoveItem() {
    if (IsEmpty())  return;
    auto& key = lruList.back();
    storage->Del(key);
    lruList.pop_back();
    queryMap.erase(key);
  }

  bool LRUAdapter::IsEmpty() const {
    return lruList.empty() || queryMap.empty();
  }

  void LRUAdapter::Put(const BinaryString& key,
                       std::shared_ptr<ValueObject> val) {
    Update(key);
    storage->Add(key, val);
  }

  void LRUAdapter::Del(const BinaryString& key) {
    storage->Del(key);
  }

  bool LRUAdapter::Contains(const BinaryString& key) const {
    Update(key);
    return storage->Contains(key);
  }

  std::shared_ptr<ValueObject> LRUAdapter::Get(const BinaryString& key) const {
    auto val = storage->GetValue(key);
    if (val.has_value()) {
      Update(key);
      return *val;
    } else {
      return nullptr;
    }
  }
}