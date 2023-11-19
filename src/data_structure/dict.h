#pragma once
#include <unordered_map>
#include <optional>
#include "binstr.h"

namespace foxbatdb {
  template<typename T>
  class Dict {
  private:
    std::unordered_map<BinaryString, T> mHashMap_;

  public:
    using Bucket = std::unordered_map<BinaryString, T>::local_iterator;

  public:
    void Add(const BinaryString& key, const T& val) { 
      mHashMap_[key] = val;
    }

    void Del(const BinaryString& key) { 
      if (!mHashMap_.contains(key)) return;
      mHashMap_.erase(key);
    }

    bool Contains(const BinaryString& key) const {
      return mHashMap_.contains(key);
    }

    std::optional<T> GetValue(const BinaryString& key) const { 
      if (!mHashMap_.contains(key))
        return {};
      return mHashMap_.at(key);
    }

    T* GetRef(const BinaryString& key) {
      if (!mHashMap_.contains(key))
        return nullptr;
      return &mHashMap_[key];
    }
  };
}