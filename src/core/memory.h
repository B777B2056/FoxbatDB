#pragma once
#include <list>
#include <unordered_map>
#include "common/common.h"

namespace foxbatdb {
  class ValueObject;

  class MaxMemoryPolicyAdapter {
   protected:
    StorageImpl* storage;

  public:
    MaxMemoryPolicyAdapter();
    MaxMemoryPolicyAdapter(StorageImpl* dict);
    virtual ~MaxMemoryPolicyAdapter() = default;

    void SetStorage(StorageImpl* dict);
    virtual void RemoveItem() = 0;
    virtual bool IsEmpty() const = 0;
    virtual void Put(const BinaryString& key,
                     std::shared_ptr<ValueObject> val) = 0;
    virtual void Del(const BinaryString& key) = 0;
    virtual bool Contains(const BinaryString& key) const = 0;
    virtual std::shared_ptr<ValueObject> Get(const BinaryString& key) const = 0;
  };

  class NoevictionAdapter : public MaxMemoryPolicyAdapter {
   public:
    void RemoveItem();
    bool IsEmpty() const;
    void Put(const BinaryString& key, std::shared_ptr<ValueObject> val);
    void Del(const BinaryString& key);
    bool Contains(const BinaryString& key) const;
    std::shared_ptr<ValueObject> Get(const BinaryString& key) const;
  };

  class LRUAdapter : public MaxMemoryPolicyAdapter {
  private:
    mutable std::list<BinaryString> lruList;
    mutable std::unordered_map<BinaryString, std::list<BinaryString>::iterator>
       queryMap;

    void Update(const BinaryString& key) const;

  public:
    LRUAdapter();
    void RemoveItem();
    bool IsEmpty() const;
    void Put(const BinaryString& key, std::shared_ptr<ValueObject> val);
    void Del(const BinaryString& key);
    bool Contains(const BinaryString& key) const;
    std::shared_ptr<ValueObject> Get(const BinaryString& key) const;
  };
}