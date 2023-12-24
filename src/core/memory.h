#pragma once
#include <list>
#include <string>
#include <unordered_map>

namespace foxbatdb {
  class StorageEngine;

  class MaxMemoryStrategy {
  public:
    MaxMemoryStrategy() = default;
    virtual ~MaxMemoryStrategy() = default;

    virtual void UpdateStateForReadOp(const std::string& key) = 0;
    virtual void UpdateStateForWriteOp(const std::string& key) = 0;
    virtual void ReleaseKey(StorageEngine* engine) = 0;
    virtual bool HaveMemoryAvailable() const = 0;
  };

  class NoevictionStrategy : public MaxMemoryStrategy {
  public:
    using MaxMemoryStrategy::MaxMemoryStrategy;
    void UpdateStateForReadOp(const std::string&) override;
    void UpdateStateForWriteOp(const std::string&) override;
    void ReleaseKey(StorageEngine*) override;
    bool HaveMemoryAvailable() const override;
  };

  class LRUStrategy : public MaxMemoryStrategy {
  private:
    mutable std::list<std::string> lruList;
    mutable std::unordered_map<std::string, std::list<std::string>::iterator> queryMap;

    void Update(const std::string& key) const;

  public:
    using MaxMemoryStrategy::MaxMemoryStrategy;
    void UpdateStateForReadOp(const std::string& key) override;
    void UpdateStateForWriteOp(const std::string& key) override;
    void ReleaseKey(StorageEngine* engine) override;
    bool HaveMemoryAvailable() const override;
  };
}