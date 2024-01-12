#pragma once
#include <deque>
#include <list>
#include <memory>
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
        virtual bool ReleaseKey(StorageEngine* engine) = 0;
        [[nodiscard]] virtual bool HaveMemoryAvailable() const = 0;
    };

    class NoevictionStrategy : public MaxMemoryStrategy {
    public:
        using MaxMemoryStrategy::MaxMemoryStrategy;
        void UpdateStateForReadOp(const std::string&) override;
        void UpdateStateForWriteOp(const std::string&) override;
        bool ReleaseKey(StorageEngine*) override;
        [[nodiscard]] bool HaveMemoryAvailable() const override;
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
        bool ReleaseKey(StorageEngine* engine) override;
        bool HaveMemoryAvailable() const override;
    };

    class RecordObject;
    struct RecordObjectMeta;

    class RecordObjectPool {
    private:
        std::deque<std::shared_ptr<RecordObject>> mAllocatedObjects_;
        std::deque<std::weak_ptr<RecordObject>> mFreeObjects_;

        RecordObjectPool();
        void ExpandPoolSize();

    public:
        RecordObjectPool(const RecordObjectPool&) = delete;
        RecordObjectPool& operator=(const RecordObjectPool&) = delete;
        RecordObjectPool(RecordObjectPool&&) noexcept = default;
        RecordObjectPool& operator=(RecordObjectPool&&) = default;
        ~RecordObjectPool() = default;

        void Init();
        static RecordObjectPool& GetInstance();

        std::weak_ptr<RecordObject> Acquire(RecordObjectMeta&& meta);
        void Release(std::weak_ptr<RecordObject> ptr);

        [[maybe_unused]] [[nodiscard]] std::size_t GetPoolSize() const;
        [[maybe_unused]] [[nodiscard]] std::size_t GetFreeObjectCount() const;
    };
}// namespace foxbatdb