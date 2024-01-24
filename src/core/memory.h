#pragma once
#include "utils/utils.h"
#include <array>
#include <deque>
#include <list>
#include <memory>
#include <memory_resource>
#include <string>
#include <unordered_map>

namespace foxbatdb {
    class MemoryIndex;

    class MaxMemoryStrategy {
    public:
        MaxMemoryStrategy() = default;
        virtual ~MaxMemoryStrategy() = default;

        virtual void UpdateStateForReadOp(const std::string& key) = 0;
        virtual void UpdateStateForWriteOp(const std::string& key) = 0;
        virtual bool ReleaseKey(MemoryIndex* engine) = 0;
        [[nodiscard]] virtual bool HaveMemoryAvailable() const = 0;
    };

    class NoevictionStrategy : public MaxMemoryStrategy {
    public:
        using MaxMemoryStrategy::MaxMemoryStrategy;
        void UpdateStateForReadOp(const std::string&) override;
        void UpdateStateForWriteOp(const std::string&) override;
        bool ReleaseKey(MemoryIndex*) override;
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
        bool ReleaseKey(MemoryIndex* engine) override;
        bool HaveMemoryAvailable() const override;
    };

    class RecordObject;
    struct RecordObjectMeta;

    using namespace utils;

    class RecordObjectPool {
    private:
        std::array<std::byte, 128_KB> mMemoryPoolBuf_;
        std::pmr::monotonic_buffer_resource mMemoryPool_;

        std::pmr::deque<std::unique_ptr<RecordObject>> mAllocatedObjects_;
        std::pmr::deque<RecordObject*> mFreeObjects_;

        RecordObjectPool();
        void ExpandPoolSize();
        void Release(RecordObject* ptr);

    public:
        RecordObjectPool(const RecordObjectPool&) = delete;
        RecordObjectPool& operator=(const RecordObjectPool&) = delete;
        RecordObjectPool(RecordObjectPool&&) noexcept = delete;
        RecordObjectPool& operator=(RecordObjectPool&&) = delete;
        ~RecordObjectPool() = default;

        void Init();
        static RecordObjectPool& GetInstance();

        std::shared_ptr<RecordObject> Acquire(const RecordObjectMeta& meta);

        [[maybe_unused]] [[nodiscard]] std::size_t GetPoolSize() const;
        [[maybe_unused]] [[nodiscard]] std::size_t GetFreeObjectCount() const;
    };
}// namespace foxbatdb