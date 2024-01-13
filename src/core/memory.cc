#include "memory.h"
#include "engine.h"
#include "flag/flags.h"

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

    RecordObjectPool::RecordObjectPool() {
        for (std::size_t i = 0; i < Flags::GetInstance().memorypoolMinSize; ++i) {
            auto ptr = std::make_unique<RecordObject>();
            mFreeObjects_.emplace_back(ptr.get());
            mAllocatedObjects_.emplace_back(std::move(ptr));
        }
    }

    void RecordObjectPool::Init() {}

    RecordObjectPool& RecordObjectPool::GetInstance() {
        static RecordObjectPool instance;
        return instance;
    }

    RecordObject* RecordObjectPool::Acquire(const RecordObjectMeta& meta) {
        if (mFreeObjects_.empty()) {
            this->ExpandPoolSize();
        }

        auto freeObj = mFreeObjects_.back();
        mFreeObjects_.pop_back();

        if (freeObj) {
            freeObj->SetMeta(meta);
            return freeObj;
        }
        return {};
    }

    void RecordObjectPool::Release(RecordObject* ptr) {
        if (ptr)
            mFreeObjects_.emplace_back(ptr);
    }

    void RecordObjectPool::ExpandPoolSize() {
        auto expandSize = mAllocatedObjects_.size();// ����
        for (std::size_t i = 0; i < expandSize; ++i) {
            auto ptr = std::make_unique<RecordObject>();
            mFreeObjects_.emplace_back(ptr.get());
            mAllocatedObjects_.emplace_back(std::move(ptr));
        }
    }

    [[maybe_unused]] std::size_t RecordObjectPool::GetPoolSize() const {
        return mAllocatedObjects_.size();
    }

    [[maybe_unused]] std::size_t RecordObjectPool::GetFreeObjectCount() const {
        return mFreeObjects_.size();
    }
}// namespace foxbatdb