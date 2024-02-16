#include "memory.h"
#include "engine.h"
#include "flag/flags.h"

namespace foxbatdb {
    void NoevictionStrategy::UpdateStateForReadOp(const std::string&) {}
    void NoevictionStrategy::UpdateStateForWriteOp(const std::string&) {}
    bool NoevictionStrategy::ReleaseKey(MemoryIndex*) { return false; }
    bool NoevictionStrategy::HaveMemoryAvailable() const { return false; }

    void LRUStrategy::Update(const std::string& key) const {
        std::unique_lock l{mt_};
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

    bool LRUStrategy::ReleaseKey(MemoryIndex* engine) {
        std::unique_lock l{mt_};
        auto& key = lruList.back();
        if (engine->Del(key)) {
            return false;
        }
        lruList.pop_back();
        queryMap.erase(key);
        return true;
    }

    bool LRUStrategy::HaveMemoryAvailable() const {
        std::unique_lock l{mt_};
        if (lruList.empty() || queryMap.empty())
            return false;
        return true;
    }

    RecordObjectPool::RecordObjectPool()
        : mMemoryPoolBuf_{},
          mMemoryPool_{mMemoryPoolBuf_.data(), mMemoryPoolBuf_.size()},
          mAllocatedObjects_{&mMemoryPool_}, mFreeObjects_{&mMemoryPool_} {
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

    std::shared_ptr<RecordObject> RecordObjectPool::Acquire(const RecordObjectMeta& meta) {
        mt_.lock();
        if (mFreeObjects_.empty()) {
            this->ExpandPoolSize();
        }

        auto freeObj = mFreeObjects_.back();
        mFreeObjects_.pop_back();
        mt_.unlock();

        if (freeObj) {
            freeObj->SetMeta(meta);
            return std::shared_ptr<RecordObject>{freeObj,
                                                 [this](RecordObject* ptr) { this->Release(ptr); }};
        }
        return {};
    }

    void RecordObjectPool::Release(RecordObject* ptr) {
        if (ptr) {
            std::unique_lock l{mt_};
            mFreeObjects_.emplace_back(ptr);
        }
    }

    void RecordObjectPool::ExpandPoolSize() {
        auto expandSize = mAllocatedObjects_.size();// ·­±¶
        for (std::size_t i = 0; i < expandSize; ++i) {
            auto ptr = std::make_unique<RecordObject>();
            mFreeObjects_.emplace_back(ptr.get());
            mAllocatedObjects_.emplace_back(std::move(ptr));
        }
    }
}// namespace foxbatdb