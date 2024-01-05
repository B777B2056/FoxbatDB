#pragma once
#include <atomic>
#include <cstddef>
#include <fstream>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace foxbatdb {
    namespace detail {
        /* 单读单写无锁环形队列 */
        template<typename T>
        class RingBuffer {
        private:
            std::unique_ptr<T[]> data;
            std::atomic<std::size_t> head, tail;

            constexpr static std::size_t size = 1024;

        public:
            RingBuffer() : data{std::make_unique<T[]>(size)}, head{0}, tail{0} {}

            [[nodiscard]] bool IsFull() const {
                std::size_t currentTail = tail.load(std::memory_order_relaxed);
                std::size_t nextTail = (currentTail + 1) % size;
                return nextTail == head.load(std::memory_order_acquire);
            }

            [[nodiscard]] bool IsEmpty() const {
                return head.load(std::memory_order_relaxed) ==
                       tail.load(std::memory_order_acquire);
            }

            bool Enqueue(const T& item) {
                std::size_t pos = tail.load(std::memory_order_relaxed);
                if ((pos + 1) % size == head.load(std::memory_order_acquire))
                    return false;
                data[pos] = item;
                tail.store((pos + 1) % size, std::memory_order_release);
                return true;
            }

            bool Enqueue(T&& item) {
                std::size_t pos = tail.load(std::memory_order_relaxed);
                if ((pos + 1) % size == head.load(std::memory_order_acquire))
                    return false;
                data[pos] = std::move(item);
                tail.store((pos + 1) % size, std::memory_order_release);
                return true;
            }

            bool Dequeue(T& val) {
                std::size_t pos = head.load(std::memory_order_relaxed);
                if (pos == tail.load(std::memory_order_acquire))
                    return false;
                val = std::move(data[pos]);
                head.store((pos + 1) % size, std::memory_order_release);
                return true;
            }
        };
    }// namespace detail

    class OperationLog {
    private:
        std::jthread mThread_;
        std::ofstream mFile_;
        std::mutex mFileMutex_;
        std::atomic_flag mNeedWriteAll_;
        std::promise<void> mWriteAllBarrier_;
        detail::RingBuffer<std::string> mCmdBuffer_;

        OperationLog();
        void WriteOneCommand();// 从缓冲队列里取出一条写命令，刷入os内核文件写缓冲区
        void WriteAllCommand();// 从缓冲队列里取出所有写命令，刷入os内核文件写缓冲区

    public:
        ~OperationLog();
        static OperationLog& GetInstance();
        void Stop();
        void AppendCommand(const std::string& cmd);
        void DumpToDisk();
    };
}// namespace foxbatdb