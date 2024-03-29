#pragma once
#include <atomic>
#include <cstddef>
#include <fstream>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

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

    struct Command;

    class OperationLog {
    private:
        std::ofstream mFile_;
        detail::RingBuffer<std::vector<std::string>> mCmdBuffer_;

        OperationLog();
        void WriteAllCommands();// 从缓冲队列里取出所有写命令，刷入os内核文件写缓冲区

    public:
        OperationLog(const OperationLog&) = delete;
        OperationLog& operator=(const OperationLog&) = delete;
        ~OperationLog();
        static OperationLog& GetInstance();
        void Init();
        void AppendCommand(Command&& data);
        void DumpToDisk();
    };
}// namespace foxbatdb