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
        /* ������д�������ζ��� */
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

    class OperationLog {
    private:
        std::ofstream mFile_;
        detail::RingBuffer<std::string> mCmdBuffer_;

        OperationLog();
        void WriteAllCommands();// �ӻ��������ȡ������д���ˢ��os�ں��ļ�д������

    public:
        OperationLog(const OperationLog&) = delete;
        OperationLog& operator=(const OperationLog&) = delete;
        ~OperationLog();
        static OperationLog& GetInstance();
        void Init();
        void AppendCommand(std::string&& cmd);
        void DumpToDisk();
    };
}// namespace foxbatdb