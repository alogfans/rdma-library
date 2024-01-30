// async_executor.h
// Copyright (C) 2024 Feng Ren

#ifndef EXECUTOR_H
#define EXECUTOR_H

#include "utils.h"

#include <glog/logging.h>
#include <unordered_map>
#include <functional>
#include <atomic>
#include <future>
#include <vector>
#include <queue>

class ThreadPool
{
    using Task = std::function<void()>;

public:
    ThreadPool()
        : running_(true),
          max_threads_(std::thread::hardware_concurrency()) {}

    ThreadPool(size_t max_threads)
        : running_(true),
          max_threads_(max_threads) {}

    ~ThreadPool();

    ThreadPool(const ThreadPool &) = delete;

    ThreadPool &operator=(const ThreadPool &) = delete;

    template <typename Func, typename... Args>
    auto SubmitTask(Func &&func, Args &&...args)
        -> std::future<decltype(func(args...))>;

    size_t GetThreadCount() const
    {
        std::lock_guard<std::mutex> guard(mutex_);
        return current_threads_;
    }

private:
    void RunWorker();

    std::atomic<bool> running_;
    size_t current_threads_ = 0;
    size_t idle_threads_ = 0;
    const size_t max_threads_;

    mutable std::mutex mutex_;
    std::condition_variable condition_variable_;
    std::queue<Task> task_queue_;
    std::unordered_map<std::thread::id, std::thread> workers_;
    std::queue<std::thread::id> joining_tid_;
};

template <typename Func, typename... Args>
auto ThreadPool::SubmitTask(Func &&func, Args &&...args)
    -> std::future<decltype(func(args...))>
{
    auto execute = std::bind(std::forward<Func>(func), std::forward<Args>(args)...);

    using ReturnType = typename std::result_of<Func(Args...)>::type;
    using PackagedTask = std::packaged_task<ReturnType()>;

    auto task = std::make_shared<PackagedTask>(std::move(execute));
    auto result = task->get_future();

    std::lock_guard<std::mutex> guard(mutex_);
    task_queue_.emplace([task]()
                        { (*task)(); });
    if (idle_threads_ > 0)
    {
        condition_variable_.notify_one();
    }
    else if (current_threads_ < max_threads_)
    {
        std::thread thread(&ThreadPool::RunWorker, this);
        workers_[thread.get_id()] = std::move(thread);
        ++current_threads_;
    }

    return result;
}

#endif // EXECUTOR_H