// async_executor.cpp
// Copyright (C) 2024 Feng Ren

#include "async_executor.h"

DEFINE_int32(pool_idle_worker_timeout, 10000,
             "Shutdown a worker if it does not receive tasks during the period");

ThreadPool::~ThreadPool()
{
    running_.exchange(false);
    condition_variable_.notify_all();
    for (auto &worker : workers_)
    {
        LOG_ASSERT(worker.second.joinable());
        worker.second.join();
    }
}

void ThreadPool::RunWorker()
{
    while (true)
    {
        Task task;
        {
            std::unique_lock<std::mutex> unique_lock(mutex_);
            ++idle_threads_;
            auto active = condition_variable_.wait_for(
                unique_lock,
                std::chrono::milliseconds(FLAGS_pool_idle_worker_timeout),
                [this]()
                {
                    return !running_.load(std::memory_order_relaxed) || !task_queue_.empty();
                });
            --idle_threads_;
            bool running = running_.load(std::memory_order_acquire);
            if (task_queue_.empty())
            {
                if (!running)
                {
                    --current_threads_;
                    return;
                }
                else if (!active)
                {
                    --current_threads_;
                    while (!joining_tid_.empty())
                    {
                        auto thread_id = std::move(joining_tid_.front());
                        joining_tid_.pop();
                        auto iter = workers_.find(thread_id);
                        LOG_ASSERT(iter != workers_.end());
                        LOG_ASSERT(iter->second.joinable());
                        iter->second.join();
                        workers_.erase(iter);
                    }
                    joining_tid_.push(std::this_thread::get_id());
                    return;
                }
            }
            task = std::move(task_queue_.front());
            task_queue_.pop();
        }
        task();
    }
}
