// async_executor_test.cpp
// Copyright (C) 2024 Feng Ren

#include "async_executor.h"

#include <thread>
#include <gtest/gtest.h>

std::mutex coutMtx; // protect std::cout

void task(int taskId)
{
    {
        std::lock_guard<std::mutex> guard(coutMtx);
        std::cout << "task-" << taskId << " begin!" << std::endl;
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
    {
        std::lock_guard<std::mutex> guard(coutMtx);
        std::cout << "task-" << taskId << " end!" << std::endl;
    }
}

void monitor(const ThreadPool &pool, int seconds)
{
    for (int i = 1; i < seconds * 10; ++i)
    {
        {
            std::lock_guard<std::mutex> guard(coutMtx);
            std::cout << "thread num: " << pool.GetThreadCount() << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

TEST(async_executor, thread_pool_test)
{
    ThreadPool pool;
    pool.SubmitTask(monitor, std::ref(pool), 13);
    for (int taskId = 0; taskId < 10; ++taskId)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        pool.SubmitTask(task, taskId);
    }
}

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
