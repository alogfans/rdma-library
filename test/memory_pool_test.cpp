// memory_pool_test.cpp
// Copyright (C) 2024 Feng Ren

#include "rdma_resources.h"
#include "memory_pool.h"

#include <thread>
#include <gtest/gtest.h>

TEST(memory_pool, basic_test)
{
    ASSERT_TRUE(!InitializeMemoryPool());
    void *ptr = AllocateMemory(13);
    EXPECT_TRUE(ptr != nullptr);
    memcpy(ptr, "hello world!", 13);
    void *ptr2 = AllocateMemory(13);
    EXPECT_TRUE(ptr != ptr2 && ptr2 != nullptr);
    for (int i = 0; i < 1000000; ++i)
    {
        EXPECT_TRUE(AllocateMemory(13));
    }
    DestroyMemoryPool();
}

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    CreateRdmaGlobalResource();
    return RUN_ALL_TESTS();
}
