// rdma_resources_test.cpp
// Copyright (C) 2024 Feng Ren

#include "rdma_resources.h"
#include "rdma_endpoint.h"
#include "connection_manager.h"
#include "memory_pool.h"
#include "executor.h"

#include <thread>
#include <gtest/gtest.h>

TEST(rdma_resources, basic_test) {
    CreateRdmaGlobalResource();
    const static size_t kMemoryPoolSize = 16 * 1024 * 1024;
    char *pool[2] = { nullptr };
    MemoryRegionKey key_list[2];
    for (int i = 0; i < 2; ++i) {
        pool[i] = (char *) malloc(kMemoryPoolSize);
        ASSERT_TRUE(pool[i]);
        memset(pool[i], 0, kMemoryPoolSize);
        key_list[i] = RegisterRdmaMemoryRegion(pool[i], kMemoryPoolSize, 
                      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
        EXPECT_TRUE(key_list[i].IsValid());
    }
    for (int i = 0; i < 2; ++i) {
        auto key = GetRdmaMemoryRegion(pool[i]);
        EXPECT_TRUE(key == key_list[i]);
        key = GetRdmaMemoryRegion((char *) pool[i] + kMemoryPoolSize / 2);
        EXPECT_TRUE(key == key_list[i]);
        key = GetRdmaMemoryRegion((char *) pool[i] + kMemoryPoolSize - 1);
        EXPECT_TRUE(key == key_list[i]);
        key = GetRdmaMemoryRegion((char *) pool[i] + kMemoryPoolSize);
        EXPECT_TRUE(!key.IsValid());
        key = GetRdmaMemoryRegion((char *) pool[i] + kMemoryPoolSize + 1);
        EXPECT_TRUE(!key.IsValid());
        key = GetRdmaMemoryRegion((char *) pool[i] - 1);
        EXPECT_TRUE(!key.IsValid());
    }

    RdmaEndpoint ep[2];
    uint32_t qp_num[2];
    ASSERT_FALSE(ep[0].Create());
    ASSERT_FALSE(ep[1].Create());
    qp_num[0] = ep[0].GetQPNum();
    qp_num[1] = ep[1].GetQPNum();
    ASSERT_FALSE(ep[0].SetupRC(GetRdmaGid(), GetRdmaLid(), qp_num[1]));
    ASSERT_FALSE(ep[1].SetupRC(GetRdmaGid(), GetRdmaLid(), qp_num[0]));
    RdmaSendWR send_wr;
    strcpy(pool[0], "Hello world!");
    ASSERT_FALSE(send_wr.Write(pool[0], (uint64_t) pool[1], key_list[1].rkey, 13));
    ASSERT_FALSE(ep[0].PostSend(send_wr));
    std::vector<ibv_wc> wc_list;
    while (wc_list.empty()) {
        ASSERT_FALSE(ep[0].PollCQ(wc_list));
    }
    EXPECT_TRUE(strcmp(pool[1], "Hello world!") == 0);
    for (int i = 0; i < 2; ++i) {
        DeregisterRdmaMemoryRegion(pool[i]);
        free(pool[i]);
        pool[i] = nullptr;
    }
}

TEST(rdma_resources, connection_manager_test) {
    std::atomic<bool> running(true);
    std::thread server_func([&]{
        ConnectionManager server;
        ASSERT_FALSE(server.Listen(12345));
        while (running) {
            ASSERT_FALSE(server.ProcessEvents());
        }
        server.Close();
    });
    ConnectionClient client[5];
    for (int i = 0; i < 5; ++i) {
        ASSERT_FALSE(client[i].Connect("localhost", 12345));
        MemoryRegionInfo info;
        info.addr = (uint64_t) i;
        info.rkey = i;
        info.length = 1024;
        info.access = 0;
        ASSERT_FALSE(client[i].RegisterMemoryRegion(info));
        if (i != 0) {
            std::vector<MemoryRegionInfo> ret_mr_info;
            ASSERT_FALSE(client[0].ListMemoryRegions(ret_mr_info));
            LOG(INFO) << "ret_mr_info: " << ret_mr_info.size() << " entries";
            for (auto &entry : ret_mr_info) {
                LOG(INFO) << "addr: " << entry.addr << " rkey: " << entry.rkey 
                          << " length: " << entry.length;
            }
        }
    }
    for (int i = 0; i < 5; ++i) {
        client[i].Close();
    }
    running.store(false);
    server_func.join();
}

TEST(rdma_resources, memory_test) {
    ASSERT_TRUE(!InitializeMemoryPool());
    void *ptr = AllocateMemory(13);
    EXPECT_TRUE(ptr != nullptr);
    memcpy(ptr, "hello world!", 13);
    void *ptr2 = AllocateMemory(13);
    EXPECT_TRUE(ptr != ptr2 && ptr2 != nullptr);
    for (int i = 0; i < 1000000; ++i) {
        EXPECT_TRUE(AllocateMemory(13));
    }
    DestroyMemoryPool();
}

std::mutex coutMtx;  // protect std::cout

void task(int taskId) {    
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

void monitor(const ThreadPool &pool, int seconds) {
    for (int i = 1; i < seconds * 10; ++i) {
        {
            std::lock_guard<std::mutex> guard(coutMtx);
            std::cout << "thread num: " << pool.GetThreadCount() << std::endl;                    
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));            
    }
}

TEST(rdma_resources, thread_pool_test) {
    ThreadPool pool;
    pool.SubmitTask(monitor, std::ref(pool), 13);
    for (int taskId = 0; taskId < 10; ++taskId) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        pool.SubmitTask(task, taskId);
    }
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
