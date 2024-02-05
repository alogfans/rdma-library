// rdma_resources.h
// Copyright (C) 2024 Feng Ren

#ifndef RDMA_RESOURCES_H
#define RDMA_RESOURCES_H

#include <glog/logging.h>
#include <gflags/gflags.h>
#include <infiniband/verbs.h>

#include <functional>

using OnReceiveAsyncEventCallback = std::function<void(struct ibv_async_event &)>;
using OnReceiveWorkCompletionCallback = std::function<void(struct ibv_wc &)>;

struct MemoryRegionKey
{
    MemoryRegionKey(uint32_t lkey = 0, uint32_t rkey = 0) : lkey(lkey), rkey(rkey) {}

    MemoryRegionKey(const MemoryRegionKey &rhs) : lkey(rhs.lkey), rkey(rhs.rkey) {}

    MemoryRegionKey &operator=(const MemoryRegionKey &rhs)
    {
        if (this != &rhs)
        {
            lkey = rhs.lkey;
            rkey = rhs.rkey;
        }
        return *this;
    }

    bool operator==(const MemoryRegionKey &rhs) const
    {
        return (lkey == rhs.lkey && rkey == rhs.rkey);
    }

    bool IsValid() const { return lkey && rkey; }

    uint32_t lkey;
    uint32_t rkey;
};

void CreateRdmaGlobalResource();

MemoryRegionKey RegisterRdmaMemoryRegion(void *addr, size_t length, int access = IBV_ACCESS_LOCAL_WRITE);

MemoryRegionKey GetRdmaMemoryRegion(void *buf);

void DeregisterRdmaMemoryRegion(void *addr);

void StartEventLoopThread();

void RegisterOnReceiveAsyncEventCallback(OnReceiveAsyncEventCallback &&callback);

void RegisterOnReceiveWorkCompletionCallback(OnReceiveWorkCompletionCallback &&callback);

// The following functions are typically called by librdma only

int GetRdmaCompVector();

ibv_context *GetRdmaContext();

ibv_pd *GetRdmaProtectionDomain();

ibv_comp_channel *GetRdmaCompletionChannel();

uint8_t GetRdmaPortNum();

uint8_t GetRdmaGidIndex();

ibv_gid GetRdmaGid();

uint16_t GetRdmaLid();

bool IsRdmaAvailable();

int ProcessEvents(int timeout, bool notify_cq_on_demand);

#endif // RDMA_RESOURCES_H
