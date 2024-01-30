// utils.h
// Copyright (C) 2024 Feng Ren

#ifndef UTILS_H
#define UTILS_H

#include <glog/logging.h>
#include <sys/time.h>
#include <cstdint>
#include <ctime>
#include <sys/mman.h>
#include <numa.h>

static inline int64_t GetCurrentTimeNanos()
{
    const int64_t kNanosPerSecond = 1000 * 1000 * 1000;
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts))
    {
        PLOG(ERROR) << "Failed to read real-time lock";
        return -1;
    }
    return (int64_t{ts.tv_sec} * kNanosPerSecond + int64_t{ts.tv_nsec});
}

static inline void *AllocateHugeMemory(size_t size)
{
    const int MAP_HUGE_2MB = (21 << MAP_HUGE_SHIFT);
    void *start_addr;
    start_addr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                      MAP_ANON | MAP_PRIVATE | MAP_HUGETLB | MAP_HUGE_2MB,
                      -1, 0);
    if (start_addr == MAP_FAILED)
    {
        PLOG(ERROR) << "Failed to allocate memory";
        return nullptr;
    }
    return start_addr;
}

static inline void BindCurrentThreadToCore(int core_id)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core_id, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

#endif // UTILS_H