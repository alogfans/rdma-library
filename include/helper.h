// helper.h
// Copyright (C) 2024 Feng Ren

#ifndef HELPER_H
#define HELPER_H

#include <glog/logging.h>
#include <sys/time.h>
#include <cstdint>
#include <ctime>
#include <atomic>
#include <sys/mman.h>
#include <numa.h>

#if defined(__x86_64__)
#include <immintrin.h>
#define PAUSE() _mm_pause()
#else
#define PAUSE()
#endif

class RWSpinlock
{
public:
    RWSpinlock() : lock_(0) {}

    ~RWSpinlock() {}

    RWSpinlock(const RWSpinlock &) = delete;

    RWSpinlock &operator=(const RWSpinlock &) = delete;

    void RLock()
    {
        while (true)
        {
            int64_t lock = lock_.fetch_add(1, std::memory_order_relaxed);
            if (lock >= 0)
                break;
            lock_.fetch_sub(1, std::memory_order_relaxed);
        }
        std::atomic_thread_fence(std::memory_order_acquire);
    }

    void RUnlock()
    {
        std::atomic_thread_fence(std::memory_order_release);
        int64_t lock = lock_.fetch_sub(1, std::memory_order_relaxed);
        LOG_ASSERT(lock > 0);
    }

    void WLock()
    {
        while (true)
        {
            int64_t lock = lock_.load(std::memory_order_relaxed);
            while (lock > 0)
                PAUSE();
            if (lock_.compare_exchange_weak(lock, kExclusiveLock, std::memory_order_relaxed))
                break;
        }
        std::atomic_thread_fence(std::memory_order_acquire);
    }

    void WUnlock()
    {
        while (true)
        {
            int64_t lock = lock_.load(std::memory_order_relaxed);
            LOG_ASSERT(lock < 0);
            std::atomic_thread_fence(std::memory_order_release);
            if (lock_.compare_exchange_weak(lock, 0, std::memory_order_relaxed))
                return;
        }
    }

    struct WriteGuard
    {
        WriteGuard(RWSpinlock &lock, bool enable) : lock(lock), enable(enable)
        {
            if (enable)
                lock.WLock();
        }

        WriteGuard(const WriteGuard &) = delete;

        WriteGuard &operator=(const WriteGuard &) = delete;

        ~WriteGuard()
        {
            if (enable)
                lock.WUnlock();
        }

        RWSpinlock &lock;
        bool enable;
    };

    struct ReadGuard
    {
        ReadGuard(RWSpinlock &lock, bool enable) : lock(lock), enable(enable)
        {
            if (enable)
                lock.RLock();
        }

        ReadGuard(const ReadGuard &) = delete;

        ReadGuard &operator=(const ReadGuard &) = delete;

        ~ReadGuard()
        {
            if (enable)
                lock.RUnlock();
        }

        RWSpinlock &lock;
        bool enable;
    };

private:
    const static int64_t kExclusiveLock = INT64_MIN / 2;

    std::atomic<int64_t> lock_;
    uint64_t padding_[15];
};

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

#endif // HELPER_H