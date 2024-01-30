#include "memory_pool.h"
#include "rdma_resources.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/mman.h>
#include <vector>
#include <mutex>

static const int RDMA_MEMORY_POOL_INITIAL_SIZE_MIN = 32;
static const int RDMA_MEMORY_POOL_INITIAL_SIZE_MAX = 1024 * 4;

static const int RDMA_MEMORY_POOL_INCREASE_SIZE_MIN = 32;
static const int RDMA_MEMORY_POOL_INCREASE_SIZE_MAX = 1024 * 4;

DEFINE_int32(rdma_memory_pool_initial_size_mb, 1024,
             "Initial size of memory pool for RDMA (MB)");
DEFINE_int32(rdma_memory_pool_increase_size_mb, 1024,
             "Increased size of memory pool for RDMA (MB)");
DEFINE_uint32(rdma_memory_pool_tls_cache_capacity, 128,
              "Capacity of memory pool TLS cache");
DEFINE_bool(rdma_memory_pool_verbose, false,
            "Show memory pool state in verbose");

#ifndef MAP_HUGE_2MB
#define MAP_HUGE_2MB (21 << MAP_HUGE_SHIFT)
#endif

static const int kMaxMemoryPoolRegions = 16;
static const size_t KB = 1024;
static const size_t MB = 1024 * 1024;
static const size_t kSegmentSize = 64 * MB;
static const int kNumOfClasses = 17;
static constexpr size_t kClassIdToBlockSize[kNumOfClasses] = {
    1 * KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB,
    32 * KB, 64 * KB, 128 * KB, 256 * KB, 512 * KB,
    1 * MB, 2 * MB, 4 * MB, 8 * MB, 16 * MB,
    32 * MB, 64 * MB};

static_assert(kSegmentSize == kMaxBlockSize, "");
static_assert(kSegmentSize == kClassIdToBlockSize[kNumOfClasses - 1], "");

struct FreeBlock
{
    FreeBlock *next;
};

struct Segment
{
    int class_id;
    void *start_addr;
    size_t total_blocks;        // == kSegmentSize / kClassIdToBlockSize[class_id]
    size_t allocated_blocks;    // blocks that have been allocated, even it is freed by user
    size_t active_blocks;       // blocks that used by users
    FreeBlock *free_block_list; // blocks that allocated but not active
    Segment *next;              // used in `g_empty_segment_list` or `g_partial_segment_list`
};

struct Region
{
    void *start_addr;
    size_t segment_count;
    Segment *segments;
    MemoryRegionKey key;
};

class TlsCacheBin
{
public:
    TlsCacheBin() : free_block_list_(nullptr), count_(0) {}

    void Push(FreeBlock *block)
    {
        block->next = free_block_list_;
        free_block_list_ = block;
        count_++;
    }

    FreeBlock *Pop()
    {
        if (!count_)
        {
            return nullptr;
        }
        FreeBlock *block = free_block_list_;
        free_block_list_ = block->next;
        LOG_ASSERT(block);
        return block;
    }

    size_t GetSize()
    {
        return count_;
    }

private:
    FreeBlock *free_block_list_;
    size_t count_;
};

static Region g_regions[kMaxMemoryPoolRegions];
static int g_region_count = 0;
static Segment *g_empty_segment_list = nullptr;
static Segment *g_partial_segment_list[kNumOfClasses] = {nullptr};
static std::mutex g_mutex;

thread_local TlsCacheBin tls_cache[kNumOfClasses];

static void *AllocateRawMemory(size_t size)
{
    void *start_addr;

    start_addr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                      MAP_ANON | MAP_PRIVATE | MAP_HUGETLB | MAP_HUGE_2MB,
                      -1, 0);
    if (start_addr != MAP_FAILED)
    {
        return start_addr;
    }
    PLOG(WARNING) << "Failed to allocate huge memory, use normal memory instead";

    start_addr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                      MAP_ANON | MAP_PRIVATE, -1, 0);
    if (start_addr == MAP_FAILED)
    {
        PLOG(ERROR) << "Failed to allocate memory";
        return nullptr;
    }

    return start_addr;
}

static inline int GetClassIdBySize(size_t size)
{
    auto iter = std::lower_bound(kClassIdToBlockSize, kClassIdToBlockSize + kNumOfClasses, size);
    if (iter == kClassIdToBlockSize + kNumOfClasses)
    {
        return -1; // too large
    }
    else
    {
        return iter - kClassIdToBlockSize;
    }
}

static inline Segment *GetSegmentByPtr(void *ptr)
{
    for (int i = 0; i < kMaxMemoryPoolRegions; ++i)
    {
        auto &entry = g_regions[i];
        if (ptr >= (char *)entry.start_addr && ptr < (char *)entry.start_addr + entry.segment_count * kSegmentSize)
        {
            size_t segment_index = (uintptr_t(ptr) - uintptr_t(entry.start_addr)) / kSegmentSize;
            return &entry.segments[segment_index];
        }
    }
    return nullptr;
}

static int ExtendMemoryRegion(size_t size)
{
    if (size % kSegmentSize)
    {
        size += (kSegmentSize - (size % kSegmentSize));
    }
    LOG_ASSERT(size % kSegmentSize == 0);

    if (g_region_count >= kMaxMemoryPoolRegions)
    {
        LOG(ERROR) << "Failed to create more than " << kMaxMemoryPoolRegions << " regions.";
        return -1;
    }

    void *start_addr = AllocateRawMemory(size);
    if (!start_addr)
    {
        return -1;
    }

    auto key = RegisterRdmaMemoryRegion(start_addr, size);
    if (!key.IsValid())
    {
        munmap(start_addr, size);
        return -1;
    }

    auto region = &g_regions[g_region_count];
    size_t segment_count = size / kSegmentSize;
    auto segments = new Segment[segment_count];
    memset(segments, 0, segment_count * sizeof(Segment));

    region->start_addr = start_addr;
    region->segment_count = segment_count;
    region->key = key;
    region->segments = segments;
    for (size_t i = 0; i < segment_count; ++i)
    {
        segments[i].class_id = -1;
        segments[i].start_addr = (char *)start_addr + i * kSegmentSize;
        segments[i].next = (i + 1 == segment_count) ? g_empty_segment_list : &segments[i + 1];
    }
    g_empty_segment_list = &segments[0];
    g_region_count++;

    if (FLAGS_rdma_memory_pool_verbose)
    {
        LOG(INFO) << "Allocated new memory region, start_addr: " << start_addr
                  << ", segment_count: " << segment_count;
    }

    return 0;
}

int InitializeMemoryPool()
{
    if (FLAGS_rdma_memory_pool_initial_size_mb < RDMA_MEMORY_POOL_INITIAL_SIZE_MIN || FLAGS_rdma_memory_pool_initial_size_mb > RDMA_MEMORY_POOL_INITIAL_SIZE_MAX)
    {
        LOG(ERROR) << "Invalid param: rdma_memory_pool_initial_size_mb";
        return -1;
    }

    if (FLAGS_rdma_memory_pool_increase_size_mb < RDMA_MEMORY_POOL_INCREASE_SIZE_MIN || FLAGS_rdma_memory_pool_increase_size_mb > RDMA_MEMORY_POOL_INCREASE_SIZE_MAX)
    {
        LOG(ERROR) << "Invalid param: rdma_memory_pool_increase_size_mb";
        return -1;
    }

    if (ExtendMemoryRegion(FLAGS_rdma_memory_pool_initial_size_mb * MB))
    {
        LOG(ERROR) << "Failed to allocate initial region";
        return -1;
    }

    return 0;
}

void DestroyMemoryPool()
{
    for (int i = 0; i < g_region_count; ++i)
    {
        auto region = &g_regions[i];
        DeregisterRdmaMemoryRegion(region->start_addr);
        munmap(region->start_addr, region->segment_count * kSegmentSize);
        delete[] region->segments;
    }
    g_region_count = 0;
    g_empty_segment_list = nullptr;
    for (int class_id = 0; class_id < kNumOfClasses; ++class_id)
    {
        g_partial_segment_list[class_id] = nullptr;
    }
}

static inline void CheckSegment(Segment *segment)
{
    LOG_ASSERT(segment);
    int class_id = segment->class_id;
    LOG_ASSERT(class_id >= 0 && class_id < kNumOfClasses);
    const size_t kBlockSize = kClassIdToBlockSize[class_id];
    LOG_ASSERT(segment->total_blocks * kBlockSize == kSegmentSize);
    LOG_ASSERT(segment->allocated_blocks <= segment->total_blocks);
    LOG_ASSERT(segment->active_blocks <= segment->allocated_blocks);
}

static void *AllocateBlockFromPartialSegment(Segment *segment)
{
    CheckSegment(segment);
    FreeBlock *free_block = segment->free_block_list;
    if (free_block)
    {
        segment->free_block_list = free_block->next;
        ++segment->active_blocks;
        if (FLAGS_rdma_memory_pool_verbose)
        {
            LOG(INFO) << "Allocated block " << free_block
                      << " from partial segment " << segment
                      << ", class_id: " << segment->class_id
                      << ", active_blocks: " << segment->active_blocks
                      << ", allocated_blocks: " << segment->allocated_blocks
                      << ", total_blocks: " << segment->total_blocks;
        }
        return free_block;
    }

    const size_t kBlockSize = kClassIdToBlockSize[segment->class_id];
    if (segment->allocated_blocks < segment->total_blocks)
    {
        char *ret = (char *)segment->start_addr + kBlockSize * segment->allocated_blocks;
        ++segment->allocated_blocks;
        ++segment->active_blocks;
        if (FLAGS_rdma_memory_pool_verbose)
        {
            LOG(INFO) << "Allocated block " << free_block
                      << " from partial segment " << segment
                      << ", class_id: " << segment->class_id
                      << ", active_blocks: " << segment->active_blocks
                      << ", allocated_blocks: " << segment->allocated_blocks
                      << ", total_blocks: " << segment->total_blocks;
        }
        return ret;
    }

    LOG(ERROR) << "Cannot allocate block on a partial free segment";
    return nullptr;
}

static void *AllocateBlockFromEmptySegment(Segment *segment, int class_id)
{
    segment->class_id = class_id;
    segment->total_blocks = kSegmentSize / kClassIdToBlockSize[class_id];
    segment->active_blocks = 1;
    segment->allocated_blocks = 1;
    segment->free_block_list = nullptr;
    segment->next = nullptr;
    if (FLAGS_rdma_memory_pool_verbose)
    {
        LOG(INFO) << "Allocated block " << segment->start_addr
                  << " from empty segment " << segment
                  << ", class_id: " << segment->class_id
                  << ", active_blocks: " << segment->active_blocks
                  << ", allocated_blocks: " << segment->allocated_blocks
                  << ", total_blocks: " << segment->total_blocks;
    }
    return segment->start_addr;
}

static void *TryAllocateMemoryFromPartialSegment(int class_id)
{
    Segment *segment = g_partial_segment_list[class_id];
    if (segment)
    {
        LOG_ASSERT(segment->class_id == class_id);
        void *ret = AllocateBlockFromPartialSegment(segment);
        if (segment->active_blocks == segment->total_blocks)
        {
            LOG_ASSERT(segment->class_id == class_id);
            if (FLAGS_rdma_memory_pool_verbose)
            {
                LOG(INFO) << "Remove segment " << segment
                          << " from partial segment list[" << segment->class_id << "]";
            }
            g_partial_segment_list[class_id] = segment->next;
            segment->next = nullptr;
        }

        return ret;
    }
    else
    {
        return nullptr;
    }
}

static void *TryAllocateMemoryFromEmptySegment(int class_id)
{
    if (!g_empty_segment_list)
    {
        if (ExtendMemoryRegion(FLAGS_rdma_memory_pool_increase_size_mb * MB))
        {
            LOG(ERROR) << "Failed to allocate initial region";
            return nullptr;
        }
    }

    Segment *segment = g_empty_segment_list;
    LOG_ASSERT(segment);
    g_empty_segment_list = segment->next;
    if (FLAGS_rdma_memory_pool_verbose)
    {
        LOG(INFO) << "Remove segment " << segment << " from empty segment list";
    }

    void *ret = AllocateBlockFromEmptySegment(segment, class_id);
    if (segment->active_blocks < segment->total_blocks)
    {
        segment->next = g_partial_segment_list[class_id];
        g_partial_segment_list[class_id] = segment;
        if (FLAGS_rdma_memory_pool_verbose)
        {
            LOG(INFO) << "Insert segment " << segment
                      << " to partial segment list[" << segment->class_id << "]";
        }
    }

    return ret;
}

void *AllocateMemory(size_t size)
{
    int class_id = GetClassIdBySize(size);
    if (class_id == -1)
    {
        LOG(ERROR) << "Cannot allocate large object, request size: " << size;
        return nullptr;
    }

    if (tls_cache[class_id].GetSize())
    {
        return tls_cache[class_id].Pop();
    }

    std::lock_guard<std::mutex> lock(g_mutex);
    void *ret = TryAllocateMemoryFromPartialSegment(class_id);
    if (ret)
    {
        return ret;
    }

    return TryAllocateMemoryFromEmptySegment(class_id);
}

static inline FreeBlock *GetFreeBlock(Segment *segment, void *ptr)
{
    const size_t kBlockSize = kClassIdToBlockSize[segment->class_id];
    uintptr_t aligned_offset =
        (uintptr_t(ptr) - uintptr_t(segment->start_addr)) & (kBlockSize - 1);
    return (FreeBlock *)((char *)segment->start_addr + aligned_offset);
}

static void DoFreeMemory(FreeBlock *free_block)
{
    Segment *segment = GetSegmentByPtr(free_block);
    free_block->next = segment->free_block_list;
    segment->free_block_list = free_block;
    --segment->active_blocks;
    if (segment->active_blocks == 0)
    {
        // Insert to empty segment list as there are no active blocks
        segment->class_id = -1;
        segment->next = g_empty_segment_list;
        g_empty_segment_list = segment;
        if (FLAGS_rdma_memory_pool_verbose)
        {
            LOG(INFO) << "Insert segment " << segment
                      << " to empty segment list";
        }
    }
    else if (segment->active_blocks == segment->total_blocks - 1)
    {
        // Insert to partial segment list because it is previously full
        auto &partial_segment_list = g_partial_segment_list[segment->class_id];
        segment->next = partial_segment_list;
        partial_segment_list = segment;
        if (FLAGS_rdma_memory_pool_verbose)
        {
            LOG(INFO) << "Insert segment " << segment
                      << " to partial segment list[" << segment->class_id << "]";
        }
    }
}

void FreeMemory(void *ptr)
{
    Segment *segment = GetSegmentByPtr(ptr);
    if (!segment)
    {
        LOG(ERROR) << "Cannot find matched segment";
        return;
    }

    CheckSegment(segment);
    FreeBlock *free_block = GetFreeBlock(segment, ptr);
    int class_id = segment->class_id;
    auto &cache = tls_cache[class_id];
    cache.Push(free_block);
    if (cache.GetSize() > FLAGS_rdma_memory_pool_tls_cache_capacity)
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        while ((free_block = cache.Pop()) != nullptr)
        {
            DoFreeMemory(free_block);
        }
    }
}
