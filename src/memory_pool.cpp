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

#ifndef MAP_HUGE_2MB
#define MAP_HUGE_2MB (21 << MAP_HUGE_SHIFT)
#endif

static const int kMaxMemoryPoolRegions = 16;
static const size_t KB = 1024;
static const size_t MB = 1024 * 1024;
static const size_t kSegmentSize = 64 * MB;
static const int kNumOfClasses = 17;
static const size_t kClassIdToBlockSize[kNumOfClasses] = {
    1 * KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB, 
    32 * KB, 64 * KB, 128 * KB, 256 * KB, 512 * KB,
    1 * MB, 2 * MB, 4 * MB, 8 * MB, 16 * MB, 
    32 * MB, 64 * MB
};

struct FreeBlock {
    FreeBlock *next;
};

struct Segment {
    int class_id;
    void *start_addr;
    size_t total_blocks;        // == kSegmentSize / kClassIdToBlockSize[class_id]
    size_t allocated_blocks;    // blocks that have been allocated, even it is freed by user
    size_t active_blocks;       // blocks that used by users
    FreeBlock *free_block_list;  // blocks that allocated but not active
    Segment *next;              // used in `g_empty_segment_list` or `g_partial_segment_list`
};

struct Region {
    void *start_addr;
    size_t segment_count;
    Segment *segments;
    MemoryRegionKey key;
};

static Region g_regions[kMaxMemoryPoolRegions];
static int g_region_count = 0;
static Segment *g_empty_segment_list = nullptr;
static Segment *g_partial_segment_list[kNumOfClasses] = { nullptr };

static std::mutex g_class_mutex[kNumOfClasses];
static std::mutex g_mutex;

static void *AllocateRawMemory(size_t size) {
    void *start_addr;

    start_addr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                      MAP_ANON | MAP_PRIVATE | MAP_HUGETLB | MAP_HUGE_2MB,
                      -1, 0);
    if (start_addr != MAP_FAILED) {
        return start_addr;
    }
    PLOG(WARNING) << "Failed to allocate huge memory, use normal memory instead";

    start_addr = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                      MAP_ANON | MAP_PRIVATE, -1, 0);
    if (start_addr == MAP_FAILED) {
        PLOG(ERROR) << "Failed to allocate memory";
        return nullptr;
    }

    return start_addr;
}

static inline int GetClassIdBySize(size_t size) {
    auto iter = std::lower_bound(kClassIdToBlockSize, kClassIdToBlockSize + kNumOfClasses, size);
    if (iter == kClassIdToBlockSize + kNumOfClasses) {
        return -1; // too large
    } else {
        return iter - kClassIdToBlockSize;
    }
}

static inline Segment *GetSegmentByPtr(void *ptr) {
    for (int i = 0; i < kMaxMemoryPoolRegions; ++i) {
        auto &entry = g_regions[i];
        if (ptr >= (char *) entry.start_addr
            && ptr < (char *) entry.start_addr + entry.segment_count * kSegmentSize) {
            size_t segment_index = (uintptr_t(ptr) - uintptr_t(entry.start_addr)) / kSegmentSize;
            return &entry.segments[segment_index];
        }
    }
    return nullptr;
}

static int ExtendMemoryRegion(size_t size) {
    if (size % kSegmentSize) {
        size += (kSegmentSize - (size % kSegmentSize));
    }
    LOG_ASSERT(size % kSegmentSize == 0);

    if (g_region_count >= kMaxMemoryPoolRegions) {
        LOG(ERROR) << "Region count exceeds kMaxMemoryPoolRegions "
                   << kMaxMemoryPoolRegions;
        return -1;
    }

    void *start_addr = AllocateRawMemory(size);
    if (!start_addr) {
        return -1;
    }

    auto key = RegisterRdmaMemoryRegion(start_addr, size);
    if (!key.IsValid()) {
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
    for (size_t i = 0; i < segment_count; ++i) {
        segments[i].class_id = -1;
        segments[i].start_addr = (char *) start_addr + i * kSegmentSize;
        segments[i].next = (i + 1 == segment_count) ? g_empty_segment_list : &segments[i + 1];
    }
    g_empty_segment_list = &segments[0];
    g_region_count++;
    return 0;
}

int InitializeMemoryPool() {
    if (FLAGS_rdma_memory_pool_initial_size_mb < RDMA_MEMORY_POOL_INITIAL_SIZE_MIN
        || FLAGS_rdma_memory_pool_initial_size_mb > RDMA_MEMORY_POOL_INITIAL_SIZE_MAX) {
        LOG(ERROR) << "Invalid param: rdma_memory_pool_initial_size_mb";
        return -1;
    }

    if (FLAGS_rdma_memory_pool_increase_size_mb < RDMA_MEMORY_POOL_INCREASE_SIZE_MIN
        || FLAGS_rdma_memory_pool_increase_size_mb > RDMA_MEMORY_POOL_INCREASE_SIZE_MAX) {
        LOG(ERROR) << "Invalid param: rdma_memory_pool_increase_size_mb";
        return -1;
    }

    if (ExtendMemoryRegion(FLAGS_rdma_memory_pool_initial_size_mb * MB)) {
        LOG(ERROR) << "Failed to allocate initial region";
        return -1;
    }

    return 0;
}

void DestroyMemoryPool() {
    for (int i = 0; i < g_region_count; ++i) {
        auto region = &g_regions[i];
        DeregisterRdmaMemoryRegion(region->start_addr);
        munmap(region->start_addr, region->segment_count * kSegmentSize);
        delete []region->segments;
    }
    g_region_count = 0;
    g_empty_segment_list = nullptr;
    for (int class_id = 0; class_id < kNumOfClasses; ++class_id) {
        g_partial_segment_list[class_id] = nullptr;
    }
}

static void *AllocateBlockFromPartialSegment(Segment *segment) {
    FreeBlock *free_block = segment->free_block_list;
    if (free_block) {
        segment->free_block_list = free_block->next;
        ++segment->active_blocks;
        return free_block;
    }

    const size_t kBlockSize = kClassIdToBlockSize[segment->class_id];
    if (segment->allocated_blocks < segment->total_blocks) {
        char *ret = (char *) segment->start_addr + kBlockSize * segment->allocated_blocks;
        ++segment->allocated_blocks;
        ++segment->active_blocks;
        return ret;
    }

    LOG(ERROR) << "Cannot allocate block on a partial free segment";
    return nullptr;
}


static void *AllocateBlockFromEmptySegment(Segment *segment, int class_id) {
    LOG_ASSERT(segment->class_id == -1);
    segment->class_id = class_id;
    segment->total_blocks = kSegmentSize / kClassIdToBlockSize[class_id];
    segment->active_blocks = 1;
    segment->allocated_blocks = 1;
    segment->free_block_list = nullptr;
    return segment->start_addr;
}

static void *TryAllocateMemoryFromPartialSegment(int class_id) {
    std::lock_guard<std::mutex> lock(g_class_mutex[class_id]);

    Segment *segment = g_partial_segment_list[class_id];
    if (segment) {
        LOG_ASSERT(segment->class_id == class_id);
        void *ret = AllocateBlockFromPartialSegment(segment);
        // This segment has been fully allocated
        if (segment->active_blocks == segment->total_blocks) {
            g_partial_segment_list[class_id] = segment->next;
            segment->next = nullptr;
        }

        return ret;
    } else {
        return nullptr;
    }
}

static void *TryAllocateMemoryFromEmptySegment(int class_id) {
    std::lock_guard<std::mutex> lock(g_mutex);
    
    if (!g_empty_segment_list && ExtendMemoryRegion(FLAGS_rdma_memory_pool_increase_size_mb)) {
        return nullptr;
    }

    Segment *segment = g_empty_segment_list;
    LOG_ASSERT(segment);
    g_empty_segment_list = segment->next;
    segment->next = nullptr;
    void *ret = AllocateBlockFromEmptySegment(segment, class_id);
    if (segment->active_blocks < segment->total_blocks) {
        std::lock_guard<std::mutex> class_lock(g_class_mutex[class_id]);
        segment->next = g_partial_segment_list[class_id];
        g_partial_segment_list[class_id] = segment;
    }

    return ret;
}

void *AllocateMemory(size_t size) {
    int class_id = GetClassIdBySize(size);
    if (class_id == -1) {
        LOG(ERROR) << "Cannot allocate large object, request size: " << size;
        return nullptr;
    }

    void *ret = TryAllocateMemoryFromPartialSegment(class_id);
    if (ret) {
        return ret;
    }

    return TryAllocateMemoryFromEmptySegment(class_id);
}

static inline FreeBlock *GetFreeBlock(Segment *segment, void *ptr) {
    const size_t kBlockSize = kClassIdToBlockSize[segment->class_id];
    uintptr_t aligned_offset = 
            (uintptr_t(ptr) - uintptr_t(segment->start_addr)) & (kBlockSize - 1);
    return (FreeBlock *) ((char *) segment->start_addr + aligned_offset);
}

void FreeMemory(void *ptr) {
    Segment *segment = GetSegmentByPtr(ptr);
    if (!segment) {
        LOG(ERROR) << "Cannot find matched segment";
        return;
    }

    FreeBlock *free_block = GetFreeBlock(segment, ptr);
    free_block->next = segment->free_block_list;
    segment->free_block_list = free_block;
    --segment->active_blocks;
    if (segment->active_blocks == 0) {
        std::lock_guard<std::mutex> lock(g_mutex);
        segment->class_id = -1;
        segment->next = g_empty_segment_list;
        g_empty_segment_list = segment;
    } else if (segment->active_blocks == segment->total_blocks - 1) {
        std::lock_guard<std::mutex> class_lock(g_class_mutex[segment->class_id]);
        auto &partial_segment_list = g_partial_segment_list[segment->class_id];
        segment->next = partial_segment_list;
        partial_segment_list = segment;
    }
}
