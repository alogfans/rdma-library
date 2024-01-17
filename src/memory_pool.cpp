#include "memory_pool.h"
#include "rdma_resources.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/mman.h>
#include <vector>
#include <mutex>

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
static const size_t kNumOfClasses = 17;
static const size_t kClassIdToSize[kNumOfClasses] = {
    1 * KB, 2 * KB, 4 * KB, 8 * KB, 16 * KB, 
    32 * KB, 64 * KB, 128 * KB, 256 * KB, 512 * KB,
    1 * MB, 2 * MB, 4 * MB, 8 * MB, 16 * MB, 
    32 * MB, 64 * MB
};

struct Region {
    void *start_addr;
    size_t segment_count;
    std::vector<int> segment_class_id;
    MemoryRegionKey key;
};

struct ListNode {
    ListNode *next;
};

static Region g_regions[kMaxMemoryPoolRegions];
static int g_region_count = 0;
static ListNode *g_free_list[kNumOfClasses] = { nullptr };
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

static int GetClassIdBySize(size_t size) {
    auto iter = std::lower_bound(kClassIdToSize, kClassIdToSize + kNumOfClasses, size);
    if (iter == kClassIdToSize + kNumOfClasses) {
        return -1; // too large
    } else {
        return iter - kClassIdToSize;
    }
}

static int GetClassIdByPtr(void *ptr) {
    if (!ptr) {
        return -1;
    }
    for (int i = 0; i < g_region_count; ++i) {
        auto &entry = g_regions[i];
        if (ptr >= (char *) entry.start_addr
            && ptr < (char *) entry.start_addr + entry.segment_count * kSegmentSize) {
            size_t segment_index = (uintptr_t(ptr) - uintptr_t(entry.start_addr)) / kSegmentSize;
            if (segment_index >= entry.segment_class_id.size()) {
                return -1; // never be allocated
            }
            return entry.segment_class_id[segment_index];
        }
    }
    return -1;
}

static int AllocateRegion(size_t size) {
    if (size % kSegmentSize) {
        size += (kSegmentSize - (size % kSegmentSize));
    }
    LOG_ASSERT(size % kSegmentSize == 0);

    if (g_region_count >= kMaxMemoryPoolRegions) {
        LOG(ERROR) << "Region count reaches the limitation "
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

    auto new_region = &g_regions[g_region_count];
    new_region->start_addr = start_addr;
    new_region->segment_count = size / kSegmentSize;
    new_region->key = key;
    new_region->segment_class_id.clear();
    g_region_count++;
    return 0;
}

int InitializeMemoryPool() {
    if (AllocateRegion(FLAGS_rdma_memory_pool_initial_size_mb * MB)) {
        LOG(ERROR) << "Failed to allocate initial region";
        return -1;
    }
    return 0;
}

void DestroyMemoryPool() {
    for (int i = 0; i < g_region_count; ++i) {
        auto entry = &g_regions[i];
        DeregisterRdmaMemoryRegion(entry->start_addr);
        munmap(entry->start_addr, entry->segment_count * kSegmentSize);
    }
}

static inline ListNode *GetListNode(void *ptr) {
    return reinterpret_cast<ListNode *>(ptr);
}

void *AllocateMemory(size_t size) {
    char *ret = nullptr;
    int class_id = GetClassIdBySize(size);
    if (class_id == -1) {
        LOG(ERROR) << "Cannot allocate large object, size: " << size;
        return nullptr;
    }
    
    if (g_free_list[class_id]) {
        ret = reinterpret_cast<char *>(g_free_list[class_id]);
        g_free_list[class_id] = g_free_list[class_id]->next;
        return ret;
    }

    int available_region = -1;
    for (int i = 0; i < g_region_count; ++i) {
        if (g_regions[i].segment_class_id.size() < g_regions[i].segment_count) {
            available_region = i;
            break;
        }
    }

    if (available_region < 0) {
        if (AllocateRegion(FLAGS_rdma_memory_pool_increase_size_mb)) {
            LOG(ERROR) << "Failed to allocate additional region";
            return nullptr;
        }
        available_region = g_region_count - 1;
    }

    auto &region = g_regions[available_region];
    size_t next_segment_id = region.segment_class_id.size();
    region.segment_class_id.push_back(class_id);
    ret = (char *) region.start_addr + kSegmentSize * next_segment_id;
    const size_t kBlockSize = kClassIdToSize[class_id];
    for (size_t j = kBlockSize; j < kSegmentSize; j += kBlockSize) {
        GetListNode(ret + j)->next = 
            j + kBlockSize == kSegmentSize ? 
                g_free_list[class_id] : 
                GetListNode(ret + j + kBlockSize);
    }
    g_free_list[class_id] = GetListNode(ret + kBlockSize);
    return ret;    
}

void FreeMemory(void *ptr) {
    int class_id = GetClassIdByPtr(ptr);
    if (class_id == -1) {
        LOG(ERROR) << "Cannot free large object";
        return;
    }

    GetListNode(ptr)->next = g_free_list[class_id];
    g_free_list[class_id] = GetListNode(ptr);
}
