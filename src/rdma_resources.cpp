// rdma_resources.cpp
// Copyright (C) 2024 Feng Ren

#include "rdma_resources.h"

#include <map>
#include <atomic>
#include <vector>
#include <fcntl.h>
#include <cstdlib>
#include <iomanip>
#include <sstream>
#include <unistd.h>
#include <sys/epoll.h>

#define DIE_IF(func)                                            \
do {                                                            \
    if (func) {                                                 \
        PLOG(ERROR) << "Failed to execute " #func;              \
        exit(1);                                                \
    }                                                           \
} while(0)

DEFINE_string(rdma_device, "", "The name of the HCA device used "
                               "(Empty means using the first active device)");
DEFINE_int32(rdma_port, 1, "The port number to use. For RoCE, it is always 1.");
DEFINE_int32(rdma_gid_index, 1, "The GID index to use.");

struct ReverseComparator {
    bool operator()(const void *lhs, const void *rhs) const {
        return lhs > rhs;
    }
};

struct RdmaGlobalResource {
    pthread_rwlock_t            rwlock;
    uint8_t                     port;
    ibv_context *               context             = nullptr;
    uint16_t                    lid;
    ibv_gid                     gid;
    int                         gid_index;
    ibv_pd *                    protection_domain   = nullptr;
    ibv_comp_channel *          completion_channel  = nullptr;
    int                         event_fd            = -1;
    std::map<void *, ibv_mr *, ReverseComparator>  memory_regions;      // start_addr -> memory region
};

static RdmaGlobalResource *g_rdma_resource = nullptr;
static std::atomic<int> g_comp_vector_index(0);
static std::atomic<bool> g_rdma_available(false);

struct DeviceDeleter {
    DeviceDeleter(ibv_device **devices) : devices(devices) {}
    ~DeviceDeleter() { ibv_free_device_list(devices); }
    ibv_device **devices;
};

static ibv_context *OpenRdmaDevice() {
    int num_devices = 0;
    struct ibv_context *context = nullptr;
    struct ibv_device **devices = ibv_get_device_list(&num_devices);
    if (!devices || num_devices <= 0) {
        PLOG(ERROR) << "Failed to get device list";
        return nullptr;
    }
    DeviceDeleter deleter(devices);
    for (int i = 0; i < num_devices; ++i) {
        const char *device_name = ibv_get_device_name(devices[i]);
        context = ibv_open_device(devices[i]);
        if (!context) {
            PLOG(WARNING) << "Failed to open device " << device_name;
            continue;
        }

        ibv_port_attr attr;
        if (ibv_query_port(context, uint8_t(FLAGS_rdma_port), &attr)) {
            PLOG(WARNING) << "Fail to query port " << FLAGS_rdma_port << " on " << device_name;
            ibv_close_device(context);
            continue;
        }

        if (attr.state != IBV_PORT_ACTIVE) {
            LOG(WARNING) << "Device " << device_name << " port not active";
            ibv_close_device(context);
            continue;
        }

        g_rdma_resource->lid = attr.lid;
        g_rdma_resource->port = uint8_t(FLAGS_rdma_port);
        g_rdma_resource->gid_index = FLAGS_rdma_gid_index;
        if (ibv_query_gid(context, g_rdma_resource->port, 
                          g_rdma_resource->gid_index, &g_rdma_resource->gid)) {
            LOG(WARNING) << "Device " << device_name
                         << " GID " << FLAGS_rdma_gid_index << " not available";
            ibv_close_device(context);
            continue;
        }

        if (FLAGS_rdma_device.empty() || FLAGS_rdma_device == device_name) {
            return context;
        } else {
            LOG(INFO) << "Device name not match: " << context->device->name
                        << " vs " << FLAGS_rdma_device;
            ibv_close_device(context);
        }
    }
    return nullptr;
}

static void DestroyRdmaGlobalResource() {
    for (auto &entry: g_rdma_resource->memory_regions) {
        ibv_dereg_mr(entry.second);
    }
    g_rdma_resource->memory_regions.clear();
    if (g_rdma_resource->event_fd >= 0) {
        close(g_rdma_resource->event_fd);
        g_rdma_resource->event_fd = -1;
    }
    if (g_rdma_resource->completion_channel) {
        ibv_destroy_comp_channel(g_rdma_resource->completion_channel);
        g_rdma_resource->completion_channel = nullptr;
    }
    if (g_rdma_resource->protection_domain) {
        ibv_dealloc_pd(g_rdma_resource->protection_domain);
        g_rdma_resource->protection_domain = nullptr;
    }
    if (g_rdma_resource->context) {
        ibv_close_device(g_rdma_resource->context);
        g_rdma_resource->context = nullptr;
    }
    pthread_rwlock_destroy(&g_rdma_resource->rwlock);
    delete g_rdma_resource;
}

static std::string GidToString() {
    std::string gid;
    char buf[16] = { 0 };
    const static size_t kGidLength = 16;
    auto &gid_raw = g_rdma_resource->gid.raw;
    for (size_t i = 0; i < kGidLength; ++i) {
        sprintf(buf, "%02x", gid_raw[i]);
        gid += i == 0 ? buf : std::string(":") + buf;
    }
    return gid;
}

static int SetFileDescriptorNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        PLOG(ERROR) << "Get F_GETFL failed";
        return -1;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        PLOG(ERROR) << "Set F_GETFL failed";
        return -1;
    }
    return 0;
}

static int SetupRdmaEventDescriptor() {
    if (SetFileDescriptorNonBlocking(g_rdma_resource->context->async_fd) 
        || SetFileDescriptorNonBlocking(g_rdma_resource->completion_channel->fd)) {
        return -1;
    }

    int event_fd = epoll_create1(0);
    if (event_fd < 0) {
        PLOG(ERROR) << "Failed to create epoll fd";
        return -1;
    }

    epoll_event event;
    memset(&event, 0, sizeof(epoll_event));
    event.events = EPOLLIN | EPOLLET;
    event.data.fd = g_rdma_resource->context->async_fd;
    if (epoll_ctl(event_fd, EPOLL_CTL_ADD, event.data.fd, &event)) {
        PLOG(ERROR) << "Failed to register comp_channel fd to epoll";
        close(event_fd);
        return -1;
    }

    event.data.fd = g_rdma_resource->completion_channel->fd;
    if (epoll_ctl(event_fd, EPOLL_CTL_ADD, event.data.fd, &event)) {
        PLOG(ERROR) << "Failed to register comp_channel fd to epoll";
        close(event_fd);
        return -1;
    }

    g_rdma_resource->event_fd = event_fd;
    return 0;
}

static void CreateRdmaGlobalResourceImpl() {
    LOG_ASSERT(!IsRdmaAvailable());
    g_rdma_resource = new RdmaGlobalResource();
    DIE_IF(pthread_rwlock_init(&g_rdma_resource->rwlock, NULL));
    DIE_IF(ibv_fork_init());
    DIE_IF(atexit(DestroyRdmaGlobalResource));
    g_rdma_resource->context = OpenRdmaDevice();
    DIE_IF(g_rdma_resource == nullptr);
    LOG(INFO) << "RDMA device: " << g_rdma_resource->context->device->name
              << ", LID: " << g_rdma_resource->lid
              << ", GID: (" << FLAGS_rdma_gid_index << ") " << GidToString();
    g_rdma_resource->protection_domain = ibv_alloc_pd(g_rdma_resource->context);
    DIE_IF(g_rdma_resource->protection_domain == nullptr);
    g_rdma_resource->completion_channel = ibv_create_comp_channel(g_rdma_resource->context);
    DIE_IF(g_rdma_resource->completion_channel == nullptr);
    DIE_IF(SetupRdmaEventDescriptor());
    
    ibv_device_attr attr;
    DIE_IF(ibv_query_device(g_rdma_resource->context, &attr));
    g_rdma_available.exchange(true);
}

static pthread_once_t initialize_rdma_once = PTHREAD_ONCE_INIT;

void CreateRdmaGlobalResource() {
    DIE_IF(pthread_once(&initialize_rdma_once, CreateRdmaGlobalResourceImpl));
}

MemoryRegionKey RegisterRdmaMemoryRegion(void *addr, size_t length, int access) {
    LOG_ASSERT(IsRdmaAvailable());
    ibv_mr *mr = ibv_reg_mr(g_rdma_resource->protection_domain, addr, length, access);
    if (!mr) {
        PLOG(ERROR) << "Fail to register memory " << addr;
        return { 0, 0 };
    }
    pthread_rwlock_wrlock(&g_rdma_resource->rwlock);
    if (g_rdma_resource->memory_regions.count(addr)) {
        LOG(WARNING) << "Memory region " << addr << " has been registered,"
                     << " the previous registration will be destroyed";
        ibv_mr *prev_mr = g_rdma_resource->memory_regions[addr];
        ibv_dereg_mr(prev_mr);
    }
    g_rdma_resource->memory_regions[addr] = mr;
    pthread_rwlock_unlock(&g_rdma_resource->rwlock);
    LOG(INFO) << "Memory region: " << addr << " -- " << (void *) ((uintptr_t) addr + length)
              << ", Length: " << length << " (" << length / 1024 / 1024 << " MB)"
              << ", Permission: " << access << std::hex
              << ", LKey: " << mr->lkey << ", RKey: " << mr->rkey;
    return { mr->lkey, mr->rkey };
}

MemoryRegionKey GetRdmaMemoryRegion(void *buf) {
    LOG_ASSERT(IsRdmaAvailable());
    ibv_mr *mr = nullptr;
    pthread_rwlock_rdlock(&g_rdma_resource->rwlock);
    auto iter = g_rdma_resource->memory_regions.lower_bound(buf);
    if (iter != g_rdma_resource->memory_regions.end()) {
        mr = iter->second;
    }
    pthread_rwlock_unlock(&g_rdma_resource->rwlock);
    if (mr && mr->addr <= buf && buf < (void *)((intptr_t(mr->addr) + mr->length))) {
        return { mr->lkey, mr->rkey };
    } else {
        LOG(ERROR) << "Memory address " << buf << " is not registered";
        return { 0, 0 };
    }
}

void DeregisterRdmaMemoryRegion(void *addr) {
    LOG_ASSERT(IsRdmaAvailable());
    pthread_rwlock_wrlock(&g_rdma_resource->rwlock);
    if (!g_rdma_resource->memory_regions.count(addr)) {
        LOG(ERROR) << "Memory address " << addr << " is not registered";
    } else {
        ibv_mr *mr = g_rdma_resource->memory_regions[addr];
        ibv_dereg_mr(mr);
        g_rdma_resource->memory_regions.erase(addr);
    }
    pthread_rwlock_unlock(&g_rdma_resource->rwlock);
}

int GetRdmaCompVector() {
    LOG_ASSERT(IsRdmaAvailable());
    return (g_comp_vector_index++) % g_rdma_resource->context->num_comp_vectors;
}

ibv_context *GetRdmaContext() {
    LOG_ASSERT(IsRdmaAvailable());
    return g_rdma_resource->context;
}

ibv_pd *GetRdmaProtectionDomain() {
    LOG_ASSERT(IsRdmaAvailable());
    return g_rdma_resource->protection_domain;
}

ibv_comp_channel *GetRdmaCompletionChannel() {
    LOG_ASSERT(IsRdmaAvailable());
    return g_rdma_resource->completion_channel;
}

uint8_t GetRdmaPortNum() {
    LOG_ASSERT(IsRdmaAvailable());
    return g_rdma_resource->port;
}

uint8_t GetRdmaGidIndex() {
    LOG_ASSERT(IsRdmaAvailable());
    return g_rdma_resource->gid_index;
}

ibv_gid GetRdmaGid() {
    LOG_ASSERT(IsRdmaAvailable());
    return g_rdma_resource->gid;
}

uint16_t GetRdmaLid() {
    LOG_ASSERT(IsRdmaAvailable());
    return g_rdma_resource->lid;
}

bool IsRdmaAvailable() {
    return g_rdma_available.load(std::memory_order_acquire);
}

int ProcessEvents() {
    LOG_ASSERT(IsRdmaAvailable());
    return -1;
}

/*
int GetAndAckEvents() {
    struct ibv_cq *cq;
    void *cq_context = nullptr;
    int events = 0;
    while (true) {
        if (ibv_get_cq_event(comp_channel_->obj_, &cq, &cq_context) < 0) {
            if (errno != EAGAIN) {
                return -1;
            }
            break;
        }
        ++events;
    }
    if (events) {
        ibv_ack_cq_events(cq, events);
    }
    return events;
}

int ProcessEvent() {
    struct epoll_event event;
    int num_events = epoll_wait(epoll_fd_, &event, 1, 1000);
    if (num_events < 0) {
        LOG_PERROR("epoll_wait");
        return -1;
    }

    if (num_events == 0) {
        return 0;
    }

    GetAndAckEvents();
    if (ibv_req_notify_cq(cq_->obj_, 0)) {
        LOG_PERROR("ibv_req_notify_cq");
    }

    // bool notified = false;
    while (true) {
        const static size_t kWCListLimit = 64;
        struct ibv_wc wc_list[kWCListLimit];
        int poll_result = ibv_poll_cq(cq_->obj_, kWCListLimit, wc_list);
        if (poll_result == 0) {
            if (GetAndAckEvents() > 0) {
                if (ibv_req_notify_cq(cq_->obj_, 0)) {
                    LOG_PERROR("ibv_req_notify_cq");
                }
                continue;
            }
            return 0;
        }

        if (poll_result < 0) {
            LOG_PERROR("ibv_poll_cq");
        }

        for (int i = 0; i < poll_result; ++i) {
            auto &entry = wc_list[i];
            if (entry.status != IBV_WC_SUCCESS) {
                fprintf(stderr, "detected wc status error: %d (%s), vendor error %x\n",
                        entry.status, ibv_wc_status_str(entry.status), entry.vendor_err);
            }
            if (entry.wr_id) {
                auto callback = (std::function<void(ibv_wc *)> *) entry.wr_id;
                (*callback)(&entry);
            }
        }
    }
}
*/
