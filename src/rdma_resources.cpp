// rdma_resources.cpp
// Copyright (C) 2024 Feng Ren

#include "rdma_resources.h"
#include "rdma_endpoint.h"
#include "helper.h"

#include <map>
#include <atomic>
#include <vector>
#include <fcntl.h>
#include <cstdlib>
#include <iomanip>
#include <sstream>
#include <unistd.h>
#include <sys/epoll.h>
#include <unordered_map>

#define EXIT_ON_ERROR(func)                                       \
    do                                                            \
    {                                                             \
        if (func)                                                 \
        {                                                         \
            PLOG(ERROR) << "Failed to execute statement: " #func; \
            exit(1);                                              \
        }                                                         \
    } while (0)

DEFINE_string(rdma_device, "", "The name of the HCA device used "
                               "(Empty means using the first active device)");
DEFINE_int32(rdma_port, 1, "The port number to use. For RoCE, it is always 1.");
DEFINE_int32(rdma_gid_index, 1, "The GID index to use.");
DEFINE_uint32(rdma_comp_channels, 1, "Number of global completion channels.");
DEFINE_bool(rdma_memory_region_lock, false, "Enable memory region lock for concurrent manipulation.");

struct DeviceDeleter
{
    DeviceDeleter(ibv_device **devices) : devices(devices) {}
    ~DeviceDeleter() { ibv_free_device_list(devices); }
    ibv_device **devices;
};

struct RdmaGlobalResource
{
    ibv_context *context = nullptr;
    ibv_pd *pd = nullptr;
    int event_fd = -1;

    ibv_comp_channel **comp_channel = nullptr;

    RWSpinlock memory_regions_lock;
    std::vector<ibv_mr *> memory_regions;

    uint8_t port;
    uint16_t lid;
    int gid_index;
    ibv_gid gid;
};

static RdmaGlobalResource *g_rdma_resource = nullptr;
static std::atomic<int> g_comp_vector_index(0);
static std::atomic<int> g_comp_channel_index(0);
static std::atomic<bool> g_rdma_available(false);
static OnReceiveAsyncEventCallback g_on_receive_async_event_callback;
static OnReceiveWorkCompletionCallback g_on_receive_work_completion_callback;

static ibv_context *OpenRdmaDevice()
{
    int num_devices = 0;
    struct ibv_context *context = nullptr;
    struct ibv_device **devices = ibv_get_device_list(&num_devices);
    if (!devices || num_devices <= 0)
    {
        PLOG(ERROR) << "Failed to get device list";
        return nullptr;
    }
    DeviceDeleter deleter(devices);
    for (int i = 0; i < num_devices; ++i)
    {
        const char *device_name = ibv_get_device_name(devices[i]);
        context = ibv_open_device(devices[i]);
        if (!context)
        {
            PLOG(WARNING) << "Failed to open device " << device_name;
            continue;
        }

        ibv_port_attr attr;
        if (ibv_query_port(context, uint8_t(FLAGS_rdma_port), &attr))
        {
            PLOG(WARNING) << "Fail to query port " << FLAGS_rdma_port << " on " << device_name;
            ibv_close_device(context);
            continue;
        }

        if (attr.state != IBV_PORT_ACTIVE)
        {
            LOG(WARNING) << "Device " << device_name << " port not active";
            ibv_close_device(context);
            continue;
        }

        g_rdma_resource->lid = attr.lid;
        g_rdma_resource->port = uint8_t(FLAGS_rdma_port);
        g_rdma_resource->gid_index = FLAGS_rdma_gid_index;
        if (ibv_query_gid(context, g_rdma_resource->port,
                          g_rdma_resource->gid_index, &g_rdma_resource->gid))
        {
            LOG(WARNING) << "Device " << device_name
                         << " GID " << FLAGS_rdma_gid_index << " not available";
            ibv_close_device(context);
            continue;
        }

        if (FLAGS_rdma_device.empty() || FLAGS_rdma_device == device_name)
        {
            return context;
        }
        else
        {
            LOG(INFO) << "Device name not match: " << context->device->name
                      << " vs " << FLAGS_rdma_device;
            ibv_close_device(context);
        }
    }
    return nullptr;
}

static void DestroyRdmaGlobalResource()
{
    for (auto &entry : g_rdma_resource->memory_regions)
        ibv_dereg_mr(entry);
    g_rdma_resource->memory_regions.clear();
    if (g_rdma_resource->event_fd >= 0)
    {
        close(g_rdma_resource->event_fd);
        g_rdma_resource->event_fd = -1;
    }
    if (g_rdma_resource->comp_channel)
    {
        for (size_t i = 0; i < FLAGS_rdma_comp_channels; ++i)
        {
            if (g_rdma_resource->comp_channel[i])
                ibv_destroy_comp_channel(g_rdma_resource->comp_channel[i]);
            delete[] g_rdma_resource->comp_channel;
            g_rdma_resource->comp_channel = nullptr;
        }
    }
    if (g_rdma_resource->pd)
    {
        ibv_dealloc_pd(g_rdma_resource->pd);
        g_rdma_resource->pd = nullptr;
    }
    if (g_rdma_resource->context)
    {
        ibv_close_device(g_rdma_resource->context);
        g_rdma_resource->context = nullptr;
    }
    delete g_rdma_resource;
}

static std::string GidToString()
{
    std::string gid;
    char buf[16] = {0};
    const static size_t kGidLength = 16;
    auto &gid_raw = g_rdma_resource->gid.raw;
    for (size_t i = 0; i < kGidLength; ++i)
    {
        sprintf(buf, "%02x", gid_raw[i]);
        gid += i == 0 ? buf : std::string(":") + buf;
    }
    return gid;
}

static int JoinNonblockingPollSet(int event_fd, int data_fd)
{
    epoll_event event;
    memset(&event, 0, sizeof(epoll_event));

    int flags = fcntl(data_fd, F_GETFL, 0);
    if (flags == -1)
    {
        PLOG(ERROR) << "Get F_GETFL failed";
        return -1;
    }
    if (fcntl(data_fd, F_SETFL, flags | O_NONBLOCK) == -1)
    {
        PLOG(ERROR) << "Set F_GETFL failed";
        return -1;
    }

    event.events = EPOLLIN | EPOLLET;
    event.data.fd = data_fd;
    if (epoll_ctl(event_fd, EPOLL_CTL_ADD, event.data.fd, &event))
    {
        PLOG(ERROR) << "Failed to register data fd to epoll";
        close(event_fd);
        return -1;
    }

    return 0;
}

static int SetupRdmaEventDescriptor()
{
    int event_fd = epoll_create1(0);
    if (event_fd < 0)
    {
        PLOG(ERROR) << "Failed to create epoll fd";
        return -1;
    }

    g_rdma_resource->event_fd = event_fd;

    if (JoinNonblockingPollSet(event_fd, g_rdma_resource->context->async_fd))
        return -1;

    for (size_t i = 0; i < FLAGS_rdma_comp_channels; ++i)
    {
        if (JoinNonblockingPollSet(event_fd, g_rdma_resource->comp_channel[i]->fd))
            return -1;
    }

    return 0;
}

static void CreateRdmaGlobalResourceImpl()
{
    LOG_ASSERT(!IsRdmaAvailable());
    if (FLAGS_rdma_comp_channels <= 0)
    {
        LOG(WARNING) << "--rdma_comp_channels must be greater than one.";
        FLAGS_rdma_comp_channels = 1;
    }

    g_rdma_resource = new RdmaGlobalResource();
    EXIT_ON_ERROR(ibv_fork_init());
    EXIT_ON_ERROR(atexit(DestroyRdmaGlobalResource));
    g_rdma_resource->context = OpenRdmaDevice();
    LOG_ASSERT(g_rdma_resource->context);
    LOG(INFO) << "RDMA device: " << g_rdma_resource->context->device->name
              << ", LID: " << g_rdma_resource->lid
              << ", GID: (" << FLAGS_rdma_gid_index << ") " << GidToString();
    g_rdma_resource->pd = ibv_alloc_pd(g_rdma_resource->context);
    LOG_ASSERT(g_rdma_resource->pd);
    g_rdma_resource->comp_channel = new ibv_comp_channel *[FLAGS_rdma_comp_channels];
    for (size_t i = 0; i < FLAGS_rdma_comp_channels; ++i)
    {
        g_rdma_resource->comp_channel[i] = ibv_create_comp_channel(g_rdma_resource->context);
        LOG_ASSERT(g_rdma_resource->comp_channel[i]);
    }
    EXIT_ON_ERROR(SetupRdmaEventDescriptor());

    ibv_device_attr attr;
    EXIT_ON_ERROR(ibv_query_device(g_rdma_resource->context, &attr));

    g_rdma_available.exchange(true);
}

static pthread_once_t initialize_rdma_once = PTHREAD_ONCE_INIT;

void CreateRdmaGlobalResource()
{
    EXIT_ON_ERROR(pthread_once(&initialize_rdma_once, CreateRdmaGlobalResourceImpl));
}

MemoryRegionKey RegisterRdmaMemoryRegion(void *addr, size_t length, int access)
{
    LOG_ASSERT(IsRdmaAvailable());
    ibv_mr *mr = ibv_reg_mr(g_rdma_resource->pd, addr, length, access);
    if (!mr)
    {
        PLOG(ERROR) << "Fail to register memory " << addr;
        return {0, 0};
    }

    RWSpinlock::WriteGuard guard(g_rdma_resource->memory_regions_lock,
                                 FLAGS_rdma_memory_region_lock);

    g_rdma_resource->memory_regions.push_back(mr);

    LOG(INFO) << "Memory region: " << addr << " -- " << (void *)((uintptr_t)addr + length)
              << ", Length: " << length << " (" << length / 1024 / 1024 << " MB)"
              << ", Permission: " << access << std::hex
              << ", LKey: " << mr->lkey << ", RKey: " << mr->rkey;
    return {mr->lkey, mr->rkey};
}

MemoryRegionKey GetRdmaMemoryRegion(void *buf)
{
    LOG_ASSERT(IsRdmaAvailable());
    RWSpinlock::ReadGuard guard(g_rdma_resource->memory_regions_lock,
                                FLAGS_rdma_memory_region_lock);
    for (auto iter = g_rdma_resource->memory_regions.begin();
         iter != g_rdma_resource->memory_regions.end();
         ++iter)
    {
        if ((*iter)->addr <= buf && buf < (char *)((*iter)->addr) + (*iter)->length)
        {
            return {(*iter)->lkey, (*iter)->rkey};
        }
    }
    return {0, 0};
}

void DeregisterRdmaMemoryRegion(void *addr)
{
    LOG_ASSERT(IsRdmaAvailable());
    RWSpinlock::WriteGuard guard(g_rdma_resource->memory_regions_lock,
                                 FLAGS_rdma_memory_region_lock);
    bool has_removed;
    do
    {
        has_removed = false;
        for (auto iter = g_rdma_resource->memory_regions.begin();
             iter != g_rdma_resource->memory_regions.end();
             ++iter)
        {
            if ((*iter)->addr <= addr && addr < (char *)((*iter)->addr) + (*iter)->length)
            {
                g_rdma_resource->memory_regions.erase(iter);
                has_removed = true;
                break;
            }
        }
    } while (has_removed);
}

int GetRdmaCompVector()
{
    LOG_ASSERT(IsRdmaAvailable());
    return (g_comp_vector_index++) % g_rdma_resource->context->num_comp_vectors;
}

ibv_context *GetRdmaContext()
{
    LOG_ASSERT(IsRdmaAvailable());
    return g_rdma_resource->context;
}

ibv_pd *GetRdmaProtectionDomain()
{
    LOG_ASSERT(IsRdmaAvailable());
    return g_rdma_resource->pd;
}

ibv_comp_channel *GetRdmaCompletionChannel()
{
    LOG_ASSERT(IsRdmaAvailable());
    int index = (g_comp_channel_index++) % FLAGS_rdma_comp_channels;
    return g_rdma_resource->comp_channel[index];
}

uint8_t GetRdmaPortNum()
{
    LOG_ASSERT(IsRdmaAvailable());
    return g_rdma_resource->port;
}

uint8_t GetRdmaGidIndex()
{
    LOG_ASSERT(IsRdmaAvailable());
    return g_rdma_resource->gid_index;
}

ibv_gid GetRdmaGid()
{
    LOG_ASSERT(IsRdmaAvailable());
    return g_rdma_resource->gid;
}

uint16_t GetRdmaLid()
{
    LOG_ASSERT(IsRdmaAvailable());
    return g_rdma_resource->lid;
}

bool IsRdmaAvailable()
{
    return g_rdma_available.load(std::memory_order_acquire);
}

void RegisterOnReceiveAsyncEventCallback(OnReceiveAsyncEventCallback &&callback)
{
    g_on_receive_async_event_callback = std::move(callback);
}

void RegisterOnReceiveWorkCompletionCallback(OnReceiveWorkCompletionCallback &&callback)
{
    g_on_receive_work_completion_callback = std::move(callback);
}

static int ProcessContextEvent()
{
    ibv_async_event event;
    int num_events = 0;
    while (true)
    {
        if (ibv_get_async_event(g_rdma_resource->context, &event) < 0)
        {
            if (errno != EAGAIN)
            {
                PLOG(ERROR) << "Failed to get context async event";
                return -1;
            }
            break; // There is no available context async event
        }
        LOG(INFO) << "Received context async event: " << ibv_event_type_str(event.event_type);
        if (g_on_receive_async_event_callback)
        {
            g_on_receive_async_event_callback(event);
        }
        ibv_ack_async_event(&event);
        num_events++;
    }
    return num_events;
}

static int GetAndAckEvents(ibv_comp_channel *comp_channel, std::vector<struct ibv_cq *> &cq_list)
{
    struct ibv_cq *cq;
    void *cq_context = nullptr;
    std::unordered_map<ibv_cq *, int> cq_events;
    int events = 0;
    while (true)
    {
        if (ibv_get_cq_event(comp_channel, &cq, &cq_context) < 0)
        {
            if (errno != EAGAIN)
            {
                PLOG(ERROR) << "Failed to get CQ event";
            }
            break; // There is no available CQ event
        }

        if (cq_events.count(cq))
            cq_events[cq]++;
        else
            cq_events[cq] = 1;
    }

    cq_list.clear();
    for (auto &entry : cq_events)
    {
        cq_list.push_back(entry.first);
        events += entry.second;
        ibv_ack_cq_events(entry.first, entry.second);
    }

    return events;
}

static void ProcessWorkCompletion(struct ibv_wc &wc)
{
    if (wc.status != IBV_WC_SUCCESS)
    {
        LOG(ERROR) << "Work completion error: " << ibv_wc_status_str(wc.status)
                   << " (" << wc.status << "), vendor error: " << wc.vendor_err;
    }
    if (wc.wr_id)
    {
        auto obj = reinterpret_cast<WorkRequestCallback *>(wc.wr_id);
        obj->RunCallback(wc);
    }
    else if (g_on_receive_work_completion_callback)
    {
        g_on_receive_work_completion_callback(wc);
    }
}

static int ProcessCompletionChannelEvent(ibv_comp_channel *comp_channel, bool notify_cq_on_demand)
{
    const static size_t kWorkCompEntries = 16;
    struct ibv_wc wc_list[kWorkCompEntries];
    std::vector<struct ibv_cq *> cq_list;
    int nr_events, nr_poll = 0;

    while (true)
    {
        nr_events = GetAndAckEvents(comp_channel, cq_list);
        if (nr_events <= 0)
        {
            return nr_events;
        }

        for (auto cq : cq_list)
        {
            if (notify_cq_on_demand && ibv_req_notify_cq(cq, 0))
            {
                PLOG(ERROR) << "Failed to request CQ notification";
                return -1;
            }

            do
            {
                nr_poll = ibv_poll_cq(cq, kWorkCompEntries, wc_list);
                if (nr_poll < 0)
                {
                    PLOG(ERROR) << "Failed to poll CQ";
                    return -1;
                }
                for (int i = 0; i < nr_poll; ++i)
                {
                    ProcessWorkCompletion(wc_list[i]);
                }
            } while (nr_poll != 0);
        }
    }
}

int ProcessEvents(int timeout, bool notify_cq_on_demand)
{
    LOG_ASSERT(IsRdmaAvailable());
    struct epoll_event event;
    int num_events = epoll_wait(g_rdma_resource->event_fd, &event, 1, timeout);
    if (num_events < 0)
    {
        PLOG(ERROR) << "Failed to call epoll_wait()";
        return -1;
    }
    if (num_events == 0)
    {
        return 0;
    }

    if (!(event.events & EPOLLIN))
    {
        LOG(ERROR) << "Unexpected event, fd: " << event.data.fd
                   << ", events: " << event.events;
        return -1;
    }

    if (event.data.fd == g_rdma_resource->context->async_fd)
    {
        return ProcessContextEvent();
    }

    for (size_t i = 0; i < FLAGS_rdma_comp_channels; ++i)
    {
        if (event.data.fd == g_rdma_resource->comp_channel[i]->fd)
        {
            return ProcessCompletionChannelEvent(g_rdma_resource->comp_channel[i],
                                                 notify_cq_on_demand);
        }
    }

    LOG(ERROR) << "Unexpected event, fd: " << event.data.fd
               << ", events: " << event.events;

    return -1;
}

static pthread_t g_eventloop_thread;
static std::atomic<bool> g_eventloop_running;

static void *EventLoopRunner(void *arg)
{
    int num_events = 0;

    while (g_eventloop_running.load(std::memory_order_relaxed))
    {
        num_events = ProcessEvents(100, true);
        if (num_events < 0)
        {
            LOG(WARNING) << "Process events failed";
        }
    }

    num_events = ProcessEvents(0, false); // drain remaining events
    if (num_events < 0)
    {
        LOG(WARNING) << "Process events failed";
    }

    return nullptr;
}

static void DestroyEventLoopThread()
{
    if (g_eventloop_running.exchange(false))
    {
        pthread_join(g_eventloop_thread, nullptr);
    }
}

void StartEventLoopThread()
{
    LOG_ASSERT(IsRdmaAvailable());
    EXIT_ON_ERROR(atexit(DestroyEventLoopThread));
    g_eventloop_running.store(true);
    EXIT_ON_ERROR(pthread_create(&g_eventloop_thread, nullptr, EventLoopRunner, nullptr));
}