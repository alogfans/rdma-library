#include "connection_manager.h"
#include "rdma_endpoint.h"
#include "async_executor.h"
#include "helper.h"

DEFINE_uint64(memory_region_mb, 256, "Memory region size in MB");
DEFINE_uint32(port, 12345, "Server port");
DEFINE_uint32(depth, 1, "Number of concurrent requests for each thread");
DEFINE_uint32(block_size, 8, "Granularity for each request");
DEFINE_bool(rc, true, "Use reliable connection");

const static size_t kMegaBytes = 1024 * 1024;
std::atomic<int> thread_id(0);
static void *region = nullptr;

ThreadPool pool;

uint64_t roundup(uint64_t a, uint64_t b)
{
    LOG_ASSERT(b);
    return a % b == 0 ? a : a - (a % b) + b;
}

uint64_t rounddown(uint64_t a, uint64_t b)
{
    LOG_ASSERT(b);
    return a % b == 0 ? a : a - (a % b);
}

std::atomic<bool> stop;

void Receiver(QueuePair *qp, CompletionQueue *cq)
{
    BindCurrentThreadToCore(thread_id);
    ++thread_id;

    uint64_t local_operation_count = 0;
    uint64_t storage_size = roundup(FLAGS_block_size, 64);
    char *local_base = (char *)region + thread_id * storage_size * FLAGS_depth;

    uint32_t inflight_wr_count = 0;
    DefaultWorkRequest wr_list;
    while (!stop.load(std::memory_order_relaxed))
    {
        if (inflight_wr_count < FLAGS_depth)
        {
            char *local = local_base + (local_operation_count % FLAGS_depth) * storage_size;
            wr_list.Reset();
            wr_list.Recv(local, FLAGS_block_size);
            qp->Post(wr_list);
            inflight_wr_count++;
        }
        if (cq->Poll(1) == 1)
        {
            local_operation_count++;
            inflight_wr_count--;
        }
    }
}

class RdmaConnectionManager : public ConnectionManager
{
public:
    RdmaConnectionManager() {}

    virtual ~RdmaConnectionManager()
    {
        for (auto &entry : fd_to_qp_)
        {
            for (auto &endpoint : entry.second)
            {
                delete endpoint;
            }
            close(entry.first);
        }
        fd_to_qp_.clear();
    }

protected:
    virtual void OnNewConnection(int fd)
    {
        stop = false;
    }

    virtual void OnCloseConnection(int fd)
    {
        stop = true;
        for (auto &entry : receiver_list_)
        {
            entry.get();
        }
        if (!fd_to_qp_.count(fd))
        {
            return;
        }
        for (auto &qp : fd_to_qp_[fd])
        {
            delete qp;
        }
        fd_to_qp_.erase(fd);
        close(fd);
    }

    virtual int OnExchangeEndpointInfo(int fd, const EndpointInfo &request,
                                       EndpointInfo &response)
    {
        QueuePair *qp = new QueuePair();
        if (FLAGS_rc)
        {
            if (qp->Create(cq_) || qp->SetupRC(request.gid, request.lid, request.qp_num))
            {
                LOG(ERROR) << "Unable to create QP";
                delete qp;
                return -1;
            }
        }
        else
        {
            if (qp->Create(cq_, QueuePair::TRANSPORT_TYPE_UD))
            {
                LOG(ERROR) << "Unable to create QP";
                delete qp;
                return -1;
            }
        }
        response.gid = GetRdmaGid();
        response.lid = GetRdmaLid();
        response.qp_num = qp->GetQPNum();
        fd_to_qp_[fd].push_back(qp);
        auto receiver = pool.SubmitTask(&Receiver, qp, &cq_);
        receiver_list_.push_back(std::move(receiver));
        return 0;
    }

private:
    std::map<int, std::vector<QueuePair *>> fd_to_qp_;
    CompletionQueue cq_;
    std::atomic<bool> stop_;
    std::vector<std::future<void>> receiver_list_;
};

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    CreateRdmaGlobalResource();
    RdmaConnectionManager manager;
    if (FLAGS_memory_region_mb < 16)
    {
        LOG(ERROR) << "Memory region must be greater than 16MB";
        exit(EXIT_FAILURE);
    }

    region = AllocateHugeMemory(FLAGS_memory_region_mb * kMegaBytes);
    if (!region)
    {
        PLOG(ERROR) << "Unable to allocate memory region";
        exit(EXIT_FAILURE);
    }

    auto key = RegisterRdmaMemoryRegion(region, FLAGS_memory_region_mb * kMegaBytes, IBV_ACCESS_LOCAL_WRITE);
    if (!key.IsValid())
    {
        LOG(ERROR) << "Unable to register memory region";
        exit(-1);
    }

    MemoryRegionInfo mr_info;
    mr_info.addr = uint64_t(region);
    mr_info.rkey = key.rkey;
    if (manager.RegisterMemoryRegion(mr_info))
    {
        LOG(ERROR) << "Unable to register memory region";
        exit(-1);
    }

    if (manager.Listen(uint16_t(FLAGS_port)))
    {
        LOG(ERROR) << "Unable to listen socket";
        exit(-1);
    }

    LOG(INFO) << "Server starts, press 'CTRL-C' to exit";
    while (true)
    {
        manager.ProcessEvents();
    }

    manager.Close();
    DeregisterRdmaMemoryRegion(region);
    munmap(region, FLAGS_memory_region_mb * kMegaBytes);
    return 0;
}
