#include "connection_manager.h"
#include "rdma_endpoint.h"
#include "utils.h"

DEFINE_uint64(memory_region_mb, 256, "Memory region size in MB");
DEFINE_uint32(port, 12345, "Server port");

const static size_t kMegaBytes = 1024 * 1024;
const static int kMemoryRegionPermission = IBV_ACCESS_LOCAL_WRITE |
                                           IBV_ACCESS_REMOTE_READ |
                                           IBV_ACCESS_REMOTE_WRITE |
                                           IBV_ACCESS_REMOTE_ATOMIC;

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
    virtual void OnNewConnection(int fd) {}

    virtual void OnCloseConnection(int fd)
    {
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

    virtual int OnEstablishRC(int fd, const EndpointInfo &request,
                              EndpointInfo &response)
    {
        QueuePair *qp = new QueuePair();
        if (qp->Create(cq_) || qp->SetupRC(request.gid, request.lid, request.qp_num))
        {
            LOG(ERROR) << "Unable to create QP";
            delete qp;
            return -1;
        }
        response.gid = GetRdmaGid();
        response.lid = GetRdmaLid();
        response.qp_num = qp->GetQPNum();
        fd_to_qp_[fd].push_back(qp);
        return 0;
    }

private:
    std::map<int, std::vector<QueuePair *>> fd_to_qp_;
    CompletionQueue cq_;
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

    void *region = AllocateHugeMemory(FLAGS_memory_region_mb * kMegaBytes);
    if (!region)
    {
        PLOG(ERROR) << "Unable to allocate memory region";
        exit(EXIT_FAILURE);
    }

    auto key = RegisterRdmaMemoryRegion(region, FLAGS_memory_region_mb * kMegaBytes, kMemoryRegionPermission);
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
