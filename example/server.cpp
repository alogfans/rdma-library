#include "connection_manager.h"
#include "rdma_endpoint.h"

#include <sys/mman.h>

class RdmaConnectionManager : public ConnectionManager {
public:
    RdmaConnectionManager() {}

    virtual ~RdmaConnectionManager() {
        for (auto &entry : fd2endpoints_) {
            for (auto &endpoint : entry.second) {
                delete endpoint;
            }
            close(entry.first);
        }
        fd2endpoints_.clear();
    }

protected:
    virtual void OnNewConnection(int fd) {}

    virtual void OnCloseConnection(int fd) {
        if (!fd2endpoints_.count(fd)) {
            return;
        }
        for (auto &endpoint : fd2endpoints_[fd]) {
            delete endpoint;
        }
        fd2endpoints_.erase(fd);
        close(fd);
    }

    virtual int OnEstablishRC(int fd, const EndpointInfo &request, 
                              EndpointInfo &response)  { 
        RdmaEndpoint *endpoint = new RdmaEndpoint();
        if (endpoint->Create() 
                || endpoint->SetupRC(request.gid, request.lid, request.qp_num)) {
            LOG(ERROR) << "Unable to create endpoint";
            return -1;
        }
        response.gid = GetRdmaGid();
        response.lid = GetRdmaLid();
        response.qp_num = endpoint->GetQPNum();
        return 0;
    }

private:
    std::map<int, std::vector<RdmaEndpoint *> > fd2endpoints_;
};

DEFINE_uint64(memory_region_mb, 256, "Memory region size in MB");
DEFINE_uint32(port, 12345, "Server port");

const static size_t kMegaBytes = 1024 * 1024;

int main(int argc, char **argv) {
    RdmaConnectionManager server;
    MemoryRegionInfo mr_info;
    
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    CreateRdmaGlobalResource();
    void *region = mmap(
        nullptr, 
        FLAGS_memory_region_mb * kMegaBytes, 
        PROT_READ | PROT_WRITE, 
        MAP_ANONYMOUS | MAP_PRIVATE, 
        -1, 0);

    if (region == MAP_FAILED) {
        LOG(ERROR) << "Unable to allocate memory region";
        exit(-1);
    }

    auto key = RegisterRdmaMemoryRegion(
        region, 
        FLAGS_memory_region_mb * kMegaBytes,
        IBV_ACCESS_LOCAL_WRITE
        | IBV_ACCESS_REMOTE_READ
        | IBV_ACCESS_REMOTE_WRITE
        | IBV_ACCESS_REMOTE_ATOMIC);

    if (!key.IsValid()) {
        LOG(ERROR) << "Unable to register memory region";
        exit(-1);
    }

    mr_info.addr = uint64_t(region);
    mr_info.rkey = key.rkey;
    if (server.RegisterMemoryRegion(mr_info)) {
        LOG(ERROR) << "Unable to register memory region";
        exit(-1);
    }

    if (server.Listen(uint16_t(FLAGS_port))) {
        LOG(ERROR) << "Unable to listen socket";
        exit(-1);
    }

    while (true) {
        server.ProcessEvents();
    }

    server.Close();
    DeregisterRdmaMemoryRegion(region);
    munmap(region, FLAGS_memory_region_mb * kMegaBytes);
    return 0;
}
