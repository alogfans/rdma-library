#include "connection_manager.h"
#include "rdma_endpoint.h"
#include <sys/mman.h>

DEFINE_uint64(memory_region_mb, 256, "Memory region size in MB");
DEFINE_string(hostname, "localhost", "Server hostname");
DEFINE_uint32(port, 12345, "Server port");

const static size_t kMegaBytes = 1024 * 1024;

int main(int argc, char **argv) {
    ConnectionClient client;
    RdmaEndpoint endpoint;
    EndpointInfo request, response;

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
        IBV_ACCESS_LOCAL_WRITE);

    if (!key.IsValid()) {
        LOG(ERROR) << "Unable to register memory region";
        exit(-1);
    }

    if (client.Connect(FLAGS_hostname.c_str(), uint16_t(FLAGS_port))) {
        LOG(ERROR) << "Unable to connect to server";
        exit(-1);
    }

    if (endpoint.Create()) {
        LOG(ERROR) << "Unable to create endpoint";
        return -1;
    }

    request.gid = GetRdmaGid();
    request.lid = GetRdmaLid();
    request.qp_num = endpoint.GetQPNum();

    if (client.EstablishRC(request, response)) {
        LOG(ERROR) << "client.EstablishRC failed";
        return -1;
    }

    if (endpoint.SetupRC(response.gid, response.lid, response.qp_num)) {
        LOG(ERROR) << "endpoint.SetupRC failed";
        return -1;
    }

    std::vector<MemoryRegionInfo> mr_list;
    if (client.ListMemoryRegions(mr_list) || mr_list.empty()) {
        LOG(ERROR) << "client.EstablishRC failed";
        return -1;
    }

    RdmaSendWR wr_list;
    memcpy(region, "Hello world", 12);
    if (wr_list.Write(region, mr_list[0].addr, mr_list[0].rkey, 12) ||
        endpoint.PostSend(wr_list)) {
        LOG(ERROR) << "wr_list.Write failed";
        return -1;
    }

    std::vector<ibv_wc> wc_list;
    while (wc_list.empty()) {
        if (endpoint.PollCQ(wc_list)) {
            LOG(ERROR) << "Poll CQ failed";
        }
    }

    LOG(INFO) << "Completed";
    
    endpoint.Reset();
    client.Close();
    DeregisterRdmaMemoryRegion(region);
    munmap(region, FLAGS_memory_region_mb * kMegaBytes);
    return 0;
}
