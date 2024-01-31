#include "connection_manager.h"
#include "rdma_endpoint.h"
#include "utils.h"

#include <thread>
#include <iomanip>
#include <random>

DEFINE_uint64(memory_region_mb, 256, "Memory region size in MB");
DEFINE_string(hostname, "localhost", "Server hostname");
DEFINE_uint32(port, 12345, "Server port");
DEFINE_uint32(threads, 1, "Number of test threads");
DEFINE_uint32(depth, 1, "Number of concurrent requests for each thread");
DEFINE_uint32(block_size, 8, "Granularity for each request");
DEFINE_uint32(duration, 10, "Test duration in seconds");
DEFINE_uint32(qp_count, 1, "Number of QP count for multiplexing");
DEFINE_uint32(read_percentage, 100, "Percentage of read operation [0-100]");

const static size_t kMegaBytes = 1024 * 1024;

static void *region = nullptr;
MemoryRegionInfo mr_info;

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

#define DIED_IF(expr)     \
    if ((expr))           \
    {                     \
        running_ = false; \
        break;            \
    }

class Benchmark
{
public:
    void AddEndPoint(QueuePair *qp, CompletionQueue *cq)
    {
        qp_list_.push_back(qp);
        cq_list_.push_back(cq);
    }

    void Run()
    {
        struct timeval start_tv, end_tv;
        running_ = true;
        operation_count_ = 0;
        pthread_barrier_init(&barrier_, nullptr, FLAGS_threads + 1);
        worker_list_.resize(FLAGS_threads);
        for (uint32_t i = 0; i < FLAGS_threads; ++i)
        {
            worker_list_[i] = std::thread(&Benchmark::Worker, this, i);
        }
        pthread_barrier_wait(&barrier_);
        gettimeofday(&start_tv, NULL);
        sleep(FLAGS_duration);
        running_ = false;
        pthread_barrier_wait(&barrier_);
        gettimeofday(&end_tv, NULL);
        for (uint32_t i = 0; i < FLAGS_threads; ++i)
        {
            worker_list_[i].join();
        }
        pthread_barrier_destroy(&barrier_);
        double elapsed_time = (end_tv.tv_sec - start_tv.tv_sec) * 1.0 +
                              (end_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
        auto bandwidth = operation_count_ * FLAGS_block_size / elapsed_time / 1024.0 / 1024.0;
        auto throughput = operation_count_ / elapsed_time / 1000000.0;
        LOG(INFO) << "threads: " << FLAGS_threads
                  << ", depth: " << FLAGS_depth
                  << ", bandwidth: " << std::fixed << std::setprecision(3) << bandwidth << " MB/s"
                  << ", throughput: " << throughput << " MOP/s";
    }

private:
    void Worker(int thread_id)
    {
        BindCurrentThreadToCore(thread_id);

        uint64_t local_operation_count = 0;
        uint64_t storage_size = roundup(FLAGS_block_size, 64);
        char *local_base = (char *)region + thread_id * storage_size * FLAGS_depth;
        size_t kSegmentSize = rounddown(FLAGS_memory_region_mb * kMegaBytes / FLAGS_threads, 4096);
        uint64_t remote_base = mr_info.addr + thread_id * kSegmentSize;
        std::mt19937 rnd;
        std::uniform_int_distribution<uint64_t> dist_offset(0, kSegmentSize / FLAGS_block_size - 1);
        std::uniform_int_distribution<uint32_t> dist_percent(0, 99);
        QueuePair *qp = qp_list_[thread_id % qp_list_.size()];
        CompletionQueue *cq = cq_list_[thread_id % qp_list_.size()];

        pthread_barrier_wait(&barrier_);
        uint32_t inflight_wr_count = 0;
        SendWRFixedList<8> wr_list;
        while (running_.load(std::memory_order_relaxed))
        {
            std::vector<ibv_wc> wc_list;
            if (inflight_wr_count < FLAGS_depth)
            {
                char *local = local_base + (local_operation_count % FLAGS_depth) * storage_size;
                uint64_t remote_addr = remote_base + uint64_t(FLAGS_block_size) * dist_offset(rnd);
                wr_list.Reset();
                if (dist_percent(rnd) < FLAGS_read_percentage)
                {
                    DIED_IF(wr_list.Read(local, remote_addr, mr_info.rkey, FLAGS_block_size));
                }
                else
                {
                    DIED_IF(wr_list.Write(local, remote_addr, mr_info.rkey, FLAGS_block_size));
                }
                DIED_IF(qp->PostSend(wr_list));
                inflight_wr_count++;
            }
            DIED_IF(cq->Poll(wc_list, 1));
            if (!wc_list.empty())
            {
                DIED_IF(wc_list[0].status != IBV_WC_SUCCESS);
                local_operation_count++;
                inflight_wr_count--;
            }
        }

        pthread_barrier_wait(&barrier_);
        operation_count_.fetch_add(local_operation_count);
    }

private:
    std::vector<QueuePair *> qp_list_;
    std::vector<CompletionQueue *> cq_list_;
    std::vector<std::thread> worker_list_;
    std::atomic<bool> running_;
    std::atomic<uint64_t> operation_count_;
    pthread_barrier_t barrier_;
};

void CreateQPCQ(ConnectionClient &client, QueuePair &qp, CompletionQueue &cq)
{
    if (qp.Create(cq))
    {
        LOG(ERROR) << "Unable to create QP";
        exit(EXIT_FAILURE);
    }

    EndpointInfo request, response;
    request.gid = GetRdmaGid();
    request.lid = GetRdmaLid();
    request.qp_num = qp.GetQPNum();

    if (client.EstablishRC(request, response))
    {
        LOG(ERROR) << "Establish RC connection failed";
        exit(EXIT_FAILURE);
    }

    if (qp.SetupRC(response.gid, response.lid, response.qp_num))
    {
        LOG(ERROR) << "Setup RC connection failed";
        exit(EXIT_FAILURE);
    }
}

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    CreateRdmaGlobalResource();

    if (FLAGS_memory_region_mb < 16)
    {
        LOG(ERROR) << "Memory region must be greater than 16MB";
        exit(EXIT_FAILURE);
    }

    region = AllocateHugeMemory(FLAGS_memory_region_mb * kMegaBytes);
    if (!region)
    {
        LOG(ERROR) << "Unable to allocate memory region";
        exit(EXIT_FAILURE);
    }

    auto key = RegisterRdmaMemoryRegion(
        region,
        FLAGS_memory_region_mb * kMegaBytes,
        IBV_ACCESS_LOCAL_WRITE);

    if (!key.IsValid())
    {
        LOG(ERROR) << "Unable to register memory region";
        exit(EXIT_FAILURE);
    }

    ConnectionClient client;
    if (client.Connect(FLAGS_hostname.c_str(), uint16_t(FLAGS_port)))
    {
        LOG(ERROR) << "Unable to connect to server";
        exit(EXIT_FAILURE);
    }

    std::vector<MemoryRegionInfo> mr_list;
    if (client.ListMemoryRegions(mr_list) || mr_list.empty())
    {
        LOG(ERROR) << "List Memory Regions failed";
        exit(EXIT_FAILURE);
    }
    mr_info = mr_list[0];

    CompletionQueue cq[FLAGS_qp_count];
    QueuePair qp[FLAGS_qp_count];
    Benchmark bench;

    for (uint32_t i = 0; i < FLAGS_qp_count; ++i)
    {
        CreateQPCQ(client, qp[i], cq[i]);
        bench.AddEndPoint(&qp[i], &cq[i]);
    }

    bench.Run();

    for (uint32_t i = 0; i < FLAGS_qp_count; ++i)
    {
        qp[i].Reset();
        cq[i].Reset();
    }
    client.Close();
    DeregisterRdmaMemoryRegion(region);
    munmap(region, FLAGS_memory_region_mb * kMegaBytes);
    return 0;
}
