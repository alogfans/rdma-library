// rdma_endpoint.h
// Copyright (C) 2024 Feng Ren

#ifndef RDMA_ENDPOINT_H
#define RDMA_ENDPOINT_H

#include "rdma_resources.h"

#include <atomic>

struct WorkPromise
{
    std::function<int(ibv_wc *)> done = nullptr;
};

template <size_t N>
struct SendWRFixedList
{
    WorkPromise promise;
    ibv_send_wr wr_list[N];
    ibv_sge sge_list[N];
    size_t size;
    ibv_send_wr *bad_wr;

    static_assert(N > 0, "N must be greater than zero!");

    SendWRFixedList()
    {
        Reset();
    }

    bool IsEmpty() const
    {
        return size == 0;
    }

    ibv_send_wr *GetNativeObject()
    {
        return &wr_list[0];
    }

    void Reset()
    {
        size = 0;
        bad_wr = nullptr;
    }

    int Read(void *local, uintptr_t remote_addr, uint32_t rkey, size_t length)
    {
        return Add(IBV_WR_RDMA_READ, local, remote_addr, rkey, length, 0, 0);
    }

    int Write(void *local, uintptr_t remote_addr, uint32_t rkey, size_t length)
    {
        return Add(IBV_WR_RDMA_WRITE, local, remote_addr, rkey, length, 0, 0);
    }

    int FetchAndAdd(void *local, uintptr_t remote_addr, uint32_t rkey, uint64_t add_val)
    {
        return Add(IBV_WR_ATOMIC_FETCH_AND_ADD, local, remote_addr, rkey,
                   sizeof(uint64_t), add_val, 0);
    }

    int CompareAndSwap(void *local, uintptr_t remote_addr, uint32_t rkey,
                       uint64_t compare_val, uint64_t swap_val)
    {
        return Add(IBV_WR_ATOMIC_CMP_AND_SWP, local, remote_addr, rkey,
                   sizeof(uint64_t), compare_val, swap_val);
    }

    int Send(void *local, size_t length)
    {
        return Add(IBV_WR_SEND, local, 0, 0, length, 0, 0);
    }

private:
    int Add(ibv_wr_opcode opcode, void *local, uintptr_t remote_addr, uint32_t rkey, size_t length,
            uint64_t compare_add, uint64_t swap);
};

int FillSendWRList(ibv_send_wr *wr_list, ibv_sge *sge_list, size_t wr_list_pos, size_t sge_list_pos,
                   ibv_wr_opcode opcode, void *local, uintptr_t remote_addr, uint32_t rkey,
                   size_t length, uint64_t compare_add, uint64_t swap);

template <size_t N>
int SendWRFixedList<N>::Add(ibv_wr_opcode opcode, void *local, uintptr_t remote_addr,
                            uint32_t rkey, size_t length,
                            uint64_t compare_add, uint64_t swap)
{
    size_t wr_list_pos = size;
    size_t sge_list_pos = size;
    if (wr_list_pos >= N)
    {
        LOG(ERROR) << "WR and SGE capacity exceeded";
        return -1;
    }
    ++size;
    wr_list[wr_list_pos].wr_id = promise.done ? (uint64_t)(&promise) : 0;
    return FillSendWRList(wr_list, sge_list, wr_list_pos, sge_list_pos,
                          opcode, local, remote_addr, rkey,
                          length, compare_add, swap);
}

struct SendWRList
{
    WorkPromise promise;
    std::vector<ibv_send_wr> wr_list;
    std::vector<ibv_sge> sge_list;
    ibv_send_wr *bad_wr;

    SendWRList()
    {
        Reset();
    }

    void Reset();

    bool IsEmpty() const
    {
        return wr_list.empty();
    }

    ibv_send_wr *GetNativeObject()
    {
        return wr_list.data();
    }

    int Read(void *local, uintptr_t remote_addr, uint32_t rkey, size_t length)
    {
        return Add(IBV_WR_RDMA_READ, local, remote_addr, rkey, length, 0, 0);
    }

    int Write(void *local, uintptr_t remote_addr, uint32_t rkey, size_t length)
    {
        return Add(IBV_WR_RDMA_WRITE, local, remote_addr, rkey, length, 0, 0);
    }

    int FetchAndAdd(void *local, uintptr_t remote_addr, uint32_t rkey, uint64_t add_val)
    {
        return Add(IBV_WR_ATOMIC_FETCH_AND_ADD, local, remote_addr, rkey,
                   sizeof(uint64_t), add_val, 0);
    }

    int CompareAndSwap(void *local, uintptr_t remote_addr, uint32_t rkey,
                       uint64_t compare_val, uint64_t swap_val)
    {
        return Add(IBV_WR_ATOMIC_CMP_AND_SWP, local, remote_addr, rkey,
                   sizeof(uint64_t), compare_val, swap_val);
    }

    int Send(void *local, size_t length)
    {
        return Add(IBV_WR_SEND, local, 0, 0, length, 0, 0);
    }

private:
    int Add(ibv_wr_opcode opcode, void *local, uintptr_t remote_addr, uint32_t rkey, size_t length,
            uint64_t compare_add, uint64_t swap);
};

struct ReceiveWRList
{
    WorkPromise promise;
    std::vector<ibv_recv_wr> wr_list;
    std::vector<ibv_sge> sge_list;
    ibv_recv_wr *bad_wr;

    ReceiveWRList() { Reset(); }

    void Reset();

    int Recv(void *local, size_t length);
};

class CompletionQueue
{
public:
    CompletionQueue();

    ~CompletionQueue();

    CompletionQueue(const CompletionQueue &) = delete;

    CompletionQueue &operator=(const CompletionQueue &) = delete;

    void Reset();

    int RequestNotify();

    int Poll(std::vector<ibv_wc> &wc_list, size_t max_count = 16);

    int Poll(size_t max_count = 16);

    bool IsAvailable() const { return cq_ != nullptr; }

    ibv_cq *GetRawObject() const { return cq_; }

private:
    ibv_cq *cq_;
};

class QueuePair
{
public:
    QueuePair();

    ~QueuePair();

    int Create(CompletionQueue &cq, bool reliable_connection = true)
    {
        return Create(cq, cq, reliable_connection);
    }

    int Create(CompletionQueue &send_cq, CompletionQueue &recv_cq, bool reliable_connection = true);

    int SetupRC(ibv_gid gid, uint16_t lid, uint32_t qp_num);

    int SetupUD();

    void Reset();

    enum State
    {
        STATE_INITIAL,
        STATE_RC_CREATING,
        STATE_UD_CREATING,
        STATE_RC_ESTABLISHED,
        STATE_UD_ESTABLISHED,
        STATE_OFFLINE
    };

    State GetState() const
    {
        return state_.load(std::memory_order_acquire);
    }

    template <class GenericSendWRList>
    int PostSend(GenericSendWRList &send_wr);

    int PostRecv(ReceiveWRList &recv_wr);

    uint32_t GetQPNum() const;

private:
    std::atomic<State> state_;
    ibv_qp *queue_pair_;
};

template <class GenericSendWRList>
int QueuePair::PostSend(GenericSendWRList &send_wr)
{
    if (GetState() != STATE_RC_ESTABLISHED)
    {
        LOG(ERROR) << "Endpoint is not in RC_ESTABLISHED state";
        return -1;
    }
    if (send_wr.IsEmpty())
    {
        LOG(WARNING) << "Skip to send empty work request";
        return 0;
    }
    if (ibv_post_send(queue_pair_, send_wr.GetNativeObject(), &send_wr.bad_wr))
    {
        PLOG(ERROR) << "Fail to post send work requests";
        return -1;
    }
    return 0;
}

#endif // RDMA_ENDPOINT_H