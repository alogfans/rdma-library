// rdma_endpoint.h
// Copyright (C) 2024 Feng Ren

#ifndef RDMA_ENDPOINT_H
#define RDMA_ENDPOINT_H

#include "rdma_resources.h"

#include <atomic>

struct RdmaSendWR {
    std::vector<ibv_send_wr> wr_list;
    std::vector<ibv_sge> sge_list;
    ibv_send_wr *bad_wr;

    RdmaSendWR() { Reset(); }

    void Reset();

    int Read(void *local, uintptr_t remote_addr, uint32_t rkey, size_t length) {
        return Add(IBV_WR_RDMA_READ, local, remote_addr, rkey, length, 0, 0);
    }
    
    int Write(void *local, uintptr_t remote_addr, uint32_t rkey, size_t length) {
        return Add(IBV_WR_RDMA_WRITE, local, remote_addr, rkey, length, 0, 0);
    }

    int FetchAndAdd(void *local, uintptr_t remote_addr, uint32_t rkey, uint64_t add_val) {
        return Add(IBV_WR_ATOMIC_FETCH_AND_ADD, local, remote_addr, rkey, 
                   sizeof(uint64_t), add_val, 0);
    }

    int CompareAndSwap(void *local, uintptr_t remote_addr, uint32_t rkey, 
                       uint64_t compare_val, uint64_t swap_val) {
        return Add(IBV_WR_ATOMIC_CMP_AND_SWP, local, remote_addr, rkey, 
                   sizeof(uint64_t), compare_val, swap_val);
    }
    
    int Send(void *local, size_t length) {
        return Add(IBV_WR_SEND, local, 0, 0, length, 0, 0);
    }

private:
    int Add(ibv_wr_opcode opcode, void *local, uintptr_t remote_addr, uint32_t rkey, size_t length, 
            uint64_t compare_add, uint64_t swap);
};

struct RdmaRecvWR {
    std::vector<ibv_recv_wr> wr_list;
    std::vector<ibv_sge> sge_list;
    ibv_recv_wr *bad_wr;

    RdmaRecvWR() { Reset(); }

    void Reset();

    int Recv(void *local, size_t length);
};

class RdmaEndpoint {
public:
    RdmaEndpoint();

    ~RdmaEndpoint();

    int Create(bool reliable_connection = true);

    int SetupRC(ibv_gid gid, uint16_t lid, uint32_t qp_num);

    int SetupUD();

    void Reset();

    enum State {
        STATE_INITIAL, 
        STATE_RC_CREATING,
        STATE_UD_CREATING,
        STATE_RC_ESTABLISHED,
        STATE_UD_ESTABLISHED, 
        STATE_OFFLINE
    };

    State GetState() const {
        return state_.load(std::memory_order_acquire);
    }

    int PostSend(RdmaSendWR &send_wr);

    int PostRecv(RdmaRecvWR &recv_wr);

    int PollCQ(std::vector<ibv_wc> &wc_list);

    uint32_t GetQPNum() const;

private:
    std::atomic<State> state_;
    ibv_qp *queue_pair_;
    ibv_cq *completion_queue_;
};


#endif // RDMA_ENDPOINT_H