// rdma_endpoint.h
// Copyright (C) 2024 Feng Ren

#ifndef RDMA_ENDPOINT_H
#define RDMA_ENDPOINT_H

#include "rdma_resources.h"
#include "work_request.h"
#include <atomic>
#include <future>

class AddressHandle
{
public:
    AddressHandle() : ah_(nullptr) {}

    ~AddressHandle();

    AddressHandle(const AddressHandle &) = delete;

    AddressHandle &operator=(const AddressHandle &) = delete;

    void Reset();

    int Create(ibv_gid gid, uint16_t lid, uint32_t qp_num);

    ibv_ah *GetIbvAH() { return ah_; }

    uint32_t GetQPNum() { return qp_num_; }

private:
    ibv_ah *ah_;
    uint32_t qp_num_;
};

// CompletionQueue MUST have longer lifetime than QueuePair
class CompletionQueue
{
public:
    CompletionQueue();

    ~CompletionQueue();

    CompletionQueue(const CompletionQueue &) = delete;

    CompletionQueue &operator=(const CompletionQueue &) = delete;

    void Reset();

    int RequestNotify();

    int Poll(size_t max_count = 16);

    bool IsAvailable() const { return cq_ != nullptr; }

    ibv_cq *GetIbvCQ() const { return cq_; }

private:
    ibv_cq *cq_;
};

class QueuePair
{
public:
    enum TransportType
    {
        TRANSPORT_TYPE_RC,
        TRANSPORT_TYPE_UD
    };

    enum State
    {
        STATE_INITIAL,
        STATE_RC_CREATING,
        STATE_UD_CREATING,
        STATE_RC_ESTABLISHED,
        STATE_UD_ESTABLISHED,
        STATE_OFFLINE
    };

public:
    QueuePair();

    ~QueuePair();

    int Create(CompletionQueue &cq, TransportType transport = TRANSPORT_TYPE_RC)
    {
        return Create(cq, cq, transport);
    }

    int Create(CompletionQueue &send_cq, CompletionQueue &recv_cq,
               TransportType transport = TRANSPORT_TYPE_RC);

    int SetupRC(ibv_gid gid, uint16_t lid, uint32_t qp_num);

    void Reset();

    State GetState() const
    {
        return state_.load(std::memory_order_acquire);
    }

    uint32_t GetQPNum() const;

    int Post(WorkRequestBase &wr_list);

private:
    int SetupUD();

    int PostRC(WorkRequestBase &wr_list);

    int PostUD(WorkRequestBase &wr_list);

private:
    std::atomic<State> state_;
    ibv_qp *qp_;
};

#endif // RDMA_ENDPOINT_H