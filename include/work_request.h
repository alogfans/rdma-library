// work_request.h
// Copyright (C) 2024 Feng Ren

#ifndef WORK_REQUEST_H
#define WORK_REQUEST_H

#include "rdma_resources.h"
#include <future>

DECLARE_int32(rdma_max_inline);

const static uint32_t UD_QP_QKEY = 0x22222222;

struct WorkRequestCallback
{
    virtual void RunCallback(const ibv_wc &wc) = 0;
};

class WorkRequestBase : public WorkRequestCallback
{
public:
    WorkRequestBase() {}

    virtual ~WorkRequestBase() {}

    virtual void Reset() {}

    virtual bool HasOneSideAndSend() const { return false; }

    virtual bool HasRecv() const { return false; }

    virtual ibv_send_wr *GetIbvSendWr() { return nullptr; }

    virtual ibv_recv_wr *GetIbvRecvWr() { return nullptr; }

    int Read(void *local, uintptr_t remote_addr, uint32_t rkey, size_t length)
    {
        return Add(IBV_WR_RDMA_READ, local, remote_addr, rkey, length, 0, 0);
    }

    int Write(void *local, uintptr_t remote_addr, uint32_t rkey, size_t length)
    {
        return Add(IBV_WR_RDMA_WRITE, local, remote_addr, rkey, length, 0, 0);
    }

    int WriteImm(void *local, uintptr_t remote_addr, uint32_t rkey, size_t length, uint32_t imm_data)
    {
        return Add(IBV_WR_RDMA_WRITE_WITH_IMM, local, remote_addr, rkey, length, 0, 0, imm_data);
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

    virtual int Recv(void *local, size_t length)
    {
        LOG(ERROR) << "Unsupported function";
        return -1;
    }

private:
    virtual int Add(ibv_wr_opcode opcode, void *local, uintptr_t remote_addr, uint32_t rkey, size_t length,
                    uint64_t compare_add, uint64_t swap, uint32_t imm_data = 0)
    {
        LOG(ERROR) << "Unsupported function";
        return -1;
    }
};

template <size_t WrListCap, size_t SgeListCap>
class WorkRequestRC : public WorkRequestBase
{
public:
    WorkRequestRC()
        : use_callback(false),
          send_size(0),
          has_recv_wr(false) {}

    virtual ~WorkRequestRC() {}

    void EnableCallback() { use_callback = true; }

    virtual void Reset()
    {
        if (use_callback)
        {
            LOG(WARNING) << "Cannot reuse this WorkRequest object";
        }
        send_size = 0;
        has_recv_wr = false;
    }

    virtual bool HasOneSideAndSend() const { return send_size; }

    virtual bool HasRecv() const { return has_recv_wr; }

    virtual ibv_send_wr *GetIbvSendWr() { return &send_wr_list[0]; }

    virtual ibv_recv_wr *GetIbvRecvWr() { return &recv_wr; }

    std::future<ibv_wc> GetFuture() { return promise.get_future(); }

    virtual int Recv(void *local, size_t length);

    virtual void RunCallback(const ibv_wc &wc)
    {
        promise.set_value(wc);
    }

private:
    virtual int Add(ibv_wr_opcode opcode, void *local, uintptr_t remote_addr, uint32_t rkey, size_t length,
                    uint64_t compare_add, uint64_t swap, uint32_t imm_data = 0);

    bool use_callback;
    std::promise<ibv_wc> promise;

    ibv_send_wr send_wr_list[WrListCap];
    ibv_sge send_sge_list[SgeListCap];
    size_t send_size;

    ibv_recv_wr recv_wr;
    ibv_sge recv_sge;
    bool has_recv_wr;
};

template <size_t WrListCap, size_t SgeListCap>
int WorkRequestRC<WrListCap, SgeListCap>::Add(ibv_wr_opcode opcode, void *local,
                                              uintptr_t remote_addr, uint32_t rkey, size_t length,
                                              uint64_t compare_add, uint64_t swap, uint32_t imm_data)
{
    if (use_callback && has_recv_wr)
    {
        LOG(ERROR) << "Not allow to issue both send and recv operations";
        return -1;
    }

    size_t wr_list_pos = send_size;
    size_t sge_list_pos = send_size;
    if (wr_list_pos >= WrListCap || sge_list_pos >= SgeListCap)
    {
        LOG(ERROR) << "WR and/or SGE capacity exceeded";
        return -1;
    }
    ++send_size;

    auto &wr = send_wr_list[wr_list_pos];
    auto &sge = send_sge_list[sge_list_pos];

    auto key = GetRdmaMemoryRegion(local);
    if (!key.lkey)
    {
        LOG(ERROR) << "Unable to find memory region at " << local;
        return -1;
    }

    sge.addr = uintptr_t(local);
    sge.length = length;
    sge.lkey = key.lkey;

    wr.wr_id = use_callback ? reinterpret_cast<uint64_t>(this) : 0;
    wr.opcode = opcode;
    wr.num_sge = 1;
    wr.sg_list = &sge;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.next = nullptr;
    wr.imm_data = htobe32(imm_data);
    if (wr_list_pos)
    {
        send_wr_list[wr_list_pos - 1].next = &wr;
        send_wr_list[wr_list_pos - 1].send_flags &= ~IBV_SEND_SIGNALED;
    }

    switch (opcode)
    {
    case IBV_WR_RDMA_WRITE:
    case IBV_WR_RDMA_WRITE_WITH_IMM:
        if (length <= size_t(FLAGS_rdma_max_inline))
        {
            wr.send_flags |= IBV_SEND_INLINE;
        }
        // fall-through

    case IBV_WR_RDMA_READ:
        wr.wr.rdma.remote_addr = remote_addr;
        wr.wr.rdma.rkey = rkey;
        break;

    case IBV_WR_ATOMIC_FETCH_AND_ADD:
    case IBV_WR_ATOMIC_CMP_AND_SWP:
        wr.wr.atomic.remote_addr = remote_addr;
        wr.wr.atomic.rkey = rkey;
        wr.wr.atomic.compare_add = compare_add;
        wr.wr.atomic.swap = swap;
        break;

    case IBV_WR_SEND:
    case IBV_WR_SEND_WITH_IMM:
        break;

    default:
        LOG(ERROR) << "Unsupported opcode " << opcode;
        return -1;
    }

    return 0;
}

template <size_t WrListCap, size_t SgeListCap>
int WorkRequestRC<WrListCap, SgeListCap>::Recv(void *local, size_t length)
{
    if (has_recv_wr)
    {
        LOG(ERROR) << "Cannot issue multiple recv operations";
        return -1;
    }
    if (use_callback && send_size)
    {
        LOG(ERROR) << "Not allow to issue both send and recv operations";
        return -1;
    }

    auto key = GetRdmaMemoryRegion(local);
    if (!key.lkey)
    {
        LOG(ERROR) << "Unable to find memory region at " << local;
        return -1;
    }

    has_recv_wr = true;
    recv_sge.addr = uintptr_t(local);
    recv_sge.length = length;
    recv_sge.lkey = key.lkey;

    recv_wr.wr_id = use_callback ? reinterpret_cast<uint64_t>(this) : 0;
    recv_wr.num_sge = 1;
    recv_wr.sg_list = &recv_sge;
    recv_wr.next = nullptr;
    return 0;
}

static const size_t kDefaultWrListCap = 16;
static const size_t kDefaultSgeListCap = 16;
using DefaultWorkRequest = WorkRequestRC<kDefaultWrListCap, kDefaultSgeListCap>;

class WorkRequestUD : public WorkRequestBase
{
public:
    WorkRequestUD()
        : use_callback(false),
          has_send_wr(false),
          has_recv_wr(false),
          ah(nullptr) {}

    virtual ~WorkRequestUD() {}

    void EnableCallback() { use_callback = true; }

    virtual void Reset()
    {
        if (use_callback)
        {
            LOG(WARNING) << "Cannot reuse this WorkRequest object";
        }
        has_send_wr = false;
        has_recv_wr = false;
        ah = nullptr;
    }

    virtual bool HasOneSideAndSend() const { return has_send_wr; }

    virtual bool HasRecv() const { return has_recv_wr; }

    virtual ibv_send_wr *GetIbvSendWr() { return &send_wr; }

    virtual ibv_recv_wr *GetIbvRecvWr() { return &recv_wr; }

    std::future<ibv_wc> GetFuture() { return promise.get_future(); }

    virtual int Recv(void *local, size_t length);

    virtual void RunCallback(const ibv_wc &wc)
    {
        promise.set_value(wc);
    }

    void SetSendOption(ibv_ah *ah, uint32_t qpnum)
    {
        this->ah = ah;
        this->qpnum = qpnum;
    }

private:
    virtual int Add(ibv_wr_opcode opcode, void *local, uintptr_t remote_addr, uint32_t rkey, size_t length,
                    uint64_t compare_add, uint64_t swap, uint32_t imm_data = 0);

    bool use_callback;
    std::promise<ibv_wc> promise;

    ibv_send_wr send_wr;
    ibv_sge send_sge;
    size_t has_send_wr;

    ibv_recv_wr recv_wr;
    ibv_sge recv_sge;
    bool has_recv_wr;

    ibv_ah *ah;
    uint32_t qpnum;
};

inline int WorkRequestUD::Add(ibv_wr_opcode opcode, void *local,
                              uintptr_t remote_addr, uint32_t rkey, size_t length,
                              uint64_t compare_add, uint64_t swap, uint32_t imm_data)
{
    (void)remote_addr;
    (void)rkey;
    (void)compare_add;
    (void)swap;

    if (use_callback && has_recv_wr)
    {
        LOG(ERROR) << "Not allow to issue both send and recv operations";
        return -1;
    }

    if (has_send_wr)
    {
        LOG(ERROR) << "Cannot issue multiple recv operations";
        return -1;
    }

    if (!ah)
    {
        LOG(ERROR) << "Unspecified address handle";
        return -1;
    }

    auto key = GetRdmaMemoryRegion(local);
    if (!key.lkey)
    {
        LOG(ERROR) << "Unable to find memory region at " << local;
        return -1;
    }

    if (opcode != IBV_WR_SEND && opcode != IBV_WR_SEND_WITH_IMM)
    {
        LOG(ERROR) << "Unsupport oepration in UD";
        return -1;
    }

    has_send_wr = true;
    send_sge.addr = uintptr_t(local);
    send_sge.length = length;
    send_sge.lkey = key.lkey;
    send_wr.wr_id = use_callback ? reinterpret_cast<uint64_t>(this) : 0;
    send_wr.opcode = opcode;
    send_wr.num_sge = 1;
    send_wr.sg_list = &send_sge;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.next = nullptr;
    send_wr.imm_data = htobe32(imm_data);
    send_wr.wr.ud.ah = ah;
    send_wr.wr.ud.remote_qpn = qpnum;
    send_wr.wr.ud.remote_qkey = UD_QP_QKEY;
    return 0;
}

inline int WorkRequestUD::Recv(void *local, size_t length)
{
    if (has_recv_wr)
    {
        LOG(ERROR) << "Cannot issue multiple recv operations";
        return -1;
    }
    if (use_callback && has_send_wr)
    {
        LOG(ERROR) << "Not allow to issue both send and recv operations";
        return -1;
    }

    auto key = GetRdmaMemoryRegion(local);
    if (!key.lkey)
    {
        LOG(ERROR) << "Unable to find memory region at " << local;
        return -1;
    }

    has_recv_wr = true;
    recv_sge.addr = uintptr_t(local);
    recv_sge.length = length;
    recv_sge.lkey = key.lkey;

    recv_wr.wr_id = use_callback ? reinterpret_cast<uint64_t>(this) : 0;
    recv_wr.num_sge = 1;
    recv_wr.sg_list = &recv_sge;
    recv_wr.next = nullptr;
    return 0;
}

#endif // WORK_REQUEST_H