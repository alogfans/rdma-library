// rdma_endpoint.cpp
// Copyright (C) 2024 Feng Ren

#include "rdma_endpoint.h"

DEFINE_int32(rdma_max_sge, 4, "Max SGE num in a WR");
DEFINE_int32(rdma_max_wr, 256, "Max WR entries num in a WR");
DEFINE_int32(rdma_num_cqe, 256, "Max CQE num in a CQ");
DEFINE_int32(rdma_max_inline, 64, "Max inline data length");

const static uint8_t MAX_HOP_LIMIT = 16;
const static uint8_t TIMEOUT = 14;
const static uint8_t RETRY_CNT = 7;

CompletionQueue::CompletionQueue()
{
    cq_ = ibv_create_cq(GetRdmaContext(),
                        FLAGS_rdma_num_cqe,
                        this /* CQ context */,
                        GetRdmaCompletionChannel(),
                        GetRdmaCompVector());
    if (!cq_)
    {
        PLOG(ERROR) << "Failed to create CQ";
    }
}

CompletionQueue::~CompletionQueue()
{
    Reset();
}

void CompletionQueue::Reset()
{
    if (cq_)
    {
        ibv_destroy_cq(cq_);
        cq_ = nullptr;
    }
}

int CompletionQueue::RequestNotify()
{
    LOG_ASSERT(IsAvailable());
    if (ibv_req_notify_cq(cq_, 0))
    {
        PLOG(ERROR) << "Failed to request CQ notification";
        return -1;
    }
    return 0;
}

int CompletionQueue::Poll(std::vector<ibv_wc> &wc_list, size_t max_count)
{
    LOG_ASSERT(IsAvailable());
    wc_list.resize(max_count);
    int nr_poll = ibv_poll_cq(cq_, max_count, wc_list.data());
    if (nr_poll < 0)
    {
        PLOG(ERROR) << "Failed to poll CQ";
        return -1;
    }
    wc_list.resize(nr_poll);
    return 0;
}

QueuePair::QueuePair() : state_(STATE_INITIAL),
                         queue_pair_(nullptr) {}

QueuePair::~QueuePair()
{
    Reset();
}

void QueuePair::Reset()
{
    if (queue_pair_)
    {
        ibv_destroy_qp(queue_pair_);
        queue_pair_ = nullptr;
    }
    state_.exchange(STATE_INITIAL);
}

static int UpdateRdmaConfiguration()
{
    ibv_device_attr attr;
    if (ibv_query_device(GetRdmaContext(), &attr))
    {
        PLOG(ERROR) << "Failed to query device attributes";
        return -1;
    }

    if (FLAGS_rdma_max_sge <= 0 || FLAGS_rdma_max_sge > attr.max_sge)
    {
        LOG(WARNING) << "--rdma_max_sge is not in range (0, " << attr.max_sge << "]."
                     << " It will be set to " << attr.max_sge;
        FLAGS_rdma_max_sge = attr.max_sge;
    }

    if (FLAGS_rdma_max_wr <= 0 || FLAGS_rdma_max_wr > attr.max_qp_wr)
    {
        LOG(WARNING) << "--rdma_max_wr is not in range (0, " << attr.max_qp_wr << "]."
                     << " It will be set to " << attr.max_qp_wr;
        FLAGS_rdma_max_wr = attr.max_qp_wr;
    }

    return 0;
}

int QueuePair::Create(CompletionQueue &send_cq, CompletionQueue &recv_cq, bool reliable_connection)
{
    LOG_ASSERT(IsRdmaAvailable());
    if (GetState() != STATE_INITIAL)
    {
        LOG(ERROR) << "Endpoint is not in INITIAL state";
        return -1;
    }

    if (UpdateRdmaConfiguration())
    {
        return -1;
    }

    ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.send_cq = send_cq.GetRawObject();
    attr.recv_cq = recv_cq.GetRawObject();
    attr.sq_sig_all = false;
    attr.qp_type = reliable_connection ? IBV_QPT_RC : IBV_QPT_UD;
    attr.cap.max_send_wr = attr.cap.max_recv_wr = FLAGS_rdma_max_wr;
    attr.cap.max_send_sge = attr.cap.max_recv_sge = FLAGS_rdma_max_sge;
    attr.cap.max_inline_data = FLAGS_rdma_max_inline;
    queue_pair_ = ibv_create_qp(GetRdmaProtectionDomain(), &attr);
    if (!queue_pair_)
    {
        PLOG(ERROR) << "Failed to create QP";
        return -1;
    }

    state_.exchange(reliable_connection ? STATE_RC_CREATING : STATE_UD_CREATING);
    return 0;
}

int QueuePair::SetupRC(ibv_gid gid, uint16_t lid, uint32_t qp_num)
{
    if (GetState() != STATE_RC_CREATING)
    {
        LOG(ERROR) << "Endpoint is not in RC_CREATING state";
        return -1;
    }

    ibv_qp_attr attr;
    // RESET -> INIT
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = GetRdmaPortNum();
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    if (ibv_modify_qp(queue_pair_, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS))
    {
        PLOG(ERROR) << "Failed to modity QP to INIT";
        state_.exchange(STATE_OFFLINE);
        return -1;
    }

    // INIT -> RTR
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_1024;
    attr.ah_attr.grh.dgid = gid;
    attr.ah_attr.grh.sgid_index = GetRdmaGidIndex();
    attr.ah_attr.grh.hop_limit = MAX_HOP_LIMIT;
    attr.ah_attr.dlid = lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.static_rate = 0;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = GetRdmaPortNum();
    attr.dest_qp_num = qp_num;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 16;
    attr.min_rnr_timer = 12; // 12 in previous implementation
    if (ibv_modify_qp(queue_pair_, &attr, IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_MIN_RNR_TIMER | IBV_QP_AV | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN))
    {
        PLOG(ERROR) << "Failed to modity QP to RTR";
        state_.exchange(STATE_OFFLINE);
        return -1;
    }

    // RTR -> RTS
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = TIMEOUT;
    attr.retry_cnt = RETRY_CNT;
    attr.rnr_retry = 7; // or 7,RNR error
    attr.sq_psn = 0;
    attr.max_rd_atomic = 16;

    if (ibv_modify_qp(queue_pair_, &attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC))
    {
        PLOG(ERROR) << "Failed to modity QP to RTS";
        state_.exchange(STATE_OFFLINE);
        return -1;
    }

    state_.exchange(STATE_RC_ESTABLISHED);
    return 0;
}

int QueuePair::PostSend(SendWRList &send_wr)
{
    if (GetState() != STATE_RC_ESTABLISHED)
    {
        LOG(ERROR) << "Endpoint is not in RC_ESTABLISHED state";
        return -1;
    }
    if (send_wr.wr_list.empty())
    {
        LOG(WARNING) << "Skip to send empty work request";
        return 0;
    }
    if (ibv_post_send(queue_pair_, send_wr.wr_list.data(), &send_wr.bad_wr))
    {
        PLOG(ERROR) << "Fail to post send work requests";
        return -1;
    }
    return 0;
}

int QueuePair::PostRecv(ReceiveWRList &recv_wr)
{
    if (GetState() != STATE_RC_ESTABLISHED)
    {
        LOG(ERROR) << "Endpoint is not in RC_ESTABLISHED state";
        return -1;
    }
    if (recv_wr.wr_list.empty())
    {
        LOG(WARNING) << "Skip to send empty work request";
        return 0;
    }
    if (ibv_post_recv(queue_pair_, recv_wr.wr_list.data(), &recv_wr.bad_wr))
    {
        PLOG(ERROR) << "Fail to post recv work requests";
        return -1;
    }
    return 0;
}

uint32_t QueuePair::GetQPNum() const
{
    if (GetState() == STATE_INITIAL)
    {
        LOG(ERROR) << "Endpoint is in INITIAL state";
        return -1;
    }
    LOG_ASSERT(queue_pair_);
    return queue_pair_->qp_num;
}

void SendWRList::Reset()
{
    wr_list.clear();
    sge_list.clear();
    bad_wr = nullptr;
    wr_list.reserve(FLAGS_rdma_max_wr);
    sge_list.reserve(FLAGS_rdma_max_wr * FLAGS_rdma_max_sge);
}

void ReceiveWRList::Reset()
{
    wr_list.clear();
    sge_list.clear();
    bad_wr = nullptr;
    wr_list.reserve(FLAGS_rdma_max_wr);
    sge_list.reserve(FLAGS_rdma_max_wr * FLAGS_rdma_max_sge);
}

int SendWRList::Add(ibv_wr_opcode opcode, void *local, uintptr_t remote_addr, uint32_t rkey,
                    size_t length, uint64_t compare_add, uint64_t swap)
{
    size_t wr_list_pos = wr_list.size();
    size_t sge_list_pos = sge_list.size();
    if (wr_list_pos >= wr_list.capacity() || sge_list_pos >= sge_list.capacity())
    {
        LOG(ERROR) << "WR and SGE capacity exceeded";
        return -1;
    }

    wr_list.resize(wr_list_pos + 1);
    sge_list.resize(sge_list_pos + 1);
    auto &wr = wr_list[wr_list_pos];
    auto &sge = sge_list[sge_list_pos];

    auto key = GetRdmaMemoryRegion(local);
    if (!key.lkey)
    {
        LOG(ERROR) << "Unable to find memory region at " << local;
        return -1;
    }

    sge.addr = uintptr_t(local);
    sge.length = length;
    sge.lkey = key.lkey;

    wr.wr_id = 0; // next_wr_id.fetch_add(1);
    wr.opcode = opcode;
    wr.num_sge = 1;
    wr.sg_list = &sge;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.next = nullptr;
    if (wr_list_pos)
    {
        wr_list[wr_list_pos - 1].next = &wr;
        wr_list[wr_list_pos - 1].send_flags &= ~IBV_SEND_SIGNALED;
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
        break;

    default:
        LOG(ERROR) << "Unsupported opcode " << opcode;
        return -1;
    }

    return 0;
}

int ReceiveWRList::Recv(void *local, size_t length)
{
    size_t wr_list_pos = wr_list.size();
    size_t sge_list_pos = sge_list.size();
    if (wr_list_pos >= wr_list.capacity() || sge_list_pos >= sge_list.capacity())
    {
        LOG(ERROR) << "WR and SGE capacity exceeded";
        return -1;
    }

    wr_list.resize(wr_list_pos + 1);
    sge_list.resize(sge_list_pos + 1);
    auto &wr = wr_list[wr_list_pos];
    auto &sge = sge_list[sge_list_pos];

    auto key = GetRdmaMemoryRegion(local);
    if (!key.lkey)
    {
        LOG(ERROR) << "Unable to find memory region at " << local;
        return -1;
    }

    sge.addr = uintptr_t(local);
    sge.length = length;
    sge.lkey = key.lkey;

    wr.wr_id = 0; // next_wr_id.fetch_add(1);
    wr.num_sge = 1;
    wr.sg_list = &sge;
    wr.next = nullptr;
    if (wr_list_pos)
    {
        wr_list[wr_list_pos - 1].next = &wr;
    }

    return 0;
}
