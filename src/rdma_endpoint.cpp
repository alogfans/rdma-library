// rdma_endpoint.cpp
// Copyright (C) 2024 Feng Ren

#include "rdma_endpoint.h"
#include "work_request.h"

DEFINE_int32(rdma_max_sge, 4, "Max SGE num in a WR");
DEFINE_int32(rdma_max_wr, 256, "Max WR entries num in a WR");
DEFINE_int32(rdma_num_cqe, 256, "Max CQE num in a CQ");
DEFINE_int32(rdma_max_inline, 64, "Max inline data length");

const static uint8_t MAX_HOP_LIMIT = 16;
const static uint8_t TIMEOUT = 14;
const static uint8_t RETRY_CNT = 7;

int AddressHandle::Create(ibv_gid gid, uint16_t lid, uint32_t qp_num)
{
    ibv_ah_attr ah_attr;
    memset(&ah_attr, 0, sizeof(ah_attr));
    ah_attr.grh.dgid = gid;
    ah_attr.grh.sgid_index = GetRdmaGidIndex();
    ah_attr.grh.hop_limit = MAX_HOP_LIMIT;
    ah_attr.dlid = lid;
    ah_attr.sl = 0;
    ah_attr.src_path_bits = 0;
    ah_attr.static_rate = 0;
    ah_attr.is_global = 1;
    ah_attr.port_num = GetRdmaPortNum();
    ah_ = ibv_create_ah(GetRdmaProtectionDomain(), &ah_attr);
    if (!ah_)
    {
        PLOG(ERROR) << "Failed to create AH";
        return -1;
    }
    qp_num_ = qp_num;
    return 0;
}

AddressHandle::~AddressHandle()
{
    Reset();
}

void AddressHandle::Reset()
{
    if (ah_)
    {
        ibv_destroy_ah(ah_);
        ah_ = nullptr;
    }
}

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

int CompletionQueue::Poll(size_t max_count)
{
    LOG_ASSERT(IsAvailable());

    const static size_t kIbvWcListCapacity = 64;
    ibv_wc wc_list[kIbvWcListCapacity];

    int nr_poll = ibv_poll_cq(cq_, std::min(max_count, kIbvWcListCapacity), wc_list);
    if (nr_poll < 0)
    {
        PLOG(ERROR) << "Failed to poll CQ";
        return -1;
    }

    for (int i = 0; i < nr_poll; ++i)
    {
        auto &wc = wc_list[i];
        if (wc.status != IBV_WC_SUCCESS)
        {
            LOG(ERROR) << "Work completion error: " << ibv_wc_status_str(wc.status)
                       << " (" << wc.status << "), vendor error: " << wc.vendor_err;
        }
        if (wc.wr_id)
        {
            auto obj = reinterpret_cast<WorkRequestCallback *>(wc.wr_id);
            obj->RunCallback(wc);
        }
    }

    return nr_poll;
}

QueuePair::QueuePair() : state_(STATE_INITIAL), qp_(nullptr) {}

QueuePair::~QueuePair()
{
    Reset();
}

void QueuePair::Reset()
{
    if (qp_)
    {
        ibv_destroy_qp(qp_);
        qp_ = nullptr;
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

int QueuePair::Create(CompletionQueue &send_cq, CompletionQueue &recv_cq, TransportType transport)
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
    attr.send_cq = send_cq.GetIbvCQ();
    attr.recv_cq = recv_cq.GetIbvCQ();
    attr.sq_sig_all = false;
    attr.qp_type = transport == TRANSPORT_TYPE_RC ? IBV_QPT_RC : IBV_QPT_UD;
    attr.cap.max_send_wr = attr.cap.max_recv_wr = FLAGS_rdma_max_wr;
    attr.cap.max_send_sge = attr.cap.max_recv_sge = FLAGS_rdma_max_sge;
    attr.cap.max_inline_data = FLAGS_rdma_max_inline;
    qp_ = ibv_create_qp(GetRdmaProtectionDomain(), &attr);
    if (!qp_)
    {
        PLOG(ERROR) << "Failed to create QP";
        return -1;
    }

    state_.exchange(transport == TRANSPORT_TYPE_RC ? STATE_RC_CREATING : STATE_UD_CREATING);

    if (transport == TRANSPORT_TYPE_UD)
        return SetupUD();
    else
        return 0;
}

int QueuePair::SetupRC(ibv_gid gid, uint16_t lid, uint32_t qp_num)
{
    if (GetState() != STATE_RC_CREATING)
    {
        LOG(ERROR) << "QP is not in RC_CREATING state";
        return -1;
    }

    ibv_qp_attr attr;
    // RESET -> INIT
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = GetRdmaPortNum();
    attr.pkey_index = 0;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    if (ibv_modify_qp(qp_, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS))
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
    if (ibv_modify_qp(qp_, &attr, IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_MIN_RNR_TIMER | IBV_QP_AV | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN))
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

    if (ibv_modify_qp(qp_, &attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC))
    {
        PLOG(ERROR) << "Failed to modity QP to RTS";
        state_.exchange(STATE_OFFLINE);
        return -1;
    }

    state_.exchange(STATE_RC_ESTABLISHED);
    return 0;
}

int QueuePair::SetupUD()
{
    if (GetState() != STATE_UD_CREATING)
    {
        LOG(ERROR) << "QP is not in UD_CREATING state";
        return -1;
    }

    ibv_qp_attr attr;
    // RESET -> INIT
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = GetRdmaPortNum();
    attr.pkey_index = 0;
    attr.qkey = UD_QP_QKEY;
    if (ibv_modify_qp(qp_, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY))
    {
        PLOG(ERROR) << "Failed to modity QP to INIT";
        state_.exchange(STATE_OFFLINE);
        return -1;
    }

    // INIT -> RTR
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    if (ibv_modify_qp(qp_, &attr, IBV_QP_STATE))
    {
        PLOG(ERROR) << "Failed to modity QP to RTR";
        state_.exchange(STATE_OFFLINE);
        return -1;
    }

    // RTR -> RTS
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.sq_psn = 0;
    if (ibv_modify_qp(qp_, &attr, IBV_QP_STATE | IBV_QP_SQ_PSN))
    {
        PLOG(ERROR) << "Failed to modity QP to RTS";
        state_.exchange(STATE_OFFLINE);
        return -1;
    }

    state_.exchange(STATE_UD_ESTABLISHED);
    return 0;
}

uint32_t QueuePair::GetQPNum() const
{
    if (GetState() == STATE_INITIAL)
    {
        LOG(ERROR) << "QP is in INITIAL state";
        return UINT32_MAX;
    }
    LOG_ASSERT(qp_);
    return qp_->qp_num;
}

int QueuePair::Post(WorkRequestBase &wr_list)
{
    auto state = GetState();
    if (state == STATE_RC_ESTABLISHED)
    {
        return PostRC(wr_list);
    }
    else if (state == STATE_UD_ESTABLISHED)
    {
        return PostUD(wr_list);
    }
    else
    {
        PLOG(ERROR) << "QP is not in ESTABLISHED state";
        return -1;
    }
    return 0;
}

int QueuePair::PostRC(WorkRequestBase &wr_list)
{
    ibv_send_wr *send_bad_wr;
    ibv_recv_wr *recv_bad_wr;
    if (wr_list.HasOneSideAndSend() && ibv_post_send(qp_, wr_list.GetIbvSendWr(), &send_bad_wr))
    {
        PLOG(ERROR) << "Fail to execute ibv_post_send";
        return -1;
    }
    if (wr_list.HasRecv() && ibv_post_recv(qp_, wr_list.GetIbvRecvWr(), &recv_bad_wr))
    {
        PLOG(ERROR) << "Fail to execute ibv_post_recv";
        return -1;
    }
    return 0;
}

int QueuePair::PostUD(WorkRequestBase &wr_list)
{
    ibv_send_wr *send_bad_wr;
    ibv_recv_wr *recv_bad_wr;
    if (wr_list.HasOneSideAndSend() && ibv_post_send(qp_, wr_list.GetIbvSendWr(), &send_bad_wr))
    {
        PLOG(ERROR) << "Fail to execute ibv_post_send";
        return -1;
    }
    if (wr_list.HasRecv() && ibv_post_recv(qp_, wr_list.GetIbvRecvWr(), &recv_bad_wr))
    {
        PLOG(ERROR) << "Fail to execute ibv_post_recv";
        return -1;
    }
    return 0;
}