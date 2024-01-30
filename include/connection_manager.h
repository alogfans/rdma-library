// connection_manager.h
// Copyright (C) 2024 Feng Ren

#ifndef CONNECTION_MANAGER_H
#define CONNECTION_MANAGER_H

#include "rdma_resources.h"

#include <map>
#include <mutex>
#include <atomic>
#include <vector>
#include <cstdint>
#include <sys/poll.h>

const static uint32_t SERVER_NODE_ID = 0;
const static uint32_t ANY_NODE_ID = UINT32_MAX;

struct EndpointInfo
{
    ibv_gid gid;
    uint16_t lid;
    uint16_t reserve;
    uint32_t qp_num;

    template <class T>
    void DecodeFrom(std::vector<T> &buf)
    {
        return DecodeFrom((EndpointInfo *)buf.data());
    }

    void DecodeFrom(EndpointInfo *buf)
    {
        LOG_ASSERT(buf);
        memcpy(&gid, &buf->gid, sizeof(ibv_gid));
        lid = le16toh(buf->lid);
        qp_num = le32toh(buf->qp_num);
        reserve = 0;
    }

    EndpointInfo EncodeTo() const
    {
        EndpointInfo buf;
        memcpy(&buf.gid, &gid, sizeof(ibv_gid));
        buf.lid = htole16(lid);
        buf.qp_num = htole32(qp_num);
        buf.reserve = 0;
        return buf;
    }
};

struct MemoryRegionInfo
{
    uint32_t node_id; // [node_id, addr] identify an unique memory region
    uint32_t access;  // permission bits
    uint64_t addr;
    uint64_t length;
    uint32_t rkey;
    uint32_t reserve;

    template <class T>
    void DecodeFrom(std::vector<T> &buf)
    {
        return DecodeFrom((MemoryRegionInfo *)buf.data());
    }

    void DecodeFrom(MemoryRegionInfo *buf)
    {
        LOG_ASSERT(buf);
        node_id = le32toh(buf->node_id);
        access = le32toh(buf->access);
        addr = le64toh(buf->addr);
        length = le64toh(buf->length);
        rkey = le32toh(buf->rkey);
        reserve = 0;
    }

    MemoryRegionInfo EncodeTo() const
    {
        MemoryRegionInfo buf;
        buf.node_id = htole32(node_id);
        buf.access = htole32(access);
        buf.addr = htole64(addr);
        buf.length = htole64(length);
        buf.rkey = htole32(rkey);
        buf.reserve = 0;
        return buf;
    }
};

class ConnectionManager
{
public:
    ConnectionManager();

    virtual ~ConnectionManager();

    int Listen(uint16_t tcp_port);

    int ProcessEvents();

    void Close();

    int RegisterMemoryRegion(const MemoryRegionInfo &mr);

    int ListMemoryRegions(std::vector<MemoryRegionInfo> &mr_list);

protected:
    virtual void OnNewConnection(int fd) {}

    virtual void OnCloseConnection(int fd) {}

    virtual int OnEstablishRC(int fd, const EndpointInfo &request, EndpointInfo &response)
    {
        LOG(WARNING) << "Unspecified establishment implementation";
        return -1;
    }

private:
    int ProcessMessage(int conn_fd);

private:
    std::mutex mutex_;
    std::vector<MemoryRegionInfo> mr_list_;

    int listen_fd_;
    std::vector<pollfd> poll_fd_;
};

class ConnectionClient
{
public:
    ConnectionClient();

    ~ConnectionClient();

    int Connect(const char *hostname, uint16_t tcp_port);

    void Close();

    int EstablishRC(const EndpointInfo &request, EndpointInfo &response);

    int RegisterMemoryRegion(const MemoryRegionInfo &mr);

    int ListMemoryRegions(std::vector<MemoryRegionInfo> &mr_list);

private:
    int conn_fd_;
};

#endif // CONNECTION_MANAGER_H
