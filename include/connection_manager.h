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

struct EndpointInfo {
    ibv_gid gid;
    uint16_t lid;
    uint32_t qp_num;
};

struct MemoryRegionInfo {
    void *addr;
    size_t length;
    int access;
    uint32_t rkey;
};

class ConnMgmtServer {
public:
    ConnMgmtServer();

    virtual ~ConnMgmtServer();

    int Listen(uint16_t tcp_port);

    int RunEventLoop();

    void Close();

    uint32_t GetLocalNodeID() const { return SERVER_NODE_ID; }

    int RegisterMemoryRegion(const MemoryRegionInfo &mr);

    int ListMemoryRegions(uint32_t node_id, std::vector<MemoryRegionInfo> &mr_list);

protected:
    virtual void OnCloseConnection(uint32_t node_id) { }

    virtual int OnEstablishRC(uint32_t node_id, const EndpointInfo &request, EndpointInfo &response)  { 
        return -1; 
    }

private:
    int ProcessMessage(int conn_fd);

private:
    struct NodeInfo {
        int fd;
        std::map<void *, MemoryRegionInfo> mr_map;
    };

    uint32_t next_node_id_;

    std::mutex mutex_;
    std::map<int, uint32_t> fd2node_map_;
    std::map<uint32_t, NodeInfo> node_map_;

    int listen_fd_;
    std::vector<pollfd> poll_fd_;
};

class ConnMgmtClient {
public:
    ConnMgmtClient();

    ~ConnMgmtClient();

    int Connect(const char *hostname, uint16_t tcp_port);

    void Close();

    uint32_t GetLocalNodeID() const { return local_node_id_; }

    int EstablishRC(const EndpointInfo &request, EndpointInfo &response);

    int RegisterMemoryRegion(const MemoryRegionInfo &mr);

    int ListMemoryRegions(uint32_t node_id, std::vector<MemoryRegionInfo> &mr_list);

private:
    int conn_fd_;
    uint32_t local_node_id_;
};

#endif // CONNECTION_MANAGER_H
