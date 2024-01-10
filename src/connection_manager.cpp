// connection_manager.cpp
// Copyright (C) 2024 Feng Ren

#include "connection_manager.h"

#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/poll.h>
#include <arpa/inet.h>

const static size_t kGidLength = 16;

struct EndpointEntry {
    char gid[kGidLength];
    uint32_t qp_num;
    uint16_t lid;
};

struct MemoryRegionEntry {
    uint64_t addr;
    uint64_t length;
    uint32_t access;
    uint32_t rkey;
};

enum {
    OOB_CTRL_OP_INVALID = 0,
    OOB_CTRL_OP_HELLO,
    OOB_CTRL_OP_ESTABLISH_RC,
    OOB_CTRL_OP_REG_MR,
    OOB_CTRL_OP_LIST_MR
};

struct MessageHeader {
    uint8_t magic[3];
    uint8_t opcode;
    uint32_t node_id;
    uint64_t payload_len;
};

static ssize_t WriteFully(int fd, const void *buf, size_t len) {
    char *pos = (char *) buf;
    size_t nbytes = len;
    while (nbytes) {
        ssize_t rc = write(fd, pos, nbytes);
        if (rc < 0 && (errno == EAGAIN || errno == EINTR)) {
            continue;
        } else if (rc < 0) {
            PLOG(ERROR) << "Write failed";
            return rc;
        } else if (rc == 0) {
            return len - nbytes;
        }
        pos += rc;
        nbytes -= rc;
    }
    return len;
}

static ssize_t ReadFully(int fd, void *buf, size_t len) {
    char *pos = (char *) buf;
    size_t nbytes = len;
    while (nbytes) {
        ssize_t rc = read(fd, pos, nbytes);
        if (rc < 0 && (errno == EAGAIN || errno == EINTR)) {
            continue;
        } else if (rc < 0) {
            PLOG(ERROR) << "Read failed";
            return rc;
        } else if (rc == 0) {
            return len - nbytes;
        }
        pos += rc;
        nbytes -= rc;
    }
    return len;
}

static int SendMessage(int fd, uint8_t opcode, uint32_t node_id, void *ptr, size_t len) {
    MessageHeader hdr;
    memset(&hdr, 0, sizeof(MessageHeader));
    memcpy(hdr.magic, "OOB", 3);
    hdr.opcode = opcode;
    hdr.node_id = htole32(node_id);
    hdr.payload_len = htole64(len);

    if (WriteFully(fd, &hdr, sizeof(MessageHeader)) != sizeof(MessageHeader)) {
        LOG(WARNING) << "Socket has been closed by peer";
        return -1;
    }

    if (len && WriteFully(fd, ptr, len) != ssize_t(len)) {
        LOG(WARNING) << "Socket has been closed by peer";
        return -1;
    }

    return 0;
}

static int RecvMessage(int fd, uint8_t *opcode, uint32_t *node_id, void **pptr, size_t *len) {
    MessageHeader hdr;

    if (ReadFully(fd, &hdr, sizeof(MessageHeader)) != sizeof(MessageHeader)) {
        LOG(WARNING) << "Socket has been closed by peer";
        return -1;
    }

    if (memcmp(hdr.magic, "OOB", 3) != 0) {
        LOG(WARNING) << "Message magic string mismatch";
        return -1;
    }

    if (opcode) 
        *opcode = hdr.opcode;
    if (node_id) 
        *node_id = le32toh(hdr.node_id);
    size_t payload_len = le64toh(hdr.payload_len);

    if (payload_len == 0) {
        if (len) *len = 0;
        return 0;
    }

    void *prealloc_buf = pptr ? *pptr : nullptr;
    if (prealloc_buf) {
        if (!len || *len < payload_len) {
            LOG(WARNING) << "No enough space to save received message";
            return -1;
        }
    } else {
        *pptr = malloc(payload_len);
        if (!*pptr) {
            PLOG(ERROR) << "Out of memory";
            return -1;
        }
    }

    if (ReadFully(fd, *pptr, payload_len) != ssize_t(payload_len)) {
        LOG(WARNING) << "Socket has been closed by peer";
        return -1;
    }

    if (len)
        *len = payload_len;
    
    return 0;
}

ConnMgmtServer::ConnMgmtServer() : next_node_id_(1), listen_fd_(-1) { }

ConnMgmtServer::~ConnMgmtServer() {
    if (!poll_fd_.empty()) {
        Close();
    }
}

int ConnMgmtServer::Listen(uint16_t tcp_port) {
    sockaddr_in bind_address;
    int on = 1, fd = -1;
    memset(&bind_address, 0, sizeof(sockaddr_in));
    bind_address.sin_family = AF_INET;
    bind_address.sin_port = htons(tcp_port);
    bind_address.sin_addr.s_addr = INADDR_ANY;

    if (!poll_fd_.empty()) {
        LOG(ERROR) << "ConnMgmtServer has been started";
        return -1;
    }

    fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        PLOG(ERROR) << "Failed to create socket";
        return -1;
    }

    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
        PLOG(ERROR) << "Failed to set address reusable";
        close(fd);
        return -1;
    }

    if (bind(fd, (sockaddr *) &bind_address, sizeof(sockaddr_in)) < 0) {
        PLOG(ERROR) << "Failed to bind address";
        close(fd);
        return -1;
    }

    if (listen(fd, 5)) {
        PLOG(ERROR) << "Failed to listen";
        close(fd);
        return -1;
    }

    poll_fd_.push_back({ fd, POLLIN, 0});
    listen_fd_ = fd;
    return 0;
}

int ConnMgmtServer::RunEventLoop() {
    if (poll_fd_.empty()) {
        LOG(ERROR) << "ConnMgmtServer is not started";
        return -1;
    }

    if (poll(poll_fd_.data(), poll_fd_.size(), 500) == -1) {
        PLOG(ERROR) << "Failed to poll sockets";
        return -1;
    }
    
    std::vector<int> opening_conn_fd_list;
    std::vector<int> closing_conn_fd_list;
    for (auto &entry: poll_fd_) {
        if (!(entry.revents & POLLIN)) {
            continue;
        }

        if (entry.fd == listen_fd_) {
            sockaddr_in addr;
            socklen_t addr_len = sizeof(sockaddr_in);
            int conn_fd = accept(listen_fd_, (sockaddr *) &addr, &addr_len);
            if (conn_fd < 0) {
                PLOG(ERROR) << "Failed to accept socket connection";
                continue;
            }

            if (addr.sin_family != AF_INET) {
                LOG(ERROR) << "Unsupported socket type: " << addr.sin_family;
                close(conn_fd);
                continue;
            }

            char inet_str[INET_ADDRSTRLEN];
            if (!inet_ntop(AF_INET, &(addr.sin_addr), inet_str, INET_ADDRSTRLEN)) {
                PLOG(ERROR) << "Failed to parse incoming address";
                close(conn_fd);
                continue;
            }

            LOG(INFO) << "New connection: "
                      << inet_str << ":" << ntohs(addr.sin_port) << ", fd: " << conn_fd;
            opening_conn_fd_list.push_back(conn_fd);
        } else {
            if (ProcessMessage(entry.fd)) {
                LOG(ERROR) << "Close connection fd: " << entry.fd;
                closing_conn_fd_list.push_back(entry.fd);
            }
        }
    }

    for (auto &entry: opening_conn_fd_list) {
        poll_fd_.push_back({entry, POLLIN, 0});
    }

    for (auto &entry: closing_conn_fd_list) {
        bool found = false;
        for (auto iter = poll_fd_.begin(); !found && iter != poll_fd_.end(); ++iter) {
            if (iter->fd == entry) {
                poll_fd_.erase(iter);
                found = true;
            }
        }
        if (fd2node_map_.count(entry)) {
            uint32_t node_id = fd2node_map_[entry];
            OnCloseConnection(node_id);
            fd2node_map_.erase(entry);
            node_map_.erase(node_id);
        }
        close(entry);
    }

    return 0;
}

void ConnMgmtServer::Close() {
    for (auto &entry : poll_fd_) {
        close(entry.fd);
    }
    poll_fd_.clear();
    listen_fd_ = -1;
}

int ConnMgmtServer::RegisterMemoryRegion(const MemoryRegionInfo &mr) {
    mutex_.lock();
    auto &mr_map_ = node_map_[SERVER_NODE_ID].mr_map;
    if (mr_map_.count(mr.addr)) {
        LOG(WARNING) << "Memory region " << mr.addr << " has been registered,"
                     << " the previous registration will be destroyed";
    }
    mr_map_[mr.addr] = mr;
    mutex_.unlock();
    return 0;
}

int ConnMgmtServer::ListMemoryRegions(uint32_t node_id, std::vector<MemoryRegionInfo> &mr_list) {
    mr_list.clear();
    mutex_.lock();
    if (node_map_.count(node_id)) {
        auto &mr_map_ = node_map_[node_id].mr_map;
        for (auto &entry : mr_map_) {
            mr_list.push_back(entry.second);
        }
    }
    mutex_.unlock();
    return 0;
}

int ConnMgmtServer::ProcessMessage(int conn_fd) {
    void *payload = nullptr;
    size_t payload_len = 0;
    uint8_t opcode;
    uint32_t node_id;

    if (RecvMessage(conn_fd, &opcode, &node_id, &payload, &payload_len)) {
        if (errno) {
            PLOG(ERROR) << "Failed to receive message";
        }
        return -1;
    }

    switch (opcode) {
    case OOB_CTRL_OP_HELLO:
    {
        mutex_.lock();
        if (node_id == ANY_NODE_ID || node_map_.count(node_id)) {
            node_id = next_node_id_;
            next_node_id_++;
        }
        node_map_[node_id].fd = conn_fd;
        fd2node_map_[conn_fd] = node_id;
        mutex_.unlock();

        if (SendMessage(conn_fd, opcode, node_id, nullptr, 0)) {
            PLOG(ERROR) << "Failed to send message";
            return -1;
        }
        break;
    }

    case OOB_CTRL_OP_ESTABLISH_RC:
    {
        EndpointEntry *data = reinterpret_cast<EndpointEntry *>(payload);
        EndpointInfo request, response;
        memcpy(&request.gid, data->gid, kGidLength);
        request.lid = le16toh(data->lid);
        request.qp_num = le32toh(data->qp_num);

        if (OnEstablishRC(node_id, request, response)) {
            PLOG(WARNING) << "Failed to establish RC connection";
            memset(data, 0, sizeof(EndpointEntry));
        } else {
            memcpy(data->gid, &response.gid, kGidLength);
            data->lid = htole16(request.lid);
            data->qp_num = htole32(request.qp_num);
        }

        if (SendMessage(conn_fd, opcode, node_id, data, sizeof(EndpointEntry))) {
            PLOG(ERROR) << "Failed to send message";
            return -1;
        }
        break;
    }
    
    case OOB_CTRL_OP_REG_MR:
    {
        MemoryRegionEntry *data = (MemoryRegionEntry *)(payload);
        MemoryRegionInfo request;
        request.addr = (void *) le64toh(data->addr);
        request.length = le64toh(data->length);
        request.access = le32toh(data->access);
        request.rkey = le32toh(data->rkey);

        mutex_.lock();
        if (!node_map_.count(node_id)) {
            LOG(WARNING) << "Node " << node_id << " does not existed";
        } else {
            auto &mr_map = node_map_[node_id].mr_map;
            if (mr_map.count(request.addr)) {
                LOG(WARNING) << "Memory region " << request.addr
                             << " at Node " << node_id << " is existed. "
                             << "Previous one will be removed.";
            }
            mr_map[request.addr] = request;
        }
        mutex_.unlock();

        if (SendMessage(conn_fd, opcode, node_id, nullptr, 0)) {
            PLOG(ERROR) << "Failed to send message";
            return -1;
        }
        break;
    }

    case OOB_CTRL_OP_LIST_MR:
    {
        MemoryRegionEntry *data = nullptr;
        int nr_entries = 0;

        mutex_.lock();
        if (!node_map_.count(node_id)) {
            LOG(WARNING) << "Node " << node_id << " does not existed";
        } else {
            auto &mr_map = node_map_[node_id].mr_map;
            if (!mr_map.empty()) {
                data = new MemoryRegionEntry[mr_map.size()];
            }
            for (auto &entry : mr_map) {
                data[nr_entries].addr = htole64(uint64_t(entry.second.addr));
                data[nr_entries].length = htole64(uint64_t(entry.second.length));
                data[nr_entries].access = htole32(uint32_t(entry.second.access));
                data[nr_entries].rkey = htole32(entry.second.rkey);
                nr_entries++;
            }
        }
        mutex_.unlock();

        if (SendMessage(conn_fd, opcode, node_id, data, nr_entries * sizeof(MemoryRegionEntry))) {
            PLOG(ERROR) << "Failed to send message";
            return -1;
        }

        if (data) {
            delete []data;
        }

        break;
    }
    
    default:
        LOG(ERROR) << "Unsupported opcode: " << opcode;
        if (payload) {
            free(payload);
        }
        return -1;
    }

    if (payload) {
        free(payload);
    }

    return 0;
}

ConnMgmtClient::ConnMgmtClient() : conn_fd_(-1), local_node_id_(ANY_NODE_ID) { }

ConnMgmtClient::~ConnMgmtClient() {
    if (conn_fd_ >= 0) {
        Close();
    }
}

int ConnMgmtClient::Connect(const char *hostname, uint16_t tcp_port) {
    int on = 1;
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    char service[16];
    sprintf(service, "%u", tcp_port);
    if (getaddrinfo(hostname, service, &hints, &result)) {
        PLOG(ERROR) << "Failed to get address from " << hostname << ":" << tcp_port;
        return -1;
    }

    for (rp = result; conn_fd_ < 0 && rp; rp = rp->ai_next) {
        conn_fd_ = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (conn_fd_ == -1) {
            LOG(ERROR) << "Failed to create socket";
            continue;
        }
        if (setsockopt(conn_fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
            PLOG(ERROR) << "Failed to set address reusable";
            close(conn_fd_);
            conn_fd_ = -1;
            continue;
        }
        if (connect(conn_fd_, rp->ai_addr, rp->ai_addrlen)) {
            PLOG(ERROR) << "Failed to connect to " << hostname << ":" << tcp_port;
            close(conn_fd_);
            conn_fd_ = -1;
            continue;
        }

        uint32_t node_id = local_node_id_;
        if (SendMessage(conn_fd_, OOB_CTRL_OP_HELLO, node_id, nullptr, 0) 
                || RecvMessage(conn_fd_, nullptr, &node_id, nullptr, nullptr)) {
            PLOG(ERROR) << "Failed to receive hello message from " << hostname << ":" << tcp_port;
            close(conn_fd_);
            conn_fd_ = -1;
            continue;
        }
        local_node_id_ = node_id;
    }

    freeaddrinfo(result);
    LOG(INFO) << "Connected to " << hostname << ":" << tcp_port
              << ", Node ID: " << local_node_id_;
    return 0;
}

void ConnMgmtClient::Close() {
    if (conn_fd_) {
        close(conn_fd_);
        conn_fd_ = -1;
    }
}

int ConnMgmtClient::EstablishRC(const EndpointInfo &request, EndpointInfo &response) {
    EndpointEntry data;
    void *data_ptr = &data;
    size_t data_len = sizeof(EndpointEntry);
    memcpy(data.gid, &request.gid, kGidLength);
    data.lid = htole16(request.lid);
    data.qp_num = htole32(request.qp_num);

    if (SendMessage(conn_fd_, OOB_CTRL_OP_ESTABLISH_RC, local_node_id_, &data, data_len) 
            || RecvMessage(conn_fd_, nullptr, nullptr, &data_ptr, &data_len)) {
        PLOG(ERROR) << "Failed to transfer message";
        return -1;
    }

    memcpy(&response.gid, data.gid, kGidLength);
    response.lid = le16toh(data.lid);
    response.qp_num = le32toh(data.qp_num);
    return 0;
}

int ConnMgmtClient::RegisterMemoryRegion(const MemoryRegionInfo &mr) {
    MemoryRegionEntry data;
    size_t data_len = sizeof(MemoryRegionEntry);

    data.addr = htole64(uint64_t(mr.addr));
    data.length = htole64(uint64_t(mr.length));
    data.access = htole32(uint32_t(mr.access));
    data.rkey = htole32(mr.rkey);

    if (SendMessage(conn_fd_, OOB_CTRL_OP_REG_MR, local_node_id_, &data, data_len) 
            || RecvMessage(conn_fd_, nullptr, nullptr, nullptr, nullptr)) {
        PLOG(ERROR) << "Failed to transfer message";
        return -1;
    }

    return 0;
}

int ConnMgmtClient::ListMemoryRegions(uint32_t node_id, std::vector<MemoryRegionInfo> &mr_list) {
    void *data_ptr = nullptr;
    size_t data_len = 0;
    if (SendMessage(conn_fd_, OOB_CTRL_OP_LIST_MR, node_id, nullptr, 0) 
            || RecvMessage(conn_fd_, nullptr, nullptr, &data_ptr, &data_len)) {
        PLOG(ERROR) << "Failed to transfer message";
        return -1;
    }
    MemoryRegionEntry *entries = reinterpret_cast<MemoryRegionEntry *>(data_ptr);
    mr_list.resize(data_len / sizeof(MemoryRegionEntry));
    for (size_t i = 0; i < mr_list.size(); ++i) {
        mr_list[i].addr = (void *) le64toh(entries[i].addr);
        mr_list[i].length = (size_t) le64toh(entries[i].length);
        mr_list[i].access = le32toh(entries[i].access);
        mr_list[i].rkey = le32toh(entries[i].rkey);
    }
    free(data_ptr);
    return 0;
}
