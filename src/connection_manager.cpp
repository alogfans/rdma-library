// connection_manager.cpp
// Copyright (C) 2024 Feng Ren

#include "connection_manager.h"
#include "socket_interface.h"

#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/poll.h>
#include <arpa/inet.h>

ConnectionManager::ConnectionManager() : listen_fd_(-1) { }

ConnectionManager::~ConnectionManager() {
    if (!poll_fd_.empty()) {
        Close();
    }
}

int ConnectionManager::Listen(uint16_t tcp_port) {
    sockaddr_in bind_address;
    int on = 1, fd = -1;
    memset(&bind_address, 0, sizeof(sockaddr_in));
    bind_address.sin_family = AF_INET;
    bind_address.sin_port = htons(tcp_port);
    bind_address.sin_addr.s_addr = INADDR_ANY;

    if (!poll_fd_.empty()) {
        LOG(ERROR) << "ConnectionManager has been started";
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

int ConnectionManager::ProcessEvents() {
    if (poll_fd_.empty()) {
        LOG(ERROR) << "ConnectionManager is not started";
        return -1;
    }

    if (poll(poll_fd_.data(), poll_fd_.size(), 500) == -1) {
        PLOG(ERROR) << "Failed to poll sockets";
        return -1;
    }
    
    std::vector<int> open_fd_list;
    std::vector<int> close_fd_list;
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

            OnNewConnection(conn_fd);
            open_fd_list.push_back(conn_fd);
        } else {
            if (ProcessMessage(entry.fd)) {
                LOG(ERROR) << "Close connection fd: " << entry.fd;
                close_fd_list.push_back(entry.fd);
            }
        }
    }

    for (auto &entry: open_fd_list) {
        poll_fd_.push_back({entry, POLLIN, 0});
    }

    for (auto &entry: close_fd_list) {
        bool found = false;
        for (auto iter = poll_fd_.begin(); !found && iter != poll_fd_.end(); ++iter) {
            if (iter->fd == entry) {
                poll_fd_.erase(iter);
                found = true;
            }
        }
        OnCloseConnection(entry);
        close(entry);
    }

    return 0;
}

void ConnectionManager::Close() {
    for (auto &entry : poll_fd_) {
        close(entry.fd);
    }
    poll_fd_.clear();
    listen_fd_ = -1;
}

int ConnectionManager::RegisterMemoryRegion(const MemoryRegionInfo &mr) {
    mutex_.lock();
    mr_list_.push_back(mr);
    mutex_.unlock();
    return 0;
}

int ConnectionManager::ListMemoryRegions(std::vector<MemoryRegionInfo> &mr_list) {
    mutex_.lock();
    mr_list = mr_list_;
    mutex_.unlock();
    return 0;
}

int ConnectionManager::ProcessMessage(int conn_fd) {
    uint8_t opcode;
    std::vector<char> payload_buffer;

    if (RecvMessage(conn_fd, opcode, payload_buffer)) {
        if (errno) {
            PLOG(ERROR) << "Failed to receive message";
        } else {
            PLOG(ERROR) << "Unexpected EOF";
        }
        return -1;
    }

    switch (opcode) {
    case OOB_CTRL_OP_ESTABLISH_RC:
    {
        if (payload_buffer.size() != sizeof(EndpointInfo)) {
            LOG(ERROR) << "Expected payload length " << sizeof(EndpointInfo)
                       << " actual " << payload_buffer.size();
            return -1;
        }
        EndpointInfo request, response;
        request.DecodeFrom(payload_buffer);
        uint8_t retcode = OOB_CTRL_RET_OK;
        if (OnEstablishRC(conn_fd, request, response)) {
            PLOG(WARNING) << "Failed to establish RC connection";
            retcode = OOB_CTRL_RET_SERVER_ERROR;
        }
        std::vector<EndpointInfo> response_list;
        response_list.push_back(response.EncodeTo());
        if (SendMessage(conn_fd, retcode, response_list)) {
            PLOG(ERROR) << "Failed to send message";
            return -1;
        }
        break;
    }
    
    case OOB_CTRL_OP_REG_MR:
    {
        if (payload_buffer.size() != sizeof(MemoryRegionInfo)) {
            LOG(ERROR) << "Unexpected payload length " << payload_buffer.size();
            return -1;
        }
        MemoryRegionInfo request;
        request.DecodeFrom(payload_buffer);
        mutex_.lock();
        mr_list_.push_back(request);
        mutex_.unlock();
        if (SendMessage(conn_fd, OOB_CTRL_RET_OK)) {
            PLOG(ERROR) << "Failed to send message";
            return -1;
        }
        break;
    }

    case OOB_CTRL_OP_LIST_MR:
    {
        std::vector<MemoryRegionInfo> response_list;
        mutex_.lock();
        for (auto &entry : mr_list_) {
            response_list.push_back(entry.EncodeTo());
        }
        mutex_.unlock();
        if (SendMessage(conn_fd, OOB_CTRL_RET_OK, response_list)) {
            PLOG(ERROR) << "Failed to send message";
            return -1;
        }
        break;
    }
    case OOB_CTRL_OP_CLOSE:
    {
        // Close the connection gracefully
        return -1;
    }
    
    default:
        LOG(ERROR) << "Unsupported opcode: " << opcode;
        return -1;
    }

    return 0;
}

ConnectionClient::ConnectionClient() : conn_fd_(-1) { }

ConnectionClient::~ConnectionClient() {
    if (conn_fd_ >= 0) {
        Close();
    }
}

int ConnectionClient::Connect(const char *hostname, uint16_t tcp_port) {
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
    }

    freeaddrinfo(result);
    LOG(INFO) << "Connected to " << hostname << ":" << tcp_port;
    return 0;
}

void ConnectionClient::Close() {
    if (conn_fd_) {
        if (SendMessage(conn_fd_, OOB_CTRL_OP_CLOSE)) {
            PLOG(ERROR) << "Failed to send message";
        }
        close(conn_fd_);
        conn_fd_ = -1;
    }
}

int ConnectionClient::EstablishRC(const EndpointInfo &request, EndpointInfo &response) {
    std::vector<EndpointInfo> request_list, response_list;
    request_list.push_back(request.EncodeTo());
    if (SendMessage(conn_fd_, OOB_CTRL_OP_ESTABLISH_RC, request_list)) {
        PLOG(ERROR) << "Failed to send message";
        return -1;
    }
    uint8_t retcode;
    if (RecvMessage(conn_fd_, retcode, response_list)) {
        PLOG(ERROR) << "Failed to receive message";
        return -1;
    }
    if (retcode != OOB_CTRL_RET_OK || response_list.empty()) {
        LOG(ERROR) << "Failed on peer node, retcode: " << int(retcode);
        return -1;
    }
    response.DecodeFrom(&response_list[0]);
    return 0;
}

int ConnectionClient::RegisterMemoryRegion(const MemoryRegionInfo &mr) {
    std::vector<MemoryRegionInfo> request_list;
    request_list.push_back(mr.EncodeTo());
    if (SendMessage(conn_fd_, OOB_CTRL_OP_REG_MR, request_list)) {
        PLOG(ERROR) << "Failed to send message";
        return -1;
    }
    uint8_t retcode;
    if (RecvMessage(conn_fd_, retcode)) {
        PLOG(ERROR) << "Failed to receive message";
        return -1;
    }
    if (retcode != OOB_CTRL_RET_OK) {
        LOG(ERROR) << "Failed on peer node, retcode: " << int(retcode);
        return -1;
    }
    return 0;
}

int ConnectionClient::ListMemoryRegions(std::vector<MemoryRegionInfo> &mr_list) {
    std::vector<MemoryRegionInfo> response_list;
    if (SendMessage(conn_fd_, OOB_CTRL_OP_LIST_MR)) {
        PLOG(ERROR) << "Failed to send message";
        return -1;
    }
    uint8_t retcode;
    if (RecvMessage(conn_fd_, retcode, response_list)) {
        PLOG(ERROR) << "Failed to receive message";
        return -1;
    }
    if (retcode != OOB_CTRL_RET_OK) {
        LOG(ERROR) << "Failed on peer node, retcode: " << int(retcode);
        return -1;
    }
    mr_list.resize(response_list.size());
    for (size_t i = 0; i < response_list.size(); ++i) {
        mr_list[i].DecodeFrom(&response_list[i]);
    }
    return 0;
}
