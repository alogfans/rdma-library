// socket_interface.h
// Copyright (C) 2024 Feng Ren

#ifndef SOCKET_INTERFACE_H
#define SOCKET_INTERFACE_H

#include <vector>
#include <cerrno>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <sys/poll.h>
#include <arpa/inet.h>

enum
{
    OOB_CTRL_OP_INVALID = 0,
    OOB_CTRL_OP_EXCHANGE_ENDPOINT_INFO,
    OOB_CTRL_OP_REG_MR,
    OOB_CTRL_OP_LIST_MR,
    OOB_CTRL_OP_CLOSE
};

enum
{
    OOB_CTRL_RET_OK = 100,
    OOB_CTRL_RET_SERVER_ERROR,
};

struct MessageHeader
{
    uint8_t magic[3];     // should match "OOB"
    uint8_t code;         // code for request, retcode for response
    uint32_t payload_len; // assume less than 2^31B
};

static ssize_t WriteFully(int fd, const void *buf, size_t len)
{
    char *pos = (char *)buf;
    size_t nbytes = len;
    while (nbytes)
    {
        ssize_t rc = write(fd, pos, nbytes);
        if (rc < 0 && (errno == EAGAIN || errno == EINTR))
        {
            continue;
        }
        else if (rc < 0)
        {
            PLOG(ERROR) << "Write failed";
            return rc;
        }
        else if (rc == 0)
        {
            return len - nbytes;
        }
        pos += rc;
        nbytes -= rc;
    }
    return len;
}

static ssize_t ReadFully(int fd, void *buf, size_t len)
{
    char *pos = (char *)buf;
    size_t nbytes = len;
    while (nbytes)
    {
        ssize_t rc = read(fd, pos, nbytes);
        if (rc < 0 && (errno == EAGAIN || errno == EINTR))
        {
            continue;
        }
        else if (rc < 0)
        {
            PLOG(ERROR) << "Read failed";
            return rc;
        }
        else if (rc == 0)
        {
            return len - nbytes;
        }
        pos += rc;
        nbytes -= rc;
    }
    return len;
}

static int SendMessage(int fd, uint8_t code)
{
    MessageHeader hdr;
    memset(&hdr, 0, sizeof(MessageHeader));
    memcpy(hdr.magic, "OOB", 3);
    hdr.code = code;
    hdr.payload_len = htole32(0);
    if (WriteFully(fd, &hdr, sizeof(MessageHeader)) != sizeof(MessageHeader))
    {
        LOG(WARNING) << "Socket has been closed by peer";
        return -1;
    }

    return 0;
}

template <class Payload>
static int SendMessage(int fd, uint8_t code, std::vector<Payload> &payload)
{
    MessageHeader hdr;
    memset(&hdr, 0, sizeof(MessageHeader));
    size_t payload_len = payload.size() * sizeof(Payload);
    if (payload_len >= UINT32_MAX)
    {
        LOG(ERROR) << "Exceeded payload length " << payload_len;
        return -1;
    }

    memcpy(hdr.magic, "OOB", 3);
    hdr.code = code;
    hdr.payload_len = htole32(uint32_t(payload_len));

    if (WriteFully(fd, &hdr, sizeof(MessageHeader)) != sizeof(MessageHeader))
    {
        LOG(WARNING) << "Socket has been closed by peer";
        return -1;
    }

    if (WriteFully(fd, payload.data(), payload_len) != ssize_t(payload_len))
    {
        LOG(WARNING) << "Socket has been closed by peer";
        return -1;
    }

    return 0;
}

static int RecvMessage(int fd, uint8_t &code)
{
    MessageHeader hdr;
    if (ReadFully(fd, &hdr, sizeof(MessageHeader)) != sizeof(MessageHeader))
    {
        LOG(WARNING) << "Socket has been closed by peer";
        return -1;
    }

    if (memcmp(hdr.magic, "OOB", 3) != 0)
    {
        LOG(WARNING) << "Message magic string mismatch";
        return -1;
    }

    code = hdr.code;
    size_t payload_len = size_t(le32toh(hdr.payload_len));
    if (payload_len)
    {
        LOG(WARNING) << "Unexpected payload attached in received message";
        return -1;
    }

    return 0;
}

template <class Payload>
static int RecvMessage(int fd, uint8_t &code, std::vector<Payload> &payload)
{
    MessageHeader hdr;
    if (ReadFully(fd, &hdr, sizeof(MessageHeader)) != sizeof(MessageHeader))
    {
        LOG(WARNING) << "Socket has been closed by peer";
        return -1;
    }

    if (memcmp(hdr.magic, "OOB", 3) != 0)
    {
        LOG(WARNING) << "Message magic string mismatch";
        return -1;
    }

    code = hdr.code;
    size_t payload_len = size_t(le32toh(hdr.payload_len));
    if (payload_len % sizeof(Payload))
    {
        LOG(WARNING) << "Unexpected payload length " << payload_len
                     << " cannot divide by " << sizeof(Payload);
        return -1;
    }

    int payload_count = payload_len / sizeof(Payload);
    payload.resize(payload_count);
    if (ReadFully(fd, payload.data(), payload_len) != ssize_t(payload_len))
    {
        LOG(WARNING) << "Socket has been closed by peer";
        return -1;
    }

    return 0;
}

#endif // SOCKET_INTERFACE_H