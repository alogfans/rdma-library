// utils.h
// Copyright (C) 2024 Feng Ren

#ifndef UTILS_H
#define UTILS_H

#include <glog/logging.h>
#include <sys/time.h>
#include <cstdint>
#include <ctime>

static int64_t GetCurrentTimeNanos() {
    const int64_t kNanosPerSecond = 1000 * 1000 * 1000;
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts)) {
        PLOG(ERROR) << "Failed to read real-time lock";
        return -1;
    }
    return (int64_t{ts.tv_sec} * kNanosPerSecond + int64_t{ts.tv_nsec});
}

#endif // UTILS_H