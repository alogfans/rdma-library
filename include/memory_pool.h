// memory_pool.h
// Copyright (C) 2024 Feng Ren

#ifndef MEMORY_POOL_H
#define MEMORY_POOL_H

#include <cstdint>
#include <cstddef>

int InitializeMemoryPool();

void DestroyMemoryPool();

void *AllocateMemory(size_t size);

void FreeMemory(void *ptr);

#endif // MEMORY_POOL_H