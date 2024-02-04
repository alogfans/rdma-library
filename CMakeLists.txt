cmake_minimum_required(VERSION 3.16)
project(sds CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-O3 -g -Wall")

include_directories(include)

add_library(rdma src/rdma_resources.cpp 
                    src/rdma_endpoint.cpp 
                    src/connection_manager.cpp 
                    src/memory_pool.cpp
                    src/async_executor.cpp)
target_link_libraries(rdma PUBLIC ibverbs glog gflags pthread)

add_subdirectory(example)
add_subdirectory(test)