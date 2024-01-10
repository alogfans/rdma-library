cmake_minimum_required(VERSION 3.16)
project(sds CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_FLAGS "-O3 -g -Wall")

include_directories(include)

add_library(librdma src/rdma_resources.cpp src/rdma_endpoint.cpp src/connection_manager.cpp)
target_link_libraries(librdma PUBLIC ibverbs glog gflags pthread)

add_executable(rdma_resources_test test/rdma_resources_test.cpp)
target_link_libraries(rdma_resources_test PUBLIC librdma gtest)