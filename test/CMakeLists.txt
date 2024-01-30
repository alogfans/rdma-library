add_executable(rdma_test rdma_test.cpp)
target_link_libraries(rdma_test PUBLIC librdma gtest)

add_executable(memory_pool_test memory_pool_test.cpp)
target_link_libraries(memory_pool_test PUBLIC librdma gtest)

add_executable(async_executor_test async_executor_test.cpp)
target_link_libraries(async_executor_test PUBLIC librdma gtest)