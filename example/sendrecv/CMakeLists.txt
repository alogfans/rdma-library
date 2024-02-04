add_executable(receiver receiver.cpp)
target_link_libraries(receiver PUBLIC rdma)

add_executable(sender sender.cpp)
target_link_libraries(sender PUBLIC rdma)
