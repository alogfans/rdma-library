add_executable(server server.cpp)
target_link_libraries(server PUBLIC rdma)

add_executable(client client.cpp)
target_link_libraries(client PUBLIC rdma)

add_executable(client_interrupt client_interrupt.cpp)
target_link_libraries(client_interrupt PUBLIC rdma)