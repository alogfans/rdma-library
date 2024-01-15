add_executable(server server.cpp)
target_link_libraries(server PUBLIC librdma)

add_executable(client client.cpp)
target_link_libraries(client PUBLIC librdma)