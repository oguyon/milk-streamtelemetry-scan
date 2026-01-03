cmake_minimum_required(VERSION 3.10)
project(milk-streamtelemetry-scan C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_DEFAULT_SOURCE")

add_executable(milk-streamtelemetry-scan src/main.c)
target_link_libraries(milk-streamtelemetry-scan m)
