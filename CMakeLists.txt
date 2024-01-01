cmake_minimum_required (VERSION 3.27)
project ("FoxbatDB" VERSION "0.0.1")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_BUILD_TYPE "Debug")
# set(CMAKE_BUILD_TYPE "Release")

set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb -fPIC -no-pie")
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall -fPIC -no-pie")

find_package(Threads REQUIRED)
add_subdirectory("3rdparty/spdlog")

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/src")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/asio/asio/include")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/cmdline")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/hat-trie/include")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/spdlog/include")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/tinyfsm/include")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/toml++")

file(GLOB_RECURSE SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")

add_executable(${PROJECT_NAME} "main.cc" ${SRC})
target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads spdlog::spdlog)

add_subdirectory("test")