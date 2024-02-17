cmake_minimum_required(VERSION 3.25)
project("FoxbatDB" VERSION "1.0.1")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb -fPIC -no-pie")
    set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O4 -Wall -fPIC -no-pie")
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    set(CMAKE_CXX_FLAGS_DEBUG "/Od /Zi /MTd")
    set(CMAKE_CXX_FLAGS_RELEASE "/O2 /MT /GS-")
endif ()

find_package(Threads REQUIRED)
add_subdirectory("3rdparty/spdlog")

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/src")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/asio/asio/include")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/hat-trie/include")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/spdlog/include")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/3rdparty/toml++")

file(GLOB_RECURSE SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")

add_executable(${PROJECT_NAME} ${SRC})
target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads spdlog::spdlog)