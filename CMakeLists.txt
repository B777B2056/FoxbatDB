cmake_minimum_required (VERSION 3.27)
project ("FoxbatDB" VERSION "0.0.1")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_BUILD_TYPE "Debug")

set(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -Wall -g -ggdb -fPIC -no-pie")
set(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3 -Wall -fPIC -no-pie")

include_directories("${CMAKE_CURRENT_SOURCE_DIR}/src")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/third")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/third/asio")

find_package(Threads REQUIRED)

include(FetchContent)
FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG        v1.x
)
FetchContent_MakeAvailable(spdlog)

file(GLOB_RECURSE SRC "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")
file(GLOB_RECURSE 3rd_SRC "${CMAKE_CURRENT_SOURCE_DIR}/third/*.cc" "${CMAKE_CURRENT_SOURCE_DIR}/third/*.cpp")

add_executable(${PROJECT_NAME} "main.cc" ${SRC} ${3rd_SRC})
target_link_libraries(${PROJECT_NAME} PRIVATE Threads::Threads spdlog::spdlog)

add_subdirectory("test")