cmake_minimum_required(VERSION 3.27)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_definitions(-D_FOXBATDB_SELF_TEST)

find_package(Threads REQUIRED)
find_package(benchmark REQUIRED)

set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -O3 -Wall -fPIC -no-pie")

enable_testing()

cmake_path(GET CMAKE_CURRENT_SOURCE_DIR PARENT_PATH TEST_ROOT_DIR)
file(GLOB_RECURSE TOOLS_TEST_SRC "${TEST_ROOT_DIR}/tools/*.cc")

add_executable(foxbatdb_benchmark_embedded "benchmark_embedded.cc" ${SRC} ${TOOLS_TEST_SRC})
target_link_libraries(foxbatdb_benchmark_embedded PRIVATE Threads::Threads benchmark::benchmark spdlog::spdlog)

add_executable(foxbatdb_benchmark_external "benchmark_external.cc" ${SRC} ${TOOLS_TEST_SRC})
target_link_libraries(foxbatdb_benchmark_external PRIVATE Threads::Threads benchmark::benchmark spdlog::spdlog)