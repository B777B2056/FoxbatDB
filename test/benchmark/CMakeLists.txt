cmake_minimum_required (VERSION 3.27)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_definitions(-D_FOXBATDB_SELF_TEST)

find_package(Threads REQUIRED)
find_package(benchmark REQUIRED)

enable_testing()

cmake_path(GET CMAKE_CURRENT_SOURCE_DIR PARENT_PATH TEST_ROOT_DIR)
file(GLOB_RECURSE TOOLS_TEST_SRC "${TEST_ROOT_DIR}/tools/*.cc")
file(GLOB_RECURSE BENCHMARK_TEST_SRC "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_executable(foxbatdb_benchmark  ${SRC} ${3rd_SRC} ${TOOLS_TEST_SRC} ${BENCHMARK_TEST_SRC})
target_link_libraries(foxbatdb_benchmark PRIVATE Threads::Threads benchmark::benchmark spdlog::spdlog)