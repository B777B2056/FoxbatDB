cmake_minimum_required(VERSION 3.27)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_definitions(-D_FOXBATDB_SELF_TEST)

find_package(Threads REQUIRED)
find_package(GTest CONFIG REQUIRED)

enable_testing()

cmake_path(GET CMAKE_CURRENT_SOURCE_DIR PARENT_PATH TEST_ROOT_DIR)
file(GLOB_RECURSE TOOLS_TEST_SRC "${TEST_ROOT_DIR}/tools/*.cc")
file(GLOB_RECURSE FUNCTION_TEST_SRC "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_executable(foxbatdb_test "main.cc" ${SRC} ${TOOLS_TEST_SRC} ${FUNCTION_TEST_SRC})
target_link_libraries(foxbatdb_test PRIVATE Threads::Threads GTest::gtest_main spdlog::spdlog)

#include(GoogleTest)
#gtest_discover_tests(foxbatdb_test DISCOVERY_TIMEOUT 600)