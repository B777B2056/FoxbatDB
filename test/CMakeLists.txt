cmake_minimum_required (VERSION 3.27)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_definitions(-D_FOXBATDB_SELF_TEST)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

find_package(Threads REQUIRED)

enable_testing()

include_directories(".")
file(GLOB_RECURSE TOOLS_TEST_SRC "${CMAKE_CURRENT_SOURCE_DIR}/tools/*.cc")
file(GLOB_RECURSE FUNCTION_TEST_SRC "${CMAKE_CURRENT_SOURCE_DIR}/function/*.cc")

add_executable(foxbatdb_test "main.cc" ${SRC} ${3rd_SRC} ${TOOLS_TEST_SRC} ${FUNCTION_TEST_SRC})
target_link_libraries(foxbatdb_test PUBLIC Threads::Threads GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(foxbatdb_test DISCOVERY_TIMEOUT 600)