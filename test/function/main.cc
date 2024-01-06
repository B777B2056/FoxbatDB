#include "tools/tools.h"
#include <gtest/gtest.h>

static std::string flagConfPath = "/mnt/e/jr/FoxbatDB/config/flag.toml";

int main(int argc, char** argv) {
    InitComponents(flagConfPath);
    testing::InitGoogleTest(&argc, argv);
    ::testing::GTEST_FLAG(filter) = "MergeTest.*";
    return RUN_ALL_TESTS();
}