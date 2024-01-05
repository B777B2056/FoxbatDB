#include "errors/runtime.h"
#include "tools/tools.h"
#include "utils/resp.h"
#include "utils/utils.h"
#include <gtest/gtest.h>

using namespace foxbatdb;
using CMDServerPtr = std::shared_ptr<foxbatdb::CMDSession>;

static std::string TimestampKeyGenerator() {
    static int i = 1;
    return std::to_string(i++) + GenRandomString(32);
}

static void EnableTx(CMDServerPtr cmdSession) {
    static auto expectResp = utils::BuildResponse("OK");
    auto cmd = ::BuildCMD("multi", {});
    EXPECT_EQ(cmdSession->DoExecOneCmd(cmd), expectResp);
}

static void ExecTx(CMDServerPtr cmdSession) {
    auto cmd = ::BuildCMD("exec", {});
    cmdSession->DoExecOneCmd(cmd);
}

static void DiscardTx(CMDServerPtr cmdSession) {
    static auto expectResp = utils::BuildResponse("OK");
    auto cmd = ::BuildCMD("discard", {});
    EXPECT_EQ(cmdSession->DoExecOneCmd(cmd), expectResp);
}

static void SendInvalidCmd(CMDServerPtr cmdSession) {
    auto cmd = ::BuildCMD("invalid command", {});
    cmdSession->DoExecOneCmd(cmd);
}

static void InsertIntoDBNoTx(CMDServerPtr cmdSession, const std::string& key, const std::string& val) {
    auto cmd = ::BuildCMD("set", {key, val});
    cmdSession->DoExecOneCmd(cmd);
}

static void InsertIntoDBWithTx(CMDServerPtr cmdSession,
                               const std::string& key, const std::string& val) {
    auto cmd = ::BuildCMD("set", {key, val});
    cmdSession->DoExecOneCmd(cmd);
}

static void ReadAndTestFromDB(CMDServerPtr cmdSession, const std::string& key, const std::string& val) {
    auto cmd = ::BuildCMD("get", {key});
    auto resp = cmdSession->DoExecOneCmd(cmd);
    EXPECT_EQ(resp, utils::BuildResponse(val)) << "key: " << key;
}

TEST(TxTest, WithDiscard) {
    TestDataset dataset{64, 64, TimestampKeyGenerator};
    auto cmdSession = ::GetMockCMDSession();
    // 注入kv存储引擎
    dataset.Foreach(
            [cmdSession](const std::string& key, const std::string& val) -> void {
                InsertIntoDBNoTx(cmdSession, key, val);
            });
    EnableTx(cmdSession);
    // 事务内更新已有key
    int i = 0;
    dataset.Foreach(
            [cmdSession, &i](const std::string& key, const std::string& val) -> void {
                InsertIntoDBWithTx(cmdSession, key, val + "#" + std::to_string(i++));
            });
    DiscardTx(cmdSession);
    // 从kv存储引擎读取并测试数据
    dataset.Foreach(
            [cmdSession](const std::string& key, const std::string& val) -> void {
                ReadAndTestFromDB(cmdSession, key, val);
            });
}

TEST(TxTest, WithWatch) {
    std::string watchedKey = "watch key";
    std::string watchedVal = GenRandomString(64);
    std::string watchedValModify = GenRandomString(64);

    TestDataset dataset{64, 64, TimestampKeyGenerator};
    auto cmdSessionTx2 = ::GetMockCMDSession();
    // 注入kv存储引擎
    dataset.Foreach(
            [cmdSessionTx2](const std::string& key, const std::string& val) -> void {
                InsertIntoDBNoTx(cmdSessionTx2, key, val);
            });
    InsertIntoDBNoTx(cmdSessionTx2, watchedKey, "init watch val");

    // watch key
    cmdSessionTx2->DoExecOneCmd(::BuildCMD("watch", {watchedKey}));

    // 事务2开始
    EnableTx(cmdSessionTx2);

    // 事务1：先于事务2修改被watch的key
    {
        auto cmdSessionTx1 = ::GetMockCMDSession();
        // 事务1开始
        EnableTx(cmdSessionTx1);
        InsertIntoDBWithTx(cmdSessionTx1, watchedKey, watchedVal);
        // 执行事务1
        ExecTx(cmdSessionTx1);
    }

    // 事务2：后于事务1修改被watch的key
    {
        std::size_t i = 0;
        auto datasetSize = dataset.Size();

        dataset.Foreach(
                [&](const std::string& key, const std::string& val) -> void {
                    if (i == datasetSize / 2) {
                        // 事务2内插入已watch的key
                        InsertIntoDBWithTx(cmdSessionTx2, watchedKey, watchedValModify);
                    } else {
                        // 事务2内更新已有key
                        InsertIntoDBWithTx(cmdSessionTx2, key, val + "#" + std::to_string(i++));
                    }
                });
        // 执行事务2
        ExecTx(cmdSessionTx2);
    }

    // 从kv存储引擎读取并测试数据：事务2执行失败，应回滚
    ReadAndTestFromDB(cmdSessionTx2, watchedKey, watchedVal);
    dataset.Foreach(
            [cmdSessionTx2](const std::string& key, const std::string& val) -> void {
                ReadAndTestFromDB(cmdSessionTx2, key, val);
            });

    cmdSessionTx2->DoExecOneCmd(::BuildCMD("del", {watchedKey}));
}

TEST(TxTest, WithNoRollback) {
    TestDataset dataset{64, 64, TimestampKeyGenerator};
    auto cmdSession = ::GetMockCMDSession();
    EnableTx(cmdSession);
    // 注入kv存储引擎
    dataset.Foreach(
            [cmdSession](const std::string& key, const std::string& val) -> void {
                InsertIntoDBWithTx(cmdSession, key, val);
            });
    ExecTx(cmdSession);
    // 从kv存储引擎读取并测试数据
    dataset.Foreach(
            [cmdSession](const std::string& key, const std::string& val) -> void {
                ReadAndTestFromDB(cmdSession, key, val);
            });
}

TEST(TxTest, WithRollback) {
    TestDataset dataset{64, 64, TimestampKeyGenerator};
    auto cmdSession = ::GetMockCMDSession();
    // 注入kv存储引擎
    dataset.Foreach(
            [cmdSession](const std::string& key, const std::string& val) -> void {
                InsertIntoDBNoTx(cmdSession, key, val);
            });
    EnableTx(cmdSession);
    // 事务内更新已有key
    std::size_t i = 0;
    auto datasetSize = dataset.Size();
    dataset.Foreach(
            [cmdSession, datasetSize, &i](const std::string& key, const std::string& val) -> void {
                if (i == datasetSize / 2) {
                    SendInvalidCmd(cmdSession);
                } else {
                    InsertIntoDBWithTx(cmdSession, key, val + "#" + std::to_string(i++));
                }
            });
    ExecTx(cmdSession);
    // 从kv存储引擎读取并测试数据
    dataset.Foreach(
            [cmdSession](const std::string& key, const std::string& val) -> void {
                ReadAndTestFromDB(cmdSession, key, val);
            });
}