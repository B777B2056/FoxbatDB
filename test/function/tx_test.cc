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
    // ע��kv�洢����
    dataset.Foreach(
            [cmdSession](const std::string& key, const std::string& val) -> void {
                InsertIntoDBNoTx(cmdSession, key, val);
            });
    EnableTx(cmdSession);
    // �����ڸ�������key
    int i = 0;
    dataset.Foreach(
            [cmdSession, &i](const std::string& key, const std::string& val) -> void {
                InsertIntoDBWithTx(cmdSession, key, val + "#" + std::to_string(i++));
            });
    DiscardTx(cmdSession);
    // ��kv�洢�����ȡ����������
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
    // ע��kv�洢����
    dataset.Foreach(
            [cmdSessionTx2](const std::string& key, const std::string& val) -> void {
                InsertIntoDBNoTx(cmdSessionTx2, key, val);
            });
    InsertIntoDBNoTx(cmdSessionTx2, watchedKey, "init watch val");

    // watch key
    cmdSessionTx2->DoExecOneCmd(::BuildCMD("watch", {watchedKey}));

    // ����2��ʼ
    EnableTx(cmdSessionTx2);

    // ����1����������2�޸ı�watch��key
    {
        auto cmdSessionTx1 = ::GetMockCMDSession();
        // ����1��ʼ
        EnableTx(cmdSessionTx1);
        InsertIntoDBWithTx(cmdSessionTx1, watchedKey, watchedVal);
        // ִ������1
        ExecTx(cmdSessionTx1);
    }

    // ����2����������1�޸ı�watch��key
    {
        std::size_t i = 0;
        auto datasetSize = dataset.Size();

        dataset.Foreach(
                [&](const std::string& key, const std::string& val) -> void {
                    if (i == datasetSize / 2) {
                        // ����2�ڲ�����watch��key
                        InsertIntoDBWithTx(cmdSessionTx2, watchedKey, watchedValModify);
                    } else {
                        // ����2�ڸ�������key
                        InsertIntoDBWithTx(cmdSessionTx2, key, val + "#" + std::to_string(i++));
                    }
                });
        // ִ������2
        ExecTx(cmdSessionTx2);
    }

    // ��kv�洢�����ȡ���������ݣ�����2ִ��ʧ�ܣ�Ӧ�ع�
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
    // ע��kv�洢����
    dataset.Foreach(
            [cmdSession](const std::string& key, const std::string& val) -> void {
                InsertIntoDBWithTx(cmdSession, key, val);
            });
    ExecTx(cmdSession);
    // ��kv�洢�����ȡ����������
    dataset.Foreach(
            [cmdSession](const std::string& key, const std::string& val) -> void {
                ReadAndTestFromDB(cmdSession, key, val);
            });
}

TEST(TxTest, WithRollback) {
    TestDataset dataset{64, 64, TimestampKeyGenerator};
    auto cmdSession = ::GetMockCMDSession();
    // ע��kv�洢����
    dataset.Foreach(
            [cmdSession](const std::string& key, const std::string& val) -> void {
                InsertIntoDBNoTx(cmdSession, key, val);
            });
    EnableTx(cmdSession);
    // �����ڸ�������key
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
    // ��kv�洢�����ȡ����������
    dataset.Foreach(
            [cmdSession](const std::string& key, const std::string& val) -> void {
                ReadAndTestFromDB(cmdSession, key, val);
            });
}