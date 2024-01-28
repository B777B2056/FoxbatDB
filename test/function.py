import unittest
import redis
import utils
import copy
import time
from typing import Dict
from threading import Thread

DBHost = "localhost"
DBPort = 7698
MaximumStrSize: int = 1024


def generateTestDataSet(dataSetSize: int) -> Dict[str, str]:
    global MaximumStrSize
    dataset = {}
    while len(dataset) < dataSetSize:
        key = utils.generateRandomStr(MaximumStrSize)
        val = utils.generateRandomStr(MaximumStrSize)
        dataset[key] = val
    return dataset


class TestKeyValueOperators(unittest.TestCase):
    DataSetSize: int = 128

    @classmethod
    def setUpClass(cls):
        cls.client = redis.Redis(host=DBHost, port=DBPort, decode_responses=True, protocol=3)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def test_set_no_option_get(self):
        dataset = generateTestDataSet(TestKeyValueOperators.DataSetSize)
        for k, v in dataset.items():
            self.assertTrue(self.client.set(k, v))

        for k, v in dataset.items():
            self.assertEqual(v, self.client.get(k))

        return

    def test_set_ex(self):
        expireSeconds: int = 3
        dataset = generateTestDataSet(TestKeyValueOperators.DataSetSize)
        for k, v in dataset.items():
            self.assertTrue(self.client.set(k, v, ex=expireSeconds))

        time.sleep(expireSeconds)

        for k, v in dataset.items():
            self.assertFalse(self.client.exists(k))

    def test_set_nx(self):
        dataset = generateTestDataSet(TestKeyValueOperators.DataSetSize)
        cnt = 0
        for k, v in dataset.items():
            if cnt > (TestKeyValueOperators.DataSetSize / 2):
                break
            self.assertTrue(self.client.set(k, v))
            cnt += 1

        cnt = 0
        for k, v in dataset.items():
            ret = self.client.set(k, v, nx=True)
            if cnt > (TestKeyValueOperators.DataSetSize / 2):
                self.assertTrue(ret)
            else:
                self.assertFalse(ret)
            cnt += 1

        for k, v in dataset.items():
            self.assertEqual(v, self.client.get(k))

    def test_set_xx(self):
        dataset = generateTestDataSet(TestKeyValueOperators.DataSetSize)
        cnt = 0
        for k, v in dataset.items():
            if cnt > (TestKeyValueOperators.DataSetSize / 2):
                break
            self.assertTrue(self.client.set(k, v))
            cnt += 1

        cnt = 0
        for k, v in dataset.items():
            ret = self.client.set(k, v, xx=True)
            if cnt <= (TestKeyValueOperators.DataSetSize / 2):
                self.assertTrue(ret)
            else:
                self.assertFalse(ret)
            cnt += 1

    def test_set_get(self):
        dataset = generateTestDataSet(TestKeyValueOperators.DataSetSize)
        cnt = 0
        for k, v in dataset.items():
            if cnt > (TestKeyValueOperators.DataSetSize / 2):
                break
            self.assertTrue(self.client.set(k, v))
            dataset[k] = "modify_" + v
            cnt += 1

        cnt = 0
        for k, v in dataset.items():
            ret = self.client.set(k, v, get=True)
            if cnt <= (TestKeyValueOperators.DataSetSize / 2):
                self.assertEqual(v[len("modify_"):], ret)
            else:
                self.assertIsNone(ret)
            cnt += 1

    def test_del(self):
        dataset = generateTestDataSet(TestKeyValueOperators.DataSetSize)
        for k, v in dataset.items():
            self.assertTrue(self.client.set(k, v))

        for k, v in dataset.items():
            self.assertEqual(1, self.client.delete(k))

        for k, _ in dataset.items():
            self.assertFalse(self.client.exists(k))


class TestTransaction(unittest.TestCase):
    DataSetSize: int = 16

    @classmethod
    def setUpClass(cls):
        cls.client = redis.Redis(host=DBHost, port=DBPort, decode_responses=True, protocol=3)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def test_tx_atomic(self):
        cnt = 0
        dataset = generateTestDataSet(TestTransaction.DataSetSize)

        self.client.execute_command("MULTI")
        for k, v in dataset.items():
            if cnt < TestTransaction.DataSetSize / 2:
                self.client.execute_command("SET", k, v)
            else:
                # Delete the unknown key, make db to raise an error for testing auto rollback
                self.client.execute_command("DEL", k)
            cnt += 1
        self.client.execute_command("EXEC")

        for k, v in dataset.items():
            self.assertFalse(self.client.exists(k))

    def test_tx_normal_set(self):
        dataset = generateTestDataSet(TestTransaction.DataSetSize)

        self.client.execute_command("MULTI")
        for k, v in dataset.items():
            self.client.execute_command("SET", k, v)
        self.client.execute_command("EXEC")

        for k, v in dataset.items():
            self.assertEqual(v, self.client.get(k))

    def test_tx_discard(self):
        dataset = generateTestDataSet(TestTransaction.DataSetSize)

        cnt = 0
        for k, v in dataset.items():
            self.assertTrue(self.client.set(k, str(cnt)))
            cnt += 1

        # 事务内更新已有key
        self.client.execute_command("MULTI")
        for k, v in dataset.items():
            self.client.execute_command("SET", k, "modify_" + v)
        self.client.execute_command("DISCARD")

        # 检查是否更新未发生
        cnt = 0
        for k, v in dataset.items():
            self.assertEqual(str(cnt), self.client.get(k))
            cnt += 1


class TestPubSub(unittest.TestCase):
    DataSetSize: int = 128

    def test_pub_sub(self):
        dataset = set([k for k, v in generateTestDataSet(TestTransaction.DataSetSize).items()])

        client1 = redis.Redis(host=DBHost, port=DBPort, decode_responses=True, protocol=3)
        client2 = redis.Redis(host=DBHost, port=DBPort, decode_responses=True, protocol=3)

        # 创建channel
        pub = client1.pubsub()
        pub.subscribe('test_channel')

        # 监听消息
        def sub():
            msg_stream = pub.listen()
            copy_dataset = copy.deepcopy(dataset)
            for msg in msg_stream:
                if msg["type"] == "message":
                    data = msg["data"]
                    self.assertIn(data, dataset)
                    copy_dataset.remove(data)
                    if len(copy_dataset) == 0:
                        break

        t = Thread(target=sub)
        t.start()

        # 推送数据
        for m in dataset:
            client2.publish('test_channel', m)

        t.join(timeout=10.0)
        client1.close()
        client2.close()


class TestDataLogFileMerge(unittest.TestCase):
    DataSetSize: int = 128

    @classmethod
    def setUpClass(cls):
        cls.client = redis.Redis(host=DBHost, port=DBPort, decode_responses=True, protocol=3)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def test_merge(self):
        dataset = generateTestDataSet(TestTransaction.DataSetSize)

        # 插入初始数据
        for k, v in dataset.items():
            self.assertTrue(self.client.set(k, v))

        # 插入更新后的数据
        for i in range(1, 6):
            cnt = 0
            for k, v in dataset.items():
                self.assertTrue(self.client.set(k, f'{i}_{cnt}_{v}'))
                cnt += 1

        # 删除索引位置可以被5整除的key-value
        cnt = 0
        for k, v in dataset.items():
            if cnt % 5 == 0:
                self.assertEqual(1, self.client.delete(k))
            cnt += 1

        # 合并文件
        self.client.execute_command("MERGE")

        # 检查
        cnt = 0
        for k, v in dataset.items():
            if cnt % 5 == 0:
                self.assertFalse(self.client.exists(k))
            else:
                self.assertTrue(f'5_{cnt}_{v}', self.client.get(k))
            cnt += 1


if __name__ == '__main__':
    unittest.main()
