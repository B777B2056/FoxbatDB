# coding=utf-8
import redis
import time
import random
import string
import math
from enum import Enum
from typing import List
import matplotlib.pyplot as plt

IterationNum = 100
MinimumStrSize = 128
MaximumStrSize = 8096
TestDataSet = []
StrSizeList = [2 ** i for i in range(int(math.log(MinimumStrSize, 2)), int(math.log(MaximumStrSize * 2, 2)))]


def InitTestDataSet() -> None:
    def generateRandomStr(length: int) -> str:
        str_list = [random.choice(string.digits + string.ascii_letters) for _ in range(length)]
        random_str = ''.join(str_list)
        return random_str

    for size in StrSizeList:
        TestDataSet.append(generateRandomStr(size))


def BenchmarkSet(clt: redis.client.Redis) -> List[float]:
    def BenchmarkSetOnce(s: str) -> float:
        start = time.time() * 1000
        clt.set(s, s)
        return time.time() * 1000 - start

    ret = []
    for data in TestDataSet:
        t = 0.0
        for _ in range(0, IterationNum):
            t += BenchmarkSetOnce(data)
        ret.append(t / IterationNum)
    return ret


def BenchmarkGet(clt: redis.client.Redis) -> List[float]:
    def BenchmarkGetOnce(s: str) -> float:
        start = time.time() * 1000
        assert s == clt.get(s)
        return time.time() * 1000 - start

    ret = []
    for data in TestDataSet:
        t = 0.0
        for _ in range(0, IterationNum):
            t += BenchmarkGetOnce(data)
        ret.append(t / IterationNum)
    return ret


class Plotter:
    fig: plt.Figure
    subIdx: int

    class Mode(Enum):
        FoxbatDB_Redis_Read = 1
        FoxbatDB_Redis_Write = 2

    def __init__(self):
        self.fig = plt.figure()
        self.subIdx = 121
        plt.rcParams['font.sans-serif'] = ['SimHei']

    def __Title(self, mode: Mode) -> str:
        if mode == Plotter.Mode.FoxbatDB_Redis_Read:
            return '读性能对比（FoxbatDB vs Redis）'
        elif mode == Plotter.Mode.FoxbatDB_Redis_Write:
            return '写性能对比（FoxbatDB vs Redis）'

    def PerformanceComparisonWithRedis(self, mode: Mode, foxbatDBTimeList: List[float],
                                       redisTimeList: List[float]) -> None:
        assert self.subIdx == 121 or self.subIdx == 122
        _ = self.fig.add_subplot(self.subIdx)
        plt.title(self.__Title(mode))
        plt.xlabel('key/value长度（字节）')
        plt.ylabel('耗时（毫秒）')
        l1, = plt.plot(StrSizeList, foxbatDBTimeList, marker='o', markersize=3)
        l2, = plt.plot(StrSizeList, redisTimeList, marker='x', markersize=3)
        plt.legend(handles=[l1, l2], labels=['FoxbatDB', 'Redis'], loc='best')
        if self.subIdx == 122:
            plt.show()
        self.subIdx += 1

    def PerformanceComparisonWithReadWrite(self, foxbatDBSetTimeList: List[float],
                                           foxbatDBGetTimeList: List[float]) -> None:
        plt.title('读写性能对比（FoxbatDB）')
        plt.xlabel('key/value长度（字节）')
        plt.ylabel('耗时（毫秒）')
        l1, = plt.plot(StrSizeList, foxbatDBSetTimeList, marker='o', markersize=3)
        l2, = plt.plot(StrSizeList, foxbatDBGetTimeList, marker='x', markersize=3)
        plt.legend(handles=[l1, l2], labels=['FoxbatDB写', 'FoxbatDB读'], loc='best')
        plt.show()


if __name__ == '__main__':
    # 初始化测试集
    InitTestDataSet()

    # 连接服务器
    foxbatDBClient = redis.Redis(host='localhost', port=7698, decode_responses=True)
    redisClient = redis.Redis(host='localhost', port=6379, decode_responses=True)

    # 获取测量数据
    foxbatDBSetTimeList = BenchmarkSet(foxbatDBClient)
    foxbatDBGetTimeList = BenchmarkGet(foxbatDBClient)

    redisSetTimeList = BenchmarkSet(redisClient)
    redisGetTimeList = BenchmarkGet(redisClient)

    # 绘图
    plotter = Plotter()
    plotter.PerformanceComparisonWithRedis(Plotter.Mode.FoxbatDB_Redis_Write, foxbatDBSetTimeList, redisSetTimeList)
    plotter.PerformanceComparisonWithRedis(Plotter.Mode.FoxbatDB_Redis_Read, foxbatDBGetTimeList, redisGetTimeList)
    plotter.PerformanceComparisonWithReadWrite(foxbatDBSetTimeList, foxbatDBGetTimeList)
