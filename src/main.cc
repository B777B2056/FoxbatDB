﻿#include "core/db.h"
#include "core/memory.h"
#include "cron/cron.h"
#include "flag/flags.h"
#include "frontend/server.h"
#include "log/datalog.h"
#include "log/oplog.h"
#include "log/serverlog.h"
#include <new>

using namespace foxbatdb;

void OutOfMemoryHandler() {
    // 所有DB进入不允许写入状态，只响应非写入命令
    DatabaseManager::GetInstance().SetNonWrite();
}

void MemoryAllocRetryFunc() {
    auto& dbm = DatabaseManager::GetInstance();
    if (dbm.HaveMemoryAvailable()) {
        dbm.ScanDBForReleaseMemory();
    } else {
        OutOfMemoryHandler();
    }
}

std::string ParseArgs(int argc, char** argv) {
    if (argc != 2) {
        throw std::runtime_error("arg number error");
    }

    std::string arg = argv[1];
    if (!arg.starts_with("--flag-conf-path=")) {
        throw std::runtime_error("arg error, please input flag-conf-path");
    }
    return arg.substr(arg.find_first_of('=') + 1);
}

void InitComponents(const std::string& flagConfPath) {
    std::set_new_handler(MemoryAllocRetryFunc);
    Flags::GetInstance().Init(flagConfPath);
    ServerLog::GetInstance().Init();
    OperationLog::GetInstance().Init();
    DatabaseManager::GetInstance().Init();
    DataLogFileManager::GetInstance().Init();
    RecordObjectPool::GetInstance().Init();
    CronJobManager::GetInstance().Init();
}

int main(int argc, char** argv) {
    // 初始化各组件
    InitComponents(ParseArgs(argc, argv));
    // kv引擎已准备好，启动服务
    DBServer::GetInstance().Run();
    return 0;
}
