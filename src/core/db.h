#pragma once
#include "engine.h"
#include "pubsub.h"
#include <cstddef>
#include <string>
#include <tuple>

namespace foxbatdb {
    class CMDSession;
    struct CommandOption;
    class Database;
    class MaxMemoryStrategy;

    class DatabaseManager {
    private:
        bool mIsNonWrite_;
        MaxMemoryStrategy* mMaxMemoryStrategy_;
        std::vector<Database> mDBList_;
        PubSubWithChannel mPubSubChannel_;

        DatabaseManager();

    public:
        DatabaseManager(const DatabaseManager&) = delete;
        DatabaseManager& operator=(const DatabaseManager&) = delete;
        ~DatabaseManager();
        static DatabaseManager& GetInstance();
        void Init();
        bool LoadRecordsFromLogFile(const std::string& path);
        bool HaveMemoryAvailable() const;
        void ScanDBForReleaseMemory();
        void SetNonWrite();
        void CancelNonWrite();
        bool IsInReadonlyMode() const;
        std::size_t GetDBListSize() const;
        Database* GetDBByIndex(std::size_t idx);

        void SubscribeWithChannel(const std::string& channel, std::weak_ptr<CMDSession> weak);
        void UnSubscribeWithChannel(const std::string& channel,
                                    std::weak_ptr<CMDSession> weak);
        std::int32_t PublishWithChannel(const std::string& channel,
                                        const std::string& msg);
    };

    class Database {
    private:
        StorageEngine mEngine_;
        std::unordered_map<std::string, std::vector<std::weak_ptr<CMDSession>>> mWatchedMap_;

        std::tuple<std::error_code, std::optional<std::string>> StrSetWithOption(
                const std::string& key, RecordObject& obj, const CommandOption& opt);

        void NotifyWatchedClientSession(const std::string& key);

    public:
        Database(std::uint8_t dbIdx, MaxMemoryStrategy* maxMemoryStrategy);
        Database(const Database&) = delete;
        Database& operator=(const Database&) = delete;
        Database(Database&&) noexcept = default;
        Database& operator=(Database&&) noexcept = default;
        ~Database() = default;

        void ReleaseMemory();
        bool HaveMemoryAvailable() const;

        void Foreach(StorageEngine::ForeachCallback&& callback);
        void InsertTxFlag(TxRuntimeState txFlag, std::size_t txCmdNum = 0);

        void StrSetForHistoryData(DataLogFileWrapper* file, std::streampos pos,
                                  const FileRecord& record);
        std::tuple<std::error_code, std::optional<std::string>> StrSet(
                const std::string& key, const std::string& val,
                const std::vector<CommandOption>& opts = {});
        void StrSetForMerge(DataLogFileWrapper* mergeFile,
                            const std::string& key, const std::string& val);
        std::optional<std::string> StrGet(const std::string& key);
        std::error_code Del(const std::string& key);
        RecordObject* Get(const std::string& key);

        void AddWatchKeyWithClient(const std::string& key, std::weak_ptr<CMDSession> clt);
        void DelWatchKeyAndClient(const std::string& key);

        std::vector<std::string> PrefixSearch(const std::string& prefix) const;
    };
}// namespace foxbatdb