module;

#include <cassert>
#include <condition_variable>
#include <expected>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

export module leveldb.database;

export import leveldb.status;
export import leveldb.write_batch;

namespace leveldb {

export struct WriteOptions {
    bool sync = false;
};

export struct ReadOptions {
    bool verify_checksums = false;
    bool fill_cache = true;
    // TODO: snapshot
};

export struct Options {};

export class Database {
public:
    auto put(WriteOptions options, std::string_view key, std::string_view value) -> std::expected<void, Status>
    {
        WriteBatch batch;
        batch.put(key, value);
        return write(options, std::move(batch));
    }

    auto remove(WriteOptions options, std::string_view key) -> std::expected<void, Status>
    {
        WriteBatch batch;
        batch.remove(key);
        return write(options, std::move(batch));
    }

    auto get(ReadOptions options, std::string_view key) -> std::expected<std::string, Status> {}

    auto write(WriteOptions options, WriteBatch batch) -> std::expected<void, Status>
    {
        Waiter waiter{ .batch = std::move(batch), .sync = options.sync };

        {
            std::unique_lock<std::mutex> locker{ m_ };
            cv_.wait(locker, [&waiter, this] { return !waiter.done && waiters_.front() == &waiter; });

            if (waiter.done)
                return {};
        }

        return {};
    }

private:
    struct Waiter {
        WriteBatch batch;
        bool sync = false;
        bool done = false;
    };

    std::mutex m_;
    std::condition_variable cv_;
    std::vector<Waiter*> waiters_;
    Status bg_status_;

    auto make_room_for_write() -> std::expected<void, Status>
    {
        assert(!waiters_.empty());

        while (true) {
            if (!bg_status_.ok()) {
                return std::unexpected(bg_status_);
            }
        }

        return {};
    }
};

} // namespace leveldb