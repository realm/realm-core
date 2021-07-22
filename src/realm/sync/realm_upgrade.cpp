#include <realm.hpp>
#include <realm/history.hpp>
#include <realm/sync/history.hpp>
#include <realm/noinst/server_history.hpp>
#include <iostream>
#include <chrono>

using namespace realm;
using namespace std::chrono;

namespace {

class HistoryContext : public _impl::ServerHistory::Context {
public:
    HistoryContext() {}
    std::mt19937_64& server_history_get_random() noexcept override final
    {
        return m_random;
    }

private:
    std::mt19937_64 m_random;
};

} // namespace

int main(int argc, char const* argv[])
{
    for (int c = 1; c < argc; c++) {
        try {
            std::string path = argv[c];
            std::cout << path << ": upgrading... ";
            std::cout.flush();
            DBOptions options;
            options.allow_file_format_upgrade = true;
            // Sync client history
            try {
                auto hist = sync::make_client_replication(path);
                auto t1 = std::chrono::steady_clock::now();
                auto db = DB::create(*hist, options);
                auto t2 = std::chrono::steady_clock::now();
                auto d = duration_cast<milliseconds>(t2 - t1).count();
                std::cout << d << "ms verifying... ";
                std::cout.flush();
                db->start_read()->verify();
                std::cout << "done" << std::endl;
                continue;
            }
            catch (const IncompatibleHistories&) {
            }

            // Sync server history
            try {
                HistoryContext context;
                _impl::ServerHistory::DummyCompactionControl compaction_control;
                _impl::ServerHistory history{path, context, compaction_control};
                auto t1 = std::chrono::steady_clock::now();
                DBRef db = DB::create(history, options);
                auto t2 = std::chrono::steady_clock::now();
                auto d = duration_cast<milliseconds>(t2 - t1).count();
                std::cout << d << "ms verifying... ";
                std::cout.flush();
                db->start_read()->verify();
                std::cout << "done" << std::endl;
                continue;
            }
            catch (const IncompatibleHistories&) {
            }

            // In realm history
            // Last chance - this one must succeed
            auto hist = make_in_realm_history(path);
            auto t1 = std::chrono::steady_clock::now();
            auto db = DB::create(*hist, options);
            auto t2 = std::chrono::steady_clock::now();
            auto d = duration_cast<milliseconds>(t2 - t1).count();
            std::cout << d << "ms verifying... ";
            std::cout.flush();
            db->start_read()->verify();
            std::cout << "done" << std::endl;
        }
        catch (const std::exception& e) {
            std::cout << e.what() << std::endl;
        }
    }
    return 0;
}
