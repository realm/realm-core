
#ifndef REALM_NOINST_VACUUM_HPP
#define REALM_NOINST_VACUUM_HPP

#include <array>
#include <string>
#include <chrono>
#include <stdexcept>

#include <realm/replication.hpp>
#include <realm/util/optional.hpp>
#include <realm/util/logger.hpp>

namespace realm {
namespace _impl {

class Vacuum {
public:
    util::Logger& logger;

    struct Options {
        util::Optional<Replication::HistoryType> history_type;
        bool no_log_compaction = false;
        bool no_file_compaction = false;
        bool no_file_upgrade = false;
        bool bump_realm_version = false;
        bool ignore_clients = false; // See sync::Server::Config::history_compaction_ignore_clients.
        std::chrono::seconds server_history_ttl = std::chrono::seconds::max();
        bool dry_run = false;
        util::Optional<std::array<char, 64>> encryption_key;
    };

    struct Results {
        std::string type_description;
        size_t before_size;
        size_t after_size;
        std::chrono::microseconds time;
        bool ignored = false;
    };

    explicit Vacuum(util::Logger& logger, Options options)
        : logger{logger}
        , m_options(std::move(options))
    {
    }

    Results vacuum(const std::string& file);
    Results dry_run(const std::string& file);

    struct VacuumFile;

private:
    Options m_options;
};

struct VacuumError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_VACUUM_HPP
