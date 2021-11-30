#include <array>
#include <iterator>
#include <string>
#include <iostream>
#include <sstream>
#include <vector>
#include <thread>

#include "encryption_transformer.hpp"

#include <realm/group.hpp>
#include <realm/db.hpp>
#include <realm/history.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/noinst/server/server_history.hpp>
#include <realm/replication.hpp>


#include <realm/util/file.hpp>
#include <realm/util/optional.hpp>

using namespace realm;
using namespace sync;
using namespace std::literals::chrono_literals;

namespace {

// FIXME: this is from test/util/server_history.hpp
// The following classes are the necessary things to instantiate a ServerHistory

class IntegrationReporter : public _impl::ServerHistory::IntegrationReporter {
public:
    void on_integration_session_begin() override {}
    void on_changeset_integrated(std::size_t) override {}
    void on_changesets_merged(long) override {}
};

class ServerHistoryContextImpl : public _impl::ServerHistory::Context {
public:
    ServerHistoryContextImpl()
        : m_transformer(sync::make_transformer())
    {
    }
    std::mt19937_64& server_history_get_random() noexcept override
    {
        return m_random;
    }

    sync::Transformer& get_transformer() override
    {
        return *m_transformer;
    }
    util::Buffer<char>& get_transform_buffer() override
    {
        return m_transform_buffer;
    }
    _impl::ServerHistory::IntegrationReporter& get_integration_reporter() override
    {
        return m_integration_reporter;
    }

private:
    std::mt19937_64 m_random;
    std::unique_ptr<sync::Transformer> m_transformer;
    util::Buffer<char> m_transform_buffer;
    IntegrationReporter m_integration_reporter;
};

} // unnamed namespace


Replication::HistoryType peek_history_type(const std::string& file_name, const char* read_key)
{
    Group group{file_name, read_key};
    using gf = _impl::GroupFriend;
    Allocator& alloc = gf::get_alloc(group);
    ref_type top_ref = gf::get_top_ref(group);
    if (top_ref == 0) {
        throw std::runtime_error("Could not determine the history type of file: " + file_name);
    }
    using version_type = _impl::History::version_type;
    version_type version;
    int history_type;
    int history_schema_version;
    gf::get_version_and_history_info(alloc, top_ref, version, history_type, history_schema_version);
    return Replication::HistoryType(history_type);
}

void do_transform(const std::string& file_name, const char* read_key, const char* write_key, bool verbose)
{
    bool success = false;
    const bool bump_version_number = false; // for all history types we can keep the current version
    switch (peek_history_type(file_name, read_key)) {
        case Replication::hist_None: {
            bool no_create = true;
            auto sg = DB::create(file_name, no_create, DBOptions{read_key});
            success = sg->compact(bump_version_number, write_key);
            break;
        }
        case Replication::hist_InRealm: {
            std::unique_ptr<Replication> hist(make_in_realm_history());
            auto sg = DB::create(*hist, file_name, DBOptions(read_key));
            success = sg->compact(bump_version_number, write_key);
            break;
        }
        case Replication::hist_OutOfRealm:
            throw std::runtime_error("Could not transform Realm file with history type 'OutOfRealm' for " +
                                     file_name);
        case Replication::hist_SyncClient: {
            std::unique_ptr<Replication> reference_history = realm::sync::make_client_replication();
            auto sg = DB::create(*reference_history, file_name, DBOptions(read_key));
            success = sg->compact(bump_version_number, write_key);
            break;
        }
        case Replication::hist_SyncServer: {
            ServerHistoryContextImpl context;
            _impl::ServerHistory::DummyCompactionControl compaction_control;
            _impl::ServerHistory history{context, compaction_control};
            auto sg = DB::create(history, file_name, DBOptions(read_key));
            success = sg->compact(bump_version_number, write_key);
            break;
        }
    }
    if (!success) {
        throw std::runtime_error("Unable to compact '" + file_name + "'. Check that it is not in use.");
    }
    else if (verbose) {
        std::cout << "Processed " << file_name << std::endl;
    }
}

void parallel_transform(const std::vector<std::string>& paths, const char* read_key, const char* write_key,
                        bool verbose, size_t jobs)
{
    REALM_ASSERT(jobs > 0);
    std::vector<std::thread> threads;
    size_t items_per_thread = size_t(std::ceil(float(paths.size()) / jobs));
    for (size_t next_index = 0; next_index < paths.size(); next_index += items_per_thread) {
        size_t begin_index = next_index;
        size_t end_index = std::min(next_index + items_per_thread, paths.size());

        threads.emplace_back([begin_index, end_index, &paths, read_key, write_key, verbose] {
            for (size_t i = begin_index; i < end_index; i++) {
                do_transform(paths[i], read_key, write_key, verbose);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
}

size_t encryption_transformer::encrypt_transform(const encryption_transformer::Configuration& config)
{
    std::vector<std::string> paths;
    if (config.type == encryption_transformer::Configuration::TransformType::File) {
        paths.push_back(config.target_path);
    }
    else {
        util::File inputs(config.target_path);
        const size_t file_size = static_cast<size_t>(inputs.get_size());
        auto buff = std::make_unique<char[]>(file_size + 1);
        inputs.read(buff.get(), file_size);
        std::string glob(buff.get(), file_size);
        std::istringstream ss{glob};
        using StrIt = std::istream_iterator<std::string>;
        paths = std::vector<std::string>{StrIt{ss}, StrIt{}};
    }
    if (config.verbose) {
        std::cout << "Will transform the following files: " << std::endl;
        for (auto& path : paths) {
            std::cout << "\t" << path << std::endl;
        }
    }
    const char* read_key = config.input_key ? config.input_key->data() : nullptr;
    const char* write_key = config.output_key ? config.output_key->data() : nullptr;

    try {
        if (config.jobs) {
            parallel_transform(paths, read_key, write_key, config.verbose, *config.jobs);
        }
        else {
            for (auto& path : paths) {
                do_transform(path, read_key, write_key, config.verbose);
            }
        }
    }
    catch (const std::exception& e) {
        // Restore things as much as possible
        if (config.verbose) {
            std::cerr << "An error occurred, attempting to recover state: " << e.what() << std::endl;
        }
        throw e;
    }
    if (config.verbose) {
        std::cout << "Transform success." << std::endl;
    }
    return paths.size();
}
