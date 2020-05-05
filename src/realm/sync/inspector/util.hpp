#include <realm/util/logger.hpp>
#include <realm/sync/changeset.hpp>
#include <realm/noinst/server_history.hpp>

namespace realm {
namespace inspector {

std::string get_gmtime(uint_fast64_t timestamp);

std::string changeset_hex_to_binary(const std::string& changeset_hex);

sync::Changeset changeset_binary_to_sync_changeset(const std::string& changeset_binary);

void do_print_changeset(const sync::Changeset& changeset);

void print_changeset(const std::string& path, bool hex = false);

class IntegrationReporter: public _impl::ServerHistory::IntegrationReporter {
public:
    void on_integration_session_begin() override;
    void on_changeset_integrated(std::size_t changeset_size) override;
    void on_changesets_merged(long num_merges) override;
};

class ServerHistoryContext: public _impl::ServerHistory::Context {
public:
    ServerHistoryContext();

    bool owner_is_sync_server() const noexcept override;
    std::mt19937_64& server_history_get_random() noexcept override;

    sync::Transformer& get_transformer() override;
    util::Buffer<char>& get_transform_buffer() override;
    IntegrationReporter& get_integration_reporter() override;
private:
    std::mt19937_64 m_random;
    std::unique_ptr<sync::Transformer> m_transformer;
    util::Buffer<char> m_transform_buffer;
    IntegrationReporter m_integration_reporter;
};

std::string data_type_to_string(DataType data_type);
void print_tables(const Group& group);
void print_server_history(const std::string& path);
void inspect_server_realm(const std::string& path);

struct MergeConfiguration {
    sync::file_ident_type client_file_ident = 0;
    sync::timestamp_type origin_timestamp = 0;
    sync::version_type last_integrated_server_version = 0;
    sync::version_type client_version = 0;
    std::string changeset_path;
    std::string realm_path;
};

void merge_changeset_into_server_realm(const MergeConfiguration&);

struct PartialSyncConfiguration {
    util::Logger::Level log_level;
    std::string user_identity;
    bool is_admin = false;
    std::string partial_realm_path;
    std::string reference_realm_path;
};

void perform_partial_sync(const PartialSyncConfiguration&);

void inspect_client_realm(const std::string& path);

} // namespace inspector
} // namespace realm
