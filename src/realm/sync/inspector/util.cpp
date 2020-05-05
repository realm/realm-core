#include <string>
#include <iostream>
#include <sstream>
#include <ctime>

#include <realm/sync/changeset_parser.hpp>
#include <realm/noinst/partial_sync.hpp>
#include <realm/util/scratch_allocator.hpp>
#include <realm/util/load_file.hpp>

#include "util.hpp"

using namespace realm;

std::string inspector::get_gmtime(uint_fast64_t timestamp)
{
    using Clock = std::chrono::system_clock;
    using TimePoint = std::chrono::time_point<Clock>;

    uint_fast64_t ms_since_epoch = 1420070400000 + timestamp;
    const Clock::duration duration_since_epoch =
        std::chrono::milliseconds(ms_since_epoch);
    const TimePoint timepoint {duration_since_epoch};
    std::time_t time_1 = std::chrono::system_clock::to_time_t(timepoint);
    struct tm* timeptr = std::gmtime(&time_1);

    constexpr int maxsize = 40;
    char s[maxsize];
    const char* format = "%F:%T GMT";
    size_t size = std::strftime(s, maxsize, format, timeptr);

    return std::string {s, size};
}

std::string inspector::changeset_hex_to_binary(const std::string& changeset_hex)
{
    std::vector<char> changeset_vec;

    std::istringstream in {changeset_hex};
    int n;
    in >> std::hex >> n;
    while (in) {
        REALM_ASSERT(n >= 0 && n <= 255);
        changeset_vec.push_back(n);
        in >> std::hex >> n;
    }

    return std::string {changeset_vec.data(), changeset_vec.size()};
}

sync::Changeset inspector::changeset_binary_to_sync_changeset(const std::string& changeset_binary)
{
    _impl::SimpleInputStream input_stream {changeset_binary.data(), changeset_binary.size()};
    sync::Changeset changeset;
    sync::parse_changeset(input_stream, changeset);

    return changeset;
}

void inspector::do_print_changeset(const sync::Changeset& changeset)
{
#if REALM_DEBUG
    changeset.print();
#else
    static_cast<void>(changeset);
    std::cerr << "Changesets can only be printed in Debug mode\n";
#endif
}

void inspector::print_changeset(const std::string& path, bool hex)
{
    std::string file_contents = util::load_file(path);
    std::string changeset_binary;
    if (hex) {
        changeset_binary = changeset_hex_to_binary(file_contents);
    }
    else {
        changeset_binary = file_contents;
    }
    sync::Changeset changeset = changeset_binary_to_sync_changeset(changeset_binary);
    do_print_changeset(changeset);
}

void inspector::IntegrationReporter::on_integration_session_begin() {
    std::cerr << "IntegrationReporter: on_integrate_session_begin\n";
}

void inspector::IntegrationReporter::on_changeset_integrated(std::size_t changeset_size)
{
    std::cerr << "IntegrationReporter: on_changeset_integrated, changeset_size = "
        << changeset_size << "\n";
}

void inspector::IntegrationReporter::on_changesets_merged(long num_merges)
{
    std::cerr << "IntegrationReporter: on_changesets_merged_, num_merges = "
        << num_merges << "\n";
}

inspector::ServerHistoryContext::ServerHistoryContext():
    m_transformer {sync::make_transformer()}
{
}

bool inspector::ServerHistoryContext::owner_is_sync_server() const noexcept
{
    return true;
}

std::mt19937_64& inspector::ServerHistoryContext::server_history_get_random() noexcept
{
    return m_random;
}

sync::Transformer& inspector::ServerHistoryContext::get_transformer()
{
    return *m_transformer;
}

util::Buffer<char>& inspector::ServerHistoryContext::get_transform_buffer()
{
    return m_transform_buffer;
}

inspector::IntegrationReporter& inspector::ServerHistoryContext::get_integration_reporter()
{
    return m_integration_reporter;
}

std::string inspector::data_type_to_string(DataType data_type)
{
    switch (data_type) {
        case (DataType::type_Int):
            return "type_Int";
        case (DataType::type_Bool):
            return "type_Bool";
        case (DataType::type_Float):
            return "type_Float";
        case (DataType::type_Double):
            return "type_Double";
        case (DataType::type_String):
            return "type_String";
        case (DataType::type_Binary):
            return "type_Binary";
        case (DataType::type_Timestamp):
            return "type_Timestamp";
        case (DataType::type_Link):
            return "type_Link";
        case (DataType::type_LinkList):
            return "type_LinkList";
        default:
            return "unknown";
    }
}

void inspector::print_tables(const Group& group)
{
    for (auto table_key : group.get_table_keys()) {
        StringData table_name = group.get_table_name(table_key);
        std::cout << "Table: " << table_name << "\n";
        ConstTableRef table = group.get_table(table_key);
        size_t nrows = table->size();
        std::cout << "  " << nrows << " rows\n";
        for (auto col_key : table->get_column_keys()) {
            StringData column_name = table->get_column_name(col_key);
            DataType column_type = table->get_column_type(col_key);
            std::string column_type_str = data_type_to_string(column_type);
            std::cout << "  " << column_name << ", " << column_type_str;
            if (column_type == DataType::type_Link || column_type == DataType::type_LinkList) {
                ConstTableRef target_table = table->get_link_target(col_key);
                StringData target_name = target_table->get_name();
                std::cout << ", " << target_name;
            }
            bool has_search_index = table->has_search_index(col_key);
            std::cout << ", " << (has_search_index ? "search_index" : "no_search_index")
                << "\n";
        }
        std::cout << "\n";
    }
}

void inspector::print_server_history(const std::string& path)
{
    ServerHistoryContext history_context;
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    _impl::ServerHistory history {path, history_context, compaction_control};
    auto sg = DB::create(history);

    {
        ReadTransaction rt{sg};

        int history_schema_version = history.get_history_schema_version();
        std::cout << "History schema version = " << history_schema_version << "\n\n";
    }

    using HistoryContents = _impl::ServerHistory::HistoryContents;

    HistoryContents hc = history.get_history_contents();

    std::cout << "Clients: " << hc.client_files.size() << "\n";
    for (size_t i = 0; i < hc.client_files.size(); ++i) {
        HistoryContents::ClientFile cf = hc.client_files[i];

        size_t rh_byte_size = 0;
        for (const util::Optional<std::string>& entry: cf.reciprocal_history) {
            if (entry)
                rh_byte_size += entry->size();
        }

        std::cout << "client_file_ident = " << i
            << ", salt = " << cf.ident_salt
            << ", client_version = " << cf.client_version
            << ", rh_base_version = " << cf.rh_base_version
            << ", reciprocal history size(entries) = " << cf.reciprocal_history.size()
            << ", reciprocal history byte size = " << rh_byte_size
            << "\n";
    }
    std::cout << "\n\n";

    std::cout << "history_base_version = " << hc.history_base_version << "\n";
    std::cout << "base_version_salt = " << hc.base_version_salt << "\n";
    std::cout << "servers_client_file_ident = " << hc.servers_client_file_ident << "\n";

    std::cout << "History entries: " << hc.sync_history.size() << "\n\n";
    for (size_t i = 0; i < hc.sync_history.size(); ++i) {
        const HistoryContents::HistoryEntry& he = hc.sync_history[i];

        std::cout << "index = " << i
            << ", version_salt = " << he.version_salt
            << ", client_file_ident = " << he.client_file_ident
            << ", client_version = " << he.client_version
            << ", cumul_byte_size = " << he.cumul_byte_size
            << ", timestamp = " << he.timestamp
            << ", timestamp = " << get_gmtime(he.timestamp)
            << ", changeset size = " << he.changeset.size()
            << "\n";
    }
}

void inspector::inspect_server_realm(const std::string& path)
{
    {
        ServerHistoryContext history_context;
        _impl::ServerHistory::DummyCompactionControl compaction_control;
        _impl::ServerHistory history {path, history_context, compaction_control};
        auto sg = DB::create(history);
        ReadTransaction rt {sg};
        const Group& group = rt.get_group();
        print_tables(group);
    }
    std::cout << "\n\n";
    print_server_history(path);
}

void inspector::merge_changeset_into_server_realm(const MergeConfiguration& config)
{
    std::string changeset_hex = util::load_file(config.changeset_path);
    std::string changeset_binary = changeset_hex_to_binary(changeset_hex);
    BinaryData changeset {changeset_binary.data(), changeset_binary.size()};
    sync::file_ident_type origin_file_ident = 0;

    sync::UploadCursor upload_cursor{config.client_version, config.last_integrated_server_version};
    _impl::ServerHistory::IntegratableChangeset integratable_changeset {
        config.client_file_ident,
        config.origin_timestamp,
        origin_file_ident,
        upload_cursor,
        changeset
    };

    _impl::ServerHistory::IntegratableChangesets integratable_changesets;
    integratable_changesets[config.client_file_ident].changesets.push_back(integratable_changeset);

    ServerHistoryContext history_context;
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    _impl::ServerHistory history {config.realm_path, history_context, compaction_control};
    auto sg = DB::create(history);

    util::StderrLogger logger;
    logger.set_level_threshold(Logger::Level::debug);

    sync::VersionInfo version_info; // Dummy
    bool backup_whole_realm; // Dummy
    _impl::ServerHistory::IntegrationResult result; // Dummy
    history.integrate_client_changesets(integratable_changesets, version_info, backup_whole_realm,
                                        result, logger); // Throws
}

void inspector::perform_partial_sync(const PartialSyncConfiguration& config)
{
    ScratchMemory scratch_memory;

    util::StderrLogger logger;
    logger.set_level_threshold(config.log_level);
    util::PrefixLogger partial_logger {"Partial: ", logger};
    util::PrefixLogger reference_logger {"Reference: ", logger};

    ServerHistoryContext history_context;

    // Reference Realm
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    _impl::ServerHistory reference_history {config.reference_realm_path, history_context, compaction_control};
    auto reference_sg = DB::create(reference_history);
    sync::VersionInfo reference_version_info;
    bool has_upstream_status; // Dummy
    sync::file_ident_type partial_file_ident; // Dummy
    sync::version_type partial_progress_reference_version; // Dummy
    reference_history.get_status(reference_version_info, has_upstream_status, partial_file_ident,
                                 partial_progress_reference_version); // Throws

    // Partial Realm
    _impl::ServerHistory partial_history {config.partial_realm_path, history_context, compaction_control};
    auto partial_sg = DB::create(partial_history);
    sync::VersionInfo partial_version_info;
    partial_history.get_status(partial_version_info, has_upstream_status, partial_file_ident,
                               partial_progress_reference_version); // Throws

    _impl::ServerHistory::QueryCache query_cache;
    partial_history.perform_partial_sync(scratch_memory,
                                         reference_history,
                                         config.is_admin,
                                         config.user_identity,
                                         partial_logger,
                                         reference_logger,
                                         partial_version_info,
                                         partial_progress_reference_version,
                                         reference_version_info,
                                         query_cache);
}

void inspector::inspect_client_realm(const std::string& path)
{
    auto history = sync::make_client_replication(path);
    auto sg = DB::create(*history);
    ReadTransaction rt {sg};
    const Group& group = rt.get_group();

    print_tables(group);
    std::cout << "\n\n";
}
