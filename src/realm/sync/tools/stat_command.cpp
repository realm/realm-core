#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <vector>
#include <locale>
#include <sstream>
#include <iostream>

#include <realm/array_integer.hpp>
#include <realm/util/optional.hpp>
#include <realm/util/quote.hpp>
#include <realm/util/timestamp_formatter.hpp>
#include <realm/util/load_file.hpp>
#include <realm/group.hpp>
#include <realm/replication.hpp>
#include <realm/bplustree.hpp>
#include <realm/version.hpp>
#include <realm/sync/protocol.hpp>

using namespace realm;

using sync::file_ident_type;
using sync::salt_type;
using sync::version_type;

using sync::DownloadCursor;
using sync::SaltedFileIdent;
using sync::SaltedVersion;
using sync::UploadCursor;


namespace {

template <class T>
std::string format_num_something(T num, const char* singular_form, const char* plural_form,
                                 std::locale loc = std::locale{})
{
    using lim = std::numeric_limits<T>;
    bool need_singular = (num == T(1) || (lim::is_signed && num == T(-1)));
    const char* form = (need_singular ? singular_form : plural_form);
    std::ostringstream out;
    out.imbue(loc);
    out << num << " " << form;
    return std::move(out).str();
}

std::string format_num_entries(std::size_t num)
{
    return format_num_something(num, "entry", "entries");
}

std::string format_num_history_entries(std::size_t num)
{
    return format_num_something(num, "history entry", "history entries");
}

std::string format_num_unconsumed_changesets(std::size_t num)
{
    return format_num_something(num, "unconsumed changeset", "unconsumed changesets");
}

std::string format_num_rows(std::size_t num)
{
    return format_num_something(num, "row", "rows");
}

std::string format_byte_size(double size, std::locale loc = std::locale{})
{
    const char* binary_prefixes[] = {"", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi", "Yi"};
    int num_binary_prefixes = sizeof binary_prefixes / sizeof *binary_prefixes;
    int binary_prefix_index = 0;
    double size_2 = size;
    for (;;) {
        if (size_2 < 999.5)
            break;
        ++binary_prefix_index;
        if (binary_prefix_index == num_binary_prefixes) {
            --binary_prefix_index;
            break;
        }
        size_2 /= 1024;
    }

    std::ostringstream out;
    out.imbue(loc);
    out.precision(3); // Must be 3
    out << size_2 << " " << binary_prefixes[binary_prefix_index] << "B";
    return std::move(out).str();
}

std::uint_fast64_t get_aggregate_size(std::initializer_list<ref_type> refs, Allocator& alloc)
{
    MemStats stats;
    for (ref_type ref : refs) {
        if (ref != 0) {
            Array array{alloc};
            array.init_from_ref(ref);
            array.stats(stats);
        }
    }
    return stats.allocated;
}

std::string format_aggregate_size(std::initializer_list<ref_type> refs, Allocator& alloc)
{
    return format_byte_size(double(get_aggregate_size(refs, alloc)));
}

const char* history_type_to_string(int type)
{
    switch (Replication::HistoryType(type)) {
        case Replication::hist_None:
            return "none";
        case Replication::hist_OutOfRealm:
            return "out-of-realm";
        case Replication::hist_InRealm:
            return "in-realm";
        case Replication::hist_SyncClient:
            return "sync-client";
        case Replication::hist_SyncServer:
            return "sync-server";
    }
    return "unknown";
}


struct ClientHistoryInfo {
    struct ContinuousTransactionsHistory {
        version_type base_version = 0;
        version_type curr_version = 0;
        std::size_t size = 0;
        std::uint_fast64_t aggr_size = 0;
    };
    struct SynchronizationHistory {
        version_type base_version = 0;
        version_type curr_version = 0;
        std::size_t size = 0;
        std::uint_fast64_t main_aggr_size = 0;
        std::uint_fast64_t recip_aggr_size = 0;
    };
    struct ServerBinding {
        SaltedFileIdent client_file_ident = {0, 0};
        SaltedVersion latest_server_version = {0, 0};
        DownloadCursor download_progress = {0, 0};
        UploadCursor upload_progress = {0, 0};
        bool defective_upload_progress = false;
    };
    struct CookedHistory {
        std::size_t size = 0;
        std::uint_fast64_t aggr_size = 0;
        std::int_fast64_t changeset_index = 0;
        std::int_fast64_t intrachangeset_progress = 0;
        version_type base_server_version = 0;
    };
    struct SchemaVersion {
        int schema_version = 0;
        bool details_are_unknown = false;
        std::string library_version;
        version_type snapshot_version = 0;
        std::time_t timestamp = 0;
    };
    ContinuousTransactionsHistory ct_history;
    SynchronizationHistory sync_history;
    util::Optional<ServerBinding> server_binding;
    util::Optional<CookedHistory> cooked_history;
    util::Optional<std::vector<SchemaVersion>> schema_versions;
};


void extract_client_history_info_1(Allocator& alloc, ref_type history_root_ref, version_type snapshot_version,
                                   ClientHistoryInfo& info)
{
    // Sizes of fixed-size arrays
    std::size_t root_size = 23;

    // Slots in history root array
    // clang-format off
    std::size_t changesets_iip                          =  0;
    std::size_t reciprocal_transforms_iip               =  1;
    std::size_t remote_versions_iip                     =  2;
    std::size_t origin_file_idents_iip                  =  3;
    std::size_t origin_timestamps_iip                   =  4;
    std::size_t progress_download_server_version_iip    =  5;
    std::size_t progress_download_client_version_iip    =  6;
    std::size_t progress_latest_server_version_iip      =  7;
    std::size_t progress_latest_server_version_salt_iip =  8;
    std::size_t progress_upload_client_version_iip      =  9;
    std::size_t client_file_ident_iip                   = 11;
    std::size_t client_file_ident_salt_iip              = 12;
    std::size_t cooked_changesets_iip                   = 18;
    std::size_t cooked_base_index_iip                   = 19;
    std::size_t cooked_intrachangeset_progress_iip      = 20;
    std::size_t ct_history_iip                          = 21;
    // clang-format on

    Array root{alloc};
    root.init_from_ref(history_root_ref);
    if (root.size() != root_size)
        throw std::runtime_error{"Unexpected size of history root array"};
    {
        ClientHistoryInfo::ContinuousTransactionsHistory cth;
        ref_type ref = root.get_as_ref(ct_history_iip);
        BinaryColumn ct_history{alloc};
        ct_history.init_from_ref(ref);
        std::size_t ct_history_size = ct_history.size();
        cth.base_version = version_type(snapshot_version - ct_history_size);
        cth.curr_version = snapshot_version;
        cth.size = ct_history_size;
        cth.aggr_size = get_aggregate_size({ref}, alloc);
        info.ct_history = std::move(cth);
    }
    {
        ClientHistoryInfo::SynchronizationHistory sh;
        ref_type ref_1 = root.get_as_ref(changesets_iip);
        ref_type ref_2 = root.get_as_ref(reciprocal_transforms_iip);
        ref_type ref_3 = root.get_as_ref(remote_versions_iip);
        ref_type ref_4 = root.get_as_ref(origin_file_idents_iip);
        ref_type ref_5 = root.get_as_ref(origin_timestamps_iip);
        // FIXME: Avoid use of optional type `std::int64_t`
        using IntegerBpTree = BPlusTree<std::int64_t>;
        IntegerBpTree sh_remote_versions{alloc};
        sh_remote_versions.init_from_ref(ref_3);
        std::size_t sync_history_size = sh_remote_versions.size();
        sh.base_version = version_type(snapshot_version - sync_history_size);
        sh.curr_version = snapshot_version;
        sh.size = sync_history_size;
        sh.main_aggr_size = get_aggregate_size({ref_1, ref_3, ref_4, ref_5}, alloc);
        sh.recip_aggr_size = get_aggregate_size({ref_2}, alloc);
        info.sync_history = std::move(sh);
    }
    file_ident_type client_file_ident =
        file_ident_type(root.get_as_ref_or_tagged(client_file_ident_iip).get_as_int());
    if (client_file_ident != 0) {
        ClientHistoryInfo::ServerBinding sb;
        sb.client_file_ident.ident = client_file_ident;
        sb.client_file_ident.salt = salt_type(root.get_as_ref_or_tagged(client_file_ident_salt_iip).get_as_int());
        sb.latest_server_version.version =
            version_type(root.get_as_ref_or_tagged(progress_latest_server_version_iip).get_as_int());
        sb.latest_server_version.salt =
            salt_type(root.get_as_ref_or_tagged(progress_latest_server_version_salt_iip).get_as_int());
        sb.download_progress.server_version =
            version_type(root.get_as_ref_or_tagged(progress_download_server_version_iip).get_as_int());
        sb.download_progress.last_integrated_client_version =
            version_type(root.get_as_ref_or_tagged(progress_download_client_version_iip).get_as_int());
        sb.upload_progress.client_version =
            version_type(root.get_as_ref_or_tagged(progress_upload_client_version_iip).get_as_int());
        sb.defective_upload_progress = true;
        info.server_binding = std::move(sb);
    }
    {
        ref_type ref = root.get_as_ref(cooked_changesets_iip);
        std::int_fast64_t changeset_index = root.get_as_ref_or_tagged(cooked_base_index_iip).get_as_int();
        std::int_fast64_t intrachangeset_progress =
            root.get_as_ref_or_tagged(cooked_intrachangeset_progress_iip).get_as_int();
        if (ref != 0 || changeset_index != 0 || intrachangeset_progress != 0) {
            ClientHistoryInfo::CookedHistory ch;
            if (ref != 0) {
                BinaryColumn cooked_history{alloc};
                cooked_history.init_from_ref(ref);
                ch.size = cooked_history.size();
                ch.aggr_size = get_aggregate_size({ref}, alloc);
            }
            ch.changeset_index = changeset_index;
            ch.intrachangeset_progress = intrachangeset_progress;
            info.cooked_history = std::move(ch);
        }
    }
}


void extract_client_history_info_2(Allocator& alloc, ref_type history_root_ref, version_type snapshot_version,
                                   ClientHistoryInfo& info)
{
    // clang-format off

    // Sizes of fixed-size arrays
    std::size_t root_size = 21;
    std::size_t cooked_history_size = 5;
    std::size_t schema_versions_size = 4;

    // Slots in root array of history compartment
    std::size_t ct_history_iip = 0;
    std::size_t client_file_ident_iip = 1;
    std::size_t client_file_ident_salt_iip = 2;
    std::size_t progress_latest_server_version_iip = 3;
    std::size_t progress_latest_server_version_salt_iip = 4;
    std::size_t progress_download_server_version_iip = 5;
    std::size_t progress_download_client_version_iip = 6;
    std::size_t progress_upload_client_version_iip = 7;
    std::size_t progress_upload_server_version_iip = 8;
    std::size_t changesets_iip = 13;
    std::size_t reciprocal_transforms_iip = 14;
    std::size_t remote_versions_iip = 15;
    std::size_t origin_file_idents_iip = 16;
    std::size_t origin_timestamps_iip = 17;
    std::size_t cooked_history_iip = 19;
    std::size_t schema_versions_iip = 20;

    // Slots in root array of `cooked_history` table
    std::size_t ch_base_index_iip = 0;
    std::size_t ch_intrachangeset_progress_iip = 1;
    std::size_t ch_base_server_version_iip = 2;
    std::size_t ch_changesets_iip = 3;

    // Slots in root array of `schema_versions` table
    std::size_t sv_schema_versions_iip = 0;
    std::size_t sv_library_versions_iip = 1;
    std::size_t sv_snapshot_versions_iip = 2;
    std::size_t sv_timestamps_iip = 3;

    // clang-format on

    Array root{alloc};
    root.init_from_ref(history_root_ref);
    if (root.size() != root_size)
        throw std::runtime_error{"Unexpected size of history root array"};
    {
        ClientHistoryInfo::ContinuousTransactionsHistory cth;
        ref_type ref = root.get_as_ref(ct_history_iip);
        BinaryColumn ct_history{alloc};
        ct_history.init_from_ref(ref);
        std::size_t ct_history_size = ct_history.size();
        cth.base_version = version_type(snapshot_version - ct_history_size);
        cth.curr_version = snapshot_version;
        cth.size = ct_history_size;
        cth.aggr_size = get_aggregate_size({ref}, alloc);
        info.ct_history = std::move(cth);
    }
    {
        ClientHistoryInfo::SynchronizationHistory sh;
        ref_type ref_1 = root.get_as_ref(changesets_iip);
        ref_type ref_2 = root.get_as_ref(reciprocal_transforms_iip);
        ref_type ref_3 = root.get_as_ref(remote_versions_iip);
        ref_type ref_4 = root.get_as_ref(origin_file_idents_iip);
        ref_type ref_5 = root.get_as_ref(origin_timestamps_iip);
        // FIXME: Avoid use of optional type `std::int64_t`
        using IntegerBpTree = BPlusTree<std::int64_t>;
        IntegerBpTree sh_remote_versions{alloc};
        sh_remote_versions.set_parent(&root, remote_versions_iip);
        sh_remote_versions.create(); // Throws
        std::size_t sync_history_size = sh_remote_versions.size();
        sh.base_version = version_type(snapshot_version - sync_history_size);
        sh.curr_version = snapshot_version;
        sh.size = sync_history_size;
        sh.main_aggr_size = get_aggregate_size({ref_1, ref_3, ref_4, ref_5}, alloc);
        sh.recip_aggr_size = get_aggregate_size({ref_2}, alloc);
        info.sync_history = std::move(sh);
    }
    file_ident_type client_file_ident =
        file_ident_type(root.get_as_ref_or_tagged(client_file_ident_iip).get_as_int());
    if (client_file_ident != 0) {
        ClientHistoryInfo::ServerBinding sb;
        sb.client_file_ident.ident = client_file_ident;
        sb.client_file_ident.salt = salt_type(root.get_as_ref_or_tagged(client_file_ident_salt_iip).get_as_int());
        sb.latest_server_version.version =
            version_type(root.get_as_ref_or_tagged(progress_latest_server_version_iip).get_as_int());
        sb.latest_server_version.salt =
            salt_type(root.get_as_ref_or_tagged(progress_latest_server_version_salt_iip).get_as_int());
        sb.download_progress.server_version =
            version_type(root.get_as_ref_or_tagged(progress_download_server_version_iip).get_as_int());
        sb.download_progress.last_integrated_client_version =
            version_type(root.get_as_ref_or_tagged(progress_download_client_version_iip).get_as_int());
        sb.upload_progress.client_version =
            version_type(root.get_as_ref_or_tagged(progress_upload_client_version_iip).get_as_int());
        sb.upload_progress.last_integrated_server_version =
            version_type(root.get_as_ref_or_tagged(progress_upload_server_version_iip).get_as_int());
        info.server_binding = std::move(sb);
    }
    {
        ref_type ref = root.get_as_ref(cooked_history_iip);
        if (ref != 0) {
            ClientHistoryInfo::CookedHistory ch;
            ch.aggr_size = get_aggregate_size({ref}, alloc);
            Array cooked_history{alloc};
            cooked_history.init_from_ref(ref);
            if (cooked_history.size() != cooked_history_size)
                throw std::runtime_error{"Unexpected size of `cooked_history` array"};
            ch.changeset_index =
                std::int_fast64_t(cooked_history.get_as_ref_or_tagged(ch_base_index_iip).get_as_int());
            ch.intrachangeset_progress =
                std::int_fast64_t(cooked_history.get_as_ref_or_tagged(ch_intrachangeset_progress_iip).get_as_int());
            ch.base_server_version =
                version_type(cooked_history.get_as_ref_or_tagged(ch_base_server_version_iip).get_as_int());
            ref_type ch_changesets_ref = cooked_history.get_as_ref(ch_changesets_iip);

            BinaryColumn ch_changesets{alloc};
            ch_changesets.init_from_ref(ch_changesets_ref);
            ch.size = ch_changesets.size();
            info.cooked_history = std::move(ch);
        }
    }
    {
        Array schema_versions{alloc};
        schema_versions.set_parent(&root, schema_versions_iip);
        schema_versions.init_from_parent();
        REALM_ASSERT(schema_versions.size() == schema_versions_size);
        Array sv_schema_versions{alloc};
        sv_schema_versions.set_parent(&schema_versions, sv_schema_versions_iip);
        sv_schema_versions.init_from_parent();
        Array sv_library_versions{alloc};
        sv_library_versions.set_parent(&schema_versions, sv_library_versions_iip);
        sv_library_versions.init_from_parent();
        Array sv_snapshot_versions{alloc};
        sv_snapshot_versions.set_parent(&schema_versions, sv_snapshot_versions_iip);
        sv_snapshot_versions.init_from_parent();
        Array sv_timestamps{alloc};
        sv_timestamps.set_parent(&schema_versions, sv_timestamps_iip);
        sv_timestamps.init_from_parent();
        Array array{alloc};
        std::string string;
        std::vector<ClientHistoryInfo::SchemaVersion> sv;
        std::size_t n = sv_schema_versions.size();
        REALM_ASSERT(n == sv_library_versions.size());
        for (std::size_t i = 0; i < n; ++i) {
            ClientHistoryInfo::SchemaVersion entry;
            entry.schema_version = int(sv_schema_versions.get(i));
            array.set_parent(&sv_library_versions, i);
            if (ref_type ref = array.get_ref_from_parent()) {
                array.init_from_ref(ref);
                std::size_t size = array.size();
                string = {};
                string.resize(size);
                using uchar = unsigned char;
                for (std::size_t j = 0; j < size; ++j)
                    string[j] = char(uchar(array.get(j)));
                entry.library_version = std::move(string);
                entry.snapshot_version = version_type(sv_snapshot_versions.get(i));
                entry.timestamp = std::time_t(sv_timestamps.get(i));
            }
            else {
                entry.details_are_unknown = true;
            }
            sv.push_back(std::move(entry));
        }
        info.schema_versions = std::move(sv);
    }
}

} // unnamed namespace


int main(int argc, char* argv[])
{
    std::string realm_path;
    std::string encryption_key;
    bool show_history = false;
    bool show_columns = false;
    bool verify = false;

    // Process command line
    {
        const char* prog = argv[0];
        --argc;
        ++argv;
        bool error = false;
        bool help = false;
        bool version = false;
        int argc_2 = 0;
        int i = 0;
        char* arg = nullptr;
        auto get_string_value = [&](std::string& var) {
            if (i < argc) {
                var = argv[i++];
                return true;
            }
            return false;
        };
        while (i < argc) {
            arg = argv[i++];
            if (arg[0] != '-') {
                argv[argc_2++] = arg;
                continue;
            }
            if (std::strcmp(arg, "-h") == 0 || std::strcmp(arg, "--help") == 0) {
                help = true;
                continue;
            }
            else if (std::strcmp(arg, "-e") == 0 || std::strcmp(arg, "--encryption-key") == 0) {
                if (get_string_value(encryption_key))
                    continue;
            }
            else if (std::strcmp(arg, "-H") == 0 || std::strcmp(arg, "--show-history") == 0) {
                show_history = true;
                continue;
            }
            else if (std::strcmp(arg, "-c") == 0 || std::strcmp(arg, "--show-columns") == 0) {
                show_columns = true;
                continue;
            }
            else if (std::strcmp(arg, "-V") == 0 || std::strcmp(arg, "--verify") == 0) {
                verify = true;
                continue;
            }
            else if (std::strcmp(arg, "-v") == 0 || std::strcmp(arg, "--version") == 0) {
                version = true;
                continue;
            }
            std::cerr << "ERROR: Bad or missing value for option: " << arg << "\n";
            error = true;
        }
        argc = argc_2;

        i = 0;
        if (!get_string_value(realm_path)) {
            error = true;
        }
        else if (i < argc) {
            error = true;
        }

        if (help) {
            std::cerr << "Synopsis: " << prog
                      << "  PATH\n"
                         "\n"
                         "Options:\n"
                         "  -h, --help           Display command-line synopsis followed by the list of\n"
                         "                       available options.\n"
                         "  -e, --encryption-key  The file-system path of a file containing a 64-byte\n"
                         "                       encryption key to be used for accessing the specified\n"
                         "                       Realm file.\n"
                         "  -H, --show-history   Show detailed breakdown of contents of history\n"
                         "                       compartment.\n"
                         "  -c, --show-columns   Show column-level schema information for each table.\n"
                         "  -V, --verify         Perform group-level verification (no-op unless built in\n"
                         "                       debug mode).\n"
                         "  -v, --version        Show the version of the Realm Sync release that this\n"
                         "                       command belongs to.\n";
            return EXIT_SUCCESS;
        }

        if (version) {
            const char* build_mode;
#if REALM_DEBUG
            build_mode = "Debug";
#else
            build_mode = "Release";
#endif
            std::cerr << "RealmSync/" REALM_VERSION_STRING " (build_mode=" << build_mode << ")\n";
            return EXIT_SUCCESS;
        }

        if (error) {
            std::cerr << "ERROR: Bad command line.\n"
                         "Try `"
                      << prog << " --help`\n";
            return EXIT_FAILURE;
        }
    }

    std::string encryption_key_2;
    const char* encryption_key_3 = nullptr;
    if (!encryption_key.empty()) {
        encryption_key_2 = util::load_file(encryption_key);
        encryption_key_3 = encryption_key_2.data();
    }
    Group group{realm_path, encryption_key_3};
    using gf = _impl::GroupFriend;
    int file_format_version = gf::get_file_format_version(group);
    if (file_format_version != 20) {
        std::cout << "ERROR: Unexpected file format version " << file_format_version << "\n";
        return EXIT_FAILURE;
    }
    std::cout << "File format version: " << file_format_version << "\n";
    Allocator& alloc = gf::get_alloc(group);
    ref_type top_ref = gf::get_top_ref(group);
    if (top_ref == 0) {
        std::cout << "Realm without top array node\n";
        return EXIT_SUCCESS;
    }
    util::TimestampFormatter timestamp_formatter;
    auto format_timestamp = [&](std::time_t timestamp) {
        return timestamp_formatter.format(timestamp, 0);
    };
    using version_type = _impl::History::version_type;
    version_type version;
    int history_type;
    int history_schema_version;
    gf::get_version_and_history_info(alloc, top_ref, version, history_type, history_schema_version);
    std::cout << "Snapshot number: " << version << "\n";
    std::cout << "History type: " << history_type_to_string(history_type)
              << " "
                 "("
              << history_type << ")\n";
    std::cout << "History schema version: " << history_schema_version << "\n";
    Array top{alloc};
    top.init_from_ref(top_ref);
    {
        auto x = top.get_as_ref_or_tagged(2);
        REALM_ASSERT(x.is_tagged());
        std::cout << "Logical file size: " << format_byte_size(double(x.get_as_int())) << "\n";
    }
    {
        MemStats stats;
        top.stats(stats);
        std::cout << "- Snapshot size: " << format_byte_size(double(stats.allocated))
                  << " "
                     "(top_ref = "
                  << top_ref << ")\n";
    }
    {
        ref_type history_ref = 0;
        if (top.size() > 7) {
            REALM_ASSERT(top.size() >= 9);
            history_ref = top.get_as_ref(8);
        }
        std::cout << "  - History size: " << format_aggregate_size({history_ref}, alloc)
                  << " "
                     "(history_ref = "
                  << history_ref << ")\n";
        if (show_history) {
            if (history_type == Replication::hist_None) {
                // No-op
            }
            else if (history_type == Replication::hist_SyncClient) {
                ClientHistoryInfo info;
                if (history_schema_version == 1) {
                    extract_client_history_info_1(alloc, history_ref, version, info);
                }
                else if (history_schema_version == 2) {
                    extract_client_history_info_2(alloc, history_ref, version, info);
                }
                else {
                    std::cerr << "ERROR: Detailed breakdown of client-side history compartment is "
                                 "unavailable for history schema version "
                              << history_schema_version << "\n";
                    return EXIT_FAILURE;
                }
                std::cout << "    - Continuous transactions history:\n";
                {
                    const ClientHistoryInfo::ContinuousTransactionsHistory& cth = info.ct_history;
                    std::cout << "      - Base version: " << cth.base_version << "\n";
                    std::cout << "      - Current version: " << cth.curr_version
                              << " (hard-linked to snapshot number)\n";
                    std::cout << "      - Size: " << format_byte_size(double(cth.aggr_size)) << " ("
                              << format_num_entries(cth.size) << ")\n";
                }
                std::cout << "    - Synchronization history:\n";
                {
                    const ClientHistoryInfo::SynchronizationHistory& sh = info.sync_history;
                    std::cout << "      - Base version: " << sh.base_version << "\n";
                    std::cout << "      - Current version: " << sh.curr_version
                              << " (hard-linked to snapshot number)\n";
                    std::cout << "      - Main history size: " << format_byte_size(double(sh.main_aggr_size)) << " ("
                              << format_num_history_entries(sh.size) << ")\n";
                    std::cout << "      - Reciprocal history size: "
                                 ""
                              << format_byte_size(double(sh.recip_aggr_size)) << "\n";
                }
                std::cout << "    - Binding to server-side file:";
                if (info.server_binding) {
                    const ClientHistoryInfo::ServerBinding& sb = *info.server_binding;
                    std::cout << "\n"
                                 "      - Client file identifier: "
                              << sb.client_file_ident.ident << " (salt=" << sb.client_file_ident.salt
                              << ")\n"
                                 "      - Latest known server version: "
                              << sb.latest_server_version.version << " (salt=" << sb.latest_server_version.salt
                              << ")\n"
                                 "      - Synchronization progress:\n"
                                 "        - Download (server version): "
                              << sb.download_progress.server_version << " (last_integrated_client_version="
                              << sb.download_progress.last_integrated_client_version
                              << ")\n"
                                 "        - Upload (client version): "
                              << sb.upload_progress.client_version;
                    if (!sb.defective_upload_progress) {
                        std::cout << " (last_integrated_server_version="
                                  << sb.upload_progress.last_integrated_server_version << ")";
                    }
                    std::cout << "\n";
                }
                else {
                    std::cout << " None\n";
                }
                std::cout << "    - Cooked history:";
                if (info.cooked_history) {
                    const ClientHistoryInfo::CookedHistory& ch = *info.cooked_history;
                    std::cout << "\n"
                                 "      - Size (unconsumed): "
                              << format_byte_size(double(ch.aggr_size)) << " ("
                              << format_num_unconsumed_changesets(ch.size)
                              << ")\n"
                                 "      - Progress: (changeset_index="
                              << ch.changeset_index << ", intrachangeset_progress=" << ch.intrachangeset_progress
                              << ")\n"
                                 "      - Base server version: "
                              << ch.base_server_version << "\n";
                }
                else {
                    std::cout << " None\n";
                }
                std::cout << "    - History compartment schema versions:";
                if (info.schema_versions) {
                    std::cout << "\n";
                    for (const ClientHistoryInfo::SchemaVersion& entry : *info.schema_versions) {
                        std::cout << "      - Version: " << entry.schema_version;
                        if (!entry.details_are_unknown) {
                            std::cout << " (sync_library_version=" << entry.library_version
                                      << ", snapshot_number=" << entry.snapshot_version
                                      << ", timestamp=" << format_timestamp(entry.timestamp) << ")";
                        }
                        else {
                            std::cout << " (details are unknown)";
                        }
                        std::cout << "\n";
                    }
                }
                else {
                    std::cout << " None\n";
                }
            }
            else if (history_type == Replication::hist_SyncServer) {
                std::cerr << "Server history detected, but this is unsupported" << std::endl;
            }
            else {
                std::cerr << "ERROR: Detailed breakdown of history compartment is unavailable "
                             "for this type of history compartment\n";
                return EXIT_FAILURE;
            }
        }
    }
    {
        ref_type ref_1 = 0;
        ref_type ref_2 = 0;
        ref_type ref_3 = 0;
        if (top.size() > 3) {
            REALM_ASSERT(top.size() >= 5);
            ref_1 = top.get_as_ref(3);
            ref_2 = top.get_as_ref(4);
            if (top.size() > 5) {
                REALM_ASSERT(top.size() >= 7);
                ref_3 = top.get_as_ref(5);
            }
        }
        std::size_t num_entries = 0;
        if (ref_1 != 0) {
            Array free_positions{alloc};
            free_positions.init_from_ref(ref_1);
            num_entries = free_positions.size();
        }
        std::cout << "  - Free-space registry size: "
                     ""
                  << format_aggregate_size({ref_1, ref_2, ref_3}, alloc)
                  << " "
                     "("
                  << format_num_entries(num_entries) << ")\n";
    }
    Array tables{alloc};
    tables.init_from_ref(top.get_as_ref(1));
    std::size_t num_tables = group.size();
    REALM_ASSERT(tables.size() == num_tables);
    {
        Array table_names{alloc};
        table_names.init_from_ref(top.get_as_ref(0));
        MemStats stats;
        table_names.stats(stats);
        tables.stats(stats);
        std::cout << "  - State size: " << format_byte_size(double(stats.allocated)) << "\n";
    }
    auto quoted = [](StringData str) {
        std::string_view str_2{str.data(), str.size()};
        return util::quoted(str_2);
    };
    std::cout << "    - Number of tables: " << num_tables << "\n";
    auto table_keys = group.get_table_keys();
    for (std::size_t i = 0; i < num_tables; ++i) {
        ConstTableRef table = group.get_table(table_keys[i]);
        StringData table_name = group.get_table_name(table_keys[i]);
        std::cout << "    - Table: " << quoted(table_name) << ": "
                  << format_aggregate_size({tables.get_as_ref(i)}, alloc) << " (" << format_num_rows(table->size())
                  << ")\n";
        if (show_columns) {
            auto col_keys = table->get_column_keys();
            for (auto col : col_keys) {
                std::cout << "      - Column: " << quoted(table->get_column_name(col)) << ": ";
                auto col_type = col.get_type();
                std::cout << get_data_type_name(DataType(col_type));
                if (Table::is_link_type(col_type)) {
                    ConstTableRef target_table = table->get_link_target(col);
                    std::cout << " -> " << quoted(target_table->get_name());
                }
                if (table->is_nullable(col))
                    std::cout << " (nullable)";
                if (table->has_search_index(col))
                    std::cout << " (indexed)";
                std::cout << "\n";
            }
        }
    }
    if (verify)
        group.verify();
}
