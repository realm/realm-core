#include <realm/db.hpp>
#include <realm/encrypt/fingerprint.hpp>
#include <realm/sync/history.hpp>
#include <realm/noinst/common_dir.hpp>
#include <realm/noinst/compression.hpp>
#include <realm/noinst/client_history_impl.hpp>
#include <realm/noinst/client_reset.hpp>
#include <realm/noinst/client_state_download.hpp>

using namespace realm;
using namespace _impl;
using namespace _impl::client_reset;

// Decription of metadata version 1.
//
// The metadata files are located in ${metadata_dir}/version-1.
//
// There are two files (not counting lock and management):
//
// 1. A metadata Realm.
// It has path ${metadata_dir}/version-1/meta.realm.
// The schema is:
//
// Table "integers"
// 5 rows containing:
// server_version
// server_version_salt
// end_offset
// max_offset
// file_size
//
// 2. A file that will become the actual realm after all pieces have been
// downloaded. Its path is ${metadata_dir}/version-1/incomplete.realm

namespace {

constexpr int g_schema_version = 1;

std::string version_dir_name()
{
    return "version-" + util::to_string(g_schema_version);
}

// Table "integers"
const char* table_name_integers = "integers";
static constexpr ObjKey s_server_version(0);
static constexpr ObjKey s_server_version_salt(1);
static constexpr ObjKey s_end_offset(2);
static constexpr ObjKey s_max_offset(3);
static constexpr ObjKey s_file_size(4);

} // namespace

ClientStateDownload::ClientStateDownload(util::Logger& logger, const std::string& realm_path,
                                         const std::string& metadata_dir, bool recover_local_changes,
                                         util::Optional<std::array<char, 64>> encryption_key)
    : logger{logger}
    , m_realm_path{realm_path}
    , m_versioned_metadata_dir{util::File::resolve(version_dir_name(), metadata_dir)}
    , m_meta_realm_path{util::File::resolve("meta.realm", m_versioned_metadata_dir)}
    , m_partially_downloaded_realm_path{util::File::resolve("partially_downloaded.realm", m_versioned_metadata_dir)}
    , m_encryption_key{encryption_key}
    , m_recover_local_changes{recover_local_changes}
{
    logger.debug("Create ClientStateDownload, realm_path = %1, metadata_dir = %2", realm_path, metadata_dir);
#ifdef REALM_ENABLE_ENCRYPTION
    if (m_encryption_key)
        m_aes_cryptor.reset(new AESCryptor(reinterpret_cast<unsigned char*>(m_encryption_key->data())));
#else
    REALM_ASSERT(!encryption_key);
#endif
    if (!util::File::is_dir(metadata_dir)) {
        const std::string msg = metadata_dir + " must be an existing directory";
        throw std::runtime_error(msg);
    }

    initialize();
}

void ClientStateDownload::set_salted_file_ident(sync::SaltedFileIdent salted_file_ident)
{
    m_salted_file_ident = salted_file_ident;
}

void ClientStateDownload::set_client_reset_client_version(sync::version_type client_version)
{
    m_client_reset_client_version = client_version;
}

bool ClientStateDownload::receive_state(sync::version_type server_version, sync::salt_type server_version_salt,
                                        uint_fast64_t begin_offset, uint_fast64_t end_offset,
                                        uint_fast64_t max_offset, BinaryData chunk)
{
    REALM_ASSERT(m_salted_file_ident.ident != 0);
    if (begin_offset == 0 && m_server_version.version != 0) {
        // The server starts from scratch with a new state Realm.
        // We reset back to the beginning of the file.
        reset();
    }

    if (begin_offset != 0) {
        // This is not the first STATE message.
        // Various invariants must be true.
        if (server_version != m_server_version.version || server_version_salt != m_server_version.salt ||
            begin_offset != m_end_offset || max_offset != m_max_offset) {
            reset();
            logger.error("The STATE message parameters are incompatible with "
                         "previous messages: "
                         "server_version = %1, "
                         "m_server_version.version = %2, "
                         "server_version_salt = %3, "
                         "m_server_version_salt = %4, "
                         "begin_offset = %5, "
                         "m_end_offset = %6, "
                         "max_offset = %7, "
                         "m_max_offset = %8",
                         server_version, m_server_version.version, server_version_salt, m_server_version.salt,
                         begin_offset, m_end_offset, max_offset, m_max_offset);
            return false;
        }
    }

    {
        std::error_code ec = compression::integrate_compressed_blocks_in_realm_file(
            chunk.data(), chunk.size(), m_partially_downloaded_realm_path, m_encryption_key, m_file_size);
        if (ec) {
            logger.error("Integration of the STATE message blocks failed, '%1'", ec);
            reset();
            return false;
        }
    }

    {
        bool no_create = false;
        DBOptions shared_group_options(m_encryption_key ? m_encryption_key->data() : nullptr);
        DBRef sg = DB::create(m_meta_realm_path, no_create, shared_group_options);
        WriteTransaction wt{sg};
        Group& group = wt.get_group();
        TableRef table_integers = group.get_table(table_name_integers);
        auto col_ints = table_integers->get_column_key("value");


        if (begin_offset == 0) {
            // First STATE message.
            REALM_ASSERT(m_max_offset == 0);
            m_server_version = {server_version, server_version_salt};
            table_integers->get_object(s_server_version).set<Int>(col_ints, m_server_version.version);
            table_integers->get_object(s_server_version_salt).set<Int>(col_ints, m_server_version.salt);
            m_max_offset = max_offset;
            table_integers->get_object(s_max_offset).set<Int>(col_ints, m_max_offset);
        }

        m_end_offset = end_offset;
        table_integers->get_object(s_end_offset).set<Int>(col_ints, m_end_offset);

        // m_file_size was set in the call to
        // compression::integrate_compressed_blocks_in_realm_file() above.
        table_integers->get_object(s_file_size).set<Int>(col_ints, m_file_size);

        wt.commit();
    }

    if (m_end_offset == m_max_offset) {
        bool success = finalize();
        if (!success) {
            reset();
            return false;
        }
    }

    return true;
}

void ClientStateDownload::initialize()
{
    if (!util::File::exists(m_versioned_metadata_dir)) {
        initialize_from_new();
    }
    else {
        try {
            bool success = initialize_from_existing();
            if (!success) {
                util::remove_dir_recursive(m_versioned_metadata_dir);
                initialize_from_new();
            }
        }
        catch (util::File::AccessError&) {
            util::remove_dir_recursive(m_versioned_metadata_dir);
            initialize_from_new();
        }
    }
}

void ClientStateDownload::initialize_from_new()
{
    logger.debug("ClientStateDownload: initialize_from_new using directory, "
                 "m_versioned_meta_data_dir = '%1'",
                 m_versioned_metadata_dir);
    REALM_ASSERT(m_server_version.version == 0);
    REALM_ASSERT(m_server_version.salt == 0);
    REALM_ASSERT(m_end_offset == 0);
    REALM_ASSERT(m_max_offset == 0);
    REALM_ASSERT(m_file_size == 0);
    REALM_ASSERT(!util::File::exists(m_versioned_metadata_dir));
    util::make_dir(m_versioned_metadata_dir);

    bool no_create = false;
    DBOptions shared_group_options(m_encryption_key ? m_encryption_key->data() : nullptr);
    DBRef sg = DB::create(m_meta_realm_path, no_create, shared_group_options);
    WriteTransaction wt{sg};
    Group& group = wt.get_group();

    TableRef table_integers = group.add_table(table_name_integers);
    auto col_ints = table_integers->add_column(type_Int, "value");
    // The inserted values are 0, but using the variable names makes the
    // interpretation clear.
    table_integers->create_object(s_server_version).set<Int>(col_ints, m_server_version.version);
    table_integers->create_object(s_server_version_salt).set<Int>(col_ints, m_server_version.salt);
    table_integers->create_object(s_end_offset).set<Int>(col_ints, m_end_offset);
    table_integers->create_object(s_max_offset).set<Int>(col_ints, m_max_offset);
    table_integers->create_object(s_file_size).set<Int>(col_ints, m_file_size);

    wt.commit();
}

bool ClientStateDownload::initialize_from_existing()
{
    REALM_ASSERT(util::File::exists(m_versioned_metadata_dir));

    bool no_create = false;
    DBOptions shared_group_options(m_encryption_key ? m_encryption_key->data() : nullptr);
    DBRef sg = DB::create(m_meta_realm_path, no_create, shared_group_options);
    ReadTransaction rt{sg};
    const Group& group = rt.get_group();

    ConstTableRef table_integers = group.get_table(table_name_integers);
    auto col_ints = table_integers->get_column_key("value");
    if (!table_integers)
        return false;
    REALM_ASSERT(table_integers->size() == 5);

    m_server_version.version = table_integers->get_object(s_server_version).get<Int>(col_ints);
    m_server_version.salt = table_integers->get_object(s_server_version_salt).get<Int>(col_ints);
    m_end_offset = table_integers->get_object(s_end_offset).get<Int>(col_ints);
    m_max_offset = table_integers->get_object(s_max_offset).get<Int>(col_ints);
    m_file_size = table_integers->get_object(s_file_size).get<Int>(col_ints);

    util::File file{m_partially_downloaded_realm_path};
    if (m_file_size != static_cast<uint_fast64_t>(file.get_size())) {
        // Here we detect a situation where the meta Realm was updated but the
        // tmp file was not fully updated. This is likely due to a crash, and
        // we need to reset.
        logger.debug("ClientStateDownload: the partially downloaded Realm had a different size "
                     "(%1) than listed in the meta Realm (%2)",
                     file.get_size(), m_file_size);
        return false;
    }

    logger.debug("ClientStateDownload: initialize_from_existing, "
                 "m_versioned_meta_dir = %1, m_server_version = %2, "
                 "m_server_version_salt = %3, m_end_offset = %4, "
                 "m_max_offset = %5, m_file_size = %6",
                 m_versioned_metadata_dir, m_server_version.version, m_server_version.salt, m_end_offset,
                 m_max_offset, m_file_size);

    return true;
}

bool ClientStateDownload::finalize()
{
    bool local_realm_exists = util::File::exists(m_realm_path);

    bool success;

    if (local_realm_exists) {
        m_is_client_reset = true;
        success = finalize_client_reset();
    }
    else {
        m_is_client_reset = false;
        success = finalize_async_open();
    }

    if (success)
        m_complete = true;

    return success;
}

bool ClientStateDownload::finalize_async_open()
{
    logger.debug("finalize_async_open, realm_path = %1", m_realm_path);
    REALM_ASSERT(!util::File::exists(m_realm_path));
    REALM_ASSERT(util::File::exists(m_meta_realm_path));
    REALM_ASSERT(util::File::exists(m_partially_downloaded_realm_path));

    // Insert the client file ident and salt in the Realm.
    try {
        ClientHistoryImpl history(m_partially_downloaded_realm_path);
        DBOptions shared_group_options(m_encryption_key ? m_encryption_key->data() : nullptr);
        DBRef sg = DB::create(history, shared_group_options);
        uint_fast64_t downloaded_bytes = m_max_offset;
        history.make_final_async_open_adjustements(m_salted_file_ident, downloaded_bytes);
    }
    catch (util::File::AccessError& e) {
        logger.error("In finalize_state_transfer, the realm %1 could not be opened, "
                     "msg = %2",
                     m_partially_downloaded_realm_path, e.what());
        return false;
    }

    // Move the tmp Realm into the proper place.
    util::File::move(m_partially_downloaded_realm_path, m_realm_path);

    // Remove the versioned metadata dir.
    util::remove_dir_recursive(m_versioned_metadata_dir);

    return true;
}

bool ClientStateDownload::finalize_client_reset()
{
    logger.debug("finalize_client_reset, realm_path = %1", m_realm_path);
    REALM_ASSERT(util::File::exists(m_realm_path));
    REALM_ASSERT(util::File::exists(m_meta_realm_path));
    REALM_ASSERT(util::File::exists(m_partially_downloaded_realm_path));

    LocalVersionIDs local_version_ids;
    try {
        uint_fast64_t downloaded_bytes = m_max_offset;
        local_version_ids = perform_client_reset_diff(
            m_partially_downloaded_realm_path, m_realm_path, m_encryption_key, m_salted_file_ident, m_server_version,
            downloaded_bytes, m_client_reset_client_version, m_recover_local_changes, logger);
    }
    catch (util::File::AccessError& e) {
        logger.error("In finalize_client_reset, the client reset failed, "
                     "realm path = %1, downloaded realm path = %2, msg = %3",
                     m_realm_path, m_partially_downloaded_realm_path, e.what());
        return false;
    }

    // Remove the versioned metadata dir.
    util::remove_dir_recursive(m_versioned_metadata_dir);

    m_client_reset_old_version = local_version_ids.old_version;
    m_client_reset_new_version = local_version_ids.new_version;

    return true;
}

void ClientStateDownload::reset()
{
    m_complete = false;

    m_is_client_reset = false;
    m_server_version = {0, 0};
    m_end_offset = 0;
    m_max_offset = 0;
    m_file_size = 0;

    util::try_remove_dir_recursive(m_versioned_metadata_dir);
    initialize_from_new();
}
