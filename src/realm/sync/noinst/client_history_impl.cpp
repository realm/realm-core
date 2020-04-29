#include <ctime>
#include <cstring>
#include <algorithm>
#include <utility>

#include <realm/util/features.h>
#include <realm/util/scope_exit.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/sync/version.hpp>
#include <realm/sync/object.hpp>
#include <realm/sync/changeset.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/instruction_replication.hpp>
#include <realm/sync/instruction_applier.hpp>

using namespace realm;


// This is a work-around for a bug in MSVC. It cannot find types mentioned in
// member function signatures as part of definitions occuring outside the body
// of the class, even when those types are visible from within the class.
using sync::BadCookedServerVersion;


using _impl::ClientHistoryImpl;

void ClientHistoryImpl::set_initial_state_realm_history_numbers(version_type current_version,
                                                                sync::SaltedVersion server_version)
{
    REALM_ASSERT(current_version == s_initial_version + 1);
    ensure_updated(current_version); // Throws
    prepare_for_write();             // Throws

    version_type client_version = m_sync_history_base_version + m_sync_history_size;
    REALM_ASSERT(client_version == current_version); // For now
    DownloadCursor download_progress = {server_version.version, 0};

    Array& root = m_arrays->root;
    root.set(s_progress_download_server_version_iip,
             RefOrTagged::make_tagged(download_progress.server_version)); // Throws
    root.set(s_progress_download_client_version_iip,
             RefOrTagged::make_tagged(download_progress.last_integrated_client_version)); // Throws
    root.set(s_progress_latest_server_version_iip,
             RefOrTagged::make_tagged(server_version.version)); // Throws
    root.set(s_progress_latest_server_version_salt_iip,
             RefOrTagged::make_tagged(server_version.salt)); // Throws

    m_progress_download = download_progress;
}


void ClientHistoryImpl::make_final_async_open_adjustements(SaltedFileIdent client_file_ident,
                                                           std::uint_fast64_t downloaded_bytes)
{
    auto wt = m_shared_group->start_write(); // Throws
    version_type local_version = wt->get_version();
    ensure_updated(local_version); // Throws
    prepare_for_write();           // Throws

    REALM_ASSERT(m_group->get_sync_file_id() == 0);

    Array& root = m_arrays->root;
    m_group->set_sync_file_id(client_file_ident.ident);                                       // Throws
    root.set(s_client_file_ident_salt_iip, RefOrTagged::make_tagged(client_file_ident.salt)); // Throws
    root.set(s_progress_downloaded_bytes_iip, RefOrTagged::make_tagged(downloaded_bytes));    // Throws

    wt->commit(); // Throws
}


void ClientHistoryImpl::set_client_file_ident_in_wt(version_type current_version, SaltedFileIdent client_file_ident)
{
    ensure_updated(current_version); // Throws
    prepare_for_write();             // Throws

    Array& root = m_arrays->root;
    m_group->set_sync_file_id(client_file_ident.ident); // Throws
    root.set(s_client_file_ident_salt_iip,
             RefOrTagged::make_tagged(client_file_ident.salt)); // Throws
}


auto ClientHistoryImpl::get_next_local_changeset(version_type current_version, version_type begin_version) const
    -> util::Optional<LocalChangeset>
{
    ensure_updated(current_version); // Throws

    if (!m_changesets)
        return none;
    REALM_ASSERT(begin_version >= 1);
    version_type end_version = m_sync_history_base_version + m_sync_history_size;
    if (begin_version < m_sync_history_base_version)
        begin_version = m_sync_history_base_version;

    for (version_type version = begin_version; version < end_version; ++version) {
        std::size_t ndx = std::size_t(version - m_sync_history_base_version);
        std::int_fast64_t origin_file_ident = m_origin_file_idents->get(ndx);
        bool not_from_server = (origin_file_ident == 0);
        if (not_from_server) {
            LocalChangeset local_changeset;
            local_changeset.version = version;
            ChunkedBinaryData changeset(*m_changesets, ndx);
            local_changeset.changeset = changeset;
            return local_changeset;
        }
    }
    return none;
}


void ClientHistoryImpl::set_client_reset_adjustments(version_type current_version, SaltedFileIdent client_file_ident,
                                                     sync::SaltedVersion server_version,
                                                     std::uint_fast64_t downloaded_bytes,
                                                     BinaryData uploadable_changeset)
{
    ensure_updated(current_version); // Throws
    prepare_for_write();             // Throws

    version_type client_version = m_sync_history_base_version + m_sync_history_size;
    REALM_ASSERT(client_version == current_version); // For now
    DownloadCursor download_progress = {server_version.version, 0};
    UploadCursor upload_progress = {0, 0};
    Array& root = m_arrays->root;
    m_group->set_sync_file_id(client_file_ident.ident); // Throws
    root.set(s_client_file_ident_salt_iip,
             RefOrTagged::make_tagged(client_file_ident.salt)); // Throws
    root.set(s_progress_download_server_version_iip,
             RefOrTagged::make_tagged(download_progress.server_version)); // Throws
    root.set(s_progress_download_client_version_iip,
             RefOrTagged::make_tagged(download_progress.last_integrated_client_version)); // Throws
    root.set(s_progress_latest_server_version_iip,
             RefOrTagged::make_tagged(server_version.version)); // Throws
    root.set(s_progress_latest_server_version_salt_iip,
             RefOrTagged::make_tagged(server_version.salt)); // Throws
    root.set(s_progress_upload_client_version_iip,
             RefOrTagged::make_tagged(upload_progress.client_version)); // Throws
    root.set(s_progress_upload_server_version_iip,
             RefOrTagged::make_tagged(upload_progress.last_integrated_server_version)); // Throws
    root.set(s_progress_downloaded_bytes_iip,
             RefOrTagged::make_tagged(downloaded_bytes)); // Throws
    root.set(s_progress_downloadable_bytes_iip,
             RefOrTagged::make_tagged(0)); // Throws
    root.set(s_progress_uploaded_bytes_iip,
             RefOrTagged::make_tagged(0)); // Throws
    root.set(s_progress_uploadable_bytes_iip,
             RefOrTagged::make_tagged(0)); // Throws

    // Discard existing synchronization history
    do_trim_sync_history(m_sync_history_size); // Throws

    m_progress_download = download_progress;
    m_client_reset_changeset = uploadable_changeset; // Picked up by prepare_changeset()
}


// Overriding member function in realm::Replication
void ClientHistoryImpl::initialize(DB& sg)
{
    REALM_ASSERT(!m_shared_group);
    SyncReplication::initialize(sg); // Throws
    m_shared_group = &sg;
}


// Overriding member function in realm::Replication
void ClientHistoryImpl::initiate_session(version_type)
{
    // No-op
}


// Overriding member function in realm::Replication
void ClientHistoryImpl::terminate_session() noexcept
{
    // No-op
}


// Overriding member function in realm::Replication
auto ClientHistoryImpl::get_history_type() const noexcept -> HistoryType
{
    return hist_SyncClient;
}


// Overriding member function in realm::Replication
int ClientHistoryImpl::get_history_schema_version() const noexcept
{
    return get_client_history_schema_version();
}


// Overriding member function in realm::Replication
bool ClientHistoryImpl::is_upgradable_history_schema(int stored_schema_version) const noexcept
{
    if (stored_schema_version == 1 || stored_schema_version == 2 || stored_schema_version > 9) {
        return true;
    }
    return false;
}


// Overriding member function in realm::Replication
void ClientHistoryImpl::upgrade_history_schema(int stored_schema_version)
{
    // upgrade_history_schema() is called only when there is a need to upgrade
    // (`stored_schema_version < get_server_history_schema_version()`), and only
    // when is_upgradable_history_schema() returned true (`stored_schema_version
    // >= 1`).
    REALM_ASSERT(stored_schema_version < get_client_history_schema_version());
    REALM_ASSERT(stored_schema_version >= 1);
    int orig_schema_version = stored_schema_version;
    int schema_version = orig_schema_version;
    if (schema_version < 2) {
        migrate_from_history_schema_version_1_to_2(orig_schema_version); // Throws
        schema_version = 2;
    }
    if (schema_version < 3) {
        migrate_from_history_schema_version_2_to_10(); // Throws
        schema_version = 10;
    }

    // NOTE: Future migration steps go here.

    REALM_ASSERT(schema_version == get_client_history_schema_version());

    // Record migration event
    record_current_schema_version(); // Throws
}

// Overriding member function in realm::Replication
_impl::History* ClientHistoryImpl::_get_history_write()
{
    // REALM_ASSERT(m_group != nullptr);
    return this;
}

// Overriding member function in realm::Replication
std::unique_ptr<_impl::History> ClientHistoryImpl::_create_history_read()
{
    auto hist_impl = std::make_unique<ClientHistoryImpl>(get_database_path(), m_owner_is_sync_client, nullptr);
    hist_impl->initialize(*m_shared_group); // Throws
    // Transfer ownership with pointer to private base class
    return std::unique_ptr<_impl::History>{hist_impl.release()};
}

// Overriding member function in realm::Replication
bool ClientHistoryImpl::is_sync_agent() const noexcept
{
    return m_owner_is_sync_client;
}

// Overriding member function in realm::Replication
void ClientHistoryImpl::do_initiate_transact(Group& group, version_type version, bool history_updated)
{
    SyncReplication::do_initiate_transact(group, version, history_updated);
}


// Overriding member function in realm::TrivialReplication
auto ClientHistoryImpl::prepare_changeset(const char* data, size_t size, version_type orig_version) -> version_type
{
    ensure_updated(orig_version);
    prepare_for_write(); // Throws
    REALM_ASSERT(m_ct_history->size() == m_ct_history_size);
    REALM_ASSERT(m_changesets->size() == m_sync_history_size);

    BinaryData ct_changeset{data, size};
    add_ct_history_entry(ct_changeset); // Throws

    HistoryEntry entry;

    REALM_ASSERT(!m_changeset_from_server || !m_client_reset_changeset);

    if (m_changeset_from_server) {
        entry = *std::move(m_changeset_from_server);

        REALM_ASSERT(get_instruction_encoder().buffer().size() == 0);
    }
    else {
        BinaryData changeset;
        if (m_client_reset_changeset) {
            changeset = *m_client_reset_changeset;
            m_client_reset_changeset = util::none;
        }
        else {
            auto& buffer = get_instruction_encoder().buffer();
            changeset = BinaryData(buffer.data(), buffer.size());
        }

        entry.origin_timestamp = sync::generate_changeset_timestamp();
        entry.origin_file_ident = 0; // Of local origin
        entry.remote_version = m_progress_download.server_version;
        entry.changeset = changeset;

        // uploadable_bytes is updated at every local Realm change. The total
        // number of uploadable bytes must be persisted in the Realm, since the
        // synchronization history is trimmed. Even if the synchronization
        // history wasn't trimmed, it would be expensive to traverse the entire
        // history at every access to uploadable bytes.
        Array& root = m_arrays->root;
        std::uint_fast64_t uploadable_bytes = root.get_as_ref_or_tagged(s_progress_uploadable_bytes_iip).get_as_int();
        uploadable_bytes += entry.changeset.size();
        root.set(s_progress_uploadable_bytes_iip, RefOrTagged::make_tagged(uploadable_bytes));
    }

    add_sync_history_entry(entry); // Throws

    REALM_ASSERT(m_ct_history->size() == m_ct_history_size);
    REALM_ASSERT(m_changesets->size() == m_sync_history_size);

    version_type new_version = m_ct_history_base_version + m_ct_history_size;
    REALM_ASSERT(new_version == m_sync_history_base_version + m_sync_history_size);
    return new_version;
}


// Overriding member function in realm::TrivialReplication
void ClientHistoryImpl::finalize_changeset() noexcept
{
    // Since the history is in the Realm, the added changeset is
    // automatically finalized as part of the commit operation.
    m_changeset_from_server = util::none;
    // m_group = nullptr;
}

// Overriding member function in realm::sync::ClientHistoryBase
void ClientHistoryImpl::get_status(version_type& current_client_version, SaltedFileIdent& client_file_ident,
                                   SyncProgress& progress) const
{
    TransactionRef rt = m_shared_group->start_read(); // Throws
    version_type current_client_version_2 = rt->get_version();
    const_cast<ClientHistoryImpl*>(this)->set_group(rt.get());
    ensure_updated(current_client_version_2); // Throws

    SaltedFileIdent client_file_ident_2{rt->get_sync_file_id(), 0};
    SyncProgress progress_2;
    if (m_arrays) {
        const Array& root = m_arrays->root;
        client_file_ident_2.salt =
            sync::salt_type(root.get_as_ref_or_tagged(s_client_file_ident_salt_iip).get_as_int());
        progress_2.latest_server_version.version =
            version_type(root.get_as_ref_or_tagged(s_progress_latest_server_version_iip).get_as_int());
        progress_2.latest_server_version.salt =
            version_type(root.get_as_ref_or_tagged(s_progress_latest_server_version_salt_iip).get_as_int());
        progress_2.download = m_progress_download;
        progress_2.upload.client_version =
            version_type(root.get_as_ref_or_tagged(s_progress_upload_client_version_iip).get_as_int());
        progress_2.upload.last_integrated_server_version =
            version_type(root.get_as_ref_or_tagged(s_progress_upload_server_version_iip).get_as_int());
    }

    current_client_version = current_client_version_2;
    client_file_ident = client_file_ident_2;
    progress = progress_2;

    REALM_ASSERT(current_client_version >= s_initial_version + 0);
    if (current_client_version == s_initial_version + 0)
        current_client_version = 0;
}


// Overriding member function in realm::sync::ClientHistoryBase
void ClientHistoryImpl::set_client_file_ident(SaltedFileIdent client_file_ident, bool fix_up_object_ids)
{
    REALM_ASSERT(client_file_ident.ident != 0);

    TransactionRef wt = m_shared_group->start_write(); // Throws
    version_type local_version = wt->get_version();
    ensure_updated(local_version); // Throws
    prepare_for_write();           // Throws
    TableInfoCache table_info_cache{*wt};

    Array& root = m_arrays->root;
    wt->set_sync_file_id(client_file_ident.ident);
    root.set(s_client_file_ident_salt_iip,
             RefOrTagged::make_tagged(client_file_ident.salt)); // Throws

    if (fix_up_object_ids) {
        // Replication must be temporarily disabled because the database must be
        // modified to fix up object IDs.
        //
        // FIXME: This is dangerous, because accessors will not be updated with
        // modifications made here.  Luckily, only set_int() modifications are
        // made, which never have an impact on accessors. However, notifications
        // will not be triggered for those updates either.
        sync::TempShortCircuitReplication tscr{*this};
        fix_up_client_file_ident_in_stored_changesets(*wt, table_info_cache, client_file_ident.ident); // Throws
    }

    // Note: This transaction produces an empty changeset. Empty changesets are
    // not uploaded to the server.
    wt->commit(); // Throws
}


// Overriding member function in realm::sync::ClientHistoryBase
void ClientHistoryImpl::set_sync_progress(const SyncProgress& progress, const std::uint_fast64_t* downloadable_bytes,
                                          VersionInfo& version_info)
{
    TransactionRef wt = m_shared_group->start_write(); // Throws
    version_type local_version = wt->get_version();
    ensure_updated(local_version); // Throws
    prepare_for_write();           // Throws

    if (m_changeset_cooker) {
        ensure_cooked_history(); // Throws
    }
    else {
        ensure_no_cooked_history(); // Throws
    }

    update_sync_progress(progress, downloadable_bytes); // Throws

    // Note: This transaction produces an empty changeset. Empty changesets are
    // not uploaded to the server.
    version_type new_version = wt->commit(); // Throws
    version_info.realm_version = new_version;
    version_info.sync_version = {new_version, 0};
}


// Overriding member function in realm::sync::ClientHistoryBase
void ClientHistoryImpl::find_uploadable_changesets(UploadCursor& upload_progress, version_type end_version,
                                                   std::vector<UploadChangeset>& uploadable_changesets,
                                                   version_type& locked_server_version) const
{
    TransactionRef rt = m_shared_group->start_read(); // Throws
    version_type local_version = rt->get_version();
    const_cast<ClientHistoryImpl*>(this)->set_group(rt.get());
    ensure_updated(local_version); // Throws

    std::size_t accum_byte_size_soft_limit = 0x20000; // 128 KB
    std::size_t accum_byte_size = 0;

    version_type begin_version_2 = upload_progress.client_version;
    version_type end_version_2 = end_version;
    clamp_sync_version_range(begin_version_2, end_version_2);
    version_type last_integrated_upstream_version = upload_progress.last_integrated_server_version;

    while (accum_byte_size < accum_byte_size_soft_limit) {
        HistoryEntry entry;
        version_type version =
            find_sync_history_entry(begin_version_2, end_version_2, entry, last_integrated_upstream_version);

        if (version == 0) {
            begin_version_2 = end_version_2;
            break;
        }
        begin_version_2 = version;

        UploadChangeset uc;
        std::size_t size = entry.changeset.copy_to(uc.buffer);
        uc.origin_timestamp = entry.origin_timestamp;
        uc.origin_file_ident = entry.origin_file_ident;
        uc.progress = UploadCursor{version, entry.remote_version};
        uc.changeset = BinaryData{uc.buffer.get(), size};
        uploadable_changesets.push_back(std::move(uc)); // Throws

        accum_byte_size += size;
    }

    upload_progress = {std::min(begin_version_2, end_version), last_integrated_upstream_version};

    if (m_arrays->cooked_history.is_attached()) {
        locked_server_version = m_ch_base_server_version;
    }
    else {
        locked_server_version = m_progress_download.server_version;
    }
}


// Overriding member function in realm::sync::ClientHistoryBase
bool ClientHistoryImpl::integrate_server_changesets(const SyncProgress& progress,
                                                    const std::uint_fast64_t* downloadable_bytes,
                                                    const RemoteChangeset* incoming_changesets,
                                                    std::size_t num_changesets, VersionInfo& version_info,
                                                    IntegrationError& integration_error, util::Logger& logger,
                                                    SyncTransactReporter* transact_reporter)
{
    // Changesets are applied to the Realm with replication temporarily
    // disabled. The main reason for diabling replication and manually adding
    // the transformed changesets to the history, is that the replication system
    // (due to technical debt) is unable in some cases to produce a correct
    // changeset while applying another one (i.e., it cannot carbon copy).

    VersionID old_version;
    TransactionRef transact;
    auto cleanup = util::make_scope_exit([&transact]() noexcept {
        switch (transact->get_transact_stage()) {
            case realm::DB::transact_Frozen:
            case realm::DB::transact_Ready:
            case realm::DB::transact_Reading:
                break;
            case realm::DB::transact_Writing:
                transact->rollback();
                break;
        }
    });

    REALM_ASSERT(num_changesets != 0);

    transact = m_shared_group->start_write(); // Throws
    // FIXME: Using SharedGroup::get_version_of_current_transaction() is not as
    // efficient as using SharedGroupFriend::get_version_of_bound_snapshot(),
    // but the former is currently needed because we pass `old_version` to
    // application through `transact_reporter` callback. Is that ncessary? Could
    // the transaction reported callback be changed to work with plain snapshot
    // numbers.
    old_version = transact->get_version_of_current_transaction();
    version_type local_version = old_version.version;

    ensure_updated(local_version); // Throws
    prepare_for_write();           // Throws

    if (m_changeset_cooker) {
        ensure_cooked_history(); // Throws
    }
    else {
        ensure_no_cooked_history(); // Throws
    }

    TableInfoCache table_info_cache{*transact};

    REALM_ASSERT(transact->get_sync_file_id() != 0);

    std::vector<char> assembled_transformed_changeset;
    util::AppendBuffer<char> cooked_changeset_buffer;
    std::vector<sync::Changeset> changesets;
    changesets.resize(num_changesets); // Throws

    std::uint_fast64_t downloaded_bytes_in_message = 0;

    try {
        for (std::size_t i = 0; i < num_changesets; ++i) {
            const RemoteChangeset& changeset = incoming_changesets[i];
            REALM_ASSERT(changeset.last_integrated_local_version <= local_version);
            REALM_ASSERT(changeset.origin_file_ident > 0 &&
                         changeset.origin_file_ident != transact->get_sync_file_id());
            downloaded_bytes_in_message += changeset.original_changeset_size;

            sync::parse_remote_changeset(changeset, changesets[i]); // Throws

            // It is possible that the synchronization history has been trimmed
            // to a point where a prefix of the merge window is no longer
            // available, but this can only happen if that prefix consisted
            // entirely of upload skippable entries. Since such entries (those
            // that are empty or of remote origin) will be skipped by the
            // transformer anyway, we can simply clamp the beginning of the
            // merge window to the beginning of the synchronization history,
            // when this situation occurs.
            //
            // See trim_sync_history() for further details.
            if (changesets[i].last_integrated_remote_version < m_sync_history_base_version)
                changesets[i].last_integrated_remote_version = m_sync_history_base_version;
        }

        Transformer& transformer = get_transformer(); // Throws
        Transformer::Reporter* reporter = nullptr;
        transformer.transform_remote_changesets(*this, transact->get_sync_file_id(), local_version, changesets.data(),
                                                changesets.size(), reporter, &logger); // Throws

        for (std::size_t i = 0; i < num_changesets; ++i) {
            util::AppendBuffer<char> transformed_changeset;
            sync::encode_changeset(changesets[i], transformed_changeset);

            if (m_changeset_cooker) {
                cooked_changeset_buffer.clear();
                bool produced = m_changeset_cooker->cook_changeset(*transact, transformed_changeset.data(),
                                                                   transformed_changeset.size(),
                                                                   cooked_changeset_buffer); // Throws
                if (produced) {
                    BinaryData cooked_changeset(cooked_changeset_buffer.data(), cooked_changeset_buffer.size());
                    save_cooked_changeset(cooked_changeset, changesets[i].version); // Throws
                }
            }

            sync::InstructionApplier applier{*transact, table_info_cache};
            {
                sync::TempShortCircuitReplication tscr{*this};
                applier.apply(changesets[i], &logger); // Throws
            }

            // The need to produce a combined changeset is unfortunate from a
            // memory pressure/allocation cost point of view. It is believed
            // that the history (list of applied changesets) will be moved into
            // the main Realm file eventually, and that would probably eliminate
            // this problem.
            std::size_t size_1 = assembled_transformed_changeset.size();
            std::size_t size_2 = size_1;
            if (util::int_add_with_overflow_detect(size_2, transformed_changeset.size()))
                throw util::overflow_error{"Changeset size overflow"};
            assembled_transformed_changeset.resize(size_2); // Throws
            std::copy(transformed_changeset.data(), transformed_changeset.data() + transformed_changeset.size(),
                      assembled_transformed_changeset.data() + size_1);
        }
    }
    catch (sync::BadChangesetError& e) {
        logger.error("Failed to parse, or apply received changeset: %1", e.what()); // Throws
        integration_error = IntegrationError::bad_changeset;
        return false;
    }
    catch (sync::TransformError& e) {
        logger.error("Failed to transform received changeset: %1", e.what()); // Throws
        integration_error = IntegrationError::bad_changeset;
        return false;
    }

    // downloaded_bytes always contains the total number of downloaded bytes
    // from the Realm. downloaded_bytes must be persisted in the Realm, since
    // the downloaded changesets are trimmed after use, and since it would be
    // expensive to traverse the entire history.
    Array& root = m_arrays->root;
    auto downloaded_bytes =
        std::uint_fast64_t(root.get_as_ref_or_tagged(s_progress_downloaded_bytes_iip).get_as_int());
    downloaded_bytes += downloaded_bytes_in_message;
    root.set(s_progress_downloaded_bytes_iip, RefOrTagged::make_tagged(downloaded_bytes)); // Throws

    // The reason we can use the `origin_timestamp`, and the `origin_file_ident`
    // from the last incoming changeset, and ignore all the other changesets, is
    // that these values are actually irrelevant for changesets of remote origin
    // stored in the client-side history (for now), except that
    // `origin_file_ident` is required to be nonzero, to mark it as having been
    // received from the server.
    const sync::Changeset& last_changeset = changesets.back();
    HistoryEntry entry;
    entry.origin_timestamp = last_changeset.origin_timestamp;
    entry.origin_file_ident = last_changeset.origin_file_ident;
    entry.remote_version = last_changeset.version;
    entry.changeset = BinaryData(assembled_transformed_changeset.data(), assembled_transformed_changeset.size());

    // m_changeset_from_server is picked up by prepare_changeset(), which then
    // calls add_sync_history_entry(). prepare_changeset() is called as a result
    // of committing the current transaction even in the "short-circuited" mode,
    // because replication isn't disabled.
    m_changeset_from_server_owner = std::move(assembled_transformed_changeset);
    REALM_ASSERT(!m_changeset_from_server);
    m_changeset_from_server = entry;

    update_sync_progress(progress, downloadable_bytes); // Throws

    version_type new_version = transact->commit_and_continue_as_read(); // Throws
#if REALM_DEBUG
    ensure_updated(new_version); // Throws
    REALM_ASSERT(m_ct_history->size() == m_ct_history_size);
    REALM_ASSERT(m_changesets->size() == m_sync_history_size);
    REALM_ASSERT(m_ct_history_base_version + m_ct_history_size == m_sync_history_base_version + m_sync_history_size);
#endif // REALM_DEBUG

    if (transact_reporter) {
        VersionID new_version_2 = transact->get_version_of_current_transaction();
        transact_reporter->report_sync_transact(old_version, new_version_2); // Throws
    }

    version_info.realm_version = new_version;
    version_info.sync_version = {new_version, 0};
    return true;
}


// Overriding member function in realm::sync::ClientHistory
void ClientHistoryImpl::get_upload_download_bytes(std::uint_fast64_t& downloaded_bytes,
                                                  std::uint_fast64_t& downloadable_bytes,
                                                  std::uint_fast64_t& uploaded_bytes,
                                                  std::uint_fast64_t& uploadable_bytes,
                                                  std::uint_fast64_t& snapshot_version)
{
    TransactionRef rt = m_shared_group->start_read(); // Throws
    version_type current_client_version = rt->get_version();
    const_cast<ClientHistoryImpl*>(this)->set_group(rt.get());
    ensure_updated(current_client_version); // Throws

    downloaded_bytes = 0;
    downloadable_bytes = 0;
    uploaded_bytes = 0;
    uploadable_bytes = 0;
    snapshot_version = current_client_version;

    if (m_arrays) {
        const Array& root = m_arrays->root;
        downloaded_bytes = root.get_as_ref_or_tagged(s_progress_downloaded_bytes_iip).get_as_int();
        downloadable_bytes = root.get_as_ref_or_tagged(s_progress_downloadable_bytes_iip).get_as_int();
        uploadable_bytes = root.get_as_ref_or_tagged(s_progress_uploadable_bytes_iip).get_as_int();
        uploaded_bytes = root.get_as_ref_or_tagged(s_progress_uploaded_bytes_iip).get_as_int();
    }
}


// Overriding member function in realm::sync::ClientHistory
void ClientHistoryImpl::get_cooked_status(version_type server_version, std::int_fast64_t& num_changesets,
                                          CookedProgress& progress, std::int_fast64_t& num_skipped_changesets) const
{
    TransactionRef rt = m_shared_group->start_read(); // Throws
    auto version = rt->get_version();
    const_cast<ClientHistoryImpl*>(this)->set_group(rt.get());
    ensure_updated(version); // Throws

    REALM_ASSERT(m_cooked_history_size <=
                 std::uint_fast64_t(std::numeric_limits<std::int_fast64_t>::max() - m_ch_base_index));
    std::int_fast64_t num_changesets_2 = m_ch_base_index + std::int_fast64_t(m_cooked_history_size);

    CookedProgress progress_2;
    std::int_fast64_t num_skipped_changesets_2 = 0;
    if (m_arrays && m_arrays->cooked_history.is_attached()) {
        REALM_ASSERT(m_ch_server_versions);
        const Array& ch = m_arrays->cooked_history;
        progress_2.changeset_index = m_ch_base_index;
        progress_2.intrachangeset_progress =
            std::int_fast64_t(ch.get_as_ref_or_tagged(s_ch_intrachangeset_progress_iip).get_as_int());

        if (server_version != 0 && server_version != m_ch_base_server_version) {
            if (server_version < m_ch_base_server_version)
                throw BadCookedServerVersion("Server version precedes beginning of cooked history");
            std::size_t i = 0;
            for (;;) {
                if (i == m_cooked_history_size)
                    throw BadCookedServerVersion("Server version not found in cooked history");
                version_type server_version_2 = version_type(m_ch_server_versions->get(i));
                if (server_version_2 == 0)
                    break;
                ++i;
                if (server_version_2 == server_version)
                    break;
            }
            num_skipped_changesets_2 = std::int_fast64_t(i);
            progress_2.changeset_index += num_skipped_changesets_2;
            progress_2.intrachangeset_progress = 0;
        }
    }

    num_changesets = num_changesets_2;
    progress = progress_2;
    num_skipped_changesets = num_skipped_changesets_2;
}


// Overriding member function in realm::sync::ClientHistory
void ClientHistoryImpl::get_cooked_changeset(std::int_fast64_t index, util::AppendBuffer<char>& buffer,
                                             version_type& server_version) const
{
    TransactionRef rt = m_shared_group->start_read(); // Throws
    version_type current_version = rt->get_version();
    const_cast<ClientHistoryImpl*>(this)->set_group(rt.get());
    ensure_updated(current_version);                        // Throws
    do_get_cooked_changeset(index, buffer, server_version); // Throws
}


// Overriding member function in realm::sync::ClientHistory
auto ClientHistoryImpl::set_cooked_progress(CookedProgress progress) -> version_type
{
    TransactionRef wt = m_shared_group->start_write(); // Throws
    auto version = wt->get_version();
    ensure_updated(version); // Throws
    prepare_for_write();     // Throws

    ensure_cooked_history();          // Throws
    update_cooked_progress(progress); // Throws

    // Note: This transaction produces an empty changeset. Empty changesets
    // are not uploaded to the server.
    return wt->commit(); // Throws
}


// Overriding member function in realm::sync::ClientHistory
auto ClientHistoryImpl::get_upload_anchor_of_current_transact(const Transaction& tr) const -> UploadCursor
{
    REALM_ASSERT(tr.get_transact_stage() != DB::transact_Ready);
    version_type current_version = tr.get_version();
    ensure_updated(current_version); // Throws
    UploadCursor upload_anchor;
    upload_anchor.client_version = current_version;
    upload_anchor.last_integrated_server_version = m_progress_download.server_version;
    return upload_anchor;
}

// Overriding member function in realm::sync::ClientHistory
util::StringView ClientHistoryImpl::get_sync_changeset_of_current_transact(const Transaction& tr) const noexcept
{
    REALM_ASSERT(tr.get_transact_stage() == DB::transact_Writing);
    const sync::ChangesetEncoder& encoder = get_instruction_encoder();
    const sync::ChangesetEncoder::Buffer& buffer = encoder.buffer();
    return {buffer.data(), buffer.size()};
}

// Overriding member function in realm::sync::TransformHistory
auto ClientHistoryImpl::find_history_entry(version_type begin_version, version_type end_version,
                                           HistoryEntry& entry) const noexcept -> version_type
{
    version_type last_integrated_server_version;
    return find_sync_history_entry(begin_version, end_version, entry, last_integrated_server_version);
}


// Overriding member function in realm::sync::TransformHistory
ChunkedBinaryData ClientHistoryImpl::get_reciprocal_transform(version_type version) const
{
    REALM_ASSERT(version > m_sync_history_base_version);

    std::size_t index = to_size_t(version - m_sync_history_base_version) - 1;
    REALM_ASSERT(index < m_sync_history_size);

    ChunkedBinaryData reciprocal{*m_reciprocal_transforms, index};
    if (!reciprocal.is_null())
        return reciprocal;
    return ChunkedBinaryData{*m_changesets, index};
}


// Overriding member function in realm::sync::TransformHistory
void ClientHistoryImpl::set_reciprocal_transform(version_type version, BinaryData data)
{
    REALM_ASSERT(version > m_sync_history_base_version);

    std::size_t index = size_t(version - m_sync_history_base_version) - 1;
    REALM_ASSERT(index < m_sync_history_size);

    // FIXME: BinaryColumn::set() currently interprets BinaryData(0,0) as
    // null. It should probably be changed such that BinaryData(0,0) is always
    // interpreted as the empty string. For the purpose of setting null values,
    // BinaryColumn::set() should accept values of type Optional<BinaryData>().
    BinaryData data_2 = (data ? data : BinaryData("", 0));
    m_reciprocal_transforms->set(index, data_2); // Throws
}


auto ClientHistoryImpl::find_sync_history_entry(version_type begin_version, version_type end_version,
                                                HistoryEntry& entry,
                                                version_type& last_integrated_server_version) const noexcept
    -> version_type
{
    REALM_ASSERT(m_changesets);
    REALM_ASSERT(m_origin_file_idents);

    if (begin_version == 0)
        begin_version = s_initial_version + 0;

    REALM_ASSERT(begin_version <= end_version);
    REALM_ASSERT(begin_version >= m_sync_history_base_version);
    REALM_ASSERT(end_version <= m_sync_history_base_version + m_sync_history_size);
    std::size_t n = to_size_t(end_version - begin_version);
    std::size_t offset = to_size_t(begin_version - m_sync_history_base_version);
    for (std::size_t i = 0; i < n; ++i) {
        std::int_fast64_t origin_file_ident = m_origin_file_idents->get(offset + i);
        last_integrated_server_version = version_type(m_remote_versions->get(offset + i));
        bool not_from_server = (origin_file_ident == 0);
        if (not_from_server) {
            ChunkedBinaryData chunked_changeset(*m_changesets, offset + i);
            if (chunked_changeset.size() > 0) {
                entry.origin_file_ident = file_ident_type(origin_file_ident);
                entry.remote_version = last_integrated_server_version;
                entry.origin_timestamp = sync::timestamp_type(m_origin_timestamps->get(offset + i));
                entry.changeset = chunked_changeset;
                return begin_version + i + 1;
            }
        }
    }
    return 0;
}


void ClientHistoryImpl::do_get_cooked_changeset(std::int_fast64_t index, util::AppendBuffer<char>& buffer,
                                                version_type& server_version) const noexcept
{
    REALM_ASSERT(index >= m_ch_base_index);
    std::size_t i = std::size_t(index - m_ch_base_index);

    REALM_ASSERT(i < m_cooked_history_size);
    REALM_ASSERT(m_ch_changesets);
    std::size_t offset = 0;
    do {
        BinaryData chunk = m_ch_changesets->get_at(i, offset);
        buffer.append(chunk.data(), chunk.size()); // Throws
    } while (offset != 0);

    server_version = version_type(m_ch_server_versions->get(i));
}


// sum_of_history_entry_sizes calculates the sum of the changeset sizes of the
// local history entries that produced a version that succeeds `begin_version`
// and precedes `end_version`.
std::uint_fast64_t ClientHistoryImpl::sum_of_history_entry_sizes(version_type begin_version,
                                                                 version_type end_version) const noexcept
{
    if (begin_version >= end_version)
        return 0;

    REALM_ASSERT(m_changesets);
    REALM_ASSERT(m_origin_file_idents);
    REALM_ASSERT(end_version <= m_sync_history_base_version + m_sync_history_size);

    version_type begin_version_2 = begin_version;
    version_type end_version_2 = end_version;
    clamp_sync_version_range(begin_version_2, end_version_2);

    std::uint_fast64_t sum_of_sizes = 0;

    std::size_t n = to_size_t(end_version_2 - begin_version_2);
    std::size_t offset = to_size_t(begin_version_2 - m_sync_history_base_version);
    for (std::size_t i = 0; i < n; ++i) {

        // Only local changesets are considered
        if (m_origin_file_idents->get(offset + i) != 0)
            continue;

        ChunkedBinaryData changeset(*m_changesets, offset + i);
        sum_of_sizes += changeset.size();
    }

    return sum_of_sizes;
}


void ClientHistoryImpl::prepare_for_write()
{
    if (m_arrays) {
        REALM_ASSERT(m_ct_history);
        REALM_ASSERT(m_changesets);
        REALM_ASSERT(m_reciprocal_transforms);
        REALM_ASSERT(m_remote_versions);
        REALM_ASSERT(m_origin_file_idents);
        REALM_ASSERT(m_origin_timestamps);
        REALM_ASSERT(m_arrays->root.size() == s_root_size);
        return;
    }

    REALM_ASSERT(m_ct_history_size == 0);
    REALM_ASSERT(m_sync_history_size == 0);
    REALM_ASSERT(!m_ct_history);
    REALM_ASSERT(!m_changesets);
    REALM_ASSERT(!m_reciprocal_transforms);
    REALM_ASSERT(!m_remote_versions);
    REALM_ASSERT(!m_origin_file_idents);
    REALM_ASSERT(!m_origin_timestamps);
    REALM_ASSERT(m_ch_base_index == 0);
    REALM_ASSERT(m_ch_base_server_version == 0);
    REALM_ASSERT(m_cooked_history_size == 0);
    REALM_ASSERT(!m_ch_changesets);
    REALM_ASSERT(!m_ch_server_versions);
    Allocator& alloc = m_shared_group->get_alloc();
    std::unique_ptr<Arrays> arrays = std::make_unique<Arrays>(alloc); // Throws
    {
        bool context_flag = false;
        std::size_t size = s_root_size;
        arrays->root.create(Array::type_HasRefs, context_flag, size); // Throws
    }
    DeepArrayDestroyGuard dg_1{&arrays->root};
    std::unique_ptr<BinaryColumn> ct_history;
    {
        ct_history = std::make_unique<BinaryColumn>(alloc); // Throws
        ct_history->set_parent(&arrays->root, s_ct_history_iip);
        ct_history->create(); // Throws
    }
    std::unique_ptr<BinaryColumn> changesets;
    {
        changesets = std::make_unique<BinaryColumn>(alloc); // Throws
        changesets->set_parent(&arrays->root, s_changesets_iip);
        changesets->create(); // Throws
    }
    std::unique_ptr<BinaryColumn> reciprocal_transforms;
    {
        reciprocal_transforms = std::make_unique<BinaryColumn>(alloc); // Throws
        reciprocal_transforms->set_parent(&arrays->root, s_reciprocal_transforms_iip);
        reciprocal_transforms->create(); // Throws
    }
    std::unique_ptr<IntegerBpTree> remote_versions;
    {
        remote_versions = std::make_unique<IntegerBpTree>(alloc); // Throws
        remote_versions->set_parent(&arrays->root, s_remote_versions_iip);
        remote_versions->create();
    }
    std::unique_ptr<IntegerBpTree> origin_file_idents;
    {
        origin_file_idents = std::make_unique<IntegerBpTree>(alloc); // Throws
        origin_file_idents->set_parent(&arrays->root, s_origin_file_idents_iip);
        origin_file_idents->create();
    }
    std::unique_ptr<IntegerBpTree> origin_timestamps;
    {
        origin_timestamps = std::make_unique<IntegerBpTree>(alloc); // Throws
        origin_timestamps->set_parent(&arrays->root, s_origin_timestamps_iip);
        origin_timestamps->create();
    }
    { // `schema_versions` table
        Array schema_versions{alloc};
        bool context_flag = false;
        std::size_t size = s_schema_versions_size;
        schema_versions.create(Array::type_HasRefs, context_flag, size); // Throws
        _impl::DeepArrayDestroyGuard adg{&schema_versions};
        { // `sv_schema_versions` column
            MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc);
            ref_type ref = mem.get_ref();
            _impl::DeepArrayRefDestroyGuard ardg{ref, alloc};
            schema_versions.set_as_ref(s_sv_schema_versions_iip, ref); // Throws
            ardg.release();                                            // Ownership transferred to parent array
        }
        { // `sv_library_versions` column
            MemRef mem = Array::create_empty_array(Array::type_HasRefs, context_flag, alloc);
            ref_type ref = mem.get_ref();
            _impl::DeepArrayRefDestroyGuard ardg{ref, alloc};
            schema_versions.set_as_ref(s_sv_library_versions_iip, ref); // Throws
            ardg.release();                                             // Ownership transferred to parent array
        }
        { // `sv_snapshot_versions` column
            MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc);
            ref_type ref = mem.get_ref();
            _impl::DeepArrayRefDestroyGuard ardg{ref, alloc};
            schema_versions.set_as_ref(s_sv_snapshot_versions_iip, ref); // Throws
            ardg.release();                                              // Ownership transferred to parent array
        }
        { // `sv_timestamps` column
            MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc);
            ref_type ref = mem.get_ref();
            _impl::DeepArrayRefDestroyGuard ardg{ref, alloc};
            schema_versions.set_as_ref(s_sv_timestamps_iip, ref); // Throws
            ardg.release();                                       // Ownership transferred to parent array
        }
        version_type snapshot_version = m_shared_group->get_version_of_latest_snapshot();
        record_current_schema_version(schema_versions, snapshot_version);          // Throws
        arrays->root.set_as_ref(s_schema_versions_iip, schema_versions.get_ref()); // Throws
        adg.release(); // Ownership transferred to parent array
    }
    _impl::GroupFriend::prepare_history_parent(*m_group, arrays->root, Replication::hist_SyncClient,
                                               get_client_history_schema_version(), 0); // Throws
    // Note: gf::prepare_history_parent() also ensures the the root array has a
    // slot for the history ref.
    arrays->root.update_parent(); // Throws
    dg_1.release();
    m_arrays = std::move(arrays);
    m_ct_history = std::move(ct_history);
    m_changesets = std::move(changesets);
    m_reciprocal_transforms = std::move(reciprocal_transforms);
    m_remote_versions = std::move(remote_versions);
    m_origin_file_idents = std::move(origin_file_idents);
    m_origin_timestamps = std::move(origin_timestamps);
}


void ClientHistoryImpl::add_ct_history_entry(BinaryData changeset)
{
    REALM_ASSERT(m_ct_history->size() == m_ct_history_size);

    // FIXME: BinaryColumn::set() currently interprets BinaryData(0,0) as
    // null. It should probably be changed such that BinaryData(0,0) is always
    // interpreted as the empty string. For the purpose of setting null values,
    // BinaryColumn::set() should accept values of type Optional<BinaryData>().
    if (changeset.is_null())
        changeset = BinaryData("", 0);
    m_ct_history->add(changeset); // Throws

    ++m_ct_history_size;
}


void ClientHistoryImpl::add_sync_history_entry(HistoryEntry entry)
{
    REALM_ASSERT(m_changesets->size() == m_sync_history_size);
    REALM_ASSERT(m_reciprocal_transforms->size() == m_sync_history_size);
    REALM_ASSERT(m_remote_versions->size() == m_sync_history_size);
    REALM_ASSERT(m_origin_file_idents->size() == m_sync_history_size);
    REALM_ASSERT(m_origin_timestamps->size() == m_sync_history_size);

    // FIXME: BinaryColumn::set() currently interprets BinaryData(0,0) as
    // null. It should probably be changed such that BinaryData(0,0) is always
    // interpreted as the empty string. For the purpose of setting null values,
    // BinaryColumn::set() should accept values of type Optional<BinaryData>().
    BinaryData changeset_2("", 0);
    if (!entry.changeset.is_null()) {
        changeset_2 = entry.changeset.get_first_chunk();
    }
    m_changesets->add(changeset_2); // Throws

    {
        // inserts null
        m_reciprocal_transforms->add(BinaryData{}); // Throws
    }
    m_remote_versions->insert(realm::npos, std::int_fast64_t(entry.remote_version));       // Throws
    m_origin_file_idents->insert(realm::npos, std::int_fast64_t(entry.origin_file_ident)); // Throws
    m_origin_timestamps->insert(realm::npos, std::int_fast64_t(entry.origin_timestamp));   // Throws

    ++m_sync_history_size;
}


void ClientHistoryImpl::update_sync_progress(const SyncProgress& progress,
                                             const std::uint_fast64_t* downloadable_bytes)
{
    Array& root = m_arrays->root;

    // Progress must never decrease
    REALM_ASSERT(progress.latest_server_version.version >=
                 version_type(root.get_as_ref_or_tagged(s_progress_latest_server_version_iip).get_as_int()));
    REALM_ASSERT(progress.download.server_version >=
                 version_type(root.get_as_ref_or_tagged(s_progress_download_server_version_iip).get_as_int()));
    REALM_ASSERT(progress.download.last_integrated_client_version >=
                 version_type(root.get_as_ref_or_tagged(s_progress_download_client_version_iip).get_as_int()));
    REALM_ASSERT(progress.upload.client_version >=
                 version_type(root.get_as_ref_or_tagged(s_progress_upload_client_version_iip).get_as_int()));
    REALM_ASSERT(progress.upload.last_integrated_server_version >=
                 version_type(root.get_as_ref_or_tagged(s_progress_upload_server_version_iip).get_as_int()));

    auto uploaded_bytes = std::uint_fast64_t(root.get_as_ref_or_tagged(s_progress_uploaded_bytes_iip).get_as_int());
    auto previous_upload_client_version =
        version_type(root.get_as_ref_or_tagged(s_progress_upload_client_version_iip).get_as_int());
    uploaded_bytes += sum_of_history_entry_sizes(previous_upload_client_version, progress.upload.client_version);

    root.set(s_progress_download_server_version_iip,
             RefOrTagged::make_tagged(progress.download.server_version)); // Throws
    root.set(s_progress_download_client_version_iip,
             RefOrTagged::make_tagged(progress.download.last_integrated_client_version)); // Throws
    root.set(s_progress_latest_server_version_iip,
             RefOrTagged::make_tagged(progress.latest_server_version.version)); // Throws
    root.set(s_progress_latest_server_version_salt_iip,
             RefOrTagged::make_tagged(progress.latest_server_version.salt)); // Throws
    root.set(s_progress_upload_client_version_iip,
             RefOrTagged::make_tagged(progress.upload.client_version)); // Throws
    root.set(s_progress_upload_server_version_iip,
             RefOrTagged::make_tagged(progress.upload.last_integrated_server_version)); // Throws
    if (downloadable_bytes) {
        root.set(s_progress_downloadable_bytes_iip,
                 RefOrTagged::make_tagged(*downloadable_bytes)); // Throws
    }
    root.set(s_progress_uploaded_bytes_iip,
             RefOrTagged::make_tagged(uploaded_bytes)); // Throws

    m_progress_download = progress.download;

    trim_sync_history(); // Throws
}


void ClientHistoryImpl::trim_ct_history()
{
    REALM_ASSERT(m_ct_history->size() == m_ct_history_size);

    version_type begin = m_ct_history_base_version;
    version_type end = m_version_of_oldest_bound_snapshot;

    // Because `m_version_of_oldest_bound_snapshot` in this history object is
    // only updated by those transactions that occur on behalf of SharedGroup
    // object that is associated with this history object, it can happen that
    // `m_version_of_oldest_bound_snapshot` precedes the beginning of the
    // history, even though that seems nonsensical. In such a case, no trimming
    // can be done yet.
    if (end > begin) {
        std::size_t n = std::size_t(end - begin);

        // The new changeset is always added before set_oldest_bound_version()
        // is called. Therefore, the trimming operation can never leave the
        // history empty.
        REALM_ASSERT(n < m_ct_history_size);

        for (std::size_t i = 0; i < n; ++i) {
            std::size_t j = (n - 1) - i;
            m_ct_history->erase(j);
        }

        m_ct_history_base_version += n;
        m_ct_history_size -= n;

        REALM_ASSERT(m_ct_history->size() == m_ct_history_size);
        REALM_ASSERT(m_ct_history_base_version + m_ct_history_size ==
                     m_sync_history_base_version + m_sync_history_size);
    }
}


// Trimming rules for synchronization history:
//
// Let C be the latest client version that was integrated on the server prior to
// the latest server version currently integrated by the client
// (`m_progress_download.last_integrated_client_version`).
//
// Definition: An *upload skippable history entry* is one whose changeset is
// either empty, or of remote origin.
//
// Then, a history entry, E, can be trimmed away if it preceeds C, or E is
// upload skippable, and there are no upload nonskippable entries between C and
// E.
//
// Since the history representation is contiguous, it is necessary that the
// trimming rule upholds the following invariant:
//
// > If a changeset can be trimmed, then any earlier changeset can also be
// > trimmed.
//
// Note that C corresponds to the earliest possible beginning of the merge
// window for the next incoming changeset from the server.
void ClientHistoryImpl::trim_sync_history()
{
    version_type begin = m_sync_history_base_version;
    version_type end = std::max(m_progress_download.last_integrated_client_version, s_initial_version + 0);
    // Note: At this point, `end` corresponds to C in the description above.

    // `end` (`m_progress_download.last_integrated_client_version`) will precede
    // the beginning of the history, if we trimmed beyond
    // `m_progress_download.last_integrated_client_version` during the previous
    // trimming session. Since new entries, that have now become eligible for
    // scanning, may also be upload skippable, we need to continue the scan from
    // the beginning of the history in that case.
    if (end < begin)
        end = begin;

    // FIXME: It seems like in some cases, a particular history entry that
    // terminates the scan may get examined over and over every time
    // trim_history() is called. For this reason, it seems like it would be
    // worth considering to cache the outcome.

    // FIXME: It seems like there is a significant overlap between what is going
    // on here and in a place like find_uploadable_changesets(). Maybe there is
    // grounds for some refactoring to take that into account, especially, to
    // avoid scanning the same parts of the history for the same information
    // multiple times.

    {
        std::size_t offset = std::size_t(end - begin);
        std::size_t n = std::size_t(m_sync_history_size - offset);
        std::size_t i = 0;
        while (i < n) {
            std::int_fast64_t origin_file_ident = m_origin_file_idents->get(offset + i);
            bool of_local_origin = (origin_file_ident == 0);
            if (of_local_origin) {
                std::size_t pos = 0;
                BinaryData chunk = m_changesets->get_at(offset + i, pos);
                bool nonempty = (chunk.size() > 0);
                if (nonempty)
                    break; // Not upload skippable
            }
            ++i;
        }
        end += i;
    }

    std::size_t n = std::size_t(end - begin);
    do_trim_sync_history(n); // Throws
}


void ClientHistoryImpl::do_trim_sync_history(std::size_t n)
{
    REALM_ASSERT(m_changesets->size() == m_sync_history_size);
    REALM_ASSERT(m_reciprocal_transforms->size() == m_sync_history_size);
    REALM_ASSERT(m_remote_versions->size() == m_sync_history_size);
    REALM_ASSERT(m_origin_file_idents->size() == m_sync_history_size);
    REALM_ASSERT(m_origin_timestamps->size() == m_sync_history_size);
    REALM_ASSERT(n <= m_sync_history_size);

    if (n > 0) {
        for (std::size_t i = 0; i < n; ++i) {
            std::size_t j = (n - 1) - i;
            m_changesets->erase(j); // Throws
        }
        for (std::size_t i = 0; i < n; ++i) {
            std::size_t j = (n - 1) - i;
            m_reciprocal_transforms->erase(j); // Throws
        }
        for (std::size_t i = 0; i < n; ++i) {
            std::size_t j = (n - 1) - i;
            m_remote_versions->erase(j); // Throws
        }
        for (std::size_t i = 0; i < n; ++i) {
            std::size_t j = (n - 1) - i;
            m_origin_file_idents->erase(j); // Throws
        }
        for (std::size_t i = 0; i < n; ++i) {
            std::size_t j = (n - 1) - i;
            m_origin_timestamps->erase(j); // Throws
        }

        m_sync_history_base_version += n;
        m_sync_history_size -= n;
    }
}


void ClientHistoryImpl::ensure_cooked_history()
{
    REALM_ASSERT(m_arrays);

    if (REALM_LIKELY(m_arrays->cooked_history.is_attached())) {
        REALM_ASSERT(m_ch_changesets);
        REALM_ASSERT(m_ch_server_versions);
        return; // Already instantiated
    }

    REALM_ASSERT(!m_ch_changesets);
    REALM_ASSERT(!m_ch_server_versions);
    bool synchronization_has_not_commenced = (m_progress_download.server_version == 0);
    if (REALM_LIKELY(synchronization_has_not_commenced))
        goto instantiate;

    // This special rule is needed because during the migration from schema
    // version 1 to 2, a file, that is being used with a cooker, might have no
    // cooked history yet, so it also won't have immediately after the
    // migration.
    if (REALM_LIKELY(was_migrated_from_schema_version_earlier_than(2)))
        goto instantiate;

    throw sync::InconsistentUseOfCookedHistory("Cannot switch to using a changeset cooker "
                                               "after synchronization has commenced");

instantiate : {
    bool context_flag = false;
    std::size_t size = s_cooked_history_size;
    m_arrays->cooked_history.create(Array::type_HasRefs, context_flag, size); // Throws
    _impl::ShallowArrayDestroyGuard adg{&m_arrays->cooked_history};
    m_arrays->cooked_history.update_parent(); // Throws
    adg.release();                            // Ref ownership transferred to parent array
}

    Allocator& alloc = m_arrays->cooked_history.get_alloc();

    m_ch_changesets = std::make_unique<BinaryColumn>(alloc); // Throws
    m_ch_changesets->set_parent(&m_arrays->cooked_history, s_ch_changesets_iip);
    m_ch_changesets->create();

    m_ch_server_versions = std::make_unique<IntegerBpTree>(alloc); // Throws
    m_ch_server_versions->set_parent(&m_arrays->cooked_history, s_ch_server_versions_iip);
    m_ch_server_versions->create();
}


void ClientHistoryImpl::ensure_no_cooked_history()
{
    REALM_ASSERT(m_arrays);

    if (REALM_LIKELY(!m_arrays->cooked_history.is_attached()))
        return;

    throw sync::InconsistentUseOfCookedHistory("Cannot switch to not using a changeset cooker "
                                               "after synchronization has commenced");
}


void ClientHistoryImpl::save_cooked_changeset(BinaryData changeset, version_type server_version)
{
    REALM_ASSERT(m_arrays);
    REALM_ASSERT(m_arrays->cooked_history.is_attached());
    REALM_ASSERT(m_ch_changesets);
    REALM_ASSERT(m_ch_server_versions);

    m_ch_changesets->add(changeset);                                              // Throws
    m_ch_server_versions->insert(realm::npos, std::int_fast64_t(server_version)); // Throws
    m_cooked_history_size++;
}


void ClientHistoryImpl::update_cooked_progress(CookedProgress progress)
{
    REALM_ASSERT(m_arrays);
    REALM_ASSERT(m_arrays->cooked_history.is_attached());
    REALM_ASSERT(m_ch_changesets);
    REALM_ASSERT(m_ch_server_versions);

    // CookedProgress::changeset_index must never decrease and must never get
    // ahead of the end of the cooked history.
    std::int_fast64_t end = m_ch_base_index + std::int_fast64_t(m_cooked_history_size);
    if (progress.changeset_index < m_ch_base_index || progress.changeset_index > end)
        throw std::runtime_error("Changeset index of cooked progress is out of range");

    // Trim cooked history
    version_type new_base_server_version = 0;
    if (progress.changeset_index > m_ch_base_index) {
        REALM_ASSERT(m_cooked_history_size > 0);
        if (progress.changeset_index >= end) {
            new_base_server_version = version_type(m_ch_server_versions->get(m_cooked_history_size - 1));
            m_ch_changesets->clear();      // Throws
            m_ch_server_versions->clear(); // Throws
            m_cooked_history_size = 0;
        }
        else {
            REALM_ASSERT(m_ch_changesets->size() == m_cooked_history_size);
            REALM_ASSERT(m_ch_server_versions->size() == m_cooked_history_size);
            std::int_fast64_t n = progress.changeset_index - m_ch_base_index;
            REALM_ASSERT(std::uint_fast64_t(n) < std::numeric_limits<std::size_t>::max());
            std::size_t n_2 = to_size_t(n);
            new_base_server_version = version_type(m_ch_server_versions->get(n_2 - 1));
            for (std::size_t i = 0; i < n_2; ++i) {
                std::size_t j = (n_2 - 1) - i;
                m_ch_changesets->erase(j);      // Throws
                m_ch_server_versions->erase(j); // Throws
            }
            m_cooked_history_size -= n_2;
        }
    }

    Array& ch = m_arrays->cooked_history;
    ch.set(s_ch_base_index_iip,
           RefOrTagged::make_tagged(std::uint_fast64_t(progress.changeset_index))); // Throws
    ch.set(s_ch_intrachangeset_progress_iip,
           RefOrTagged::make_tagged(std::uint_fast64_t(progress.intrachangeset_progress))); // Throws
    m_ch_base_index = progress.changeset_index;

    // At this point, `new_base_server_version` can be zero either because no
    // trimming was done, or because the last trimmed-away entry was produced
    // before migration to history schema version 2.
    if (new_base_server_version != 0) {
        REALM_ASSERT(new_base_server_version > m_ch_base_server_version);
        ch.set(s_ch_base_server_version_iip,
               RefOrTagged::make_tagged(std::uint_fast64_t(new_base_server_version))); // Throws
        m_ch_base_server_version = new_base_server_version;
    }
}

void ClientHistoryImpl::fix_up_client_file_ident_in_stored_changesets(Transaction& group, TableInfoCache&,
                                                                      file_ident_type client_file_ident)
{
    // Must be in write transaction!

    REALM_ASSERT(client_file_ident != 0);
    using Instruction = realm::sync::Instruction;

    auto promote_global_key = [&](GlobalKey& oid) {
        REALM_ASSERT(oid.hi() == 0); // client_file_ident == 0
        oid = GlobalKey{uint64_t(client_file_ident), oid.lo()};
    };

    auto promote_primary_key = [&](Instruction::PrimaryKey& pk) {
        mpark::visit(util::overloaded{[&](GlobalKey& key) {
                                          promote_global_key(key);
                                      },
                                      [](auto&&) {}},
                     pk);
    };

    auto get_table_for_class = [&](StringData class_name) -> ConstTableRef {
        REALM_ASSERT(class_name.size() < Group::max_table_name_length - 6);
        sync::TableNameBuffer buffer;
        return group.get_table(sync::class_name_to_table_name(class_name, buffer));
    };

    // Fix up changesets. We know that all of these are of our own creation.
    size_t uploadable_bytes = 0;
    for (size_t i = 0; i < m_changesets->size(); ++i) {
        // FIXME: We have to do this when transmitting/receiving changesets
        // over the network instead.
        ChunkedBinaryData changeset{*m_changesets, i};
        ChunkedBinaryInputStream in{changeset};
        sync::Changeset log;
        sync::parse_changeset(in, log);

        auto last_class_name = sync::InternString::npos;
        ConstTableRef selected_table;
        for (auto instr : log) {
            if (!instr)
                continue;

            if (auto obj_instr = instr->get_if<Instruction::ObjectInstruction>()) {
                // Cache the TableRef
                if (obj_instr->table != last_class_name) {
                    StringData class_name = log.get_string(obj_instr->table);
                    last_class_name = obj_instr->table;
                    selected_table = get_table_for_class(class_name);
                }

                // Fix up instructions using GlobalKey to identify objects.
                promote_primary_key(obj_instr->object);

                // Fix up the payload for Set and ArrayInsert.
                Instruction::Payload* payload = nullptr;
                if (auto set_instr = instr->get_if<Instruction::Set>()) {
                    payload = &set_instr->value;
                }
                else if (auto list_insert_instr = instr->get_if<Instruction::ArrayInsert>()) {
                    payload = &list_insert_instr->value;
                }

                if (payload && payload->type == Instruction::Payload::Type::Link) {
                    promote_primary_key(payload->data.link.target);
                }
            }
        }

        util::AppendBuffer<char> modified;
        encode_changeset(log, modified);
        BinaryData result = BinaryData{modified.data(), modified.size()};
        m_changesets->set(i, result);
        uploadable_bytes += modified.size();
    }

    Array& root = m_arrays->root;
    root.set(s_progress_uploadable_bytes_iip, RefOrTagged::make_tagged(uploadable_bytes));
}

// Overriding member function in realm::sync::ClientHistory
void ClientHistoryImpl::set_group(Group* group, bool updated)
{
    _impl::History::set_group(group, updated);
    if (m_arrays)
        _impl::GroupFriend::set_history_parent(*m_group, m_arrays->root);
}

void ClientHistoryImpl::migrate_from_history_schema_version_1_to_2(int orig_schema_version)
{
    // FIXME: Make it clear in the documentation of
    // Replication::upgrade_history_schema(), that it is called in the context
    // of a write transaction.
    using gf = _impl::GroupFriend;
    Allocator& alloc = gf::get_alloc(*m_group);
    auto root_ref = gf::get_history_ref(*m_group);
    REALM_ASSERT(root_ref != 0);
    Array root{alloc};
    gf::set_history_parent(*m_group, root);
    root.init_from_ref(root_ref);

    // Introduce new slot `progress_upload_server_version` into the history root
    // array (in place of obsolete slot).
    {
        // Sizes of fixed-size arrays
        std::size_t root_size = 23;

        // Slots in root array of history compartment
        std::size_t remote_versions_iip = 2;
        std::size_t progress_upload_client_version_iip = 9;
        std::size_t progress_upload_server_version_iip = 10;

        if (root.size() != root_size)
            throw std::runtime_error{"Unexpected size of history root array"};

        IntegerBpTree remote_versions{alloc};
        remote_versions.set_parent(&root, remote_versions_iip);
        remote_versions.init_from_parent(); // Throws
        version_type current_version = m_shared_group->get_version_of_latest_snapshot();
        std::size_t sync_history_size = remote_versions.size();
        version_type sync_history_base_version = version_type(current_version - sync_history_size);
        version_type progress_upload_client_version =
            version_type(root.get_as_ref_or_tagged(progress_upload_client_version_iip).get_as_int());
        version_type progress_upload_server_version;
        if (progress_upload_client_version > sync_history_base_version) {
            // If `progress_upload_client_version` is greater than the base
            // version of the history, then set `progress_upload_server_version`
            // to `remote_version` of the history entry that produced
            // `progress_upload_client_version`.
            std::size_t i = std::size_t(progress_upload_client_version - sync_history_base_version - 1);
            progress_upload_server_version = version_type(remote_versions.get(i));
        }
        else {
            // Otherwise, we unfortunately don't have the information we
            // need. Setting `progress_upload_server_version` to zero works for
            // now, as the information is not used for anything critical yet.
            progress_upload_server_version = 0;
        }
        root.set(progress_upload_server_version_iip,
                 RefOrTagged::make_tagged(progress_upload_server_version)); // Throws
    }

    // Reorder slots in the history root array
    {
        // Sizes of fixed-size arrays
        const std::size_t root_size = 23;

        // Slots in root array of history compartment
        std::size_t changesets_iip = 0;
        std::size_t reciprocal_transforms_iip = 1;
        std::size_t remote_versions_iip = 2;
        std::size_t origin_file_idents_iip = 3;
        std::size_t origin_timestamps_iip = 4;
        std::size_t progress_download_server_version_iip = 5;
        std::size_t progress_download_client_version_iip = 6;
        std::size_t progress_latest_server_version_iip = 7;
        std::size_t progress_latest_server_version_salt_iip = 8;
        std::size_t progress_upload_client_version_iip = 9;
        std::size_t progress_upload_server_version_iip = 10;
        std::size_t client_file_ident_iip = 11;
        std::size_t client_file_ident_salt_iip = 12;
        std::size_t timestamp_threshold_iip = 13;
        std::size_t progress_downloaded_bytes_iip = 14;
        std::size_t progress_downloadable_bytes_iip = 15;
        std::size_t progress_uploaded_bytes_iip = 16;
        std::size_t progress_uploadable_bytes_iip = 17;
        std::size_t cooked_changesets_iip = 18;
        std::size_t cooked_base_index_iip = 19;
        std::size_t cooked_intrachangeset_progress_iip = 20;
        std::size_t ct_history_iip = 21;
        std::size_t object_id_history_state_iip = 22;

        if (root.size() != root_size)
            throw std::runtime_error{"Unexpected size of history root array"};

        std::size_t new_order[root_size] = {
            ct_history_iip,
            client_file_ident_iip,
            client_file_ident_salt_iip,
            progress_latest_server_version_iip,
            progress_latest_server_version_salt_iip,
            progress_download_server_version_iip,
            progress_download_client_version_iip,
            progress_upload_client_version_iip,
            progress_upload_server_version_iip,
            progress_downloaded_bytes_iip,
            progress_downloadable_bytes_iip,
            progress_uploaded_bytes_iip,
            progress_uploadable_bytes_iip,
            changesets_iip,
            reciprocal_transforms_iip,
            remote_versions_iip,
            origin_file_idents_iip,
            origin_timestamps_iip,
            object_id_history_state_iip,
            cooked_base_index_iip,
            cooked_intrachangeset_progress_iip,
            cooked_changesets_iip,
            timestamp_threshold_iip,
        };

        // Decompose (inverse) permuation into (inverse) cycles
        bool seen[root_size] = {};
        for (std::size_t i = 0; i < root_size; ++i) {
            std::size_t index_1 = i;
            if (seen[index_1])
                continue;
            seen[index_1] = true;
            std::size_t index_2 = new_order[index_1];
            if (seen[index_2])
                continue; // Skip cycles of length 1
            seen[index_2] = true;
            std::int_fast64_t value_1 = root.get(index_1);
            for (;;) {
                std::int_fast64_t value_2 = root.get(index_2);
                root.set(index_1, value_2); // Throws
                index_1 = index_2;
                index_2 = new_order[index_1];
                if (seen[index_2])
                    break;
                seen[index_2] = true;
            }
            root.set(index_1, value_1); // Throws
        }
    }

    // Add schema versions table
    {
        // Sizes of fixed-size arrays
        std::size_t root_size = 24;
        std::size_t schema_versions_size = 4;

        // Slots in root array of history compartment
        std::size_t schema_versions_iip = 23;

        // Slots in root array of `schema_versions` table
        std::size_t sv_schema_versions_iip = 0;
        std::size_t sv_library_versions_iip = 1;
        std::size_t sv_snapshot_versions_iip = 2;
        std::size_t sv_timestamps_iip = 3;

        root.add(0); // Throws
        if (root.size() != root_size)
            throw std::runtime_error{"Unexpected size of history root array"};

        Array schema_versions{alloc};
        bool context_flag = false;
        std::size_t size = schema_versions_size;
        schema_versions.create(Array::type_HasRefs, context_flag, size); // Throws
        _impl::DeepArrayDestroyGuard adg{&schema_versions};
        { // sv_schema_versions
            Array sv_schema_versions{alloc};
            sv_schema_versions.create(Array::type_Normal);
            _impl::ShallowArrayDestroyGuard adg_2{&sv_schema_versions};
            std::int_fast64_t value = orig_schema_version;
            sv_schema_versions.add(value); // Throws
            ref_type ref = sv_schema_versions.get_ref();
            schema_versions.set_as_ref(sv_schema_versions_iip, ref); // Throws
            adg_2.release();                                         // Ownership transferred to parent array
        }
        { // sv_library_versions
            Array sv_library_versions{alloc};
            sv_library_versions.create(Array::type_HasRefs);
            _impl::ShallowArrayDestroyGuard adg_2{&sv_library_versions};
            // NOTE: Storing a null ref in place of the library version
            // indicates that this entry was created as part of the migration
            // from schema version 1 to schema version 2. For such an entry, the
            // library version, snapshot number, and timestamp are unknown, and
            // the values stored for `snapshot_version` and `timestamp` has no
            // meaning.
            std::int_fast64_t value = 0;    // Null ref
            sv_library_versions.add(value); // Throws
            ref_type ref = sv_library_versions.get_ref();
            schema_versions.set_as_ref(sv_library_versions_iip, ref); // Throws
            adg_2.release();                                          // Ownership transferred to parent array
        }
        { // sv_snapshot_versions
            Array sv_snapshot_versions{alloc};
            sv_snapshot_versions.create(Array::type_Normal);
            _impl::ShallowArrayDestroyGuard adg_2{&sv_snapshot_versions};
            std::int_fast64_t value = 0;     // Dummy
            sv_snapshot_versions.add(value); // Throws
            ref_type ref = sv_snapshot_versions.get_ref();
            schema_versions.set_as_ref(sv_snapshot_versions_iip, ref); // Throws
            adg_2.release();                                           // Ownership transferred to parent array
        }
        { // sv_timestamps
            Array sv_timestamps{alloc};
            sv_timestamps.create(Array::type_Normal);
            _impl::ShallowArrayDestroyGuard adg_2{&sv_timestamps};
            std::int_fast64_t value = 0; // Dummy
            sv_timestamps.add(value);    // Throws
            ref_type ref = sv_timestamps.get_ref();
            schema_versions.set_as_ref(sv_timestamps_iip, ref); // Throws
            adg_2.release();                                    // Ownership transferred to parent array
        }
        root.set_as_ref(schema_versions_iip, schema_versions.get_ref()); // Throws
        adg.release();                                                   // Ownership transferred to parent array
    }

    // Move cooked history stuff to subarray
    {
        // Sizes of fixed-size arrays
        std::size_t old_root_size = 24;
        std::size_t new_root_size = 21;
        std::size_t cooked_history_size = 4;

        // Slots in root array of history compartment
        std::size_t cooked_history_iip = 19;                 // Being created
        std::size_t cooked_base_index_iip = 19;              // Being removed
        std::size_t cooked_intrachangeset_progress_iip = 20; // Being removed
        std::size_t cooked_changesets_iip = 21;              // Being removed

        // Slots in root array of `cooked_history` substructure
        std::size_t ch_base_index_iip = 0;
        std::size_t ch_intrachangeset_progress_iip = 1;
        std::size_t ch_changesets_iip = 2;

        if (root.size() != old_root_size)
            throw std::runtime_error{"Unexpected size of history root array"};

        std::int_fast64_t base_index =
            std::int_fast64_t(root.get_as_ref_or_tagged(cooked_base_index_iip).get_as_int());
        std::int_fast64_t intrachangeset_progress =
            std::int_fast64_t(root.get_as_ref_or_tagged(cooked_intrachangeset_progress_iip).get_as_int());
        ref_type changesets_ref = root.get_as_ref(cooked_changesets_iip);

        // The 4 old slots of the root array are located at indexes 19 through
        // 22. Remove three of them, and nullify the remaining one
        {
            std::size_t begin = 19;
            std::size_t end = 19 + 3;
            root.erase(begin, end); // Throws
        }
        root.set(19, 0); // Throws

        DeepArrayRefDestroyGuard changesets_adg{changesets_ref, alloc};

        bool instantiate = (base_index != 0 || intrachangeset_progress != 0 || changesets_ref != 0);
        if (instantiate) {
            if (changesets_ref == 0) {
                BinaryColumn tmp(alloc);
                tmp.create();
                changesets_ref = tmp.get_ref();
                changesets_adg.reset(changesets_ref);
            }
            Array cooked_history{alloc};
            bool context_flag = false;
            std::size_t size = cooked_history_size;
            cooked_history.create(Array::type_HasRefs, context_flag, size); // Throws
            _impl::ShallowArrayDestroyGuard adg{&cooked_history};
            cooked_history.set(ch_base_index_iip, RefOrTagged::make_tagged(base_index)); // Throws
            cooked_history.set(ch_intrachangeset_progress_iip,
                               RefOrTagged::make_tagged(intrachangeset_progress)); // Throws
            cooked_history.set_as_ref(ch_changesets_iip, changesets_ref);          // Throws
            changesets_adg.release();
            root.set_as_ref(cooked_history_iip, cooked_history.get_ref()); // Throws
            adg.release();
        }

        if (root.size() != new_root_size)
            throw std::runtime_error{"Unexpected size of history root array"};
    }

    // Add slots `base_server_version` and `server_versions` to `cooked_history`
    // array
    {
        // Sizes of fixed-size arrays
        std::size_t root_size = 21;
        std::size_t cooked_history_size = 5;

        // Slots in root array of history compartment
        std::size_t cooked_history_iip = 19;

        // Slots in root array of `cooked_history` substructure
        std::size_t ch_base_server_version_iip = 2;
        std::size_t ch_changesets_iip = 3;
        std::size_t ch_server_versions_iip = 4;

        if (root.size() != root_size)
            throw std::runtime_error{"Unexpected size of history root array"};

        ref_type ref = root.get_as_ref(cooked_history_iip);
        if (ref != 0) {
            Array cooked_history{alloc};
            cooked_history.init_from_ref(ref);
            cooked_history.set_parent(&root, cooked_history_iip);

            cooked_history.insert(ch_base_server_version_iip, 0);
            if (cooked_history.size() != cooked_history_size)
                throw std::runtime_error{"Unexpected size of `cooked_history` array"};

            version_type server_version =
                version_type(root.get_as_ref_or_tagged(s_progress_download_server_version_iip).get_as_int());
            cooked_history.set(ch_base_server_version_iip,
                               RefOrTagged::make_tagged(std::uint_fast64_t(server_version))); // Throws

            std::size_t cooked_history_size;
            {
                ref_type ref_2 = cooked_history.get_as_ref(ch_changesets_iip);
                BinaryColumn ch_changesets{alloc}; // Throws
                ch_changesets.init_from_ref(ref_2);
                cooked_history_size = ch_changesets.size();
            }
            IntegerBpTree ch_server_versions{alloc}; // Throws
            ch_server_versions.set_parent(&cooked_history, ch_server_versions_iip);
            ch_server_versions.create();
            for (std::size_t i = 0; i < cooked_history_size; ++i) {
                std::int_fast64_t value = 0;                   // Means "unknown"
                ch_server_versions.insert(realm::npos, value); // Throws
            }
        }
    }
}


void ClientHistoryImpl::record_current_schema_version()
{
    using gf = _impl::GroupFriend;
    Allocator& alloc = gf::get_alloc(*m_group);
    auto ref = gf::get_history_ref(*m_group);
    REALM_ASSERT(ref != 0);
    Array root{alloc};
    gf::set_history_parent(*m_group, root);
    root.init_from_ref(ref);
    Array schema_versions{alloc};
    schema_versions.set_parent(&root, s_schema_versions_iip);
    schema_versions.init_from_parent();
    version_type snapshot_version = m_shared_group->get_version_of_latest_snapshot();
    record_current_schema_version(schema_versions, snapshot_version); // Throws
}


void ClientHistoryImpl::record_current_schema_version(Array& schema_versions, version_type snapshot_version)
{
    static_assert(s_schema_versions_size == 4, "");
    REALM_ASSERT(schema_versions.size() == s_schema_versions_size);

    Allocator& alloc = schema_versions.get_alloc();
    {
        Array sv_schema_versions{alloc};
        sv_schema_versions.set_parent(&schema_versions, s_sv_schema_versions_iip);
        sv_schema_versions.init_from_parent();
        int schema_version = get_client_history_schema_version();
        sv_schema_versions.add(schema_version); // Throws
    }
    {
        Array sv_library_versions{alloc};
        sv_library_versions.set_parent(&schema_versions, s_sv_library_versions_iip);
        sv_library_versions.init_from_parent();
        const char* library_version = REALM_SYNC_VER_STRING;
        std::size_t size = std::strlen(library_version);
        Array value{alloc};
        bool context_flag = false;
        value.create(Array::type_Normal, context_flag, size); // Throws
        _impl::ShallowArrayDestroyGuard adg{&value};
        using uchar = unsigned char;
        for (std::size_t i = 0; i < size; ++i)
            value.set(i, std::int_fast64_t(uchar(library_version[i]))); // Throws
        sv_library_versions.add(std::int_fast64_t(value.get_ref()));    // Throws
        adg.release();                                                  // Ownership transferred to parent array
    }
    {
        Array sv_snapshot_versions{alloc};
        sv_snapshot_versions.set_parent(&schema_versions, s_sv_snapshot_versions_iip);
        sv_snapshot_versions.init_from_parent();
        sv_snapshot_versions.add(std::int_fast64_t(snapshot_version)); // Throws
    }
    {
        Array sv_timestamps{alloc};
        sv_timestamps.set_parent(&schema_versions, s_sv_timestamps_iip);
        sv_timestamps.init_from_parent();
        std::time_t timestamp = std::time(nullptr);
        sv_timestamps.add(std::int_fast64_t(timestamp)); // Throws
    }
}

void ClientHistoryImpl::migrate_from_history_schema_version_2_to_10()
{
    using gf = _impl::GroupFriend;
    Allocator& alloc = gf::get_alloc(*m_group);
    auto ref = gf::get_history_ref(*m_group);
    Array top(alloc);
    gf::set_history_parent(*m_group, top);
    top.init_from_ref(ref);
    size_t top_client_file_ident = 1; // s_top_client_file_ident was 1 in version 2
    uint64_t file_ident = uint64_t(top.get_as_ref_or_tagged(top_client_file_ident).get_as_int());
    m_group->set_sync_file_id(file_ident);

    _impl::ObjectIDHistoryState object_id_history(alloc);
    size_t top_object_id_history_state = 18; // s_top_object_id_history_state was 18 in version 2
    object_id_history.set_parent(&top, top_object_id_history_state);
    object_id_history.upgrade(m_group);
}

bool ClientHistoryImpl::was_migrated_from_schema_version_earlier_than(int schema_version) const noexcept
{
    REALM_ASSERT(m_arrays);
    Array& root = m_arrays->root;
    Allocator& alloc = root.get_alloc();
    Array schema_versions{alloc};
    schema_versions.set_parent(&root, s_schema_versions_iip);
    schema_versions.init_from_parent();
    Array sv_schema_versions{alloc};
    sv_schema_versions.set_parent(&schema_versions, s_sv_schema_versions_iip);
    sv_schema_versions.init_from_parent();
    std::size_t n = sv_schema_versions.size();
    for (std::size_t i = 0; i < n; ++i) {
        std::int_fast64_t schema_version_2 = sv_schema_versions.get(i);
        if (schema_version_2 < schema_version)
            return true;
    }
    return false;
}


// Overriding member function in realm::_impl::History
void ClientHistoryImpl::update_from_ref_and_version(ref_type ref, version_type version)
{
    using gf = _impl::GroupFriend;
    if (ref == 0) {
        // No history
        m_ct_history_base_version = version;
        m_ct_history_size = 0;
        m_sync_history_base_version = version;
        m_sync_history_size = 0;
        m_arrays.reset();
        m_ct_history.reset();
        m_changesets.reset();
        m_reciprocal_transforms.reset();
        m_remote_versions.reset();
        m_origin_file_idents.reset();
        m_origin_timestamps.reset();
        m_progress_download = {0, 0};
        m_ch_base_index = 0;
        m_ch_base_server_version = 0;
        m_cooked_history_size = 0;
        m_ch_changesets.reset();
        m_ch_server_versions.reset();
        return;
    }
    if (REALM_LIKELY(m_arrays)) {
        Array& root = m_arrays->root;
        REALM_ASSERT(m_ct_history);
        REALM_ASSERT(m_changesets);
        REALM_ASSERT(m_reciprocal_transforms);
        REALM_ASSERT(m_remote_versions);
        REALM_ASSERT(m_origin_file_idents);
        REALM_ASSERT(m_origin_timestamps);
        root.init_from_ref(ref);
        REALM_ASSERT(m_arrays->root.size() == s_root_size);
        {
            ref_type ref_2 = root.get_as_ref(s_ct_history_iip);
            m_ct_history->init_from_ref(ref_2); // Throws
        }
        {
            ref_type ref_2 = root.get_as_ref(s_changesets_iip);
            m_changesets->init_from_ref(ref_2); // Throws
        }
        {
            ref_type ref_2 = root.get_as_ref(s_reciprocal_transforms_iip);
            m_reciprocal_transforms->init_from_ref(ref_2); // Throws
        }
        m_remote_versions->init_from_parent();    // Throws
        m_origin_file_idents->init_from_parent(); // Throws
        m_origin_timestamps->init_from_parent();  // Throws
    }
    else {
        REALM_ASSERT(!m_ct_history);
        REALM_ASSERT(!m_changesets);
        REALM_ASSERT(!m_reciprocal_transforms);
        REALM_ASSERT(!m_remote_versions);
        REALM_ASSERT(!m_origin_file_idents);
        REALM_ASSERT(!m_origin_timestamps);
        REALM_ASSERT(!m_ch_changesets);
        REALM_ASSERT(!m_ch_server_versions);
        Allocator& alloc = m_shared_group->get_alloc();
        std::unique_ptr<Arrays> arrays(new Arrays(alloc)); // Throws
        arrays->root.init_from_ref(ref);
        if (m_group)
            gf::set_history_parent(*m_group, arrays->root);
        std::unique_ptr<BinaryColumn> ct_history;
        {
            ct_history.reset(new BinaryColumn(alloc)); // Throws
            ct_history->set_parent(&arrays->root, s_ct_history_iip);
            ct_history->init_from_parent();
        }
        std::unique_ptr<BinaryColumn> changesets;
        {
            changesets.reset(new BinaryColumn(alloc)); // Throws
            changesets->set_parent(&arrays->root, s_changesets_iip);
            changesets->init_from_parent(); // Throws
        }
        std::unique_ptr<BinaryColumn> reciprocal_transforms;
        {
            reciprocal_transforms.reset(new BinaryColumn(alloc)); // Throws
            reciprocal_transforms->set_parent(&arrays->root, s_reciprocal_transforms_iip);
            reciprocal_transforms->init_from_parent();
        }
        std::unique_ptr<IntegerBpTree> remote_versions;
        {
            remote_versions.reset(new IntegerBpTree(alloc)); // Throws
            remote_versions->set_parent(&arrays->root, s_remote_versions_iip);
            remote_versions->init_from_parent();
        }
        std::unique_ptr<IntegerBpTree> origin_file_idents;
        {
            origin_file_idents.reset(new IntegerBpTree(alloc)); // Throws
            origin_file_idents->set_parent(&arrays->root, s_origin_file_idents_iip);
            origin_file_idents->init_from_parent();
        }
        std::unique_ptr<IntegerBpTree> origin_timestamps;
        {
            origin_timestamps.reset(new IntegerBpTree(alloc)); // Throws
            origin_timestamps->set_parent(&arrays->root, s_origin_timestamps_iip);
            origin_timestamps->init_from_parent();
        }
        m_arrays = std::move(arrays);
        m_ct_history = std::move(ct_history);
        m_changesets = std::move(changesets);
        m_reciprocal_transforms = std::move(reciprocal_transforms);
        m_remote_versions = std::move(remote_versions);
        m_origin_file_idents = std::move(origin_file_idents);
        m_origin_timestamps = std::move(origin_timestamps);
    }

    m_ct_history_size = m_ct_history->size();
    m_ct_history_base_version = version - m_ct_history_size;

    m_sync_history_size = m_changesets->size();
    m_sync_history_base_version = version - m_sync_history_size;
    REALM_ASSERT(m_reciprocal_transforms->size() == m_sync_history_size);
    REALM_ASSERT(m_remote_versions->size() == m_sync_history_size);
    REALM_ASSERT(m_origin_file_idents->size() == m_sync_history_size);
    REALM_ASSERT(m_origin_timestamps->size() == m_sync_history_size);

    const Array& root = m_arrays->root;
    m_progress_download.server_version =
        version_type(root.get_as_ref_or_tagged(s_progress_download_server_version_iip).get_as_int());
    m_progress_download.last_integrated_client_version =
        version_type(root.get_as_ref_or_tagged(s_progress_download_client_version_iip).get_as_int());

    ref_type cooked_history_ref = m_arrays->cooked_history.get_ref_from_parent();
    if (cooked_history_ref != 0) {
        m_arrays->cooked_history.init_from_ref(cooked_history_ref);
        Allocator& alloc = m_arrays->cooked_history.get_alloc();
        {
            ref_type ref_2 = m_arrays->cooked_history.get_as_ref(s_ch_changesets_iip);
            if (m_ch_changesets) {
                m_ch_changesets->init_from_ref(ref_2); // Throws
            }
            else {
                m_ch_changesets = std::make_unique<BinaryColumn>(alloc); // Throws
                m_ch_changesets->set_parent(&m_arrays->cooked_history, s_ch_changesets_iip);
                m_ch_changesets->init_from_ref(ref_2);
            }
        }
        {
            ref_type ref_2 = m_arrays->cooked_history.get_as_ref(s_ch_server_versions_iip);
            if (m_ch_server_versions) {
                m_ch_server_versions->init_from_ref(ref_2); // Throws
            }
            else {
                m_ch_server_versions = std::make_unique<IntegerBpTree>(alloc); // Throws
                m_ch_server_versions->set_parent(&m_arrays->cooked_history, s_ch_server_versions_iip);
                m_ch_server_versions->init_from_ref(ref_2);
            }
        }
        m_ch_base_index =
            std::int_fast64_t(m_arrays->cooked_history.get_as_ref_or_tagged(s_ch_base_index_iip).get_as_int());
        m_ch_base_server_version =
            version_type(m_arrays->cooked_history.get_as_ref_or_tagged(s_ch_base_server_version_iip).get_as_int());
        m_cooked_history_size = m_ch_changesets->size();
    }
    else {
        m_ch_base_index = 0;
        m_ch_base_server_version = 0;
        m_cooked_history_size = 0;
        m_arrays->cooked_history.detach();
        m_ch_changesets.reset();
        m_ch_server_versions.reset();
    }
}


// Overriding member function in realm::_impl::History
void ClientHistoryImpl::update_from_parent(version_type current_version)
{
    using gf = GroupFriend;
    ref_type ref = gf::get_history_ref(*m_group);
    update_from_ref_and_version(ref, current_version); // Throws
}


// Overriding member function in realm::_impl::History
void ClientHistoryImpl::get_changesets(version_type begin_version, version_type end_version,
                                       BinaryIterator* iterators) const noexcept
{
    REALM_ASSERT(begin_version <= end_version);
    REALM_ASSERT(begin_version >= m_ct_history_base_version);
    REALM_ASSERT(end_version <= m_ct_history_base_version + m_ct_history_size);
    std::size_t n = to_size_t(end_version - begin_version);
    REALM_ASSERT(n == 0 || m_changesets);
    std::size_t offset = to_size_t(begin_version - m_ct_history_base_version);
    for (std::size_t i = 0; i < n; ++i)
        iterators[i] = BinaryIterator(m_ct_history.get(), offset + i);
}


// Overriding member function in realm::_impl::History
void ClientHistoryImpl::set_oldest_bound_version(version_type version)
{
    REALM_ASSERT(version >= m_version_of_oldest_bound_snapshot);
    if (version > m_version_of_oldest_bound_snapshot) {
        m_version_of_oldest_bound_snapshot = version;
        trim_ct_history(); // Throws
    }
}

// Overriding member function in realm::_impl::History
BinaryData ClientHistoryImpl::get_uncommitted_changes() const noexcept
{
    return TrivialReplication::get_uncommitted_changes();
}


// Overriding member function in realm::_impl::History
void ClientHistoryImpl::verify() const
{
#ifdef REALM_DEBUG
    // The size of the continuous transactions history can only be zero when the
    // Realm is in the initial empty state where top-ref is null.
    REALM_ASSERT(m_ct_history_size != 0 || m_ct_history_base_version == s_initial_version + 0);

    if (!m_arrays) {
        REALM_ASSERT(!m_ct_history);
        REALM_ASSERT(!m_changesets);
        REALM_ASSERT(!m_reciprocal_transforms);
        REALM_ASSERT(!m_remote_versions);
        REALM_ASSERT(!m_origin_file_idents);
        REALM_ASSERT(!m_origin_timestamps);
        REALM_ASSERT(m_ct_history_size == 0);
        REALM_ASSERT(m_sync_history_size == 0);
        REALM_ASSERT(m_progress_download.server_version == 0);
        REALM_ASSERT(m_progress_download.last_integrated_client_version == 0);
        REALM_ASSERT(m_ch_base_index == 0);
        REALM_ASSERT(m_ch_base_server_version == 0);
        REALM_ASSERT(m_cooked_history_size == 0);
        REALM_ASSERT(!m_ch_changesets);
        REALM_ASSERT(!m_ch_server_versions);
        return;
    }
    const Array& root = m_arrays->root;
    REALM_ASSERT(m_ct_history);
    REALM_ASSERT(m_changesets);
    REALM_ASSERT(m_reciprocal_transforms);
    REALM_ASSERT(m_remote_versions);
    REALM_ASSERT(m_origin_file_idents);
    REALM_ASSERT(m_origin_timestamps);
    root.verify();
    m_ct_history->verify();
    m_changesets->verify();
    m_reciprocal_transforms->verify();
    m_remote_versions->verify();
    m_origin_file_idents->verify();
    m_origin_timestamps->verify();
    REALM_ASSERT(m_arrays->root.size() == s_root_size);
    REALM_ASSERT(m_ct_history->size() == m_ct_history_size);
    REALM_ASSERT(m_changesets->size() == m_sync_history_size);
    REALM_ASSERT(m_reciprocal_transforms->size() == m_sync_history_size);
    REALM_ASSERT(m_remote_versions->size() == m_sync_history_size);
    REALM_ASSERT(m_origin_file_idents->size() == m_sync_history_size);
    REALM_ASSERT(m_origin_timestamps->size() == m_sync_history_size);

    version_type progress_download_server_version =
        version_type(root.get_as_ref_or_tagged(s_progress_download_server_version_iip).get_as_int());
    version_type progress_download_client_version =
        version_type(root.get_as_ref_or_tagged(s_progress_download_client_version_iip).get_as_int());
    REALM_ASSERT(progress_download_server_version == m_progress_download.server_version);
    REALM_ASSERT(progress_download_client_version == m_progress_download.last_integrated_client_version);
    REALM_ASSERT(progress_download_client_version <= m_sync_history_base_version + m_sync_history_size);
    version_type remote_version_of_last_entry = 0;
    if (m_sync_history_size > 0)
        remote_version_of_last_entry = m_remote_versions->get(m_sync_history_size - 1);
    REALM_ASSERT(progress_download_server_version >= remote_version_of_last_entry);

    if (!m_arrays->cooked_history.is_attached()) {
        REALM_ASSERT(!m_ch_changesets);
        REALM_ASSERT(!m_ch_server_versions);
        REALM_ASSERT(m_ch_base_index == 0);
        REALM_ASSERT(m_ch_base_server_version == 0);
        REALM_ASSERT(m_cooked_history_size == 0);
    }
    else {
        REALM_ASSERT(m_ch_changesets);
        REALM_ASSERT(m_ch_server_versions);
        m_arrays->cooked_history.verify();
        m_ch_changesets->verify();
        REALM_ASSERT(
            m_ch_base_index ==
            std::int_fast64_t(m_arrays->cooked_history.get_as_ref_or_tagged(s_ch_base_index_iip).get_as_int()));
        REALM_ASSERT(
            m_ch_base_server_version ==
            version_type(m_arrays->cooked_history.get_as_ref_or_tagged(s_ch_base_server_version_iip).get_as_int()));
        REALM_ASSERT(m_ch_changesets->size() == m_cooked_history_size);
        version_type prev_server_version = m_ch_base_server_version;
        for (std::size_t i = 0; i < m_cooked_history_size; ++i) {
            version_type server_version = version_type(m_ch_server_versions->get(i));
            // `server_version` can be zero, but only if the file was migrated
            // from history schema version 1
            if (server_version != 0) {
                REALM_ASSERT(server_version > prev_server_version);
                prev_server_version = server_version;
            }
        }
    }
#endif // REALM_DEBUG
}
