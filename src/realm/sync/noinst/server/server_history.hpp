
#ifndef REALM_NOINST_SERVER_HISTORY_HPP
#define REALM_NOINST_SERVER_HISTORY_HPP

#include <cstdint>
#include <ctime>
#include <random>
#include <string>
#include <unordered_map>

#include <realm/util/buffer.hpp>
#include <realm/util/logger.hpp>
#include <realm/util/backtrace.hpp>
#include <realm/chunked_binary.hpp>
#include <realm/impl/cont_transact_hist.hpp>
#include <realm/replication.hpp>
#include <realm/transaction.hpp>
#include <realm/sync/noinst/server/clock.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/object_id.hpp>
#include <realm/sync/transform.hpp>
#include <realm/sync/instruction_replication.hpp>
#include <realm/sync/noinst/server/permissions.hpp>
#include <realm/array_integer.hpp>
#include <realm/array_ref.hpp>

namespace realm {
namespace sync {
struct Changeset;
}

namespace _impl {

// As new schema versions come into existence, describe them here.
//
//  0  Initial version.
//
//  1  Added support for stable IDs.
//
//  2  Added support for partial sync (`s_psp_server_version_ndx_in_parent`,
//     `s_psp_master_version_ndx_in_parent`).
//
//  3  Added write capability to partial sync (introduction of optional subarray
//     `ServerHistory::Arrays::partial_sync`).
//
//  4  Added a new first entry to `client_files` table. This special entry
//     represents the "invalid" client file identifier with value zero. The
//     references to client file entries from history entries are now stored as
//     client file identifiers. Before, they were stored as the client file
//     identifier minus one. Added `proxy_file` column to `client_files`
//     table. Added upstream client functionality (introduction of optional
//     subarray `ServerHistory::Arrays::upstream_status`).
//
//  5  Added support for history log compaction. The top-level fields
//     `last_compacted_at` and `compacted_until_version` were added, and the
//     `client_files` table gained a `last_seen` timestamp entry.
//
//  6  Misplaced `ct_history_entries` was moved out of table `history_entries`
//     and into history's root array.
//
//     Changed the format for downloadable_bytes which is used in the download
//     progress system. The history entry array is enlarged with a new column
//     containing cumulative byte sizes of changesets.  upload_byte_size is made
//     obsolete.  history_byte_size is made obsolete.
//
//  7  Convert full-state partial views to reduced-state partial views.
//
//  8  Added new column `locked_server_versions` to the `client_files` table.
//
//     Added a `schema_versions` table for the purpose of recording the creation
//     of, and the migrations of the history compartment from one schema version
//     to the next.
//
//  9  When `last_seen_at` is zero for a particular entry in the "client files"
//     table, it now means that that entry has been expired.
//
// 10  Added new column `client_types` to the `client_files` table.
//
//     The entry in `client_files` table representing the file itself no longer
//     has a nonzero `ident_salt`. It was useless anyway.
//
//     In a reference file, new entries in `client_files` created to represent
//     clients of partial views will no longer have a nonzero
//     `ident_salt`. Additionally, they will now have a nonzero `proxy_file`
//     specifying the identifier of the partial view. Preexisting entries will
//     not be modified, but will be marked as "legacy" entries in the
//     `client_types` column.
//
//     Only entries corresponding to direct clients (including partial views and
//     legacy entries) have nonzero values in the `last_seen_timestamp`
//     column. Previously, indirect clients and the self entry also had nonzero
//     `last_seen_timestamp`.
//
//     The special entry an index zero no longer has a nonzero
//     `locked_server_version`. It was useless anyway.
//
// 11..19 Reserved
//
// 20  ObjectIDHistoryState enhanced with m_table_map

constexpr int get_server_history_schema_version() noexcept
{
    return 20;
}


class ServerHistory : public sync::SyncReplication,
                      public _impl::History,
                      public std::enable_shared_from_this<ServerHistory> {
public:
    // clang-format off
    using file_ident_type        = sync::file_ident_type;
    using version_type           = sync::version_type;
    using salt_type              = sync::salt_type;
    using timestamp_type         = sync::timestamp_type;
    using SaltedFileIdent        = sync::SaltedFileIdent;
    using SaltedVersion          = sync::SaltedVersion;
    using DownloadCursor         = sync::DownloadCursor;
    using UploadCursor           = sync::UploadCursor;
    using SyncProgress           = sync::SyncProgress;
    using HistoryEntry           = sync::HistoryEntry;
    using RemoteChangeset        = sync::RemoteChangeset;
    // clang-format on

    enum class BootstrapError {
        no_error = 0,
        client_file_expired,
        bad_client_file_ident,
        bad_client_file_ident_salt,
        bad_download_server_version,
        bad_download_client_version,
        bad_server_version,
        bad_server_version_salt,
        bad_client_type
    };

    struct HistoryEntryHandler {
        virtual void handle(version_type server_version, const HistoryEntry&, std::size_t original_size) = 0;
        virtual ~HistoryEntryHandler() {}
    };

    // See table at top of `server_history.cpp`.
    //
    // CAUTION: The values of these are fixed by the history schema.
    enum class ClientType {
        // clang-format off
        upstream  = 0, // Reachable via upstream server
        self      = 6, // The file itself
        indirect  = 1, // Client of subserver
        legacy    = 5, // Precise type is unknown
        regular   = 2, // Direct regular client
        subserver = 4, // Direct subserver
        // clang-format on
    };

    struct FileIdentAllocSlot {
        file_ident_type proxy_file;
        ClientType client_type;
        SaltedFileIdent file_ident;
    };

    using FileIdentAllocSlots = std::vector<FileIdentAllocSlot>;

    enum class ExtendedIntegrationError { client_file_expired, bad_origin_file_ident, bad_changeset };

    struct IntegratableChangeset {
        file_ident_type client_file_ident; // Identifier of sending client's file
        timestamp_type origin_timestamp;
        file_ident_type origin_file_ident; // Zero if otherwise equal to client_file_ident
        UploadCursor upload_cursor;
        std::string changeset;

        IntegratableChangeset(file_ident_type client_file_ident, timestamp_type origin_timestamp,
                              file_ident_type origin_file_ident, UploadCursor upload_cursor,
                              BinaryData changeset) noexcept;

        operator RemoteChangeset() const noexcept
        {
            RemoteChangeset rc;
            rc.remote_version = upload_cursor.client_version;
            rc.last_integrated_local_version = upload_cursor.last_integrated_server_version;
            rc.data = BinaryData{changeset.data(), changeset.size()};
            rc.origin_timestamp = origin_timestamp;
            rc.origin_file_ident = (origin_file_ident != 0 ? origin_file_ident : client_file_ident);
            return rc;
        }
    };

    struct IntegratableChangesetList {
        UploadCursor upload_progress = {0, 0};
        version_type locked_server_version = 0;
        std::vector<IntegratableChangeset> changesets;

        bool has_changesets() const noexcept
        {
            return !changesets.empty();
        }
    };

    /// Key is identifier of client file from which the changes were
    /// received. That client file is not necessarily the client file from which
    /// the changes originated (star topology).
    using IntegratableChangesets = std::map<file_ident_type, IntegratableChangesetList>;

    struct IntegrationResult {
        std::map<file_ident_type, ExtendedIntegrationError> excluded_client_files;

        std::vector<const IntegratableChangeset*> integrated_changesets;

        void partial_clear() noexcept
        {
            integrated_changesets.clear();
        }
    };

    struct IntegratedBackup {
        bool success;
        sync::VersionInfo version_info;
    };

    class IntegrationReporter;
    class Context;

    static constexpr bool is_direct_client(ClientType) noexcept;

    ServerHistory(Context&);

    /// Get the current Realm version and server version.
    ///
    /// If this file has been initiated as a partial view, \a partial_file_ident
    /// is set to the file identifier allocated in the reference file for this
    /// partial view, and \a partial_progress_reference_version is set to the
    /// last sync version of the reference file that has been integrated into
    /// the partial view. Otherwise both are set to zero.
    void get_status(sync::VersionInfo&, bool& has_upstream_status, file_ident_type& partial_file_ident,
                    version_type& partial_progress_reference_version) const;

    /// Validate the specified client file identifier, download progress, and
    /// server version as received in an IDENT message. If they are valid, fetch
    /// the upload progress representing the last integrated changeset from the
    /// specified client file.
    ///
    /// The validation step in this function will fail if `server_version`
    /// refers to an earlier server version than `download_progress`, and it
    /// will fail if the server history has been trimmed, and
    /// `download_progress` refers to an earlier server version than the base
    /// version of the history.
    ///
    /// On entry, the download progress (`download_progress`) should be as
    /// specified by the client in the IDENT message. If
    /// `download_progress.server_version` is zero, and the history base version
    /// is not zero, and the history was not trimmed, then
    /// `download_progress.server_version` will be set to the base version of
    /// the history before being checked as described above. In all other cases,
    /// `download_progress` will be checked as specified.
    ///
    /// \param client_type Must be one for which is_direct_client() returns
    /// true, and must not be ClientType::legacy. If the actual type stored in
    /// the `client_files` entry for the specified client file is not
    /// ClientType::legacy, this function generates
    /// BootstrapError::bad_client_type if the stored type differs from the
    /// specified type.
    ///
    /// \param upload_progress Will be set to the position in the client-side
    /// history corresponding to the last client version that has been
    /// integrated on the server side.
    BootstrapError bootstrap_client_session(SaltedFileIdent client_file_ident, DownloadCursor download_progress,
                                            SaltedVersion server_version, ClientType client_type,
                                            UploadCursor& upload_progress, version_type& locked_server_version,
                                            util::Logger&) const;

    /// Allocate new file identifiers.
    ///
    /// This function must not be used with files that are either associated
    /// with an upstream server, or initialized as a partial view. It throws
    /// std::exception if used with any such file.
    ///
    /// This function is guaranteed to never introduce a new synchronization
    /// version (sync::VersionInfo::sync_version).
    void allocate_file_identifiers(FileIdentAllocSlots&, sync::VersionInfo&);

    /// Register a file identifier in the local file, that has been allocated by
    /// an upstream server, or in case of partial sync, one that has been
    /// allocated in the context of the reference file.
    ///
    /// In any case, the specified identifier must be valid (greater than 0),
    /// and greater than any previously registered file identifier, including
    /// any origin file identifier of an integrated changeset that was
    /// downloaded from the upstream server, and including the implicit
    /// registration of the root server's own file identifier (with value 1). If
    /// it is not, this function shall return false.
    ///
    /// If the identifier was requested on behalf of a direct client (regular
    /// client, subserver, or partial view), \a proxy_file_ident must be zero,
    /// and \a client_type must be set to indicate the type of the direct
    /// client.
    ///
    /// If the identifier was requested on behalf of a client of a direct
    /// client, including on behalf of a client of a subserver, and on behalf of
    /// a client of a partial view of this file, \a proxy_file_ident must
    /// specify the direct client, and \a client_type must be set to
    /// ClientType::indirect.
    ///
    /// When, and only when true is returned, \a file_ident_salt is set to the
    /// salt that was assigned to the registered identifier in the context of
    /// this file, and \a version_info is set to reflect the produced Realm
    /// snapshot.
    ///
    /// This function must be called only on histories with upstream sync
    /// status, or files that are initialized as partial views.
    bool register_received_file_identifier(file_ident_type received_file_ident, file_ident_type proxy_file_ident,
                                           ClientType client_type, salt_type& file_ident_salt,
                                           sync::VersionInfo& version_info);

    /// FIXME: Fully document this function.
    ///
    /// \param version_info Will be set when this function returns true. Will be
    /// left unmodified when this function returns false.
    ///
    /// \param backup_whole_realm Will be set to true when a change was made
    /// that necessitates a full Realm backup (nonincremental). Otherwise it is
    /// left unmodified.
    ///
    /// \param result Updatet to reflect the result of the integration
    /// process. This happens regardless of whether the function returns true or
    /// false.
    ///
    /// \return True when, and only when a new Realm version (snapshot) was
    /// created.
    bool integrate_client_changesets(const IntegratableChangesets&, sync::VersionInfo& version_info,
                                     bool& backup_whole_realm, IntegrationResult& result, util::Logger&);

    /// EXPLAIN BACKUP incremental
    auto integrate_backup_idents_and_changeset(version_type expected_realm_version, salt_type server_version_salt,
                                               const FileIdentAllocSlots&, const std::vector<IntegratableChangeset>&,
                                               util::Logger&) -> IntegratedBackup;

    /// \brief Scan through the history for changesets to be downloaded.
    ///
    /// This function scans the history for changesets to be downloaded, i.e.,
    /// for changesets that are not empty, and were not produced by integration
    /// of changesets recieved from the specified client file. The scan begins
    /// at the position specified by the initial value of \a
    /// download_progress.server_version, and ends no later than at the position
    /// specified by \a end_version.
    ///
    /// The implementation is allowed to end the scan before \a end_version,
    /// such as to limit the combined size of returned changesets. However, if
    /// the specified range contains any changesets that are supposed to be
    /// downloaded, this function must return at least one.
    ///
    /// Upon return, \a download_progress will have been updated to point to the
    /// position from which the next scan should resume. This must be a position
    /// after the last returned changeset, and before any remaining changesets
    /// that are supposed to be downloaded, although never a position that
    /// succeeds \a end_version.
    ///
    /// In each history entry passed to the specified handler,
    /// sync::HistoryEntry::remote_version will have been replaced with the last
    /// client version integrated prior to that entry, and for changesets of
    /// local origin, `sync::HistoryEntry::origin_file_ident` will have been
    /// replaced by the actual identifier for the local file.
    ///
    /// FIXME: Describe requirements on argument validity.
    ///
    /// \param upload_progress Set to refer to the last client version
    /// integrated into the history.
    ///
    /// \param cumulative_byte_size_current is the cumulative byte size of all
    /// changesets up to the end of the changesets fetched in this call.
    ///
    /// \param cumulative_byte_size_total is the cumulative byte size of
    /// the entire history.
    ///
    /// \param accum_byte_size_soft_limit denotes a soft limit on the total size
    /// of the uncompacted changesets. When the total size of the changesets
    /// exceeds this limit, no more changesets will be added.
    ///
    /// \return False if the client file entry of the specified client file has
    /// expired. Otherwise true.
    bool fetch_download_info(file_ident_type client_file_ident, DownloadCursor& download_progress,
                             version_type end_version, UploadCursor& upload_progress, HistoryEntryHandler&,
                             std::uint_fast64_t& cumulative_byte_size_current,
                             std::uint_fast64_t& cumulative_byte_size_total,
                             std::size_t accum_byte_size_soft_limit = 0x20000) const;

    /// The application must call this function before using the history as an
    /// upstream client history.
    ///
    /// This function must be called at most once for each Realm file. Use
    /// get_status() to determine whether it has been called already.
    ///
    /// This function throws std::runtime_error if the history is nonempty or if
    /// new client file identifiers have already been allocated from this file.
    void add_upstream_sync_status();

    /// Perform a transaction on the shared group associated with this
    /// history. If the handler returns true, the transaction will be comitted,
    /// and the version info will be set accordingly. If the handler returns
    /// false, the transaction will be rolled back, and the version info will be
    /// left unmodified.
    ///
    /// \return True if, and only if the handler retured true.
    template <class H>
    bool transact(H handler, sync::VersionInfo&);

    std::vector<sync::Changeset> get_parsed_changesets(version_type begin, version_type end) const;

    // History inspection for debugging purposes and for testing the
    // backup.
    struct HistoryContents {

        struct ClientFile {
            std::uint_fast64_t ident_salt;
            std::uint_fast64_t client_version;
            std::uint_fast64_t rh_base_version;
            std::int_fast64_t proxy_file;
            std::int_fast64_t client_type;
            std::uint_fast64_t locked_server_version;
            std::vector<util::Optional<std::string>> reciprocal_history;
        };

        std::vector<ClientFile> client_files;

        std::uint_fast64_t history_base_version;
        std::uint_fast64_t base_version_salt;

        struct HistoryEntry {
            std::uint_fast64_t version_salt;
            std::uint_fast64_t client_file_ident;
            std::uint_fast64_t client_version;
            std::uint_fast64_t timestamp;
            std::uint_fast64_t cumul_byte_size;
            std::string changeset;
        };

        std::vector<HistoryEntry> sync_history;

        std::uint_fast64_t servers_client_file_ident;
    };

    // The contents of the entire Realm is returned in a HistoryContents object.
    // This function is used for testing the backup and for debugging.
    HistoryContents get_history_contents() const;

    // FIXME: This function was not designed to be public. At least document the
    // special conditions under which it can be called!
    SaltedVersion get_salted_server_version() const noexcept;

    // Overriding member functions in Replication
    void initialize(DB&) override;
    HistoryType get_history_type() const noexcept override;
    int get_history_schema_version() const noexcept override;
    bool is_upgradable_history_schema(int) const noexcept override;
    void upgrade_history_schema(int) override;
    _impl::History* _get_history_write() override;
    std::unique_ptr<_impl::History> _create_history_read() override;

    // Overriding member functions in Replication
    version_type prepare_changeset(const char*, std::size_t, version_type) override;

    // Overriding member functions in _impl::History
    void update_from_ref_and_version(ref_type ref, version_type) override;
    void update_from_parent(version_type) override;

    void set_group(Group* group, bool updated = false) override
    {
        _impl::History::set_group(group, updated);
        if (REALM_LIKELY(m_acc)) {
            _impl::GroupFriend::set_history_parent(*m_group, m_acc->root);
        }

        m_local_file_ident = group->get_sync_file_id();
    }

    // Overriding member functions in _impl::History
    void get_changesets(version_type, version_type, BinaryIterator*) const noexcept override;
    void set_oldest_bound_version(version_type) override;
    void verify() const override;

private:
    // FIXME: Avoid use of optional type `std::int64_t`
    using IntegerBpTree = BPlusTree<std::int64_t>;

    class ReciprocalHistory;
    class TransformHistoryImpl;
    class DiscardAccessorsGuard;

    // clang-format off

    // Sizes of fixed-size arrays
    static constexpr int s_root_size = 11;
    static constexpr int s_client_files_size = 8;
    static constexpr int s_sync_history_size = 6;
    static constexpr int s_upstream_status_size = 8;
    static constexpr int s_partial_sync_size = 5;
    static constexpr int s_schema_versions_size = 4;

    // Slots in root array of history compartment
    static constexpr int s_client_files_iip = 0;              // table ref
    static constexpr int s_history_base_version_iip = 1;      // version
    static constexpr int s_base_version_salt_iip = 2;         // salt
    static constexpr int s_sync_history_iip = 3;              // table ref
    static constexpr int s_ct_history_iip = 4;                // column ref
    static constexpr int s_object_id_history_state_iip = 5;   // ref
    static constexpr int s_upstream_status_iip = 6;           // optional array ref
    static constexpr int s_partial_sync_iip = 7;              // optional array ref
    static constexpr int s_compacted_until_version_iip = 8;   // version
    static constexpr int s_last_compaction_timestamp_iip = 9; // UNIX timestamp (in seconds)
    static constexpr int s_schema_versions_iip = 10;          // ref

    // Slots in root array of `client_files` table
    static constexpr int s_cf_ident_salts_iip = 0;            // column ref
    static constexpr int s_cf_client_versions_iip = 1;        // column ref
    static constexpr int s_cf_rh_base_versions_iip = 2;       // column ref
    static constexpr int s_cf_recip_hist_refs_iip = 3;        // column ref
    static constexpr int s_cf_proxy_files_iip = 4;            // column ref
    static constexpr int s_cf_client_types_iip = 5;           // column ref
    static constexpr int s_cf_last_seen_timestamps_iip = 6;   // column ref (UNIX timestamps in seconds)
    static constexpr int s_cf_locked_server_versions_iip = 7; // column ref

    // Slots in root array of `sync_history` table
    static constexpr int s_sh_version_salts_iip = 0;    // column ref
    static constexpr int s_sh_origin_files_iip = 1;     // column ref
    static constexpr int s_sh_client_versions_iip = 2;  // column ref
    static constexpr int s_sh_timestamps_iip = 3;       // column ref
    static constexpr int s_sh_changesets_iip = 4;       // column ref
    static constexpr int s_sh_cumul_byte_sizes_iip = 5; // column_ref

    // Slots in `upstream_status` array
    static constexpr int s_us_client_file_ident_iip = 0;                   // file ident
    static constexpr int s_us_client_file_ident_salt_iip = 1;              // salt
    static constexpr int s_us_progress_latest_server_version_iip = 2;      // version
    static constexpr int s_us_progress_latest_server_version_salt_iip = 3; // salt
    static constexpr int s_us_progress_download_server_version_iip = 4;    // version
    static constexpr int s_us_progress_download_client_version_iip = 5;    // version
    static constexpr int s_us_progress_upload_client_version_iip = 6;      // version
    static constexpr int s_us_progress_upload_server_version_iip = 7;      // version

    // Slots in `partial_sync` array
    static constexpr int s_ps_partial_file_ident_iip = 0;              // file ident
    static constexpr int s_ps_partial_file_ident_salt_iip = 1;         // salt
    static constexpr int s_ps_progress_partial_version_iip = 2;        // version
    static constexpr int s_ps_progress_reference_version_iip = 3;      // version
    static constexpr int s_ps_progress_reference_version_salt_iip = 4; // salt

    // Slots in root array of `schema_versions` table
    static constexpr int s_sv_schema_versions_iip = 0;   // integer
    static constexpr int s_sv_library_versions_iip = 1;  // ref
    static constexpr int s_sv_snapshot_versions_iip = 2; // integer (version_type)
    static constexpr int s_sv_timestamps_iip = 3;        // integer (seconds since epoch)

    // clang-format on

    struct Accessors {
        Array root;
        Array client_files;    // List of columns
        Array sync_history;    // List of columns
        Array upstream_status; // Optional
        Array partial_sync;    // Optional
        Array schema_versions;

        // Columns of Accessors::client_files
        BPlusTree<int64_t> cf_ident_salts;
        BPlusTree<int64_t> cf_client_versions;
        BPlusTree<int64_t> cf_rh_base_versions;
        BPlusTree<ref_type> cf_recip_hist_refs;
        BPlusTree<int64_t> cf_proxy_files;
        BPlusTree<int64_t> cf_client_types;
        BPlusTree<int64_t> cf_last_seen_timestamps;
        BPlusTree<int64_t> cf_locked_server_versions;

        // Columns of Accessors::sync_history
        BPlusTree<int64_t> sh_version_salts;
        BPlusTree<int64_t> sh_origin_files;
        BPlusTree<int64_t> sh_client_versions;
        BPlusTree<int64_t> sh_timestamps;
        BinaryColumn sh_changesets;
        BPlusTree<int64_t> sh_cumul_byte_sizes;

        // Continuous transactions history
        BinaryColumn ct_history;

        Accessors(Allocator&) noexcept;

        void set_parent(ArrayParent* parent, size_t ndx_in_parent) noexcept;
        void init_from_ref(ref_type ref);
        void create();

    private:
        void init_children();
    };

    Context& m_context;

    // Salt to attach to new server versions (history entries) produced on
    // behalf of this history object. The salt is allowed to differ between
    // every server version, but for the purpose of compressibility (on the
    // wire), it is best to use the same when we can. What matters, is that if
    // the server state regresses (restore of backup), and a new server version
    // is generated with the same numerical value as one that existed before the
    // regression, then the two will have different salts attached to them (with
    // a high probability).
    salt_type m_salt_for_new_server_versions;

    DB* m_db = nullptr;

    version_type m_version_of_oldest_bound_snapshot = 0;

    // The identifier of the local Realm file. If this file is used on a subtier
    // node of a star topology server cluster, the identifier is allocated in
    // the context of a different Realm file.
    //
    // In a file on a subtier node of a star topology server cluster, that is
    // not used as a partila view, it will be 1 until a file identifier is
    // allocated.
    //
    // In a file that is not used as a partial view, and is not on a subtier
    // node of a star topology server cluster, it is always equal to 1.
    //
    // It is never zero.
    mutable file_ident_type m_local_file_ident;

    // Current number of client file entries (Array::client_files). A cache of
    // `m_cf_ident_salts.size()`.
    mutable std::size_t m_num_client_files;

    // Server version produced by the changeset associated with the last entry
    // in the discarded prefix of the history (Array::sync_history), or zero if
    // no entries were ever discarded.
    mutable version_type m_history_base_version;

    // Current number of entries in the history (Array::sync_history). A cache
    // of `m_sh_changesets->size()`.
    mutable std::size_t m_history_size;

    // Salt associated with current server version (get_server_version()).
    mutable salt_type m_server_version_salt;

    /// Realm version (snapshot number) on which the changeset associated with
    /// the first entry in the continuous transactions history is based, or if
    /// that history is empty, the version associated with the currently bound
    /// snapshot. In general, the version associated with currently bound
    /// snapshot is equal to `m_ct_base_version + m_ct_history_size`, but after
    /// add_core_history_entry() is called, the snapshot version is equal to
    /// `m_ct_base_version + m_ct_history_size - 1`.
    mutable version_type m_ct_base_version;

    // Current number of entries in the continuous transaction history. A cache
    // of `m_ct_history.size()`.
    mutable std::size_t m_ct_history_size;

    // The construction of the array accessors need to be delayed, because the
    // allocator (Allocator) is not known at the time of construction of the
    // ServerHistory object.
    mutable util::Optional<Accessors> m_acc;

    bool m_is_local_changeset = true;

    std::vector<file_ident_type> m_client_file_order_buffer;

    void discard_accessors() const noexcept;
    void prepare_for_write();
    void create_empty_history();
    version_type get_server_version() const noexcept;
    salt_type get_server_version_salt(version_type server_version) const noexcept;
    bool is_valid_proxy_file_ident(file_ident_type) const noexcept;
    void add_core_history_entry(BinaryData);
    void add_sync_history_entry(const HistoryEntry&);
    void trim_cont_transact_history();
    ChunkedBinaryData get_changeset(version_type server_version) const noexcept;
    version_type find_history_entry(file_ident_type remote_file_ident, version_type begin_version,
                                    version_type end_version, HistoryEntry&) const noexcept;
    version_type find_history_entry(file_ident_type remote_file_ident, version_type begin_version,
                                    version_type end_version, HistoryEntry&,
                                    version_type& last_integrated_remote_version) const noexcept;
    HistoryEntry get_history_entry(version_type server_version) const noexcept;
    bool received_from(const HistoryEntry&, file_ident_type remote_file_ident) const noexcept;

    SaltedFileIdent allocate_file_ident(file_ident_type proxy_file_ident, ClientType);
    void register_assigned_file_ident(file_ident_type file_ident);
    bool try_register_file_ident(file_ident_type file_ident, file_ident_type proxy_file_ident, ClientType,
                                 salt_type& file_ident_salt);
    salt_type register_client_file_by_index(std::size_t file_index, file_ident_type proxy_file_ident, ClientType);
    bool ensure_upstream_file_ident(file_ident_type file_ident);
    void add_client_file(salt_type file_ident_salt, file_ident_type proxy_file_ident, ClientType);
    void save_upstream_sync_progress(const SyncProgress&);

    BootstrapError do_bootstrap_client_session(SaltedFileIdent client_file_ident, DownloadCursor download_progress,
                                               SaltedVersion server_version, ClientType client_type,
                                               UploadCursor& upload_progress, version_type& locked_server_version,
                                               util::Logger& logger) const noexcept;

    /// Behaviour is undefined if `last_integrated_local_version` of any
    /// changeset is less than the value passed during any previous successful
    /// invocation of this function. The application can use
    /// validate_client_identity() to obtain the minimum value (as
    /// upload_progress.last_integrated_local_version).
    ///
    /// Behaviour is undefined if `last_integrated_local_version` of any
    /// changeset is greater than the current server version (as returned by
    /// get_server_version()).
    ///
    /// \param remote_file_ident The identifier of the remote file from which
    /// the changes were received, or zero if the changes were received from the
    /// upstream server. It is an error to specify a client file whose entry in
    /// the "client files" table is expired.
    ///
    /// \return True if, and only if changes were made to the Realm file (state
    /// or history compartment).
    ///
    /// \throw sync::BadChangesetError If parsing of a changeset fails, or if
    /// application of the parsed changeset fails due to a problem with the
    /// changeset.
    ///
    /// \throw sync::TransformError If operational transformation of the
    /// changeset fails due to a problem with the changeset.
    ///
    /// FIXME: Bad changesets should not cause exceptions to be thrown. Use
    /// std::error_code instead.
    bool integrate_remote_changesets(file_ident_type remote_file_ident, UploadCursor upload_progress,
                                     version_type locked_server_version, const RemoteChangeset* changesets,
                                     std::size_t num_changesets, util::Logger&);

    bool update_upload_progress(version_type orig_client_version, ReciprocalHistory& recip_hist,
                                UploadCursor upload_progress);

    void fixup_state_and_changesets_for_assigned_file_ident(Transaction&, file_ident_type);

    void record_current_schema_version();
    static void record_current_schema_version(Array& schema_versions, version_type snapshot_version);
};


class ServerHistory::Context {
public:
    virtual std::mt19937_64& server_history_get_random() noexcept = 0;

protected:
    Context() noexcept = default;
};


std::ostream& operator<<(std::ostream& os, const ServerHistory::HistoryContents& hc);
bool operator==(const ServerHistory::HistoryContents&, const ServerHistory::HistoryContents&);


// Implementation

constexpr bool ServerHistory::is_direct_client(ClientType client_type) noexcept
{
    switch (client_type) {
        case ClientType::legacy:
        case ClientType::regular:
        case ClientType::subserver:
            return true;
        case ClientType::upstream:
        case ClientType::indirect:
        case ClientType::self:
            break;
    }
    return false;
}

inline ServerHistory::IntegratableChangeset::IntegratableChangeset(file_ident_type cfi, timestamp_type ot,
                                                                   file_ident_type ofi, UploadCursor uc,
                                                                   BinaryData c) noexcept
    : client_file_ident{cfi}
    , origin_timestamp{ot}
    , origin_file_ident{ofi}
    , upload_cursor(uc)
    , changeset(c.data(), c.size())
{
}

inline ServerHistory::ServerHistory(Context& context)
    : m_context{context}
{
    // The synchronization protocol specification requires that server version
    // salts are nonzero positive integers that fit in 63 bits.
    std::mt19937_64& random = context.server_history_get_random();
    m_salt_for_new_server_versions =
        std::uniform_int_distribution<std::int_fast64_t>(1, 0x0'7FFF'FFFF'FFFF'FFFF)(random);
}

template <class H>
bool ServerHistory::transact(H handler, sync::VersionInfo& version_info)
{
    auto wt = m_db->start_write();                 // Throws
    if (handler(*wt)) {                            // Throws
        version_info.realm_version = wt->commit(); // Throws
        version_info.sync_version = get_salted_server_version();
        return true;
    }
    return false;
}

inline ServerHistory::Accessors::Accessors(Allocator& alloc) noexcept
    : root{alloc}
    , client_files{alloc}
    , sync_history{alloc}
    , upstream_status{alloc}
    , partial_sync{alloc}
    , schema_versions{alloc}
    , cf_ident_salts{alloc}
    , cf_client_versions{alloc}
    , cf_rh_base_versions{alloc}
    , cf_recip_hist_refs{alloc}
    , cf_proxy_files{alloc}
    , cf_client_types{alloc}
    , cf_last_seen_timestamps{alloc}
    , cf_locked_server_versions{alloc}
    , sh_version_salts{alloc}
    , sh_origin_files{alloc}
    , sh_client_versions{alloc}
    , sh_timestamps{alloc}
    , sh_changesets{alloc}
    , sh_cumul_byte_sizes{alloc}
    , ct_history{alloc}
{
    client_files.set_parent(&root, s_client_files_iip);
    sync_history.set_parent(&root, s_sync_history_iip);
    upstream_status.set_parent(&root, s_upstream_status_iip);
    partial_sync.set_parent(&root, s_partial_sync_iip);
    schema_versions.set_parent(&root, s_schema_versions_iip);

    cf_ident_salts.set_parent(&client_files, s_cf_ident_salts_iip);
    cf_client_versions.set_parent(&client_files, s_cf_client_versions_iip);
    cf_rh_base_versions.set_parent(&client_files, s_cf_rh_base_versions_iip);
    cf_recip_hist_refs.set_parent(&client_files, s_cf_recip_hist_refs_iip);
    cf_proxy_files.set_parent(&client_files, s_cf_proxy_files_iip);
    cf_client_types.set_parent(&client_files, s_cf_client_types_iip);
    cf_last_seen_timestamps.set_parent(&client_files, s_cf_last_seen_timestamps_iip);
    cf_locked_server_versions.set_parent(&client_files, s_cf_locked_server_versions_iip);

    sh_version_salts.set_parent(&sync_history, s_sh_version_salts_iip);
    sh_origin_files.set_parent(&sync_history, s_sh_origin_files_iip);
    sh_client_versions.set_parent(&sync_history, s_sh_client_versions_iip);
    sh_timestamps.set_parent(&sync_history, s_sh_timestamps_iip);
    sh_changesets.set_parent(&sync_history, s_sh_changesets_iip);
    sh_cumul_byte_sizes.set_parent(&sync_history, s_sh_cumul_byte_sizes_iip);

    ct_history.set_parent(&root, s_ct_history_iip);
}

inline void ServerHistory::Accessors::set_parent(ArrayParent* parent, size_t index) noexcept
{
    root.set_parent(parent, index);
}

inline void ServerHistory::prepare_for_write()
{
    if (!m_acc)
        create_empty_history(); // Throws

    REALM_ASSERT(m_acc->sh_changesets.is_attached());
    REALM_ASSERT(m_acc->root.size() == s_root_size);
}

// Note: This function can be safely called during or after a transaction.
inline auto ServerHistory::get_server_version() const noexcept -> version_type
{
    return m_history_base_version + m_history_size;
}

// Note: This function can be safely called during or after a transaction.
inline auto ServerHistory::get_salted_server_version() const noexcept -> SaltedVersion
{
    version_type version = get_server_version();
    return SaltedVersion{version, m_server_version_salt};
}

inline auto ServerHistory::find_history_entry(file_ident_type remote_file_ident, version_type begin_version,
                                              version_type end_version, HistoryEntry& entry) const noexcept
    -> version_type
{
    version_type last_integrated_remote_version; // Dummy
    return find_history_entry(remote_file_ident, begin_version, end_version, entry, last_integrated_remote_version);
}

} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_SERVER_HISTORY_HPP
