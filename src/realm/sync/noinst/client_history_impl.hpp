
#ifndef REALM_NOINST_CLIENT_HISTORY_IMPL_HPP
#define REALM_NOINST_CLIENT_HISTORY_IMPL_HPP

#include <realm/util/optional.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/noinst/object_id_history_state.hpp>

namespace realm {
namespace _impl {

// As new schema versions come into existence, describe them here.
//
//  0  Initial version
//
//  1  Added support for stable IDs.
//
//  2  Now allowing continuous transactions history and synchronization history
//     to be separately trimmed.
//
//     Added a slot for `progress_upload_server_version` to the root array.
//
//     Reordered slots in root array.
//
//     Added a `schema_versions` table for the purpose of recording the creation
//     of, and the migrations of the history compartment from one schema version
//     to the next.
//
//     Slots pertaining to cooked history were moved into subarray
//     `cooked_history`.
//
//     Added slots `base_server_version` and `server_versions` to
//     `cooked_history` array. The former contains a server version, and the
//     latter contains a ref to a column of server versions.
//
//  3..9 Reserved for Core-5 based sync
//
//  10   Stable IDs supported by core.
//
constexpr int get_client_history_schema_version() noexcept
{
    return 10;
}

class ClientHistoryImpl : public sync::ClientReplication, private History, public sync::TransformHistory {
public:
    using file_ident_type = sync::file_ident_type;
    using version_type = sync::version_type;

    // clang-format off
    using SaltedFileIdent = sync::SaltedFileIdent;
    using SaltedVersion   = sync::SaltedVersion;
    using DownloadCursor  = sync::DownloadCursor;
    using UploadCursor    = sync::UploadCursor;
    using SyncProgress    = sync::SyncProgress;
    using VersionInfo     = sync::VersionInfo;
    using HistoryEntry    = sync::HistoryEntry;
    using Transformer     = sync::Transformer;
    using TableInfoCache  = sync::TableInfoCache;
    // clang-format on

    struct LocalChangeset {
        version_type version;
        ChunkedBinaryData changeset;
    };

    ClientHistoryImpl(const std::string& realm_path, Config config = {});
    ClientHistoryImpl(const std::string& realm_path, bool owner_is_sync_client,
                      std::unique_ptr<ChangesetCooker> changeset_cooker);

    /// set_client_file_ident_and_downloaded_bytes() sets the salted client
    /// file ident and downloaded_bytes. The function is used when a state
    /// Realm has been downloaded from the server. The function creates a write
    /// transaction.
    void make_final_async_open_adjustements(SaltedFileIdent client_file_ident, uint_fast64_t downloaded_bytes);

    /// set_initial_state_realm_history_numbers() sets the history numbers for a
    /// new state Realm. The function is used when the server creates a new
    /// State Realm.
    ///
    /// The history object must be in a write transaction before this function
    /// is called.
    void set_initial_state_realm_history_numbers(version_type local_version, sync::SaltedVersion server_version);

    // virtual void set_client_file_ident_in_wt() sets the client file ident.
    // The history must be in a write transaction with version 'current_version'.
    void set_client_file_ident_in_wt(version_type current_version, SaltedFileIdent client_file_ident);

    // get_next_local_changeset returns the first changeset with version
    // greater than or equal to 'begin_version'. 'begin_version' must be at
    // least 1.
    //
    // The history must be in a transaction when this function is called.
    // The return value is none if there are no such local changesets.
    util::Optional<LocalChangeset> get_next_local_changeset(version_type current_version,
                                                            version_type begin_version) const;

    /// set_client_reset_adjustments() is used by client reset to adjust the
    /// content of the history compartment. The shared group associated with
    /// this history object must be in a write transaction when this function
    /// is called.
    void set_client_reset_adjustments(version_type current_version, SaltedFileIdent client_file_ident,
                                      sync::SaltedVersion server_version, uint_fast64_t downloaded_bytes,
                                      BinaryData uploadable_changeset);

    // Overriding member functions in realm::Replication
    void initialize(DB& sg) override final;
    void initiate_session(version_type) override final;
    void terminate_session() noexcept override final;
    HistoryType get_history_type() const noexcept override final;
    int get_history_schema_version() const noexcept override final;
    bool is_upgradable_history_schema(int) const noexcept override final;
    void upgrade_history_schema(int) override final;
    History* _get_history_write() override;
    std::unique_ptr<History> _create_history_read() override;
    bool is_sync_agent() const noexcept override final;
    void do_initiate_transact(Group& group, version_type version, bool history_updated) override final;

    // Overriding member functions in realm::TrivialReplication
    version_type prepare_changeset(const char*, size_t, version_type) override final;
    void finalize_changeset() noexcept override final;

    // Overriding member functions in realm::sync::ClientReplicationBase
    void get_status(version_type&, SaltedFileIdent&, SyncProgress&) const override final;
    void set_client_file_ident(SaltedFileIdent, bool) override final;
    void set_sync_progress(const SyncProgress&, const std::uint_fast64_t*, VersionInfo&) override final;
    void find_uploadable_changesets(UploadCursor&, version_type, std::vector<UploadChangeset>&,
                                    version_type&) const override final;
    bool integrate_server_changesets(const SyncProgress&, const std::uint_fast64_t*, const RemoteChangeset*,
                                     std::size_t, VersionInfo&, IntegrationError&, util::Logger&,
                                     SyncTransactReporter*) override final;

    // Overriding member functions in realm::sync::ClientHistory
    void get_upload_download_bytes(std::uint_fast64_t&, std::uint_fast64_t&, std::uint_fast64_t&, std::uint_fast64_t&,
                                   std::uint_fast64_t&) override final;
    void get_cooked_status(version_type, std::int_fast64_t&, CookedProgress&,
                           std::int_fast64_t&) const override final;
    void get_cooked_changeset(std::int_fast64_t, util::AppendBuffer<char>&, version_type&) const override final;
    version_type set_cooked_progress(CookedProgress) override final;
    UploadCursor get_upload_anchor_of_current_transact(const Transaction&) const override final;
    util::StringView get_sync_changeset_of_current_transact(const Transaction&) const noexcept override final;

    // Overriding member functions in realm::sync::TransformHistory
    version_type find_history_entry(version_type, version_type, HistoryEntry&) const noexcept override final;
    ChunkedBinaryData get_reciprocal_transform(version_type) const override final;
    void set_reciprocal_transform(version_type, BinaryData) override final;

private:
    static constexpr version_type s_initial_version = 1;

    DB* m_shared_group = nullptr;

    // FIXME: All history objects belonging to a particular client object
    // (sync::Client) should use a single shared transformer object.
    std::unique_ptr<Transformer> m_transformer;

    /// The version on which the first changeset in the continuous transactions
    /// history is based, or if that history is empty, the version associated
    /// with currently bound snapshot. In general, `m_ct_history_base_version +
    /// m_ct_history_size` is equal to the version, that is associated with the
    /// currently bound snapshot, but after add_ct_history_entry() is called, it
    /// is equal to that plus one.
    mutable version_type m_ct_history_base_version;

    /// Current number of entries in the continuous transactions history (a
    /// cache of `m_ct_history->size()`).
    mutable std::size_t m_ct_history_size;

    /// Version on which the first changeset in the synchronization history is
    /// based, or if that history is empty, the version on which the next
    /// changeset, that is added, is based.  In general,
    /// `m_sync_history_base_version + m_sync_history_size` is equal to the
    /// version, that is associated with the currently bound snapshot, but after
    /// add_sync_history_entry() is called, it is equal to that plus one.
    mutable version_type m_sync_history_base_version;

    /// Current number of entries in the synchronization history (a cache of
    /// `m_changesets->size()`).q
    mutable std::size_t m_sync_history_size;

    struct Arrays {
        Array root;           // Root of history compartment
        Array cooked_history; // Optional
        Arrays(Allocator&) noexcept;
    };

    // clang-format off

    // Sizes of fixed-size arrays
    static constexpr int s_root_size            = 21;
    static constexpr int s_cooked_history_size  =  5;
    static constexpr int s_schema_versions_size =  4;

    // Slots in root array of history compartment
    static constexpr int s_ct_history_iip = 0;                          // column ref
    static constexpr int s_client_file_ident_iip = 1;                   // integer
    static constexpr int s_client_file_ident_salt_iip = 2;              // integer
    static constexpr int s_progress_latest_server_version_iip = 3;      // integer
    static constexpr int s_progress_latest_server_version_salt_iip = 4; // integer
    static constexpr int s_progress_download_server_version_iip = 5;    // integer
    static constexpr int s_progress_download_client_version_iip = 6;    // integer
    static constexpr int s_progress_upload_client_version_iip = 7;      // integer
    static constexpr int s_progress_upload_server_version_iip = 8;      // integer
    static constexpr int s_progress_downloaded_bytes_iip = 9;           // integer
    static constexpr int s_progress_downloadable_bytes_iip = 10;        // integer
    static constexpr int s_progress_uploaded_bytes_iip = 11;            // integer
    static constexpr int s_progress_uploadable_bytes_iip = 12;          // integer
    static constexpr int s_changesets_iip = 13;                         // column ref
    static constexpr int s_reciprocal_transforms_iip = 14;              // column ref
    static constexpr int s_remote_versions_iip = 15;                    // column ref
    static constexpr int s_origin_file_idents_iip = 16;                 // column ref
    static constexpr int s_origin_timestamps_iip = 17;                  // column ref
    static constexpr int s_object_id_history_state_iip = 18;            // ref
    static constexpr int s_cooked_history_iip = 19;                     // ref (optional)
    static constexpr int s_schema_versions_iip = 20;                    // table ref

    // Slots in root array of `cooked_history` substructure
    static constexpr int s_ch_base_index_iip = 0;              // integer
    static constexpr int s_ch_intrachangeset_progress_iip = 1; // integer
    static constexpr int s_ch_base_server_version_iip = 2;     // integer
    static constexpr int s_ch_changesets_iip = 3;              // column ref
    static constexpr int s_ch_server_versions_iip = 4;         // column ref

    // Slots in root array of `schema_versions` table
    static constexpr int s_sv_schema_versions_iip = 0;   // integer
    static constexpr int s_sv_library_versions_iip = 1;  // ref
    static constexpr int s_sv_snapshot_versions_iip = 2; // integer (version_type)
    static constexpr int s_sv_timestamps_iip = 3;        // integer (seconds since epoch)

    // clang-format on

    // `progress_server_version` is the latest server version, V, that has been
    // integrated locally (client-side) prior to the currently bound snapshot,
    // or any later server version W, such that W and all server versions
    // between V and W are produced by the servers integration of changesets
    // originating from this client.
    //
    // `progress_client_version` is the latest local client version produced by
    // a changeset that was uploaded by this client and integrated by the server
    // prior to `progress_server_version`.
    //
    // The construction of the array accessors need to be delayed, because the
    // allocator (Allocator) is not known at the time of construction of the
    // ServerHistory object.
    mutable std::unique_ptr<Arrays> m_arrays;

    /// Continuous transactions history
    mutable std::unique_ptr<BinaryColumn> m_ct_history;

    /// A column of changesets, one row for each entry in the history.
    ///
    /// FIXME: Ideally, the B+tree accessor below should have been just
    /// Bptree<BinaryData>, but Bptree<BinaryData> seems to not allow that yet.
    ///
    /// FIXME: The memory-wise indirection is an unfortunate consequence of the
    /// fact that it is impossible to construct a BinaryColumn without already
    /// having a ref to a valid underlying node structure. This, in turn, is an
    /// unfortunate consequence of the fact that a column accessor contains a
    /// dynamically allocated root node accessor, and that the type of the
    /// required root node accessor depends on the size of the B+-tree.
    mutable std::unique_ptr<BinaryColumn> m_changesets;
    mutable std::unique_ptr<BinaryColumn> m_reciprocal_transforms;

    using IntegerBpTree = BPlusTree<int64_t>;
    mutable std::unique_ptr<IntegerBpTree> m_remote_versions;
    mutable std::unique_ptr<IntegerBpTree> m_origin_file_idents;
    mutable std::unique_ptr<IntegerBpTree> m_origin_timestamps;

    mutable std::vector<char> m_changeset_from_server_owner;
    mutable util::Optional<HistoryEntry> m_changeset_from_server;

    util::Optional<BinaryData> m_client_reset_changeset;

    // Cache of s_progress_download_server_version_iip and
    // s_progress_download_client_version_iip slots of history compartment root
    // array.
    mutable DownloadCursor m_progress_download = {0, 0};

    version_type m_version_of_oldest_bound_snapshot = 0;

    const bool m_owner_is_sync_client;

    const std::shared_ptr<ChangesetCooker> m_changeset_cooker;

    /// A cache of the `s_ch_base_index_iip` slot in the history compartment
    /// root array. When the cooked history is not empty, this is the index into
    /// the total untrimmed sequence of cooked changesets of the first cooked
    /// changeset that is currently in the Realm file.
    mutable std::int_fast64_t m_ch_base_index;

    /// A cache of the `s_ch_base_server_version_iip` slot in the history
    /// compartment root array. This is a server version that is less than the
    /// versions produced on the server by the original versions of all the
    /// unconsumed cooked changesets. In general, it will be the latest server
    /// version satisfying this chriterion.
    mutable version_type m_ch_base_server_version;

    /// Current number of entries in the cooked history. A cache of
    /// `m_ch_changesets->size()`.
    mutable std::size_t m_cooked_history_size;

    mutable std::unique_ptr<BinaryColumn> m_ch_changesets; // Not nullable
    mutable std::unique_ptr<IntegerBpTree> m_ch_server_versions;

    version_type find_sync_history_entry(version_type begin_version, version_type end_version, HistoryEntry& entry,
                                         version_type& last_integrated_server_version) const noexcept;
    void do_get_cooked_changeset(std::int_fast64_t index, util::AppendBuffer<char>& buffer,
                                 version_type& server_version) const noexcept;

    // sum_of_history_entry_sizes calculates the sum of the changeset sizes of the local history
    // entries that produced a version that succeeds `begin_version` and precedes `end_version`.
    std::uint_fast64_t sum_of_history_entry_sizes(version_type begin_version, version_type end_version) const
        noexcept;

    void prepare_for_write();
    void add_ct_history_entry(BinaryData changeset);
    void add_sync_history_entry(HistoryEntry);
    void update_sync_progress(const SyncProgress&, const std::uint_fast64_t* downloadable_bytes);
    void trim_ct_history();
    void trim_sync_history();
    void do_trim_sync_history(std::size_t n);
    void clamp_sync_version_range(version_type& begin, version_type& end) const noexcept;
    Transformer& get_transformer();
    void ensure_cooked_history();
    void ensure_no_cooked_history();
    void save_cooked_changeset(BinaryData changeset, version_type server_version);
    void update_cooked_progress(CookedProgress progress);
    void fix_up_client_file_ident_in_stored_changesets(Transaction&, TableInfoCache&, file_ident_type);
    void migrate_from_history_schema_version_1_to_2(int orig_schema_version);
    void migrate_from_history_schema_version_2_to_10();
    void record_current_schema_version();
    static void record_current_schema_version(Array& schema_versions, version_type snapshot_version);
    bool was_migrated_from_schema_version_earlier_than(int schema_version) const noexcept;

    // Overriding member functions in realm::_impl::History
    void set_group(Group* group, bool updated = false) override final;
    void update_from_ref_and_version(ref_type ref, version_type version) override final;
    void update_from_parent(version_type current_version) override final;
    void get_changesets(version_type, version_type, BinaryIterator*) const noexcept override final;
    void set_oldest_bound_version(version_type) override final;
    BinaryData get_uncommitted_changes() const noexcept override final;
    void verify() const override final;
};


// Implementation

inline ClientHistoryImpl::ClientHistoryImpl(const std::string& realm_path, Config config)
    : ClientReplication{realm_path}
    , m_owner_is_sync_client{config.owner_is_sync_agent}
    , m_changeset_cooker{std::move(config.changeset_cooker)}
{
}

inline ClientHistoryImpl::ClientHistoryImpl(const std::string& realm_path, bool owner_is_sync_client,
                                            std::unique_ptr<ChangesetCooker> changeset_cooker)
    : ClientReplication{realm_path} // Throws
    , m_owner_is_sync_client{owner_is_sync_client}
    , m_changeset_cooker{std::move(changeset_cooker)}
{
}

inline ClientHistoryImpl::Arrays::Arrays(Allocator& alloc) noexcept
    : root{alloc}
    , cooked_history{alloc}
{
    cooked_history.set_parent(&root, s_cooked_history_iip);
}

// Clamp the beginning of the specified upload skippable version range to the
// beginning of the synchronization history.
//
// A version range whose beginning is related to
// `m_progress_download.last_intergated_client_version` is susceptible to fall
// wholly, or partly before the beginning of the synchronization history due to
// aggressive trimming.
//
// This is not a problem because
//
// - all such ranges are used in contexts where upload skippable history entries
//   have no effect,
//
// - the beginning of such a range is always greater than or equal to
//   `m_progress_download.last_integrated_client_version`, and
//
// - the trimming rules of the synchronization history ensure that whenever such
//   a range refers to a history entry that is no longer in the history, then
//   that entry is upload skippable.
//
// See trim_sync_history() for further details, and in particular, for a
// definition of *upload skippable*.
inline void ClientHistoryImpl::clamp_sync_version_range(version_type& begin, version_type& end) const noexcept
{
    REALM_ASSERT(begin <= end);
    REALM_ASSERT(m_progress_download.last_integrated_client_version <= begin);
    if (begin < m_sync_history_base_version) {
        begin = m_sync_history_base_version;
        if (end < m_sync_history_base_version)
            end = m_sync_history_base_version;
    }
}

inline auto ClientHistoryImpl::get_transformer() -> Transformer&
{
    if (!m_transformer)
        m_transformer = sync::make_transformer(); // Throws
    return *m_transformer;
}


} // namespace _impl
} // namespace realm

#endif // REALM_NOINST_CLIENT_HISTORY_IMPL_HPP
