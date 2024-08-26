///////////////////////////////////////////////////////////////////////////
//
// Copyright 2022 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REALM_NOINST_CLIENT_HISTORY_IMPL_HPP
#define REALM_NOINST_CLIENT_HISTORY_IMPL_HPP

#include <realm/array_integer.hpp>
#include <realm/sync/client_base.hpp>
#include <realm/sync/history.hpp>
#include <realm/sync/instruction_replication.hpp>
#include <realm/sync/transform.hpp>
#include <realm/util/functional.hpp>
#include <realm/util/optional.hpp>

namespace realm::_impl::client_reset {
struct RecoveredChange;
}

namespace realm::sync {

class ClientReplication;
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
//  11   Path-based instruction format for MongoDB Realm Sync (v10)
//
//       Cooked history was removed, except to verify that there is no cooked history.
//
//  12   History entries are compressed.

constexpr int get_client_history_schema_version() noexcept
{
    return 12;
}

class IntegrationException : public Exception {
public:
    IntegrationException(ErrorCodes::Error error, std::string message,
                         ProtocolError error_for_server = ProtocolError::other_session_error)
        : Exception(error, message)
        , error_for_server(error_for_server)
    {
    }

    explicit IntegrationException(Status status)
        : Exception(std::move(status))
        , error_for_server(ProtocolError::other_session_error)
    {
    }

    ProtocolError error_for_server;
};

class ClientHistory final : public _impl::History, public TransformHistory {
public:
    using version_type = sync::version_type;

    struct UploadChangeset {
        timestamp_type origin_timestamp;
        file_ident_type origin_file_ident;
        UploadCursor progress;
        ChunkedBinaryData changeset;
        std::unique_ptr<char[]> buffer;
    };

    /// set_history_adjustments() is used by client reset to adjust the
    /// content of the history compartment. The DB associated with
    /// this history object must be in a write transaction when this function
    /// is called.
    void set_history_adjustments(util::Logger& logger, version_type current_version,
                                 SaltedFileIdent client_file_ident, SaltedVersion server_version,
                                 const std::vector<_impl::client_reset::RecoveredChange>&);

    struct LocalChange {
        version_type version;
        ChunkedBinaryData changeset;
    };
    /// get_local_changes returns a list of changes which have not been uploaded yet
    /// 'current_version' is the version that the history should be updated to.
    ///
    /// The history must be in a transaction when this function is called.
    std::vector<LocalChange> get_local_changes(version_type current_version) const;

    /// Get the version of the latest snapshot of the associated Realm, as well
    /// as the client file identifier and the synchronization progress as they
    /// are stored in that snapshot.
    ///
    /// The returned current client version is the version produced by the last
    /// changeset in the history. The type of version returned here, is the one
    /// that identifies an entry in the sync history. Whether this is the same
    /// as the snapshot number of the Realm file depends on the history
    /// implementation.
    ///
    /// The returned client file identifier is the one that was last stored by
    /// set_client_file_ident(), or `SaltedFileIdent{0, 0}` if
    /// set_client_file_ident() has never been called.
    ///
    /// The returned SyncProgress is the one that was last stored by
    /// set_sync_progress(), or `SyncProgress{}` if set_sync_progress() has
    /// never been called.
    void get_status(version_type& current_client_version, SaltedFileIdent& client_file_ident,
                    SyncProgress& progress) const;

    /// Stores the server assigned client file identifier in the associated
    /// Realm file, such that it is available via get_status() during future
    /// synchronization sessions. It is an error to set this identifier more
    /// than once per Realm file.
    ///
    /// \param client_file_ident The server assigned client-side file
    /// identifier. A client-side file identifier is a non-zero positive integer
    /// strictly less than 2**64. The server guarantees that all client-side
    /// file identifiers generated on behalf of a particular server Realm are
    /// unique with respect to each other. The server is free to generate
    /// identical identifiers for two client files if they are associated with
    /// different server Realms.
    ///
    /// \param fix_up_object_ids The object ids that depend on client file ident
    /// will be fixed in both state and history if this parameter is true. If
    /// it is known that there are no objects to fix, it can be set to false to
    /// achieve higher performance.
    ///
    /// The client is required to obtain the file identifier before engaging in
    /// synchronization proper, and it must store the identifier and use it to
    /// reestablish the connection between the client file and the server file
    /// when engaging in future synchronization sessions.
    void set_client_file_ident(SaltedFileIdent client_file_ident, bool fix_up_object_ids);

    /// Stores the synchronization progress in the associated Realm file in a
    /// way that makes it available via get_status() during future
    /// synchronization sessions. Progress is reported by the server in the
    /// DOWNLOAD message.
    ///
    /// See struct SyncProgress for a description of \a progress.
    ///
    /// \param downloadable_bytes If specified, and if the implementation cares
    /// about byte-level progress, this function updates the persistent record
    /// of the estimate of the number of remaining bytes to be downloaded.
    void set_sync_progress(const SyncProgress& progress, DownloadableProgress downloadable_bytes, VersionInfo&);

    /// \brief Scan through the history for changesets to be uploaded.
    ///
    /// This function scans the history for changesets to be uploaded, i.e., for
    /// changesets that are not empty, and were not produced by integration of
    /// changesets recieved from the server. The scan begins at the position
    /// specified by the initial value of \a upload_progress.client_version, and
    /// ends no later than at the position specified by \a end_version.
    ///
    /// The implementation is allowed to end the scan before \a end_version,
    /// such as to limit the combined size of returned changesets. However, if
    /// the specified range contains any changesets that are supposed to be
    /// uploaded, this function must return at least one.
    ///
    /// Upon return, \a upload_progress will have been updated to point to the
    /// position from which the next scan should resume. This must be a position
    /// after the last returned changeset, and before any remaining changesets
    /// that are supposed to be uploaded, although never a position that
    /// succeeds \a end_version.
    ///
    /// The value passed as \a upload_progress by the caller, must either be one
    /// that was produced by an earlier invocation of
    /// find_uploadable_changesets(), one that was returned by get_status(), or
    /// one that was received by the client in a DOWNLOAD message from the
    /// server. When the value comes from a DOWNLOAD message, it is supposed to
    /// reflect a value of UploadChangeset::progress produced by an earlier
    /// invocation of find_uploadable_changesets().
    ///
    /// Found changesets are added to \a uploadable_changesets.
    ///
    /// \param locked_server_version will be set to the value that should be
    /// used as `<locked server version>` in a DOWNLOAD message.
    ///
    /// For changesets of local origin, UploadChangeset::origin_file_ident will
    /// be zero.
    void find_uploadable_changesets(UploadCursor& upload_progress, version_type end_version,
                                    std::vector<UploadChangeset>& uploadable_changesets,
                                    version_type& locked_server_version) const;

    /// \brief Integrate a sequence of changesets received from the server using
    /// a single Realm transaction.
    ///
    /// Each changeset will be transformed as if by a call to
    /// Transformer::transform_remote_changeset(), and then applied to the
    /// associated Realm.
    ///
    /// As a final step, each changeset will be added to the local history (list
    /// of applied changesets).
    ///
    /// This function checks whether the specified changesets specify valid
    /// remote origin file identifiers and whether the changesets contain valid
    /// sequences of instructions. The caller must already have ensured that the
    /// origin file identifiers are strictly positive and not equal to the file
    /// identifier assigned to this client by the server.
    ///
    /// If any of the changesets are invalid, this function returns false and
    /// sets `integration_error` to the appropriate value. If they are all
    /// deemed valid, this function updates \a version_info to reflect the new
    /// version produced by the transaction.
    ///
    /// \param progress The synchronization progress is what was received in the
    /// DOWNLOAD message along with the specified changesets. The progress will
    /// be persisted along with the changesets.
    ///
    /// \param downloadable_bytes If specified, and if the implementation cares
    /// about byte-level progress, this function updates the persistent record
    /// of the estimate of the number of remaining bytes to be downloaded.
    ///
    /// \param transact If specified, it is a transaction to be used to commit
    /// the server changesets after they were transformed.
    /// Note: In FLX, the transaction is left in reading state when bootstrap ends.
    /// In all other cases, the transaction is left in reading state when the function returns.
    void integrate_server_changesets(
        const SyncProgress& progress, DownloadableProgress downloadable_bytes,
        util::Span<const RemoteChangeset> changesets, VersionInfo& new_version, DownloadBatchState download_type,
        util::Logger&, const TransactionRef& transact,
        util::UniqueFunction<void(const Transaction&, util::Span<Changeset>)> run_in_write_tr = nullptr);

    static void get_upload_download_state(Transaction&, Allocator& alloc, std::uint_fast64_t&, DownloadableProgress&,
                                          std::uint_fast64_t&, std::uint_fast64_t&, std::uint_fast64_t&,
                                          version_type&);
    static void get_upload_download_state(DB*, std::uint_fast64_t&, std::uint_fast64_t&);

    /// Record the current download progress.
    ///
    /// This is used when storing FLX bootstraps to make the progress available
    /// to other processes which are observing the file. It must be called
    /// inside of a write transaction. The data stored here is only meaningful
    /// until the next call of integrate_server_changesets(), which will
    /// overwrite it.
    static void set_download_progress(Transaction& tr, DownloadableProgress);

    // Overriding member functions in realm::TransformHistory
    version_type find_history_entry(version_type, version_type, HistoryEntry&) const noexcept override;
    ChunkedBinaryData get_reciprocal_transform(version_type, bool&) const override;
    void set_reciprocal_transform(version_type, BinaryData) override;

public: // Stuff in this section is only used by CLI tools.
    /// set_local_origin_timestamp_override() allows you to override the origin timestamp of new changesets
    /// of local origin. This should only be used for testing and defaults to calling
    /// generate_changeset_timestamp().
    void set_local_origin_timestamp_source(util::UniqueFunction<timestamp_type()> source_fn);

private:
    friend class ClientReplication;
    static constexpr version_type s_initial_version = 1;

    ClientHistory(ClientReplication& owner)
        : m_replication(owner)
    {
    }

    ClientReplication& m_replication;
    DB* m_db = nullptr;

    /// The version on which the first changeset in the continuous transactions
    /// history is based, or if that history is empty, the version associated
    /// with currently bound snapshot. In general, `m_ct_history_base_version +
    /// m_ct_history.size()` is equal to the version that is associated with the
    /// currently bound snapshot, but after add_ct_history_entry() is called, it
    /// is equal to that plus one.
    mutable version_type m_ct_history_base_version = 0;

    /// Version on which the first changeset in the synchronization history is
    /// based, or if that history is empty, the version on which the next
    /// changeset, that is added, is based.  In general,
    /// `m_sync_history_base_version + m_sync_history_size` is equal to the
    /// version, that is associated with the currently bound snapshot, but after
    /// add_sync_history_entry() is called, it is equal to that plus one.
    mutable version_type m_sync_history_base_version = 0;

    using IntegerBpTree = BPlusTree<int64_t>;
    struct Arrays {
        // Create the client history arrays in the target group
        Arrays(DB&, Group& group);
        // Initialize accessors for the existing history arrays
        Arrays(Allocator& alloc, Group* group, ref_type ref);

        void init_from_ref(ref_type ref);
        void verify() const;

        // Root of history compartment
        Array root;

        /// Continuous transactions history
        BinaryColumn ct_history;

        /// A column of changesets, one row for each entry in the history.
        ///
        /// FIXME: Ideally, the B+tree accessor below should have been just
        /// Bptree<BinaryData>, but Bptree<BinaryData> seems to not allow that yet.
        BinaryColumn changesets;
        BinaryColumn reciprocal_transforms;

        IntegerBpTree remote_versions;
        IntegerBpTree origin_file_idents;
        IntegerBpTree origin_timestamps;

    private:
        Arrays(Allocator&) noexcept;
    };

    // clang-format off

    // Sizes of fixed-size arrays
    static constexpr int s_root_size            = 21;
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
    static constexpr int s_cooked_history_iip = 19;                     // ref (removed)
    static constexpr int s_schema_versions_iip = 20;                    // table ref

    // Slots in root array of `schema_versions` table
    static constexpr int s_sv_schema_versions_iip = 0;   // integer
    static constexpr int s_sv_library_versions_iip = 1;  // ref
    static constexpr int s_sv_snapshot_versions_iip = 2; // integer (version_type)
    static constexpr int s_sv_timestamps_iip = 3;        // integer (seconds since epoch)

    // clang-format on

    // The construction of the array accessors need to be delayed, because the
    // allocator (Allocator) is not known at the time of construction of the
    // ServerHistory object.
    mutable util::Optional<Arrays> m_arrays;

    // When applying server changesets, we create a history entry with the data
    // from the server instead of using the one generated from applying the
    // instructions to the local data. integrate_server_changesets() sets this
    // to true to indicate to add_changeset() that it should skip creating a
    // history entry.
    //
    // This field is guarded by the DB's write lock and should only be accessed
    // while that is held.
    mutable bool m_applying_server_changeset = false;
    bool m_applying_client_reset = false;

    // Cache of s_progress_download_server_version_iip and
    // s_progress_download_client_version_iip slots of history compartment root
    // array.
    mutable DownloadCursor m_progress_download = {0, 0};

    version_type m_version_of_oldest_bound_snapshot = 0;

    util::UniqueFunction<timestamp_type()> m_local_origin_timestamp_source = generate_changeset_timestamp;

    void initialize(DB& db) noexcept
    {
        m_db = &db;
    }

    static version_type find_sync_history_entry(Arrays& arrays, version_type base_version, version_type begin_version,
                                                version_type end_version, HistoryEntry& entry,
                                                version_type& last_integrated_server_version) noexcept;

    // sum_of_history_entry_sizes calculates the sum of the changeset sizes of the local history
    // entries that produced a version that succeeds `begin_version` and precedes `end_version`.
    std::uint_fast64_t sum_of_history_entry_sizes(version_type begin_version,
                                                  version_type end_version) const noexcept;

    size_t transform_and_apply_server_changesets(util::Span<Changeset> changesets_to_integrate, TransactionRef,
                                                 util::Logger&, std::uint64_t& downloaded_bytes,
                                                 bool allow_lock_release);

    void prepare_for_write();
    Replication::version_type add_changeset(BinaryData changeset, BinaryData sync_changeset);
    void add_sync_history_entry(const HistoryEntry&);
    void update_sync_progress(const SyncProgress&, DownloadableProgress downloadable_bytes);
    void trim_ct_history();
    void trim_sync_history();
    void do_trim_sync_history(std::size_t n);
    void clamp_sync_version_range(version_type& begin, version_type& end) const noexcept;
    void fix_up_client_file_ident_in_stored_changesets(Transaction&, file_ident_type);
    void record_current_schema_version();
    static void record_current_schema_version(Array& schema_versions, version_type snapshot_version);
    void compress_stored_changesets();

    size_t sync_history_size() const noexcept
    {
        return m_arrays ? m_arrays->changesets.size() : 0;
    }
    size_t ct_history_size() const noexcept
    {
        return m_arrays ? m_arrays->ct_history.size() : 0;
    }

    // Overriding member functions in realm::_impl::History
    void set_group(Group* group, bool updated = false) override;
    void update_from_ref_and_version(ref_type ref, version_type version) override;
    void update_from_parent(version_type current_version) override;
    void get_changesets(version_type, version_type, BinaryIterator*) const noexcept override;
    void set_oldest_bound_version(version_type) override;
    void verify() const override;
    bool no_pending_local_changes(version_type version) const override;
};

class ClientReplication final : public SyncReplication {
public:
    ClientReplication(bool apply_server_changes = true)
        : m_history(*this)
        , m_apply_server_changes(apply_server_changes)
    {
    }

    // A write validator factory takes a write transaction and returns a UniqueFunction containing a
    // SyncReplication::WriteValidator. The factory will get called at the start of a write transaction
    // and the WriteValidator it returns will be re-used for all mutations within the transaction.
    using WriteValidatorFactory = util::UniqueFunction<WriteValidator>(Transaction&);
    void set_write_validator_factory(util::UniqueFunction<WriteValidatorFactory> validator_factory)
    {
        m_write_validator_factory = std::move(validator_factory);
    }

    // Overriding member functions in realm::Replication
    void initialize(DB& sg) override;
    HistoryType get_history_type() const noexcept override;
    int get_history_schema_version() const noexcept override;
    bool is_upgradable_history_schema(int) const noexcept override;
    void upgrade_history_schema(int) override;

    _impl::History* _get_history_write() override
    {
        return &m_history;
    }
    std::unique_ptr<_impl::History> _create_history_read() override
    {
        auto hist = std::unique_ptr<ClientHistory>(new ClientHistory(*this));
        hist->initialize(*m_history.m_db);
        return hist;
    }

    // Overriding member functions in realm::Replication
    version_type prepare_changeset(const char*, size_t, version_type) override;

    ClientHistory& get_history() noexcept
    {
        return m_history;
    }

    const ClientHistory& get_history() const noexcept
    {
        return m_history;
    }

    bool apply_server_changes() const noexcept
    {
        return m_apply_server_changes;
    }

protected:
    util::UniqueFunction<WriteValidator> make_write_validator(Transaction& tr) override;

private:
    ClientHistory m_history;
    const bool m_apply_server_changes;
    util::UniqueFunction<WriteValidatorFactory> m_write_validator_factory;
};


// Implementation

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
inline void ClientHistory::clamp_sync_version_range(version_type& begin, version_type& end) const noexcept
{
    REALM_ASSERT(begin <= end);
    REALM_ASSERT(m_progress_download.last_integrated_client_version <= begin);
    if (begin < m_sync_history_base_version) {
        begin = m_sync_history_base_version;
        if (end < m_sync_history_base_version)
            end = m_sync_history_base_version;
    }
}


/// \brief Create a "sync history" implementation of the realm::Replication
/// interface.
///
/// The intended role for such an object is as a plugin for new
/// realm::DB objects.
std::unique_ptr<ClientReplication> make_client_replication();

} // namespace realm::sync

#endif // REALM_NOINST_CLIENT_HISTORY_IMPL_HPP
