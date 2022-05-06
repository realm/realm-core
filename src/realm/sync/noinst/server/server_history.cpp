#include <algorithm>
#include <cstring>
#include <stack>

#include <realm/sync/changeset_encoder.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/impl/clamped_hex_dump.hpp>
#include <realm/sync/instruction_applier.hpp>
#include <realm/sync/noinst/compact_changesets.hpp>
#include <realm/sync/noinst/server/server_history.hpp>
#include <realm/table_view.hpp>
#include <realm/util/hex_dump.hpp>
#include <realm/util/input_stream.hpp>
#include <realm/util/value_reset_guard.hpp>
#include <realm/version.hpp>

using namespace realm;
using namespace realm::sync;
using namespace realm::util;
using namespace _impl;


/*

Client file            Entry  Client  Identifier  Proxy  Reciprocal   Last seen      Locked server
entry type             index  type    Salt        file   history (1)  timestamp (2)  version
---------------------------------------------------------------------------------------------------
Special (3)            0      0       no          no     YES          no             no
Root (4)               1      0       no          no     no           no             no
Upstream (5)           2+     0       no          no     no           no             no
Self (6)               2+     6       no          no     no           no             no
Indirect client        2+     1       no          YES    no           no             no
Legacy (7)             2+     5       YES         no     YES          YES            YES
Direct regular client  2+     2       YES         no     YES          YES            YES
Direct subserver       2+     4       YES         no     YES          YES            YES
Direct partial view    2+     3       YES         no     YES          YES            YES

1) Entries that may have a reciprocal history may also have nonzero
   `client_version` and nonzero `rh_base_version`. The reciprocal history is
   absent in expired entries.

2) An entry that represents a direct client has expired if and only if this
   timestamp is zero. Here, subservers, partial views, and legacy entries are
   considered to be direct clients.

3) This is a special entry used to represent the upstream server on a subtier
   node of a star topology server cluster. Note that there is no valid client
   file identifier associated with this entry, because a valid client file
   identifier is strictly greater than zero.

4) This entry represents the root of a star topology server cluster. This entry
   is always present. For files that do not function as partial views (or have
   not yet been initialized as such) and do not reside on a subtier node of a
   star topology server cluster (or have not yet been initialized as such), this
   entry represents the file itself.

5) For a nonpartial file on a subtier server, this is an entry for the upstream
   server or for a file reachable via the upstream server. For a partial file,
   this is an entry for the reference file, or a file reachable via the
   reference file.

6) This is the entry that represents the file itself. It is present if if the
   file functions as a partial view (and has been initialized as such), or if it
   resides on a subtier node of a star topology server cluster (and has been
   initialized as such).

7) This is an entry created prior to history schema version 10. The exact type
   of this entry is unknown. However, since the star topology feature has not
   been publicly released yet, it is likely to represent either an immediate
   regular client, an immediate partial view, or a client of an immediate
   partial view.


Abstract history schema
-----------------------

  table client_files:
    int ident_salt
    int client_version
    int rh_base_version (0 if recip_hist is null) (doubles as last server version integrated by client in
`client_version`)
    // recip_hist_index = produced_server_version - recip_hist_base_version - 1
    table recip_hist: (nullable)
      string changeset (nullable)
    link proxy_file (references client_files)
    int client_type
    int last_seen (UNIX timestamp)
    int locked_server_version

  // Server version on which the first entry in `sync_history` is based. This
  // will be zero unless the history has been trimmed (note, history trimming is
  // not yet implemented, so for now, it is always zero).
  //
  // `curr_server_version = history_base_version + sync_history.size()`
  int history_base_version

  // Server version until which the history has been compacted. This can never
  // decrease.
  int compacted_until_version

  // Salt attached to `history_base_version`. This is required to be zero if
  // `history_base_version` is zero.
  int base_version_salt

  // History of server versions (one entry per server version).
  //
  // For entries whose changesets are of local origin, `origin_file` is
  // zero. For other entries, `origin_file` points to the entry in
  // `client_files` representing the file in which the initial untransformed
  // changeset originated.
  //
  // For entries whose changesets are of local origin, `client_version` is
  // always zero. For other entries, the a changeset was produced by integration
  // of a changeset received from a remote peer (client or upstream server), and
  // `client_version` is the version produced on that remote peer by the
  // received changeset.
  //
  // `history_entry_index = produced_server_version - history_base_version - 1`
  table sync_history:
    int version_salt (random) (formerly known as server_session_ident)
    link origin_file (references client_files)
    int client_version
    int timestamp
    string changeset

  // Continuous transactions history (one entry per Realm version (snapshot number), trimmed)
  //
  // `ct_history_entry_index = realm_version - ct_history_base_version - 1`
  table ct_history:
    link history_entry (nullable) (references sync_history) (null for empty changesets of local origin) // FIXME: This
looks wrong!

  int history_byte_size

  // This object is only present after a successful invocation of
  // ServerHistory::add_upstream_sync_status()
  optional object upstream_status:
    SaltedFileIdent client_file_ident
    SaltedVersion   progress_latest_server_version
    DownloadCursor  progress_download
    UploadCursor    progress_upload

  // This array is only present after a successful invocation of
  // ServerHistory::initiate_as_partial_view()
  optional array partial_sync:
    // A file identifier allocated in the context of both the reference, and the
    // partial Realm, and with the purpose of identifying changesets in the
    // history of the reference Realm, that originate from, or via the partial
    // Realm.
    //
    // At the same time, it is used to identify outgoing changesets in the
    // history of the partial Realm that originate from, or via the reference
    // Realm.
    int partial_file_ident
    int partial_file_ident_salt
    int progress_server_version
    int progress_reference_version
    int progress_reference_version_salt



History representation
----------------------

  mixed_array_ref history:
    0 -> mixed_array_ref client_files:
      0 -> int_bptree_ref client_file_ident_salts:
        file_ident -> ident_salt
      1 -> int_bptree_ref client_file_client_versions:
        file_ident -> client_version
      2 -> int_bptree_ref client_file_rh_base_versions:
        file_ident -> rh_base_version
      3 -> mixed_bptree_ref cf_recip_hist_refs:
        file_ident -> nullable_binary_bptree_ref recip_hist:
          recip_hist_index -> changeset
      4 -> int_bptree_ref cf_proxy_files:
        file_ident -> proxy_file_ident
      5 -> int_bptree_ref cf_client_types:
        file_ident -> client_type
      6 -> int_bptree_ref cf_last_seen:
        file_indet -> unix_timestamp
      7 -> int_bptree_ref cf_locked_server_versions:
        file_ident -> locked_server_version
    1 -> tagged_int history_base_version
    2 -> tagged_int base_version_salt
    3 -> mixed_array_ref sync_history:
      0 -> int_bptree_ref sh_version_salts:
        history_entry_index -> server_version_salt
      1 -> int_bptree_ref sh_origin_files:
        history_entry_index -> file_ident
      2 -> int_bptree_ref sh_client_versions:
        history_entry_index -> sync_history.client_version
      3 -> int_bptree_ref sync_history_timestamps:
        history_entry_index -> sync_history.timestamp
      4 -> binary_bptree_ref sh_changesets:
        history_entry_index -> sync_history.changeset
    4 -> binary_bptree_ref ct_history
      Core history_entry_index -> (changeset in Core's format)
    5 -> tagged_int history_byte_size
    6 -> ref to ObjectIDHistoryState
    7 -> int_array_ref upstream_status:
      0 -> int client_file_ident
      1 -> int client_file_ident_salt
      2 -> int progress_latest_server_version
      3 -> int progress_latest_server_version_salt
      4 -> int progress_download_server_version
      5 -> int progress_download_client_version
      6 -> int progress_upload_client_version
      7 -> int progress_upload_server_version
    8 -> int_array_ref partial_sync:
      0 -> int partial_file_ident
      1 -> int partial_file_ident_salt
      2 -> int progress_server_version
      3 -> int progress_reference_version
      4 -> int progress_reference_version_salt
    9 -> tagged_int compacted_until_version
   10 -> tagged_int last_compaction_at


History compaction
------------------

History compaction refers to the act of running log compaction on the server's
main history periodically. The server considers whether history compaction
should run after every successfully integrated changeset, local or uploaded from
a client.

See also `/doc/history_compaction.md` in the `realm-sync` Git repository.

The decision to compact the history depends on a number of variable:

  - The "last seen" timestamp associated with each client, which is updated
    every time an action is performed related to a client, such as receiving an
    uploaded changeset or producing a DOWNLOAD message for that client.

  - The `rh_base_version` associated with a client. This indicates the highest
    known server version that a given client may rely on.

  - The configuration parameter `history_ttl`. Client that are "last seen"
    within this time interval will have their `rh_base_version` taken into
    consideration, i.e. the server will not compact history such that it
    interferes with this client and causes it to be reset.

  - The configuration parameter `history_compaction_interval`. The server will
    consider compacting the history on average every time this amount of time
    has passed. A random fuzz factor of `history_compaction_interval/2` is
    applied in order to avoid cascading effects (meaning it is unlikely that the
    server decides to compact many files at the same time).

  - The configuration parameter `disable_history_compaction`, which can be used
    to turn off log compaction altogether.

An expired client is one where the difference between the "last seen" timestamp
and `now` is higher than `history_ttl`, and its `rh_base_version` is higher than
`compacted_until_version`. If a client has been offline for longer than
`history_ttl`, but the server has not decided to run log compaction further than
its `rh_base_version`, that client is not expired.

A client may also have an `rh_base_version` lower than `compacted_until_version`
while it is downloading the compacted history. In this case, that client's "last
seen" timestamp will be recent, and the server will not compact the history any
further while this download is taking place.


History trimming (NOT YET IMPLEMENTED)
----------------

History trimming refers to the act of removing a prefix of the history for the
purpose of limiting the size of it. History trimming is not yet supported. Also,
if the history was trimmed, there would be no way for a new client to get
started (because of the lack of the initial prefix of the history).

On the other hand, with partial synchronization, it is expected that there will
be a way to bootstrap a client from the latest server version, rather than by
sending the entire history to the client. With such a mechanism in place,
history trimming becomes possible.

Still, if a client has been offline for a long time, and then reconnects, it is
possible that it refers to an old server version that no longer exists. In such
a case, the client needs to reset itself before it can continue. However, after
it does that, it can resume synchronization by being bootstrapped off of the
last server version as explained above.

The need for a client reset needs to be detectable by the server during session
initiation (no later than at the time of reception of the IDENT message). There
are two cases to consider:

 - The client wishes to resume download from an expired server version.

 - The next changeset uploaded by the client specifies a last integrated server
   version that has expired. The only feasible way we can check this at session
   initiation time, is to check whether `recip_hist_base_version` precedes the
   history base version (alternatively the trimming process would discard any
   `client_files` entry where `recip_hist_base_version` would become less than
   `history_base_version`. However, to make this fair for the client, it is
   necessary that the protocol is expanded to allow for the client to tell the
   server about integrated server versions even when the client has nothing to
   upload.

*/


namespace {

// This is the hard-coded file identifier that represents changes of local
// origin in a file on the root node of a star topology server cluster, or a
// file on a server that is not part of a cluster.
constexpr ServerHistory::file_ident_type g_root_node_file_ident = 1;


} // unnamed namespace


void ServerHistory::get_status(VersionInfo& version_info, bool& has_upstream_sync_status,
                               file_ident_type& partial_file_ident,
                               version_type& partial_progress_reference_version) const
{
    TransactionRef rt = m_db->start_read(); // Throws
    version_type realm_version = rt->get_version();
    const_cast<ServerHistory*>(this)->set_group(rt.get());
    ensure_updated(realm_version); // Throws
    version_info.realm_version = realm_version;
    version_info.sync_version = get_salted_server_version();
    has_upstream_sync_status = (m_acc && m_acc->upstream_status.is_attached());
    bool is_initiated_as_partial_view = (m_acc && m_acc->partial_sync.is_attached());
    if (is_initiated_as_partial_view) {
        partial_file_ident = file_ident_type(m_acc->partial_sync.get(s_ps_partial_file_ident_iip));
        REALM_ASSERT(partial_file_ident != 0);
        partial_progress_reference_version =
            version_type(m_acc->partial_sync.get(s_ps_progress_reference_version_iip));
    }
    else {
        partial_file_ident = 0;
        partial_progress_reference_version = 0;
    }
}


version_type ServerHistory::get_compacted_until_version() const
{
    TransactionRef rt = m_db->start_read(); // Throws
    version_type realm_version = rt->get_version();
    const_cast<ServerHistory*>(this)->set_group(rt.get());
    ensure_updated(realm_version); // Throws
    if (m_acc && m_acc->root.is_attached()) {
        auto value = m_acc->root.get_as_ref_or_tagged(s_compacted_until_version_iip).get_as_int();
        return version_type(value);
    }
    return 0;
}


void ServerHistory::allocate_file_identifiers(FileIdentAllocSlots& slots, VersionInfo& version_info)
{
    TransactionRef tr = m_db->start_write(); // Throws
    version_type realm_version = tr->get_version();
    ensure_updated(realm_version); // Throws
    prepare_for_write();           // Throws

    if (REALM_UNLIKELY(m_acc->upstream_status.is_attached())) {
        throw util::runtime_error("Cannot allocate new client file identifiers in a file "
                                  "that is associated with an upstream server");
    }

    for (FileIdentAllocSlot& slot : slots)
        slot.file_ident = allocate_file_ident(slot.proxy_file, slot.client_type); // Throws

    version_type new_realm_version = tr->commit(); // Throws
    version_info.realm_version = new_realm_version;
    version_info.sync_version = get_salted_server_version();
}


bool ServerHistory::register_received_file_identifier(file_ident_type received_file_ident,
                                                      file_ident_type proxy_file_ident, ClientType client_type,
                                                      salt_type& file_ident_salt, VersionInfo& version_info)
{
    TransactionRef tr = m_db->start_write(); // Throws
    version_type realm_version = tr->get_version();
    ensure_updated(realm_version); // Throws
    prepare_for_write();           // Throws

    salt_type salt = 0;
    bool success = try_register_file_ident(received_file_ident, proxy_file_ident, client_type,
                                           salt); // Throws
    if (REALM_UNLIKELY(!success))
        return false;

    version_type new_realm_version = tr->commit(); // Throws
    file_ident_salt = salt;
    version_info.realm_version = new_realm_version;
    version_info.sync_version = get_salted_server_version();
    return true;
}


bool ServerHistory::integrate_client_changesets(const IntegratableChangesets& integratable_changesets,
                                                VersionInfo& version_info, bool& backup_whole_realm,
                                                IntegrationResult& result, util::Logger& logger)
{
    REALM_ASSERT(!integratable_changesets.empty());

    // Determine the order in which to process client files. Client files with
    // serialized transactions must be processed first. At most one of the
    // available serialized transactions can succeed.
    //
    // Subordinately, the order in which to process client files is randomized
    // to prevent integer ordering between client file identifiers from giving
    // unfair priority to some client files.
    std::vector<file_ident_type>& client_file_order = m_client_file_order_buffer;
    client_file_order.clear();
    bool has_changesets = false;
    for (const auto& pair : integratable_changesets) {
        file_ident_type client_file_ident = pair.first;
        client_file_order.push_back(client_file_ident);
        const IntegratableChangesetList& list = pair.second;
        if (list.has_changesets())
            has_changesets = true;
    }

    result = {};
    for (;;) {
        if (has_changesets) {
            result.partial_clear();
        }

        bool anything_to_do = (integratable_changesets.size() > result.excluded_client_files.size());
        if (REALM_UNLIKELY(!anything_to_do))
            return false;

        file_ident_type current_client_file_ident = 0;
        ExtendedIntegrationError current_error_potential = {};
        std::size_t num_changesets_to_dump = 0;
        bool dump_changeset_info = false;

        try {
            TransactionRef tr = m_db->start_write(); // Throws
            version_type realm_version = tr->get_version_of_current_transaction().version;
            ensure_updated(realm_version); // Throws
            prepare_for_write();           // Throws

            bool dirty = false;
            bool backup_whole_realm_2 = false;
            for (file_ident_type client_file_ident : client_file_order) {
                REALM_ASSERT(client_file_ident > 0);
                REALM_ASSERT(client_file_ident != g_root_node_file_ident);
                REALM_ASSERT(client_file_ident != m_local_file_ident);
                bool excluded =
                    (result.excluded_client_files.find(client_file_ident) != result.excluded_client_files.end());
                if (REALM_UNLIKELY(excluded))
                    continue;
                current_client_file_ident = client_file_ident;
                // Verify that the client file entry has not expired
                current_error_potential = ExtendedIntegrationError::client_file_expired;
                std::size_t client_file_index = std::size_t(client_file_ident);
                std::int_fast64_t last_seen_timestamp = m_acc->cf_last_seen_timestamps.get(client_file_index);
                bool expired = (last_seen_timestamp == 0);
                if (REALM_UNLIKELY(expired))
                    goto error;
                const IntegratableChangesetList& list = integratable_changesets.find(client_file_ident)->second;
                std::vector<RemoteChangeset> changesets;
                current_error_potential = ExtendedIntegrationError::bad_origin_file_ident;
                for (const IntegratableChangeset& ic : list.changesets) {
                    REALM_ASSERT(ic.client_file_ident == client_file_ident);
                    // Verify that the origin file identifier either is the
                    // client's file identifier, or a file identifier of a
                    // subordinate client for which the sending client acts as a
                    // proxy.
                    file_ident_type origin_file_ident = ic.origin_file_ident;
                    if (origin_file_ident != 0) {
                        static_assert(g_root_node_file_ident == 1, "");
                        if (REALM_UNLIKELY(origin_file_ident <= g_root_node_file_ident))
                            goto error;
                        if (REALM_UNLIKELY(std::uint_fast64_t(origin_file_ident) >= m_num_client_files))
                            goto error;
                        std::size_t index = std::size_t(origin_file_ident);
                        file_ident_type proxy_file_ident = file_ident_type(m_acc->cf_proxy_files.get(index));
                        if (REALM_UNLIKELY(proxy_file_ident != ic.client_file_ident))
                            goto error;
                    }
                    RemoteChangeset changeset{ic};
                    changesets.push_back(changeset);             // Throws
                    result.integrated_changesets.push_back(&ic); // Throws
                }

                auto integrate = [&](std::size_t begin, std::size_t end, UploadCursor upload_progress) {
                    const RemoteChangeset* changesets_2 = changesets.data() + begin;
                    std::size_t num_changesets = end - begin;
                    num_changesets_to_dump += num_changesets;
                    bool dirty_2 = integrate_remote_changesets(
                        client_file_ident, upload_progress, list.locked_server_version, changesets_2, num_changesets,
                        logger); // Throws
                    if (dirty_2) {
                        dirty = true;
                        bool backup_whole_realm_3 =
                            (begin == end || upload_progress.client_version != changesets[end - 1].remote_version ||
                             list.locked_server_version != upload_progress.last_integrated_server_version);
                        if (backup_whole_realm_3)
                            backup_whole_realm_2 = true;
                    }
                };

                // Note: This value will be read if an exception is thrown
                // below. Clang's static analyzer incorrectly reports it as
                // a dead store.
                current_error_potential = ExtendedIntegrationError::bad_changeset;
                static_cast<void>(current_error_potential);

                std::size_t num_changesets = changesets.size();
                std::size_t aug_num_changesets = num_changesets;
                logger.debug("Integrating %1 changesets from client file %2", aug_num_changesets, client_file_ident);

                integrate(0, num_changesets, list.upload_progress); // Throws
            }

            if (dirty) {
                bool force = false;
                bool dirty_2 = do_compact_history(logger, force); // Throws
                if (dirty_2)
                    backup_whole_realm_2 = true;

                auto ta = util::make_temp_assign(m_is_local_changeset, false, true);
                version_info.realm_version = tr->commit(); // Throws
                version_info.sync_version = get_salted_server_version();
                if (backup_whole_realm_2)
                    backup_whole_realm = true;
                return true;
            }
            return false;
        }
        catch (BadChangesetError& e) {
            logger.error("Failed to parse, or apply changeset received from client: %1",
                         e.what()); // Throws
            dump_changeset_info = true;
        }
        catch (TransformError& e) {
            logger.error("Failed to transform changeset received from client: %1",
                         e.what()); // Throws
            dump_changeset_info = true;
        }

        if (dump_changeset_info) {
            std::size_t changeset_ndx = 0;
            std::size_t num_parts = num_changesets_to_dump;
            for (std::size_t i = 0; i < num_parts; ++i) {
                // Regular changeset
                const IntegratableChangeset& ic = *result.integrated_changesets[changeset_ndx];
                std::string hex_dump = util::hex_dump(ic.changeset.data(),
                                                      ic.changeset.size()); // Throws
                logger.error("Failed transaction (part %1/%2): Changeset "
                             "(client_file_ident=%3, origin_timestamp=%4, "
                             "origin_file_ident=%5, client_version=%6, "
                             "last_integrated_server_version=%7): %8",
                             (i + 1), num_parts, ic.client_file_ident, ic.origin_timestamp, ic.origin_file_ident,
                             ic.upload_cursor.client_version, ic.upload_cursor.last_integrated_server_version,
                             hex_dump); // Throws
                ++changeset_ndx;
                continue;
            }
        }

    error:
        REALM_ASSERT(current_client_file_ident != 0);
        result.excluded_client_files[current_client_file_ident] = current_error_potential; // Throws
    }
}


auto ServerHistory::integrate_backup_idents_and_changeset(
    version_type expected_realm_version, salt_type server_version_salt,
    const FileIdentAllocSlots& file_ident_alloc_slots,
    const std::vector<IntegratableChangeset>& integratable_changesets, util::Logger& logger) -> IntegratedBackup
{
    IntegratedBackup result;
    result.success = false;

    try {
        TransactionRef tr = m_db->start_write(); // Throws
        version_type realm_version = tr->get_version_of_current_transaction().version;
        ensure_updated(realm_version); // Throws
        prepare_for_write();           // Throws

        result.version_info.realm_version = realm_version;

        if (realm_version + 1 != expected_realm_version)
            return result;

        // To ensure identity of a server Realm and its backup, it is necessary
        // to set the server_version_salt of the backup Realm to the same value
        // as that of the original Realm.
        m_salt_for_new_server_versions = server_version_salt;

        for (const auto& slot : file_ident_alloc_slots) {
            if (std::uint_fast64_t(slot.file_ident.ident) != m_num_client_files)
                return result;
            add_client_file(slot.file_ident.salt, slot.proxy_file, slot.client_type); // Throws
        }

        std::map<file_ident_type, std::vector<RemoteChangeset>> changesets;

        for (const IntegratableChangeset& ic : integratable_changesets)
            changesets[ic.client_file_ident].push_back(ic);

        for (auto& pair : changesets) {
            file_ident_type client_file_ident = pair.first;
            // FIXME: Backup should also get the proper upload progress and
            // locked server version. This requires extending the backup
            // protocol.
            const auto& back = pair.second.back();
            UploadCursor upload_progress = {back.remote_version, back.last_integrated_local_version};
            version_type locked_server_version = upload_progress.last_integrated_server_version;
            integrate_remote_changesets(client_file_ident, upload_progress, locked_server_version, pair.second.data(),
                                        pair.second.size(), logger); // Throws
        }

        auto ta = util::make_temp_assign(m_is_local_changeset, false, true);
        result.version_info.realm_version = tr->commit(); // Throws
        result.version_info.sync_version = get_salted_server_version();
        result.success = true;
    }
    catch (BadChangesetError& e) {
        logger.error("Bad incremental backup", e.what()); // Throws
    }
    catch (TransformError& e) {
        logger.error("Bad incremental backup", e.what()); // Throws
    }

    return result;
}


auto ServerHistory::allocate_file_ident(file_ident_type proxy_file_ident, ClientType client_type) -> SaltedFileIdent
{
    REALM_ASSERT(!m_acc->upstream_status.is_attached());

    std::size_t file_index = m_num_client_files;
    salt_type salt = register_client_file_by_index(file_index, proxy_file_ident, client_type); // Throws

    if (file_index > std::uint_fast64_t(get_max_file_ident()))
        throw util::overflow_error{"File identifier"};

    file_ident_type ident = file_ident_type(file_index);
    return {ident, salt};
}


void ServerHistory::register_assigned_file_ident(file_ident_type file_ident)
{
    file_ident_type proxy_file_ident = 0; // No proxy
    ClientType client_type = ClientType::self;
    salt_type file_ident_salt; // Dummy
    bool success = try_register_file_ident(file_ident, proxy_file_ident, client_type,
                                           file_ident_salt); // Throws
    REALM_ASSERT(success);
}


bool ServerHistory::try_register_file_ident(file_ident_type file_ident, file_ident_type proxy_file_ident,
                                            ClientType client_type, salt_type& file_ident_salt)
{
    REALM_ASSERT(m_acc->upstream_status.is_attached());
    static_assert(g_root_node_file_ident == 1, "");
    if (REALM_UNLIKELY(file_ident < 2))
        return false;
    std::size_t max = std::numeric_limits<std::size_t>::max();
    if (REALM_UNLIKELY(std::uint_fast64_t(file_ident) > max))
        throw util::overflow_error{"Client file index"};
    std::size_t file_index = std::size_t(file_ident);
    if (file_index < m_num_client_files)
        return false;
    file_ident_salt = register_client_file_by_index(file_index, proxy_file_ident, client_type); // Throws
    return true;
}


auto ServerHistory::register_client_file_by_index(std::size_t file_index, file_ident_type proxy_file_ident,
                                                  ClientType client_type) -> salt_type
{
    REALM_ASSERT(file_index >= m_num_client_files);
    REALM_ASSERT(proxy_file_ident == 0 || is_valid_proxy_file_ident(proxy_file_ident));
    bool generate_salt = is_direct_client(client_type);
    salt_type salt = 0;
    if (generate_salt) {
        auto max_salt = 0x0'7FFF'FFFF'FFFF'FFFF;
        std::mt19937_64& random = m_context.server_history_get_random();
        salt = std::uniform_int_distribution<salt_type>(1, max_salt)(random);
    }
    while (file_index > m_num_client_files)
        add_client_file(0, 0, ClientType::upstream);      // Throws
    add_client_file(salt, proxy_file_ident, client_type); // Throws
    return salt;
}


bool ServerHistory::ensure_upstream_file_ident(file_ident_type file_ident)
{
    REALM_ASSERT(m_acc->upstream_status.is_attached());

    static_assert(g_root_node_file_ident == 1, "");
    if (REALM_UNLIKELY(file_ident < 2))
        return (file_ident == 1);
    std::size_t max = std::numeric_limits<std::size_t>::max();
    if (REALM_UNLIKELY(std::uint_fast64_t(file_ident) > max))
        throw util::overflow_error{"Client file index"};
    std::size_t file_index = std::size_t(file_ident);
    if (REALM_LIKELY(file_index < m_num_client_files)) {
        auto client_type = m_acc->cf_client_types.get(file_index);
        if (REALM_UNLIKELY(client_type != int(ClientType::upstream)))
            return false;
        REALM_ASSERT(m_acc->cf_ident_salts.get(file_index) == 0);
        REALM_ASSERT(m_acc->cf_proxy_files.get(file_index) == 0);
        return true;
    }
    do
        add_client_file(0, 0, ClientType::upstream); // Throws
    while (file_index >= m_num_client_files);
    return true;
}


void ServerHistory::add_client_file(salt_type file_ident_salt, file_ident_type proxy_file_ident,
                                    ClientType client_type)
{
    switch (client_type) {
        case ClientType::upstream:
        case ClientType::self:
            REALM_ASSERT(file_ident_salt == 0);
            REALM_ASSERT(proxy_file_ident == 0);
            break;
        case ClientType::indirect:
            REALM_ASSERT(file_ident_salt == 0);
            REALM_ASSERT(proxy_file_ident != 0);
            break;
        case ClientType::regular:
        case ClientType::subserver:
            REALM_ASSERT(file_ident_salt != 0);
            REALM_ASSERT(proxy_file_ident == 0);
            break;
        case ClientType::legacy:
            REALM_ASSERT(false);
            break;
    }
    std::int_fast64_t client_version = 0;
    std::int_fast64_t recip_hist_base_version = 0;
    ref_type recip_hist_ref = 0;
    std::int_fast64_t last_seen_timestamp = 0;
    std::int_fast64_t locked_server_version = 0;
    if (is_direct_client(client_type)) {
        auto now_1 = m_context.get_compaction_clock_now();
        auto now_2 = std::chrono::duration_cast<std::chrono::seconds>(now_1.time_since_epoch());
        last_seen_timestamp = std::int_fast64_t(now_2.count());
        // Make sure we never assign zero, as that means "expired"
        if (REALM_UNLIKELY(last_seen_timestamp <= 0))
            last_seen_timestamp = 1;
    }
    m_acc->cf_ident_salts.insert(realm::npos, std::int_fast64_t(file_ident_salt));  // Throws
    m_acc->cf_client_versions.insert(realm::npos, client_version);                  // Throws
    m_acc->cf_rh_base_versions.insert(realm::npos, recip_hist_base_version);        // Throws
    m_acc->cf_recip_hist_refs.insert(realm::npos, recip_hist_ref);                  // Throws
    m_acc->cf_proxy_files.insert(realm::npos, std::int_fast64_t(proxy_file_ident)); // Throws
    m_acc->cf_client_types.insert(realm::npos, std::int_fast64_t(client_type));     // Throws
    m_acc->cf_last_seen_timestamps.insert(realm::npos, last_seen_timestamp);        // Throws
    m_acc->cf_locked_server_versions.insert(realm::npos, locked_server_version);    // Throws
    std::size_t max_size = std::numeric_limits<std::size_t>::max();
    if (m_num_client_files == max_size)
        throw util::overflow_error{"Client file index"};
    ++m_num_client_files;
}


void ServerHistory::save_upstream_sync_progress(const SyncProgress& progress)
{
    Array& us = m_acc->upstream_status;
    us.set(s_us_progress_download_server_version_iip,
           std::int_fast64_t(progress.download.server_version)); // Throws
    us.set(s_us_progress_download_client_version_iip,
           std::int_fast64_t(progress.download.last_integrated_client_version)); // Throws
    us.set(s_us_progress_latest_server_version_iip,
           std::int_fast64_t(progress.latest_server_version.version)); // Throws
    us.set(s_us_progress_latest_server_version_salt_iip,
           std::int_fast64_t(progress.latest_server_version.salt)); // Throws
    us.set(s_us_progress_upload_client_version_iip,
           std::int_fast64_t(progress.upload.client_version)); // Throws
    us.set(s_us_progress_upload_server_version_iip,
           std::int_fast64_t(progress.upload.last_integrated_server_version)); // Throws
}


auto ServerHistory::do_bootstrap_client_session(SaltedFileIdent client_file_ident, DownloadCursor download_progress,
                                                SaltedVersion server_version, ClientType client_type,
                                                UploadCursor& upload_progress, version_type& locked_server_version,
                                                Logger& logger) const noexcept -> BootstrapError
{
    REALM_ASSERT(is_direct_client(client_type));
    REALM_ASSERT(client_type != ClientType::legacy);

    // Validate `client_file_ident`
    if (!m_acc)
        return BootstrapError::bad_client_file_ident;
    {
        bool good =
            (client_file_ident.ident >= 1 && util::int_less_than(client_file_ident.ident, m_num_client_files));
        if (!good)
            return BootstrapError::bad_client_file_ident;
    }
    std::size_t client_file_index = std::size_t(client_file_ident.ident);
    {
        auto correct_salt = salt_type(m_acc->cf_ident_salts.get(client_file_index));
        bool good = (correct_salt != 0 && // Prevent (spoofed) match on special entries with no salt
                     client_file_ident.salt == correct_salt);
        if (!good)
            return BootstrapError::bad_client_file_ident_salt;
    }

    // Besides being superfluous, it is also a protocol violation if a client
    // asks to download from a point before the base of its reciprocal history.
    auto recip_hist_base_version = version_type(m_acc->cf_rh_base_versions.get(client_file_index));
    if (download_progress.server_version < recip_hist_base_version) {
        logger.debug("Bad download progress: %1 < %2", download_progress.server_version, recip_hist_base_version);
        return BootstrapError::bad_download_server_version;
    }

    // If the main history has been trimmed or compacted to a point beyond the
    // beginning of the reciprocal history, then the client file entry has
    // expired.
    //
    // NOTE: History trimming (removal of leading history entries) is currently
    // never done on server-side files.
    //
    // NOTE: For an overview of the in-place history compaction mechanism, see
    // `/doc/history_compaction.md` in the `realm-sync` Git repository.
    std::int_fast64_t last_seen_timestamp = m_acc->cf_last_seen_timestamps.get(client_file_index);
    bool expired_due_to_compaction = (last_seen_timestamp == 0);
    if (REALM_UNLIKELY(expired_due_to_compaction)) {
        logger.debug("Client expired because history has been compacted");
        return BootstrapError::client_file_expired;
    }

    REALM_ASSERT_RELEASE(recip_hist_base_version >= m_history_base_version);

    // Validate `download_progress`
    version_type current_server_version = get_server_version();
    if (download_progress.server_version > current_server_version)
        return BootstrapError::bad_download_server_version;
    auto last_integrated_client_version = version_type(m_acc->cf_client_versions.get(client_file_index));
    if (download_progress.last_integrated_client_version > last_integrated_client_version)
        return BootstrapError::bad_download_client_version;

    // Validate `server_version`
    {
        bool good = (server_version.version >= download_progress.server_version &&
                     server_version.version <= current_server_version);
        if (!good)
            return BootstrapError::bad_server_version;
    }
    {
        salt_type correct_salt = get_server_version_salt(server_version.version);
        bool good = (server_version.salt == correct_salt);
        if (!good)
            return BootstrapError::bad_server_version_salt;
    }

    // Validate client type
    {
        auto client_type_2 = ClientType(m_acc->cf_client_types.get(client_file_index));
        bool good = (client_type_2 == ClientType::legacy || client_type == client_type_2);
        if (!good)
            return BootstrapError::bad_client_type;
    }

    upload_progress.client_version = last_integrated_client_version;
    upload_progress.last_integrated_server_version = recip_hist_base_version;
    locked_server_version = version_type(m_acc->cf_locked_server_versions.get(client_file_index));
    return BootstrapError::no_error;
}


auto ServerHistory::bootstrap_client_session(SaltedFileIdent client_file_ident, DownloadCursor download_progress,
                                             SaltedVersion server_version, ClientType client_type,
                                             UploadCursor& upload_progress, version_type& locked_server_version,
                                             Logger& logger) const -> BootstrapError
{
    TransactionRef tr = m_db->start_read(); // Throws
    auto realm_version = tr->get_version();
    const_cast<ServerHistory*>(this)->set_group(tr.get());
    ensure_updated(realm_version); // Throws

    BootstrapError error = do_bootstrap_client_session(client_file_ident, download_progress, server_version,
                                                       client_type, upload_progress, locked_server_version, logger);
    return error;
}


bool ServerHistory::fetch_download_info(file_ident_type client_file_ident, DownloadCursor& download_progress,
                                        version_type end_version, UploadCursor& upload_progress,
                                        HistoryEntryHandler& handler,
                                        std::uint_fast64_t& cumulative_byte_size_current,
                                        std::uint_fast64_t& cumulative_byte_size_total,
                                        bool disable_download_compaction,
                                        std::size_t accum_byte_size_soft_limit) const
{
    REALM_ASSERT(client_file_ident != 0);
    REALM_ASSERT(download_progress.server_version <= end_version);

    TransactionRef tr = m_db->start_read(); // Throws
    version_type realm_version = tr->get_version();
    const_cast<ServerHistory*>(this)->set_group(tr.get());
    ensure_updated(realm_version); // Throws

    REALM_ASSERT(download_progress.server_version >= m_history_base_version);

    std::size_t client_file_index = std::size_t(client_file_ident);
    {
        auto client_type = ClientType(m_acc->cf_client_types.get(client_file_index));
        REALM_ASSERT_RELEASE(is_direct_client(client_type));
        std::int_fast64_t last_seen_timestamp = m_acc->cf_last_seen_timestamps.get(client_file_index);
        bool expired = (last_seen_timestamp == 0);
        if (REALM_UNLIKELY(expired))
            return false;
    }

    std::size_t accum_byte_size = 0;
    DownloadCursor download_progress_2 = download_progress;

    std::vector<Changeset> changesets;
    std::vector<std::size_t> original_changeset_sizes;
    if (!disable_download_compaction) {
        std::size_t reserve = to_size_t(end_version - download_progress_2.server_version);
        changesets.reserve(reserve);               // Throws
        original_changeset_sizes.reserve(reserve); // Throws
    }

    for (;;) {
        version_type begin_version = download_progress_2.server_version;
        HistoryEntry entry;
        version_type version = find_history_entry(client_file_ident, begin_version, end_version, entry,
                                                  download_progress_2.last_integrated_client_version);
        if (version == 0) {
            // End of history reached
            download_progress_2.server_version = end_version;
            break;
        }

        download_progress_2.server_version = version;

        entry.remote_version = download_progress_2.last_integrated_client_version;

        if (entry.origin_file_ident == 0)
            entry.origin_file_ident = m_local_file_ident;

        if (!disable_download_compaction) {
            ChunkedBinaryInputStream stream{entry.changeset};
            Changeset changeset;
            parse_changeset(stream, changeset); // Throws
            changeset.version = download_progress_2.server_version;
            changeset.last_integrated_remote_version = entry.remote_version;
            changeset.origin_timestamp = entry.origin_timestamp;
            changeset.origin_file_ident = entry.origin_file_ident;
            changesets.push_back(std::move(changeset));                 // Throws
            original_changeset_sizes.push_back(entry.changeset.size()); // Throws
        }
        else {
            handler.handle(download_progress_2.server_version, entry, entry.changeset.size()); // Throws
        }

        accum_byte_size += entry.changeset.size();

        if (accum_byte_size > accum_byte_size_soft_limit)
            break;
    }

    if (!disable_download_compaction) {
        compact_changesets(changesets.data(), changesets.size());

        ChangesetEncoder::Buffer encode_buffer;
        for (std::size_t i = 0; i < changesets.size(); ++i) {
            auto& changeset = changesets[i];
            encode_changeset(changeset, encode_buffer); // Throws
            HistoryEntry entry;
            entry.remote_version = changeset.last_integrated_remote_version;
            entry.origin_file_ident = changeset.origin_file_ident;
            entry.origin_timestamp = changeset.origin_timestamp;
            entry.changeset = BinaryData{encode_buffer.data(), encode_buffer.size()};
            handler.handle(changeset.version, entry, original_changeset_sizes[i]); // Throws
            encode_buffer.clear();
        }
    }

    // Set cumulative byte sizes.
    std::int_fast64_t cumulative_byte_size_current_2 = 0;
    std::int_fast64_t cumulative_byte_size_total_2 = 0;
    if (download_progress_2.server_version > m_history_base_version) {
        std::size_t begin_ndx = to_size_t(download_progress_2.server_version - m_history_base_version) - 1;
        cumulative_byte_size_current_2 = m_acc->sh_cumul_byte_sizes.get(begin_ndx);
        REALM_ASSERT(cumulative_byte_size_current_2 >= 0);
    }
    if (m_history_size > 0) {
        std::size_t end_ndx = m_history_size - 1;
        cumulative_byte_size_total_2 = m_acc->sh_cumul_byte_sizes.get(end_ndx);
    }
    REALM_ASSERT(cumulative_byte_size_current_2 <= cumulative_byte_size_total_2);

    version_type upload_client_version = version_type(m_acc->cf_client_versions.get(client_file_index));
    version_type upload_server_version = version_type(m_acc->cf_rh_base_versions.get(client_file_index));

    download_progress = download_progress_2;
    cumulative_byte_size_current = std::uint_fast64_t(cumulative_byte_size_current_2);
    cumulative_byte_size_total = std::uint_fast64_t(cumulative_byte_size_total_2);
    upload_progress = UploadCursor{upload_client_version, upload_server_version};

    return true;
}


void ServerHistory::add_upstream_sync_status()
{
    TransactionRef tr = m_db->start_write(); // Throws
    version_type realm_version = tr->get_version();
    ensure_updated(realm_version); // Throws
    prepare_for_write();           // Throws

    REALM_ASSERT(!m_acc->upstream_status.is_attached());
    REALM_ASSERT(m_local_file_ident == g_root_node_file_ident);

    // An upstream status cannot be added to a file from which new client file
    // identifiers have already been allocated, since in a star topology server
    // cluster, all file identifiers must be allocated by the root node.
    static_assert(g_root_node_file_ident == 1, "");
    if (REALM_UNLIKELY(m_num_client_files > 2)) {
        throw util::runtime_error("Realm file has registered client file identifiers, "
                                  "so can no longer be associated with upstream server "
                                  "(star topology server cluster)");
    }

    bool context_flag_no = false;
    std::size_t size = s_upstream_status_size;
    m_acc->upstream_status.create(Array::type_Normal, context_flag_no, size); // Throws
    _impl::ShallowArrayDestroyGuard adg{&m_acc->upstream_status};
    m_acc->upstream_status.update_parent(); // Throws
    adg.release();                          // Ref ownership transferred to parent array
    tr->commit();                           // Throws
}

bool ServerHistory::compact_history(const TransactionRef& wt, Logger& logger)
{
    version_type realm_version = wt->get_version();
    ensure_updated(realm_version); // Throws
    prepare_for_write();           // Throws
    bool force = true;
    return do_compact_history(logger, force); // Throws
}


std::vector<sync::Changeset> ServerHistory::get_parsed_changesets(version_type begin, version_type end) const
{
    TransactionRef rt = m_db->start_read(); // Throws
    version_type realm_version = rt->get_version();
    const_cast<ServerHistory*>(this)->set_group(rt.get());
    ensure_updated(realm_version);

    REALM_ASSERT(begin > m_history_base_version);
    if (end == version_type(-1)) {
        end = m_history_base_version + m_history_size + 1;
    }
    REALM_ASSERT(begin <= end);

    std::vector<sync::Changeset> changesets;
    changesets.reserve(std::size_t(end - begin)); // Throws
    for (version_type version = begin; version < end; ++version) {
        std::size_t ndx = std::size_t(version - m_history_base_version - 1);
        Changeset changeset;

        auto binary = ChunkedBinaryData{m_acc->sh_changesets, ndx};
        ChunkedBinaryInputStream stream{binary};
        parse_changeset(stream, changeset); // Throws

        // Add the attributes for the changeset.
        changeset.last_integrated_remote_version = m_acc->sh_client_versions.get(ndx);
        changeset.origin_file_ident = m_acc->sh_origin_files.get(ndx);
        changeset.origin_timestamp = m_acc->sh_timestamps.get(ndx);
        changeset.version = version;
        changesets.emplace_back(std::move(changeset));
    }
    return changesets;
}


bool ServerHistory::do_compact_history(Logger& logger, bool force)
{
    // NOTE: For an overview of the in-place history compaction mechanism, see
    // `/doc/history_compaction.md` in the `realm-sync` Git repository.

    // Must be in write transaction!

    static const std::size_t compaction_input_soft_limit = 1024 * 1024 * 1024; // 1 GB
    namespace chrono = std::chrono;

    if (!m_enable_compaction)
        return false;

    bool dirty = false;

    // Flush "last seen" cache.
    std::size_t num_client_files = m_acc->cf_rh_base_versions.size();
    REALM_ASSERT(m_acc->cf_last_seen_timestamps.size() == num_client_files);
    {
        using Entry = CompactionControl::LastClientAccessesEntry;
        using Range = CompactionControl::LastClientAccessesRange;
        Range range = m_compaction_control.get_last_client_accesses(); // Throws
        const Entry* begin = range.first;
        const Entry* end = range.second;
        for (auto i = begin; i != end; ++i) {
            std::size_t client_file_index = std::size_t(i->client_file_ident);
            REALM_ASSERT_RELEASE(client_file_index < num_client_files);
            auto client_type = ClientType(m_acc->cf_client_types.get(client_file_index));
            REALM_ASSERT(is_direct_client(client_type));
            // Take the opportunity to upgrade legacy entries when their type
            // gets discovered
            if (client_type == ClientType::legacy) {
                client_type = ClientType::regular;
                m_acc->cf_client_types.set(client_file_index, std::int_fast64_t(client_type)); // Throws
            }
            // Take care to never de-expire a client file entry
            std::int_fast64_t last_seen_timestamp = m_acc->cf_last_seen_timestamps.get(client_file_index);
            bool expired = (last_seen_timestamp == 0);
            if (REALM_UNLIKELY(expired))
                continue;
            last_seen_timestamp = std::int_fast64_t(i->last_seen_timestamp);
            // Make sure we never assign zero, as that means "expired"
            if (REALM_UNLIKELY(last_seen_timestamp <= 0))
                last_seen_timestamp = 1;
            m_acc->cf_last_seen_timestamps.set(client_file_index, last_seen_timestamp); // Throws
            dirty = true;
        }
    }

    REALM_ASSERT(m_compaction_ttl.count() != 0);

    // Decide whether we should compact the history now, based on the average
    // history compaction interval plus/minus a fuzz factor (currently half the
    // interval).
    auto now = m_context.get_compaction_clock_now();
    chrono::seconds last_compaction_time_from_epoch{
        m_acc->root.get_as_ref_or_tagged(s_last_compaction_timestamp_iip).get_as_int()};
    chrono::system_clock::time_point last_compaction_time{last_compaction_time_from_epoch};
    auto duration_since_last_compaction = chrono::duration_cast<chrono::seconds>(now - last_compaction_time);
    auto& random = m_context.server_history_get_random();
    auto duration_fuzz =
        std::uniform_int_distribution<std::int_fast64_t>(0, m_compaction_interval.count() / 2)(random);
    auto minimum_duration_until_compact =
        chrono::duration_cast<chrono::seconds>(m_compaction_interval) + chrono::seconds{duration_fuzz};
    if (!force && duration_since_last_compaction.count() < minimum_duration_until_compact.count()) {
        logger.trace("History compaction: Skipping because we are still within the compaction interval (%1 < %2)",
                     duration_since_last_compaction.count(), minimum_duration_until_compact.count()); // Throws
        return dirty;
    }

    version_type compacted_until_version =
        version_type(m_acc->root.get_as_ref_or_tagged(s_compacted_until_version_iip).get_as_int());

    version_type current_version = get_server_version();
    if (current_version <= compacted_until_version) {
        logger.trace("History compaction: Everything is already compacted (%1 <= %2)", current_version,
                     compacted_until_version); // Throws
        return dirty;
    }

    version_type limit_due_to_state_realms = m_compaction_control.get_max_compactable_server_version(); // Throws
    if (limit_due_to_state_realms <= compacted_until_version) {
        logger.debug("History compaction: Further progress blocked by state Realms (%1 <= %2)",
                     limit_due_to_state_realms, compacted_until_version); // Throws
        return dirty;
    }

    version_type can_compact_until_version = current_version;
    bool has_upstream_sync_status = m_acc->upstream_status.is_attached();
    if (has_upstream_sync_status) {
        // This is a subtier server, so the upstream entry must be taken into
        // account, and it can never be allowed to expire.
        std::size_t client_file_index = 0;
        auto rh_base_version = version_type(m_acc->cf_rh_base_versions.get(client_file_index));
        auto locked_version = rh_base_version;
        if (locked_version <= compacted_until_version) {
            logger.debug("History compaction: Further progress blocked by upstream server, which "
                         "has not progressed far enough in terms of synchronization (%1 <= %2)",
                         locked_version, compacted_until_version); // Throws
            return dirty;
        }
        if (locked_version < can_compact_until_version)
            can_compact_until_version = locked_version;
    }
    auto expire_client_file = [&](std::size_t client_file_index) {
        // Mark as expired
        m_acc->cf_last_seen_timestamps.set(client_file_index, 0); // Throws
        // Discard reciprocal history
        ref_type recip_hist_ref = to_ref(m_acc->cf_recip_hist_refs.get(client_file_index));
        if (recip_hist_ref != 0) {
            Allocator& alloc = m_acc->cf_recip_hist_refs.get_alloc();
            BinaryColumn recip_hist{alloc};
            recip_hist.init_from_ref(recip_hist_ref);
            recip_hist.destroy();                                // Throws
            m_acc->cf_recip_hist_refs.set(client_file_index, 0); // Throws
        }
        dirty = true;
    };
    if (REALM_LIKELY(!m_compaction_ignore_clients)) {
        for (std::size_t i = 1; i < num_client_files; ++i) {
            auto client_type = ClientType(m_acc->cf_client_types.get(i));
            if (REALM_LIKELY(!is_direct_client(client_type)))
                continue;
            REALM_ASSERT(m_acc->cf_ident_salts.get(i) != 0);
            REALM_ASSERT(m_acc->cf_proxy_files.get(i) == 0);
            file_ident_type file_ident = file_ident_type(i);
            REALM_ASSERT(file_ident != g_root_node_file_ident);
            REALM_ASSERT(file_ident != m_local_file_ident);
            std::int_fast64_t last_seen_timestamp = m_acc->cf_last_seen_timestamps.get(i);
            bool previously_expired = (last_seen_timestamp == 0);
            if (REALM_LIKELY(previously_expired))
                continue;
            std::int_fast64_t age = 0;
            auto max_time_to_live = std::int_fast64_t(m_compaction_ttl.count());
            auto now_2 = chrono::duration_cast<chrono::seconds>(now.time_since_epoch());
            auto now_3 = std::int_fast64_t(now_2.count());
            if (REALM_LIKELY(last_seen_timestamp <= now_3)) {
                age = now_3 - last_seen_timestamp;
                bool expire_now = (age > max_time_to_live);
                if (REALM_UNLIKELY(expire_now)) {
                    logger.debug("History compaction: Expiring client file %1 due to age (%2 > "
                                 "%3)",
                                 file_ident, age, max_time_to_live); // Throws
                    expire_client_file(i);                           // Throws
                    continue;
                }
            }
            auto rh_base_version = version_type(m_acc->cf_rh_base_versions.get(i));
            auto locked_server_version = version_type(m_acc->cf_locked_server_versions.get(i));
            auto locked_version = std::min(rh_base_version, locked_server_version);
            if (locked_version <= compacted_until_version) {
                logger.debug("History compaction: Further progress blocked by client file %1, "
                             "that has not progressed far enough in terms of synchronization (%2 "
                             "<= min(%3, %4)), and has also not yet expired (%5 <= %6)",
                             file_ident, rh_base_version, locked_server_version, compacted_until_version, age,
                             max_time_to_live); // Throws
                return dirty;
            }
            if (locked_version < can_compact_until_version)
                can_compact_until_version = locked_version;
        }
    }
    else {
        auto now_2 = chrono::duration_cast<chrono::seconds>(now.time_since_epoch());
        auto now_3 = std::time_t(now_2.count());
        auto max_time_to_live = std::time_t(m_compaction_ttl.count());
        REALM_ASSERT(can_compact_until_version >= compacted_until_version);
        auto num_entries = size_t(can_compact_until_version - compacted_until_version);
        auto offset = size_t(compacted_until_version - m_history_base_version);
        for (std::size_t i = 0; i < num_entries; ++i) {
            std::size_t history_entry_index = offset + i;
            auto timestamp = timestamp_type(m_acc->sh_timestamps.get(history_entry_index));
            std::time_t seconds_since_epoch = 0;
            long nanoseconds = 0; // Dummy
            map_changeset_timestamp(timestamp, seconds_since_epoch, nanoseconds);
            std::time_t age = now_3 - seconds_since_epoch;
            if (age <= max_time_to_live) {
                if (i == 0) {
                    logger.debug("History compaction: Further progress blocked because first "
                                 "uncompacted history entry (%1) is too young (%2 <= %3)",
                                 compacted_until_version + 1, age, max_time_to_live); // Throws
                    return dirty;
                }
                can_compact_until_version = version_type(m_history_base_version + history_entry_index);
                break;
            }
        }
        // Expire all client file entries that have not yet cleared
        // `can_compact_until_version`
        std::size_t num_expirations = 0;
        for (std::size_t i = 1; i < num_client_files; ++i) {
            auto client_type = ClientType(m_acc->cf_client_types.get(i));
            if (REALM_LIKELY(!is_direct_client(client_type)))
                continue;
            REALM_ASSERT(m_acc->cf_ident_salts.get(i) != 0);
            REALM_ASSERT(m_acc->cf_proxy_files.get(i) == 0);
            file_ident_type file_ident = file_ident_type(i);
            REALM_ASSERT(file_ident != g_root_node_file_ident);
            REALM_ASSERT(file_ident != m_local_file_ident);
            std::int_fast64_t last_seen_timestamp = m_acc->cf_last_seen_timestamps.get(i);
            bool previously_expired = (last_seen_timestamp == 0);
            if (REALM_LIKELY(previously_expired))
                continue;
            auto rh_base_version = version_type(m_acc->cf_rh_base_versions.get(i));
            auto locked_server_version = version_type(m_acc->cf_locked_server_versions.get(i));
            auto locked_version = std::min(rh_base_version, locked_server_version);
            if (locked_version < can_compact_until_version) {
                logger.debug("History compaction: Expiring client file %1 due to lack of progress "
                             "(min(%2, %3) < %4)",
                             file_ident, rh_base_version, locked_server_version, can_compact_until_version); // Throws
                expire_client_file(i);                                                                       // Throws
                ++num_expirations;
            }
        }
        if (num_expirations > 0) {
            logger.info("History compaction: Increase in number of expired client files: %1",
                        num_expirations); // Throws
        }
    }

    REALM_ASSERT(can_compact_until_version > compacted_until_version);
    logger.debug("History compaction: Compacting until version %1 (was previously compacted "
                 "until version %2) (latest version is %3)",
                 can_compact_until_version, compacted_until_version, current_version); // Throws

    dirty = true;

    std::size_t num_compactable_changesets = std::size_t(can_compact_until_version - m_history_base_version);
    version_type compaction_begin_version = m_history_base_version; // always 0 for now
    std::size_t before_size = 0;
    std::size_t after_size = 0;

    // Chunk compactions to limit memory usage.
    while (compaction_begin_version < can_compact_until_version) {
        auto num_compactable_changesets_this_iteration =
            size_t(num_compactable_changesets - (compaction_begin_version - m_history_base_version));
        std::vector<Changeset> compact_bootstrap_changesets;
        compact_bootstrap_changesets.resize(num_compactable_changesets_this_iteration); // Throws
        version_type begin_version = compaction_begin_version;
        version_type end_version = compaction_begin_version;
        std::size_t compaction_input_size = 0;
        for (std::size_t i = 0; i < num_compactable_changesets_this_iteration; ++i) {
            Changeset& changeset = compact_bootstrap_changesets[i];
            version_type server_version = begin_version + i + 1;

            // Get attributes for the changeset
            changeset.version = server_version;
            changeset.last_integrated_remote_version =
                version_type(m_acc->sh_client_versions.get(size_t(server_version - 1 - m_history_base_version)));
            changeset.origin_timestamp =
                timestamp_type(m_acc->sh_timestamps.get(size_t(server_version - 1 - m_history_base_version)));
            changeset.origin_file_ident =
                file_ident_type(m_acc->sh_origin_files.get(size_t(server_version - 1 - m_history_base_version)));

            // Get the changeset itself
            ChunkedBinaryData data = get_changeset(server_version);
            before_size += data.size();
            compaction_input_size += data.size();
            ChunkedBinaryInputStream stream{data};
            parse_changeset(stream, compact_bootstrap_changesets[i]); // Throws
            end_version = server_version;
            if (compaction_input_size >= compaction_input_soft_limit)
                break;
        }

        compact_changesets(compact_bootstrap_changesets.data(),
                           compact_bootstrap_changesets.size()); // Throws


        ChangesetEncoder::Buffer buffer;
        for (std::size_t i = 0; i < num_compactable_changesets_this_iteration; ++i) {
            buffer.clear();
            encode_changeset(compact_bootstrap_changesets[i], buffer);
            after_size += buffer.size();
            version_type server_version = begin_version + i + 1;
            m_acc->sh_changesets.set(size_t(server_version - 1), BinaryData{buffer.data(), buffer.size()}); // Throws
        }
        compaction_begin_version = end_version;
    }

    // Recalculate the cumulative byte sizes.
    {
        size_t num_history_entries = m_acc->sh_changesets.size();
        REALM_ASSERT(m_acc->sh_cumul_byte_sizes.size() == num_history_entries);
        size_t history_byte_size = 0;
        for (size_t i = 0; i < num_history_entries; ++i) {
            size_t changeset_size = ChunkedBinaryData(m_acc->sh_changesets, i).size();
            history_byte_size += changeset_size;
            m_acc->sh_cumul_byte_sizes.set(i, history_byte_size);
        }
    }

    // Get new 'now' because compaction can potentially take a long time, and
    // if it takes longer than the server's average history compaction
    // interval, the server could end up spending all its time doing compaction.
    auto new_now = m_context.get_compaction_clock_now();
    auto new_now_2 = chrono::duration_cast<chrono::seconds>(new_now.time_since_epoch());
    auto new_now_3 = std::int_fast64_t(new_now_2.count());
    m_acc->root.set(s_last_compaction_timestamp_iip, RefOrTagged::make_tagged(new_now_3)); // Throws

    REALM_ASSERT(can_compact_until_version >
                 version_type(m_acc->root.get_as_ref_or_tagged(s_compacted_until_version_iip).get_as_int()));
    m_acc->root.set(s_compacted_until_version_iip,
                    RefOrTagged::make_tagged(can_compact_until_version)); // Throws

    logger.detail("History compaction: Processed %1 changesets (saved %2 bytes in %3 "
                  "milliseconds)",
                  num_compactable_changesets, before_size - after_size,
                  chrono::duration_cast<chrono::milliseconds>(new_now - now).count()); // Throws
    return dirty;
}


class ServerHistory::ReciprocalHistory : private ArrayParent {
public:
    ReciprocalHistory(BPlusTree<ref_type>& cf_recip_hist_refs, std::size_t remote_file_index,
                      version_type base_version)
        : m_cf_recip_hist_refs{cf_recip_hist_refs}
        , m_remote_file_index{remote_file_index}
        , m_base_version{base_version}
    {
        if (ref_type ref = to_ref(cf_recip_hist_refs.get(remote_file_index))) {
            init(ref);                     // Throws
            m_size = m_changesets->size(); // Relatively expensive
        }
    }

    std::size_t remote_file_index() const noexcept
    {
        return m_remote_file_index;
    }

    version_type base_version() const noexcept
    {
        return m_base_version;
    }

    std::size_t size() const noexcept
    {
        return m_size;
    }

    // Returns true iff the reciprocal history has been instantiated
    explicit operator bool() const noexcept
    {
        return bool(m_changesets);
    }

    void ensure_instantiated()
    {
        if (m_changesets)
            return;

        // Instantiate the reciprocal history
        Allocator& alloc = m_cf_recip_hist_refs.get_alloc();
        BinaryColumn recip_hist(alloc);
        recip_hist.create();
        auto ref = recip_hist.get_ref();
        DeepArrayRefDestroyGuard adg{ref, alloc};
        m_cf_recip_hist_refs.set(m_remote_file_index, ref); // Throws
        adg.release();                                      // Ref ownership transferred to parent array
        init(ref);                                          // Throws
    }

    // The reciprocal history must have been instantiated (see
    // ensure_instantiated()).
    bool get(version_type server_version, ChunkedBinaryData& transform) const noexcept
    {
        REALM_ASSERT(m_changesets);
        REALM_ASSERT(server_version > m_base_version);

        std::size_t i = std::size_t(server_version - m_base_version - 1);
        if (i < m_size) {
            ChunkedBinaryData transform_2{*m_changesets, i};
            if (!transform_2.is_null()) {
                transform = transform_2;
                return true;
            }
        }
        return false;
    }

    // The reciprocal history must have been instantiated (see
    // ensure_instantiated()).
    void set(version_type server_version, BinaryData transform)
    {
        REALM_ASSERT(m_changesets);
        REALM_ASSERT(server_version > m_base_version);
        std::size_t i = std::size_t(server_version - m_base_version - 1);
        while (m_size <= i) {
            m_changesets->add({}); // Throws
            m_size++;
        }
        // FIXME: BinaryColumn::set() currently interprets BinaryData(0,0) as
        // null. It should probably be changed such that BinaryData(0,0) is
        // always interpreted as the empty string. For the purpose of setting
        // null values, BinaryColumn::set() should accept values of type
        // util::Optional<BinaryData>().
        BinaryData transform_2 = (transform.is_null() ? BinaryData{"", 0} : transform);
        m_changesets->set(i, transform_2); // Throws
    }

    // Requires that new_base_version > base_version()
    void trim(version_type new_base_version)
    {
        REALM_ASSERT(new_base_version > m_base_version);
        std::size_t n = std::size_t(new_base_version - m_base_version);
        if (n >= m_size) {
            if (m_changesets)
                m_changesets->clear(); // Throws
            m_base_version = new_base_version;
            m_size = 0;
            return;
        }
        REALM_ASSERT(m_changesets);
        while (n) {
            m_changesets->erase(0);
            --n;
        }
        m_base_version = new_base_version;
        m_size -= n;
    }

    void update_child_ref(std::size_t child_ndx, ref_type new_ref) override final
    {
        m_cf_recip_hist_refs.set(child_ndx, new_ref); // Throws
    }

    ref_type get_child_ref(std::size_t child_ndx) const noexcept override final
    {
        return m_cf_recip_hist_refs.get(child_ndx);
    }

private:
    BPlusTree<ref_type>& m_cf_recip_hist_refs;
    const std::size_t m_remote_file_index;
    version_type m_base_version;
    std::size_t m_size = 0;
    util::Optional<BinaryColumn> m_changesets;

    void init(ref_type ref)
    {
        Allocator& alloc = m_cf_recip_hist_refs.get_alloc();
        m_changesets.emplace(alloc); // Throws
        m_changesets->init_from_ref(ref);
        m_changesets->set_parent(this, m_remote_file_index);
    }
};


class ServerHistory::TransformHistoryImpl : public TransformHistory {
public:
    TransformHistoryImpl(file_ident_type remote_file_ident, ServerHistory& history,
                         ReciprocalHistory& recip_hist) noexcept
        : m_remote_file_ident{remote_file_ident}
        , m_history{history}
        , m_recip_hist{recip_hist}
    {
    }

    version_type find_history_entry(version_type begin_version, version_type end_version,
                                    HistoryEntry& entry) const noexcept override final
    {
        return m_history.find_history_entry(m_remote_file_ident, begin_version, end_version, entry);
    }

    ChunkedBinaryData get_reciprocal_transform(version_type server_version,
                                               bool& is_compressed) const noexcept override final
    {
        is_compressed = false;
        ChunkedBinaryData transform;
        if (m_recip_hist.get(server_version, transform))
            return transform;
        HistoryEntry entry = m_history.get_history_entry(server_version);
        return entry.changeset;
    }

    void set_reciprocal_transform(version_type server_version, BinaryData transform) override final
    {
        m_recip_hist.set(server_version, transform); // Throws
    }

private:
    const file_ident_type m_remote_file_ident; // Zero for server
    ServerHistory& m_history;
    ReciprocalHistory& m_recip_hist;
};


bool ServerHistory::integrate_remote_changesets(file_ident_type remote_file_ident, UploadCursor upload_progress,
                                                version_type locked_server_version, const RemoteChangeset* changesets,
                                                std::size_t num_changesets, util::Logger& logger)
{
    std::size_t remote_file_index = std::size_t(remote_file_ident);
    REALM_ASSERT(remote_file_index < m_num_client_files);
    bool from_downstream = (remote_file_ident != 0);
    if (from_downstream) {
        auto client_type = ClientType(m_acc->cf_client_types.get(remote_file_index));
        REALM_ASSERT_RELEASE(is_direct_client(client_type));
        std::int_fast64_t last_seen_timestamp = m_acc->cf_last_seen_timestamps.get(remote_file_index);
        bool expired = (last_seen_timestamp == 0);
        REALM_ASSERT_RELEASE(!expired);
    }
    version_type orig_client_version = version_type(m_acc->cf_client_versions.get(remote_file_index));
    version_type recip_hist_base_version = version_type(m_acc->cf_rh_base_versions.get(remote_file_index));
    ReciprocalHistory recip_hist(m_acc->cf_recip_hist_refs, remote_file_index,
                                 recip_hist_base_version); // Throws

    {
        UploadCursor prev_upload_cursor = {orig_client_version, recip_hist_base_version};
        for (std::size_t i = 0; i < num_changesets; ++i) {
            // Note: remote_file_ident may be different from
            // changeset.origin_file_ident in a cluster setup.
            REALM_ASSERT(changesets[i].origin_file_ident > 0);
            UploadCursor upload_cursor = {changesets[i].remote_version, changesets[i].last_integrated_local_version};
            REALM_ASSERT(upload_cursor.client_version > prev_upload_cursor.client_version);
            REALM_ASSERT(are_mutually_consistent(upload_cursor, prev_upload_cursor));
            prev_upload_cursor = upload_cursor;
        }
    }

    if (num_changesets > 0) {
        recip_hist.ensure_instantiated(); // Throws

        version_type lowest_last_integrated_local_version = changesets[0].last_integrated_local_version;

        // Parse the changesets
        std::vector<Changeset> parsed_transformed_changesets;
        parsed_transformed_changesets.resize(num_changesets);
        for (std::size_t i = 0; i < num_changesets; ++i)
            parse_remote_changeset(changesets[i], parsed_transformed_changesets[i]); // Throws

        // Transform the changesets
        version_type current_server_version = get_server_version();
        bool may_have_causally_unrelated_changes = (current_server_version > lowest_last_integrated_local_version);
        if (may_have_causally_unrelated_changes) {
            // Merge with causally unrelated changesets, and resolve the
            // conflicts if there are any.
            TransformHistoryImpl transform_hist{remote_file_ident, *this, recip_hist};
            Transformer& transformer = m_context.get_transformer(); // Throws
            transformer.transform_remote_changesets(transform_hist, m_local_file_ident, current_server_version,
                                                    parsed_transformed_changesets.data(), num_changesets,
                                                    &logger); // Throws
        }

        // Apply the transformed changesets to the Realm state
        Group& group = *m_group;
        Transaction& transaction = dynamic_cast<Transaction&>(group);
        for (std::size_t i = 0; i < num_changesets; ++i) {
            REALM_ASSERT(get_instruction_encoder().buffer().size() == 0);
            const Changeset& changeset = parsed_transformed_changesets[i];

            HistoryEntry entry;
            entry.origin_timestamp = changeset.origin_timestamp;
            entry.origin_file_ident = changeset.origin_file_ident;
            entry.remote_version = changeset.version;

            ChangesetEncoder::Buffer changeset_buffer;

            TempShortCircuitReplication tdr{*this}; // Short-circuit while integrating changes
            InstructionApplier applier{transaction};
            applier.apply(parsed_transformed_changesets[i], &logger);             // Throws
            encode_changeset(parsed_transformed_changesets[i], changeset_buffer); // Throws
            entry.changeset = BinaryData{changeset_buffer.data(), changeset_buffer.size()};

            add_sync_history_entry(entry); // Throws
            reset();                       // Reset the instruction encoder
        }
    }

    bool dirty = (num_changesets > 0);

    if (update_upload_progress(orig_client_version, recip_hist, upload_progress)) // Throws
        dirty = true;

    if (from_downstream) {
        version_type orig_version = version_type(m_acc->cf_locked_server_versions.get(remote_file_index));
        if (locked_server_version > orig_version) {
            m_acc->cf_locked_server_versions.set(remote_file_index,
                                                 std::int_fast64_t(locked_server_version)); // Throws
            dirty = true;
        }
    }

    if (from_downstream && dirty) {
        auto now_1 = m_context.get_compaction_clock_now();
        auto now_2 = std::chrono::duration_cast<std::chrono::seconds>(now_1.time_since_epoch());
        auto last_seen_timestamp = std::int_fast64_t(now_2.count());
        // Make sure we never assign zero, as that means "expired"
        if (REALM_UNLIKELY(last_seen_timestamp <= 0))
            last_seen_timestamp = 1;
        m_acc->cf_last_seen_timestamps.set(remote_file_index, last_seen_timestamp);
    }

    return dirty;
}


bool ServerHistory::update_upload_progress(version_type orig_client_version, ReciprocalHistory& recip_hist,
                                           UploadCursor upload_progress)
{
    UploadCursor orig_upload_progress = {orig_client_version, recip_hist.base_version()};
    REALM_ASSERT(upload_progress.client_version >= orig_upload_progress.client_version);
    REALM_ASSERT(are_mutually_consistent(upload_progress, orig_upload_progress));
    std::size_t client_file_index = recip_hist.remote_file_index();
    bool update_client_version = (upload_progress.client_version > orig_upload_progress.client_version);
    if (update_client_version) {
        auto value_1 = std::int_fast64_t(upload_progress.client_version);
        m_acc->cf_client_versions.set(client_file_index, value_1); // Throws
        bool update_server_version =
            (upload_progress.last_integrated_server_version > orig_upload_progress.last_integrated_server_version);
        if (update_server_version) {
            recip_hist.trim(upload_progress.last_integrated_server_version); // Throws
            auto value_2 = std::int_fast64_t(upload_progress.last_integrated_server_version);
            m_acc->cf_rh_base_versions.set(client_file_index, value_2); // Throws
        }
        return true;
    }
    return false;
}


// Overriding member in Replication
void ServerHistory::initialize(DB& sg)
{
    REALM_ASSERT(!m_db);
    SyncReplication::initialize(sg); // Throws
    m_db = &sg;
}


// Overriding member in Replication
auto ServerHistory::get_history_type() const noexcept -> HistoryType
{
    return hist_SyncServer;
}


// Overriding member in Replication
int ServerHistory::get_history_schema_version() const noexcept
{
    return get_server_history_schema_version();
}


// Overriding member in Replication
bool ServerHistory::is_upgradable_history_schema(int stored_schema_version) const noexcept
{
    if (stored_schema_version >= 20) {
        return true;
    }
    return false;
}


// Overriding member in Replication
void ServerHistory::upgrade_history_schema(int stored_schema_version)
{
    // upgrade_history_schema() is called only when there is a need to upgrade
    // (`stored_schema_version < get_server_history_schema_version()`), and only
    // when is_upgradable_history_schema() returned true (`stored_schema_version
    // >= 1`).
    REALM_ASSERT(stored_schema_version < get_server_history_schema_version());
    REALM_ASSERT(stored_schema_version >= 1);
    int orig_schema_version = stored_schema_version;
    int schema_version = orig_schema_version;
    // NOTE: Future migration steps go here.

    REALM_ASSERT(schema_version == get_server_history_schema_version());

    // Record migration event
    record_current_schema_version(); // Throws
}


// Overriding member in Replication
_impl::History* ServerHistory::_get_history_write()
{
    return this;
}

// Overriding member in Replication
std::unique_ptr<_impl::History> ServerHistory::_create_history_read()
{
    _impl::ServerHistory::DummyCompactionControl compaction_control;
    auto server_hist = std::make_unique<ServerHistory>(m_context, compaction_control); // Throws
    server_hist->initialize(*m_db);                                                    // Throws
    return std::unique_ptr<_impl::History>(server_hist.release());
}


// Overriding member in Replication
auto ServerHistory::prepare_changeset(const char* data, std::size_t size, version_type realm_version) -> version_type
{
    ensure_updated(realm_version);
    prepare_for_write(); // Throws

    bool nonempty_changeset_of_local_origin = (m_is_local_changeset && size != 0);

    if (nonempty_changeset_of_local_origin) {
        auto& buffer = get_instruction_encoder().buffer();
        BinaryData changeset{buffer.data(), buffer.size()};
        HistoryEntry entry;
        entry.origin_timestamp = sync::generate_changeset_timestamp();
        entry.origin_file_ident = 0; // Of local origin
        entry.remote_version = 0;    // Of local origin on server-side
        entry.changeset = changeset;

        add_sync_history_entry(entry); // Throws
    }

    // Add the standard ct changeset.
    // This is done for changes of both local and remote origin.
    BinaryData core_changeset{data, size};
    add_core_history_entry(core_changeset); // Thows

    return m_ct_base_version + m_ct_history_size; // New snapshot number
}


// Overriding member in _impl::History
void ServerHistory::update_from_parent(version_type realm_version)
{
    using gf = _impl::GroupFriend;
    ref_type ref = gf::get_history_ref(*m_group);
    update_from_ref_and_version(ref, realm_version); // Throws
}


// Overriding member in _impl::History
void ServerHistory::get_changesets(version_type begin_version, version_type end_version,
                                   BinaryIterator* iterators) const noexcept
{
    REALM_ASSERT(begin_version <= end_version);
    REALM_ASSERT(begin_version >= m_ct_base_version);
    REALM_ASSERT(end_version <= m_ct_base_version + m_ct_history_size);
    std::size_t n = to_size_t(end_version - begin_version);
    REALM_ASSERT(n == 0 || m_acc);
    std::size_t offset = to_size_t(begin_version - m_ct_base_version);
    for (std::size_t i = 0; i < n; ++i) {
        iterators[i] = BinaryIterator(&m_acc->ct_history, offset + i);
    }
}


// Overriding member in _impl::History
void ServerHistory::set_oldest_bound_version(version_type realm_version)
{
    REALM_ASSERT(realm_version >= m_version_of_oldest_bound_snapshot);
    if (realm_version > m_version_of_oldest_bound_snapshot) {
        m_version_of_oldest_bound_snapshot = realm_version;
        trim_cont_transact_history(); // Throws
    }
}


// Overriding member in _impl::History
void ServerHistory::verify() const
{
#ifdef REALM_DEBUG
    // The size of the continuous transactions history can only be zero when the
    // Realm is in the initial empty state where top-ref is null.
    version_type initial_realm_version = 1;
    REALM_ASSERT(m_ct_history_size != 0 || m_ct_base_version == initial_realm_version);

    if (!m_acc) {
        REALM_ASSERT(m_local_file_ident == g_root_node_file_ident);
        REALM_ASSERT(m_num_client_files == 0);
        REALM_ASSERT(m_history_size == 0);
        REALM_ASSERT(m_server_version_salt == 0);
        REALM_ASSERT(m_history_base_version == 0);
        REALM_ASSERT(m_ct_history_size == 0);
        return;
    }

    m_acc->root.verify();
    m_acc->client_files.verify();
    m_acc->sync_history.verify();
    if (m_acc->upstream_status.is_attached())
        m_acc->upstream_status.verify();
    if (m_acc->partial_sync.is_attached())
        m_acc->partial_sync.verify();
    m_acc->cf_ident_salts.verify();
    m_acc->cf_client_versions.verify();
    m_acc->cf_rh_base_versions.verify();
    m_acc->cf_recip_hist_refs.verify();
    m_acc->cf_proxy_files.verify();
    m_acc->cf_client_types.verify();
    m_acc->cf_last_seen_timestamps.verify();
    m_acc->cf_locked_server_versions.verify();
    m_acc->sh_version_salts.verify();
    m_acc->sh_origin_files.verify();
    m_acc->sh_client_versions.verify();
    m_acc->sh_timestamps.verify();
    m_acc->sh_changesets.verify();
    m_acc->sh_cumul_byte_sizes.verify();
    m_acc->ct_history.verify();

    REALM_ASSERT(m_history_base_version == m_acc->root.get_as_ref_or_tagged(s_history_base_version_iip).get_as_int());
    salt_type base_version_salt = m_acc->root.get_as_ref_or_tagged(s_base_version_salt_iip).get_as_int();
    REALM_ASSERT((m_history_base_version == 0) == (base_version_salt == 0));

    REALM_ASSERT(m_acc->cf_ident_salts.size() == m_num_client_files);
    REALM_ASSERT(m_acc->cf_client_versions.size() == m_num_client_files);
    REALM_ASSERT(m_acc->cf_rh_base_versions.size() == m_num_client_files);
    REALM_ASSERT(m_acc->cf_recip_hist_refs.size() == m_num_client_files);
    REALM_ASSERT(m_acc->cf_proxy_files.size() == m_num_client_files);
    REALM_ASSERT(m_acc->cf_client_types.size() == m_num_client_files);
    REALM_ASSERT(m_acc->cf_last_seen_timestamps.size() == m_num_client_files);
    REALM_ASSERT(m_acc->cf_locked_server_versions.size() == m_num_client_files);

    REALM_ASSERT(m_acc->sh_version_salts.size() == m_history_size);
    REALM_ASSERT(m_acc->sh_origin_files.size() == m_history_size);
    REALM_ASSERT(m_acc->sh_client_versions.size() == m_history_size);
    REALM_ASSERT(m_acc->sh_timestamps.size() == m_history_size);
    REALM_ASSERT(m_acc->sh_changesets.size() == m_history_size);
    REALM_ASSERT(m_acc->sh_cumul_byte_sizes.size() == m_history_size);

    salt_type server_version_salt =
        (m_history_size == 0 ? base_version_salt : salt_type(m_acc->sh_version_salts.get(m_history_size - 1)));
    REALM_ASSERT(m_server_version_salt == server_version_salt);

    REALM_ASSERT(m_local_file_ident > 0 && std::uint_fast64_t(m_local_file_ident) < m_num_client_files);

    // Check history entries
    std::int_fast64_t accum_byte_size = 0;
    struct ClientFile {
        version_type last_integrated_client_version;
    };
    std::unordered_map<file_ident_type, ClientFile> client_files;
    for (std::size_t i = 0; i < m_history_size; ++i) {
        auto salt = m_acc->sh_version_salts.get(i);
        REALM_ASSERT(salt > 0 && salt <= 0x0'7FFF'FFFF'FFFF'FFFF);
        file_ident_type origin_file_ident = 0;
        REALM_ASSERT(!util::int_cast_with_overflow_detect(m_acc->sh_origin_files.get(i), origin_file_ident));
        REALM_ASSERT(origin_file_ident != m_local_file_ident);
        std::size_t origin_file_index = 0;
        REALM_ASSERT(!util::int_cast_with_overflow_detect(origin_file_ident, origin_file_index));
        REALM_ASSERT(origin_file_index < m_num_client_files);
        version_type client_version = 0;
        REALM_ASSERT(!util::int_cast_with_overflow_detect(m_acc->sh_client_versions.get(i), client_version));
        bool of_local_origin = (origin_file_ident == 0);
        if (of_local_origin) {
            REALM_ASSERT(client_version == 0);
        }
        else {
            file_ident_type client_file_ident = 0;
            bool from_reference_file = (origin_file_ident == m_local_file_ident);
            if (!from_reference_file) {
                auto client_type = m_acc->cf_client_types.get(origin_file_index);
                bool good_client_type = false;
                switch (ClientType(client_type)) {
                    case ClientType::upstream:
                        good_client_type = true;
                        break;
                    case ClientType::indirect: {
                        auto proxy_file = m_acc->cf_proxy_files.get(origin_file_index);
                        REALM_ASSERT(!util::int_cast_with_overflow_detect(proxy_file, client_file_ident));
                        good_client_type = true;
                        break;
                    }
                    case ClientType::self:
                        break;
                    case ClientType::legacy:
                    case ClientType::regular:
                    case ClientType::subserver:
                        client_file_ident = origin_file_ident;
                        good_client_type = true;
                        break;
                }
                REALM_ASSERT(good_client_type);
            }
            ClientFile& client_file = client_files[client_file_ident];
            if (from_reference_file) {
                REALM_ASSERT(client_version >= client_file.last_integrated_client_version);
            }
            else {
                REALM_ASSERT(client_version > client_file.last_integrated_client_version);
            }
            client_file.last_integrated_client_version = client_version;
        }

        std::size_t changeset_size = ChunkedBinaryData(m_acc->sh_changesets, i).size();
        accum_byte_size += changeset_size;
        REALM_ASSERT(m_acc->sh_cumul_byte_sizes.get(i) == accum_byte_size);
    }

    // Check client file entries
    version_type current_server_version = m_history_base_version + m_history_size;
    REALM_ASSERT(m_num_client_files >= 2);
    bool found_self = false;
    for (std::size_t i = 0; i < m_num_client_files; ++i) {
        file_ident_type client_file_ident = file_ident_type(i);
        auto j = client_files.find(client_file_ident);
        ClientFile* client_file = (j == client_files.end() ? nullptr : &j->second);
        version_type last_integrated_client_version = 0;
        if (client_file)
            last_integrated_client_version = client_file->last_integrated_client_version;
        auto ident_salt = m_acc->cf_ident_salts.get(i);
        auto client_version = m_acc->cf_client_versions.get(i);
        auto rh_base_version = m_acc->cf_rh_base_versions.get(i);
        auto recip_hist_ref = m_acc->cf_recip_hist_refs.get(i);
        auto proxy_file = m_acc->cf_proxy_files.get(i);
        auto client_type = m_acc->cf_client_types.get(i);
        auto last_seen_timestamp = m_acc->cf_last_seen_timestamps.get(i);
        auto locked_server_version = m_acc->cf_locked_server_versions.get(i);
        version_type client_version_2 = 0;
        REALM_ASSERT(!util::int_cast_with_overflow_detect(client_version, client_version_2));
        file_ident_type proxy_file_2 = 0;
        REALM_ASSERT(!util::int_cast_with_overflow_detect(proxy_file, proxy_file_2));
        version_type locked_server_version_2 = 0;
        REALM_ASSERT(!util::int_cast_with_overflow_detect(locked_server_version, locked_server_version_2));
        if (client_file_ident == 0) {
            // Special entry
            REALM_ASSERT(ident_salt == 0);
            REALM_ASSERT(proxy_file_2 == 0);
            REALM_ASSERT(client_type == 0);
            REALM_ASSERT(last_seen_timestamp == 0);
            REALM_ASSERT(locked_server_version_2 == 0);
            // Upstream server
            REALM_ASSERT(client_version_2 >= last_integrated_client_version);
        }
        else if (client_file_ident == g_root_node_file_ident) {
            // Root node's entry
            REALM_ASSERT(ident_salt == 0);
            REALM_ASSERT(client_version_2 == 0);
            REALM_ASSERT(rh_base_version == 0);
            REALM_ASSERT(recip_hist_ref == 0);
            REALM_ASSERT(proxy_file_2 == 0);
            REALM_ASSERT(client_type == 0);
            REALM_ASSERT(last_seen_timestamp == 0);
            REALM_ASSERT(locked_server_version_2 == 0);
            REALM_ASSERT(!client_file);
            if (m_local_file_ident == g_root_node_file_ident)
                found_self = true;
        }
        else if (client_file_ident == m_local_file_ident) {
            // Entry representing the Realm file itself
            REALM_ASSERT(ident_salt == 0);
            REALM_ASSERT(client_version_2 == 0);
            REALM_ASSERT(rh_base_version == 0);
            REALM_ASSERT(recip_hist_ref == 0);
            REALM_ASSERT(proxy_file_2 == 0);
            REALM_ASSERT(client_type == int(ClientType::self));
            REALM_ASSERT(last_seen_timestamp == 0);
            REALM_ASSERT(locked_server_version_2 == 0);
            REALM_ASSERT(!client_file);
            found_self = true;
        }
        else if (ident_salt == 0) {
            if (proxy_file_2 == 0) {
                // This entry represents a file reachable via the upstream
                // server.
                REALM_ASSERT(client_version_2 == 0);
                REALM_ASSERT(rh_base_version == 0);
                REALM_ASSERT(recip_hist_ref == 0);
                REALM_ASSERT(client_type == int(ClientType::upstream));
                REALM_ASSERT(last_seen_timestamp == 0);
                REALM_ASSERT(locked_server_version_2 == 0);
                REALM_ASSERT(!client_file);
            }
            else {
                // This entry represents a client of a direct client, such as
                // client of a partial view, or a client of a subserver.
                REALM_ASSERT(client_version_2 == 0);
                REALM_ASSERT(rh_base_version == 0);
                REALM_ASSERT(recip_hist_ref == 0);
                REALM_ASSERT(client_type == int(ClientType::indirect));
                REALM_ASSERT(last_seen_timestamp == 0);
                REALM_ASSERT(locked_server_version_2 == 0);
                REALM_ASSERT(is_valid_proxy_file_ident(proxy_file_2));
                REALM_ASSERT(!client_file);
            }
        }
        else {
            // This entry represents a direct client, which can be a regular
            // client, a subserver, or a partial view.
            bool expired = (last_seen_timestamp == 0);
            REALM_ASSERT(ident_salt > 0 && ident_salt <= 0x0'7FFF'FFFF'FFFF'FFFF);
            REALM_ASSERT(client_version_2 >= last_integrated_client_version);
            REALM_ASSERT(!expired || (recip_hist_ref == 0));
            REALM_ASSERT(proxy_file_2 == 0);
            REALM_ASSERT(is_direct_client(ClientType(client_type)));
            REALM_ASSERT(locked_server_version_2 <= current_server_version);
        }
    }
    REALM_ASSERT(found_self);

    REALM_ASSERT(m_ct_history_size >= 1); // See comment above
    REALM_ASSERT(m_acc->ct_history.size() == m_ct_history_size);
#endif // REALM_DEBUG
}

void ServerHistory::discard_accessors() const noexcept
{
    m_acc = util::none;
}


class ServerHistory::DiscardAccessorsGuard {
public:
    DiscardAccessorsGuard(const ServerHistory& sh) noexcept
        : m_server_history{&sh}
    {
    }
    ~DiscardAccessorsGuard() noexcept
    {
        if (REALM_UNLIKELY(m_server_history))
            m_server_history->discard_accessors();
    }
    void release() noexcept
    {
        m_server_history = nullptr;
    }

private:
    const ServerHistory* m_server_history;
};


// Overriding member in _impl::History
void ServerHistory::update_from_ref_and_version(ref_type ref, version_type realm_version)
{
    if (ref == 0) {
        // No history schema yet
        m_local_file_ident = g_root_node_file_ident;
        m_num_client_files = 0;
        m_history_base_version = 0;
        m_history_size = 0;
        m_server_version_salt = 0;
        m_ct_base_version = realm_version;
        m_ct_history_size = 0;
        discard_accessors();
        return;
    }
    if (REALM_LIKELY(m_acc)) {
        m_acc->init_from_ref(ref); // Throws
    }
    else {
        Allocator& alloc = _impl::GroupFriend::get_alloc(*m_group);
        m_acc.emplace(alloc);
        DiscardAccessorsGuard dag{*this};
        m_acc->init_from_ref(ref);
        _impl::GroupFriend::set_history_parent(*m_group, m_acc->root);

        if (m_acc->upstream_status.is_attached()) {
            REALM_ASSERT(m_acc->upstream_status.size() == s_upstream_status_size);
        }
        if (m_acc->partial_sync.is_attached()) {
            REALM_ASSERT(m_acc->partial_sync.size() == s_partial_sync_size);
        }
        dag.release();
    }

    if (m_acc->upstream_status.is_attached()) {
        file_ident_type file_ident = m_group->get_sync_file_id();
        m_local_file_ident = (file_ident == 0 ? g_root_node_file_ident : file_ident);
    }
    else {
        m_local_file_ident = g_root_node_file_ident;
    }

    m_num_client_files = m_acc->cf_ident_salts.size();
    REALM_ASSERT(m_acc->cf_client_versions.size() == m_num_client_files);
    REALM_ASSERT(m_acc->cf_rh_base_versions.size() == m_num_client_files);
    REALM_ASSERT(m_acc->cf_recip_hist_refs.size() == m_num_client_files);
    REALM_ASSERT(m_acc->cf_proxy_files.size() == m_num_client_files);
    REALM_ASSERT(m_acc->cf_client_types.size() == m_num_client_files);
    REALM_ASSERT(m_acc->cf_last_seen_timestamps.size() == m_num_client_files);
    REALM_ASSERT(m_acc->cf_locked_server_versions.size() == m_num_client_files);

    m_history_base_version = version_type(m_acc->root.get_as_ref_or_tagged(s_history_base_version_iip).get_as_int());
    m_history_size = m_acc->sh_changesets.size();
    REALM_ASSERT(m_acc->sh_version_salts.size() == m_history_size);
    REALM_ASSERT(m_acc->sh_origin_files.size() == m_history_size);
    REALM_ASSERT(m_acc->sh_client_versions.size() == m_history_size);
    REALM_ASSERT(m_acc->sh_timestamps.size() == m_history_size);
    REALM_ASSERT(m_acc->sh_cumul_byte_sizes.size() == m_history_size);

    m_server_version_salt =
        (m_history_size > 0 ? salt_type(m_acc->sh_version_salts.get(m_history_size - 1))
                            : salt_type(m_acc->root.get_as_ref_or_tagged(s_base_version_salt_iip).get_as_int()));

    m_ct_history_size = m_acc->ct_history.size();
    m_ct_base_version = realm_version - m_ct_history_size;
}


void ServerHistory::Accessors::init_from_ref(ref_type ref)
{
    root.init_from_ref(ref);
    client_files.init_from_parent();
    sync_history.init_from_parent();

    {
        ref_type ref_2 = upstream_status.get_ref_from_parent();
        if (ref_2 != 0) {
            upstream_status.init_from_ref(ref_2);
        }
        else {
            upstream_status.detach();
        }
    }

    cf_ident_salts.init_from_parent();            // Throws
    cf_client_versions.init_from_parent();        // Throws
    cf_rh_base_versions.init_from_parent();       // Throws
    cf_recip_hist_refs.init_from_parent();        // Throws
    cf_proxy_files.init_from_parent();            // Throws
    cf_client_types.init_from_parent();           // Throws
    cf_last_seen_timestamps.init_from_parent();   // Throws
    cf_locked_server_versions.init_from_parent(); // Throws
    sh_version_salts.init_from_parent();          // Throws
    sh_origin_files.init_from_parent();           // Throws
    sh_client_versions.init_from_parent();        // Throws
    sh_timestamps.init_from_parent();             // Throws
    sh_changesets.init_from_parent();             // Throws
    sh_cumul_byte_sizes.init_from_parent();       // Throws
    ct_history.init_from_parent();                // Throws

    // Note: If anything throws above, then accessors will be left in an
    // undefined state. However, all IntegerBpTree accessors will still have
    // a root array, and all optional BinaryColumn accessors will still
    // exist, so it will be safe to call update_from_ref() again.
}


void ServerHistory::create_empty_history()
{
    using gf = _impl::GroupFriend;

    REALM_ASSERT(m_local_file_ident == g_root_node_file_ident);
    REALM_ASSERT(m_num_client_files == 0);
    REALM_ASSERT(m_history_base_version == 0);
    REALM_ASSERT(m_history_size == 0);
    REALM_ASSERT(m_server_version_salt == 0);
    REALM_ASSERT(m_ct_history_size == 0);
    REALM_ASSERT(!m_acc);
    Allocator& alloc = m_db->get_alloc();
    m_acc.emplace(alloc);
    DiscardAccessorsGuard dag{*this};
    gf::prepare_history_parent(*m_group, m_acc->root, Replication::hist_SyncServer,
                               get_server_history_schema_version(), m_local_file_ident); // Throws
    m_acc->create();                                                                     // Throws
    dag.release();

    // Add the special client file entry (index = 0), and the root servers entry
    // (index = 1).
    static_assert(g_root_node_file_ident == 1, "");
    REALM_ASSERT(m_num_client_files == 0);
    for (int i = 0; i < 2; ++i) {
        m_acc->cf_ident_salts.insert(realm::npos, 0);            // Throws
        m_acc->cf_client_versions.insert(realm::npos, 0);        // Throws
        m_acc->cf_rh_base_versions.insert(realm::npos, 0);       // Throws
        m_acc->cf_recip_hist_refs.insert(realm::npos, 0);        // Throws
        m_acc->cf_proxy_files.insert(realm::npos, 0);            // Throws
        m_acc->cf_client_types.insert(realm::npos, 0);           // Throws
        m_acc->cf_last_seen_timestamps.insert(realm::npos, 0);   // Throws
        m_acc->cf_locked_server_versions.insert(realm::npos, 0); // Throws
        ++m_num_client_files;
    }
}

void ServerHistory::Accessors::create()
{
    // Note: `Array::create()` does *NOT* call `Node::update_parent()`, while
    // `BPlusTree<T>::create()` *DOES* update its parent in an exception-safe
    // way. This means that we need destruction guards for arrays, but not
    // BPlusTrees/BinaryColumns.

    // Note: The arrays `upstream_status` and `partial_sync` are created
    // on-demand instead of here.

    bool context_flag_no = false;
    root.create(Array::type_HasRefs, context_flag_no, s_root_size); // Throws
    _impl::DeepArrayDestroyGuard destroy_guard(&root);

    client_files.create(Array::type_HasRefs, context_flag_no, s_client_files_size); // Throws
    client_files.update_parent();                                                   // Throws

    sync_history.create(Array::type_HasRefs, context_flag_no, s_sync_history_size); // Throws
    sync_history.update_parent();                                                   // Throws

    schema_versions.create(Array::type_HasRefs, context_flag_no, s_schema_versions_size); // Throws
    schema_versions.update_parent();
    Allocator& alloc = schema_versions.get_alloc();
    for (int i = 0; i < s_schema_versions_size; i++) {
        MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag_no, alloc);
        ref_type ref = mem.get_ref();
        schema_versions.set_as_ref(i, ref);
    }

    cf_ident_salts.create();            // Throws
    cf_client_versions.create();        // Throws
    cf_rh_base_versions.create();       // Throws
    cf_recip_hist_refs.create();        // Throws
    cf_proxy_files.create();            // Throws
    cf_client_types.create();           // Throws
    cf_last_seen_timestamps.create();   // Throws
    cf_locked_server_versions.create(); // Throws

    sh_version_salts.create();    // Throws
    sh_origin_files.create();     // Throws
    sh_client_versions.create();  // Throws
    sh_timestamps.create();       // Throws
    sh_changesets.create();       // Throws
    sh_cumul_byte_sizes.create(); // Throws

    ct_history.create(); // Throws

    destroy_guard.release();
    root.update_parent(); // Throws
}


auto ServerHistory::get_server_version_salt(version_type server_version) const noexcept -> salt_type
{
    REALM_ASSERT(server_version >= m_history_base_version);
    if (server_version == m_history_base_version)
        return salt_type(m_acc->root.get(s_base_version_salt_iip));
    std::size_t history_entry_index = to_size_t(server_version - m_history_base_version) - 1;
    REALM_ASSERT(history_entry_index < m_history_size);
    return salt_type(m_acc->sh_version_salts.get(history_entry_index));
}


bool ServerHistory::is_valid_proxy_file_ident(file_ident_type file_ident) const noexcept
{
    static_assert(g_root_node_file_ident == 1, "");
    REALM_ASSERT(file_ident >= 2);
    REALM_ASSERT(std::uint_fast64_t(file_ident) < m_num_client_files);
    std::size_t i = std::size_t(file_ident);
    auto client_type = m_acc->cf_client_types.get(i);
    return is_direct_client(ClientType(client_type));
}


void ServerHistory::add_core_history_entry(BinaryData changeset)
{
    REALM_ASSERT(m_acc->ct_history.size() == m_ct_history_size);

    if (changeset.is_null())
        changeset = BinaryData("", 0);

    m_acc->ct_history.add(changeset); // Throws
    ++m_ct_history_size;
}


void ServerHistory::add_sync_history_entry(const HistoryEntry& entry)
{
    REALM_ASSERT(m_acc->sh_version_salts.size() == m_history_size);
    REALM_ASSERT(m_acc->sh_origin_files.size() == m_history_size);
    REALM_ASSERT(m_acc->sh_client_versions.size() == m_history_size);
    REALM_ASSERT(m_acc->sh_timestamps.size() == m_history_size);
    REALM_ASSERT(m_acc->sh_changesets.size() == m_history_size);
    REALM_ASSERT(m_acc->sh_cumul_byte_sizes.size() == m_history_size);

    std::int_fast64_t client_file = std::int_fast64_t(entry.origin_file_ident);
    std::int_fast64_t client_version = std::int_fast64_t(entry.remote_version);
    std::int_fast64_t timestamp = std::int_fast64_t(entry.origin_timestamp);

    // FIXME: BinaryColumn::set() currently interprets BinaryData(0,0) as
    // null. It should probably be changed such that BinaryData(0,0) is
    // always interpreted as the empty string. For the purpose of setting
    // null values, BinaryColumn::set() should accept values of type
    // Optional<BinaryData>().
    BinaryData changeset("", 0);
    if (!entry.changeset.is_null())
        changeset = entry.changeset.get_first_chunk();

    m_acc->sh_version_salts.insert(realm::npos, m_salt_for_new_server_versions); // Throws
    m_acc->sh_origin_files.insert(realm::npos, client_file);                     // Throws
    m_acc->sh_client_versions.insert(realm::npos, client_version);               // Throws
    m_acc->sh_timestamps.insert(realm::npos, timestamp);                         // Throws
    m_acc->sh_changesets.add(changeset);                                         // Throws

    // Update the cumulative byte size.
    std::int_fast64_t previous_history_byte_size =
        (m_history_size == 0 ? 0 : m_acc->sh_cumul_byte_sizes.get(m_history_size - 1));
    std::int_fast64_t history_byte_size = previous_history_byte_size + changeset.size();
    m_acc->sh_cumul_byte_sizes.insert(realm::npos, history_byte_size);

    ++m_history_size;
    m_server_version_salt = m_salt_for_new_server_versions;
}


void ServerHistory::trim_cont_transact_history()
{
    REALM_ASSERT(m_acc->ct_history.size() == m_ct_history_size);

    // `m_version_of_oldest_bound_snapshot` is not updated by transactions
    // occuring through other DB objects than the one associated with
    // this history object. For that reason, it can sometimes happen that it
    // precedes the beginning of the history, even though it seems
    // nonsensical. It would happen if the history was already trimmed via one
    // of the other DB objects. In such a case, no trimming can be done
    // yet.
    if (m_version_of_oldest_bound_snapshot > m_ct_base_version) {
        std::size_t num_entries_to_erase = std::size_t(m_version_of_oldest_bound_snapshot - m_ct_base_version);
        // The new changeset is always added before
        // set_oldest_bound_version() is called. Therefore, the trimming
        // operation can never leave the history empty.
        REALM_ASSERT(num_entries_to_erase < m_ct_history_size);
        for (std::size_t i = 0; i < num_entries_to_erase; ++i) {
            std::size_t j = num_entries_to_erase - i - 1;
            m_acc->ct_history.erase(j);
        }
        m_ct_base_version += num_entries_to_erase;
        m_ct_history_size -= num_entries_to_erase;
    }
}


ChunkedBinaryData ServerHistory::get_changeset(version_type server_version) const noexcept
{
    REALM_ASSERT(server_version > m_history_base_version && server_version <= get_server_version());
    std::size_t history_entry_ndx = to_size_t(server_version - m_history_base_version) - 1;
    return ChunkedBinaryData(m_acc->sh_changesets, history_entry_ndx);
}


// Skips history entries with empty changesets, and history entries produced by
// integration of changes received from the specified remote file.
//
// Pass zero for `remote_file_ident` if the remote file is on the upstream
// server, or the reference file.
//
// Returns zero if no history entry was found. Otherwise it returns the version
// produced by the changeset of the located history entry.
auto ServerHistory::find_history_entry(file_ident_type remote_file_ident, version_type begin_version,
                                       version_type end_version, HistoryEntry& entry,
                                       version_type& last_integrated_remote_version) const noexcept -> version_type
{
    REALM_ASSERT(remote_file_ident != g_root_node_file_ident);
    REALM_ASSERT(begin_version >= m_history_base_version);
    REALM_ASSERT(begin_version <= end_version);
    auto server_version = begin_version;
    while (server_version < end_version) {
        ++server_version;
        // FIXME: Find a way to avoid dynamically allocating a buffer for, and
        // copying the changeset for all the skipped history entries.
        HistoryEntry entry_2 = get_history_entry(server_version);
        bool received_from_client = received_from(entry_2, remote_file_ident);
        if (received_from_client) {
            last_integrated_remote_version = entry_2.remote_version;
            continue;
        }
        if (entry_2.changeset.size() == 0)
            continue; // Empty
        // These changes were not received from the specified client, and the
        // changeset was not empty.
        entry = entry_2;
        return server_version;
    }
    return 0;
}


auto ServerHistory::get_history_entry(version_type server_version) const noexcept -> HistoryEntry
{
    REALM_ASSERT(server_version > m_history_base_version && server_version <= get_server_version());
    std::size_t history_entry_ndx = to_size_t(server_version - m_history_base_version) - 1;
    auto origin_file = m_acc->sh_origin_files.get(history_entry_ndx);
    auto client_version = m_acc->sh_client_versions.get(history_entry_ndx);
    auto timestamp = m_acc->sh_timestamps.get(history_entry_ndx);
    ChunkedBinaryData chunked_changeset(m_acc->sh_changesets, history_entry_ndx);
    HistoryEntry entry;
    entry.origin_file_ident = file_ident_type(origin_file);
    entry.remote_version = version_type(client_version);
    entry.origin_timestamp = timestamp_type(timestamp);
    entry.changeset = chunked_changeset;
    return entry;
}


// Returns true if, and only if the specified history entry was produced by
// integratrion of a changeset that was received from the specified remote
// file. Use `remote_file_ident = 0` to specify the upstream server when on a
// subtier node of a star topology server cluster, or to specify the reference
// file when in a partial view.
bool ServerHistory::received_from(const HistoryEntry& entry, file_ident_type remote_file_ident) const noexcept
{
    file_ident_type origin_file_ident = entry.origin_file_ident;
    std::size_t origin_file_index = std::size_t(origin_file_ident);
    bool from_upstream_server = (remote_file_ident == 0);
    if (!from_upstream_server) {
        std::size_t remote_file_index = std::size_t(remote_file_ident);
        REALM_ASSERT(is_direct_client(ClientType(m_acc->cf_client_types.get(remote_file_index))));
        if (origin_file_ident == remote_file_ident)
            return true;
        file_ident_type proxy_file = file_ident_type(m_acc->cf_proxy_files.get(origin_file_index));
        return (proxy_file == remote_file_ident);
    }
    bool of_local_origin = (origin_file_ident == 0);
    if (of_local_origin)
        return false;
    ClientType client_type = ClientType(m_acc->cf_client_types.get(origin_file_index));
    return (client_type == ClientType::upstream);
}


auto ServerHistory::get_history_contents() const -> HistoryContents
{
    HistoryContents hc;

    TransactionRef tr = m_db->start_read(); // Throws
    version_type realm_version = tr->get_version();
    const_cast<ServerHistory*>(this)->set_group(tr.get());
    ensure_updated(realm_version); // Throws

    util::AppendBuffer<char> buffer;
    hc.client_files = {};
    for (std::size_t i = 0; i < m_num_client_files; ++i) {
        HistoryContents::ClientFile cf;
        cf.ident_salt = m_acc->cf_ident_salts.get(i);
        cf.client_version = m_acc->cf_client_versions.get(i);
        cf.rh_base_version = m_acc->cf_rh_base_versions.get(i);
        cf.proxy_file = m_acc->cf_proxy_files.get(i);
        cf.client_type = m_acc->cf_client_types.get(i);
        cf.locked_server_version = m_acc->cf_locked_server_versions.get(i);
        cf.reciprocal_history = {};
        version_type recip_hist_base_version = version_type(cf.rh_base_version);
        ReciprocalHistory recip_hist(m_acc->cf_recip_hist_refs, i, recip_hist_base_version); // Throws
        std::size_t recip_hist_size = recip_hist.size();
        for (std::size_t j = 0; j < recip_hist_size; ++j) {
            version_type version = recip_hist_base_version + i + 1;
            ChunkedBinaryData transform;
            if (recip_hist.get(version, transform)) {
                transform.copy_to(buffer);
                cf.reciprocal_history.push_back(std::string{buffer.data(), buffer.size()});
            }
            else {
                cf.reciprocal_history.push_back(util::none);
            }
        }
        hc.client_files.push_back(cf);
    }

    hc.history_base_version = m_acc->root.get_as_ref_or_tagged(s_history_base_version_iip).get_as_int();
    hc.base_version_salt = m_acc->root.get_as_ref_or_tagged(s_base_version_salt_iip).get_as_int();

    hc.sync_history = {};
    for (size_t i = 0; i < m_history_size; ++i) {
        HistoryContents::HistoryEntry he;
        he.version_salt = m_acc->sh_version_salts.get(i);
        he.client_file_ident = m_acc->sh_origin_files.get(i);
        he.client_version = m_acc->sh_client_versions.get(i);
        he.timestamp = m_acc->sh_timestamps.get(i);
        he.cumul_byte_size = m_acc->sh_cumul_byte_sizes.get(i);
        ChunkedBinaryData chunked_changeset(m_acc->sh_changesets, i);
        chunked_changeset.copy_to(buffer);
        he.changeset = std::string(buffer.data(), buffer.size());
        hc.sync_history.push_back(he);
    }

    hc.servers_client_file_ident = m_local_file_ident;

    return hc;
}


void ServerHistory::fixup_state_and_changesets_for_assigned_file_ident(Transaction& group, file_ident_type file_ident)
{
    // Must be in write transaction!

    REALM_ASSERT(file_ident != 0);
    REALM_ASSERT(file_ident != g_root_node_file_ident);
    REALM_ASSERT(m_acc->upstream_status.is_attached());
    REALM_ASSERT(m_local_file_ident == g_root_node_file_ident);
    using Instruction = realm::sync::Instruction;

    auto promote_global_key = [&](GlobalKey& oid) {
        REALM_ASSERT(oid.hi() == 0); // client_file_ident == 0
        oid = GlobalKey{uint64_t(file_ident), oid.lo()};
    };

    auto promote_primary_key = [&](Instruction::PrimaryKey& pk) {
        mpark::visit(overload{[&](GlobalKey& key) {
                                  promote_global_key(key);
                              },
                              [](auto&&) {}},
                     pk);
    };

    auto get_table_for_class = [&](StringData class_name) -> ConstTableRef {
        REALM_ASSERT(class_name.size() < Group::max_table_name_length - 6);
        Group::TableNameBuffer buffer;
        return group.get_table(Group::class_name_to_table_name(class_name, buffer));
    };

    // Fix up changesets in history. We know that all of these are of our own
    // creation.
    for (std::size_t i = 0; i < m_acc->sh_changesets.size(); ++i) {
        ChunkedBinaryData changeset{m_acc->sh_changesets, i};
        ChunkedBinaryInputStream in{changeset};
        Changeset log;
        parse_changeset(in, log);

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
                if (auto set_instr = instr->get_if<Instruction::Update>()) {
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

        ChangesetEncoder::Buffer modified;
        encode_changeset(log, modified);
        BinaryData result = BinaryData{modified.data(), modified.size()};
        m_acc->sh_changesets.set(i, result);
    }
}

void ServerHistory::record_current_schema_version()
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
    version_type snapshot_version = m_db->get_version_of_latest_snapshot();
    record_current_schema_version(schema_versions, snapshot_version); // Throws
}


void ServerHistory::record_current_schema_version(Array& schema_versions, version_type snapshot_version)
{
    static_assert(s_schema_versions_size == 4, "");
    REALM_ASSERT(schema_versions.size() == s_schema_versions_size);

    Allocator& alloc = schema_versions.get_alloc();
    {
        Array sv_schema_versions{alloc};
        sv_schema_versions.set_parent(&schema_versions, s_sv_schema_versions_iip);
        sv_schema_versions.init_from_parent();
        int schema_version = get_server_history_schema_version();
        sv_schema_versions.add(schema_version); // Throws
    }
    {
        Array sv_library_versions{alloc};
        sv_library_versions.set_parent(&schema_versions, s_sv_library_versions_iip);
        sv_library_versions.init_from_parent();
        const char* library_version = REALM_VERSION_STRING;
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


bool ServerHistory::Context::get_compaction_params(bool&, std::chrono::seconds&, std::chrono::seconds&) noexcept
{
    return false;
}


Clock::time_point ServerHistory::Context::get_compaction_clock_now() const noexcept
{
    return Clock::clock::now();
}


Transformer& ServerHistory::Context::get_transformer()
{
    throw util::runtime_error("Not supported");
}


util::Buffer<char>& ServerHistory::Context::get_transform_buffer()
{
    throw util::runtime_error("Not supported");
}


std::ostream& _impl::operator<<(std::ostream& out, const ServerHistory::HistoryContents& hc)
{
    out << "client files:\n";
    for (std::size_t i = 0; i < hc.client_files.size(); ++i) {
        out << "\n";
        out << "  client_file_ident = " << i << "\n";
        out << "  ident_salt = " << hc.client_files[i].ident_salt << "\n";
        out << "  client_version = " << hc.client_files[i].client_version << "\n";
        out << "  rh_base_version = " << hc.client_files[i].rh_base_version << "\n";
        out << "  proxy_file = " << hc.client_files[i].proxy_file << "\n";
        out << "  client_type = " << hc.client_files[i].client_type << "\n";
        out << "  locked_server_version = " << hc.client_files[i].locked_server_version << "\n";
        out << "  reciprocal history:\n";
        for (const util::Optional<std::string>& transform : hc.client_files[i].reciprocal_history) {
            if (transform) {
                out << "    " << util::hex_dump((*transform).data(), (*transform).size()) << "\n";
            }
            else {
                out << "    NULL\n";
            }
        }
        out << "\n";
    }
    out << "\n";

    out << "history_base_version = " << hc.history_base_version << "\n";
    out << "base_version_salt = " << hc.base_version_salt << "\n";
    out << "\n";

    out << "history entries:\n";
    for (std::size_t i = 0; i < hc.sync_history.size(); ++i) {
        out << "\n";
        out << "  version_salt = " << hc.sync_history[i].version_salt << "\n";
        out << "  client_file_ident = " << hc.sync_history[i].client_file_ident << "\n";
        out << "  client_version = " << hc.sync_history[i].client_version << "\n";
        out << "  timestamp = " << hc.sync_history[i].timestamp << "\n";
        out << "  cumul_byte_size = " << hc.sync_history[i].cumul_byte_size << "\n";
        const std::string& changeset = hc.sync_history[i].changeset;
        out << "  changeset = " << util::hex_dump(changeset.data(), changeset.size()) << "\n";
        out << "\n";
    }
    out << "\n";

    out << "servers_client_file_ident = " << hc.servers_client_file_ident << "\n";

    return out;
}

bool _impl::operator==(const ServerHistory::HistoryContents& hc_1, const ServerHistory::HistoryContents& hc_2)
{
    if (hc_1.client_files.size() != hc_2.client_files.size())
        return false;

    for (std::size_t i = 0; i < hc_1.client_files.size(); ++i) {
        ServerHistory::HistoryContents::ClientFile cf_1 = hc_1.client_files[i];
        ServerHistory::HistoryContents::ClientFile cf_2 = hc_2.client_files[i];

        bool partially_equal =
            (cf_1.ident_salt == cf_2.ident_salt && cf_1.client_version == cf_2.client_version &&
             cf_1.rh_base_version == cf_2.rh_base_version && cf_1.proxy_file == cf_2.proxy_file &&
             cf_1.client_type == cf_2.client_type && cf_1.locked_server_version == cf_2.locked_server_version &&
             cf_1.reciprocal_history.size() == cf_2.reciprocal_history.size());
        if (!partially_equal)
            return false;

        for (std::size_t j = 0; j < cf_1.reciprocal_history.size(); ++j) {
            if (cf_1.reciprocal_history[j] != cf_2.reciprocal_history[j])
                return false;
        }
    }

    bool same_base_version =
        (hc_1.history_base_version == hc_2.history_base_version && hc_1.base_version_salt == hc_2.base_version_salt);
    if (!same_base_version)
        return false;

    if (hc_1.sync_history.size() != hc_2.sync_history.size())
        return false;

    for (std::size_t i = 0; i < hc_1.sync_history.size(); ++i) {
        ServerHistory::HistoryContents::HistoryEntry sh_1 = hc_1.sync_history[i];
        ServerHistory::HistoryContents::HistoryEntry sh_2 = hc_2.sync_history[i];
        bool equal = (sh_1.version_salt == sh_2.version_salt && sh_1.client_file_ident == sh_2.client_file_ident &&
                      sh_1.client_version == sh_2.client_version && sh_1.timestamp == sh_2.timestamp &&
                      sh_1.cumul_byte_size == sh_2.cumul_byte_size);
        if (!equal)
            return false;
    }

    if (hc_1.servers_client_file_ident != hc_2.servers_client_file_ident)
        return false;

    return true;
}
