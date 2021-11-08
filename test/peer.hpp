/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_TEST_PEER_HPP
#define REALM_TEST_PEER_HPP

#include <fstream>

#include <realm/db.hpp>
#include <realm/replication.hpp>

#include <realm/sync/history.hpp>
#include <realm/sync/protocol.hpp>
#include <realm/sync/transform.hpp>
#include <realm/sync/instruction_replication.hpp>
#include <realm/sync/instruction_applier.hpp>
#include <realm/sync/changeset.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/noinst/compact_changesets.hpp>
#include <realm/sync/noinst/protocol_codec.hpp>
#include <realm/util/file.hpp>

#include "util/test_path.hpp"
#include "util/compare_groups.hpp"
#include "util/unit_test.hpp"

#include <fstream>
#include <numeric>

namespace realm {
namespace test_util {

using realm::sync::HistoryEntry;
using realm::sync::SyncReplication;
using realm::sync::Transformer;
using realm::sync::TransformHistory;


class ShortCircuitHistory : public SyncReplication {
public:
    using file_ident_type = sync::file_ident_type;
    using version_type = sync::version_type;
    using timestamp_type = sync::timestamp_type;
    using Changeset = sync::Changeset;

    static constexpr file_ident_type servers_file_ident() noexcept
    {
        return 1;
    }

    version_type get_history_entry(version_type version, HistoryEntry& entry) const noexcept
    {
        entry = m_entries.at(size_t(version - s_initial_version - 1));
        version_type prev_version = (version == 2 ? 0 : version - 1);
        return prev_version;
    }

    version_type integrate_remote_changeset(file_ident_type remote_file_ident, DB& sg,
                                            const Transformer::RemoteChangeset& changeset,
                                            util::Logger* replay_logger)
    {
        std::size_t num_changesets = 1;
        return integrate_remote_changesets(remote_file_ident, sg, &changeset, num_changesets, replay_logger);
    }

    version_type integrate_remote_changesets(file_ident_type remote_file_ident, DB&,
                                             const Transformer::RemoteChangeset* incoming_changesets,
                                             std::size_t num_changesets, util::Logger* replay_logger);

    version_type prepare_changeset(const char* data, std::size_t size, version_type orig_version) override
    {
        REALM_ASSERT(orig_version == s_initial_version + m_core_entries.size());
        version_type new_version = orig_version + 1;
        m_incoming_core_changeset.reset(new char[size]); // Throws
        std::copy(data, data + size, m_incoming_core_changeset.get());

        // Make space for the new history entry in m_entries such that we can be
        // sure no exception will be thrown whan adding adding in
        // finalize_changeset().
        m_core_entries.reserve(m_core_entries.size() + 1); // Throws

        if (!is_short_circuited()) {
            auto& buffer = get_instruction_encoder().buffer();
            m_incoming_changeset = util::make_unique<char[], util::DefaultAllocator>(buffer.size()); // Throws
            std::copy(buffer.data(), buffer.data() + buffer.size(), m_incoming_changeset.get());

            m_incoming_entry.origin_timestamp = m_current_time;
            m_incoming_entry.origin_file_ident = 0;
            m_incoming_entry.remote_version = 0; // Should be set on clients, but is not used in this context
            m_incoming_entry.changeset = BinaryData(m_incoming_changeset.get(), buffer.size());

            ChunkedBinaryInputStream stream(m_incoming_entry.changeset);
            Changeset changeset;
            parse_changeset(stream, changeset);

            // Make space for the new history entry in m_entries such that we can be
            // sure no exception will be thrown whan adding adding in
            // finalize_changeset().
            m_sync_entries.reserve(m_sync_entries.size() + 1); // Throws
            m_entries.reserve(m_entries.size() + 1);           // Throws
        }
        return new_version;
    }

    void finalize_changeset() noexcept override
    {
        // The following operation will not throw due to the space reservation
        // carried out in prepare_new_changeset().
        m_core_entries.push_back(std::move(m_incoming_core_changeset));

        if (!is_short_circuited()) {
            ChunkedBinaryInputStream stream(m_incoming_entry.changeset);
            Changeset changeset;
            parse_changeset(stream, changeset);

            m_entries.push_back(m_incoming_entry);
            m_entries_data_owner.push_back(std::move(m_incoming_changeset)); // Ownership successfully handed over
        }
    }

    HistoryType get_history_type() const noexcept override
    {
        return Replication::hist_None;
    }

    int get_history_schema_version() const noexcept override
    {
        return 0;
    }

    bool is_upgradable_history_schema(int) const noexcept override
    {
        REALM_ASSERT(false);
        return false;
    }

    void upgrade_history_schema(int) override
    {
        REALM_ASSERT(false);
    }

    _impl::History* _get_history_write() override
    {
        return m_write_history.get();
    }

    std::unique_ptr<_impl::History> _create_history_read() override
    {
        return std::make_unique<History>(*this);
    }

    version_type find_history_entry(version_type begin_version, version_type end_version,
                                    file_ident_type remote_file_ident, bool only_nonempty,
                                    HistoryEntry& entry) const noexcept
    {
        if (begin_version == 0) {
            begin_version = s_initial_version;
            if (end_version == 0)
                end_version = s_initial_version;
        }
        for (size_t i = 0; i < size_t(end_version - begin_version); ++i) {
            version_type version = begin_version + i + 1;
            get_history_entry(version, entry);
            if (entry.changeset.size() > 0 || !only_nonempty) {
                bool is_server = (m_local_file_ident == servers_file_ident());
                bool from_remote;
                if (is_server) {
                    REALM_ASSERT(remote_file_ident != servers_file_ident());
                    from_remote = (entry.origin_file_ident == remote_file_ident);
                }
                else {
                    REALM_ASSERT(remote_file_ident == servers_file_ident());
                    from_remote = (entry.origin_file_ident != 0);
                }
                if (!from_remote)
                    return version;
            }
        }
        return 0;
    }

    void do_initiate_transact(Group& group, version_type current_version, bool history_updated) override
    {
        SyncReplication::do_initiate_transact(group, current_version, history_updated);
        using gf = _impl::GroupFriend;
        Array arr(gf::get_alloc(group));
        group.set_sync_file_id(m_local_file_ident);
    }


    /*
        void initiate_transform_session(DB&) override
        {
            // No-op.
        }
    */

    timestamp_type get_time() const noexcept
    {
        return m_current_time;
    }

    void set_time(timestamp_type time)
    {
        m_current_time = time;
    }

    // `amount` may be negative
    void advance_time(int amount)
    {
        m_current_time += amount;
    }

    void set_disable_compaction(bool b)
    {
        m_disable_compaction = b;
    }

    std::map<TableKey, std::unordered_map<GlobalKey, ObjKey>> m_optimistic_object_id_collisions;

    ShortCircuitHistory(file_ident_type local_file_ident,
                        TestDirNameGenerator* changeset_dump_dir_gen)
        : m_write_history(std::make_unique<History>(*this)) // Throws
        , m_local_file_ident(local_file_ident)
        , m_transformer(std::make_unique<TransformerImpl>(changeset_dump_dir_gen)) // Throws
        , m_incoming_changeset(nullptr, util::STLDeleter<char[]>{util::DefaultAllocator::get_default()})
    {
    }

private:
    class TempDisableReplication;
    class TransformHistoryImpl;
    class TransformerImpl;

    struct History : _impl::History {
        History(ShortCircuitHistory& sch)
            : m_impl(sch)
        {
        }
        ShortCircuitHistory& m_impl;

        void update_from_ref_and_version(ref_type, version_type) override final {}
        void update_from_parent(version_type) final {}
        void set_oldest_bound_version(version_type) override final {}
        void verify() const override final {}

        void get_changesets(version_type, version_type, BinaryIterator*) const noexcept override final
        {
            REALM_ASSERT(false);
        }
    };
    friend struct History;
    std::unique_ptr<History> m_write_history;

    static const version_type s_initial_version = 1;
    const file_ident_type m_local_file_ident;
    const std::unique_ptr<TransformerImpl> m_transformer;
    timestamp_type m_current_time = 0;
    std::unique_ptr<char[], util::STLDeleter<char[]>> m_incoming_changeset;
    std::unique_ptr<char[]> m_incoming_core_changeset;
    HistoryEntry m_incoming_entry;
    std::vector<std::unique_ptr<char[]>> m_sync_entries;
    std::vector<std::unique_ptr<char[]>> m_core_entries;
    std::vector<HistoryEntry> m_entries;
    std::vector<std::unique_ptr<char[], util::STLDeleter<char[]>>> m_entries_data_owner;
    std::map<version_type, std::map<file_ident_type, std::string>> m_reciprocal_transforms;
    bool m_disable_compaction = false;

    ChunkedBinaryData get_reciprocal_transform(file_ident_type remote_file_ident, version_type version) const
    {
        auto i_1 = m_reciprocal_transforms.find(version);
        if (i_1 != m_reciprocal_transforms.cend()) {
            auto& transforms_2 = i_1->second;
            auto i_2 = transforms_2.find(remote_file_ident);
            if (i_2 != transforms_2.cend()) {
                const std::string& transform = i_2->second;
                return ChunkedBinaryData{BinaryData{transform}};
            }
        }
        const HistoryEntry& entry = m_entries.at(size_t(version - s_initial_version - 1));
        return entry.changeset;
    }

    void set_reciprocal_transform(file_ident_type remote_file_ident, version_type version, BinaryData data)
    {
        auto& transforms_2 = m_reciprocal_transforms[version]; // Throws
        auto p = transforms_2.insert(make_pair(remote_file_ident, std::string()));
        std::string& transform = p.first->second;
        transform.replace(0, transform.size(), data.data(), data.size()); // Throws
    }
};


// Temporarily disable replication on the specified group
class ShortCircuitHistory::TempDisableReplication {
public:
    TempDisableReplication(DB& sg)
        : m_db(sg)
    {
        m_repl = static_cast<SyncReplication*>(m_db.get_replication());
        m_repl->set_short_circuit(true);
    }
    ~TempDisableReplication()
    {
        m_repl->set_short_circuit(false);
    }

private:
    DB& m_db;
    SyncReplication* m_repl;
};


class ShortCircuitHistory::TransformHistoryImpl : public TransformHistory {
public:
    TransformHistoryImpl(ShortCircuitHistory& history, file_ident_type remote_file_ident) noexcept
        : m_history{history}
        , m_remote_file_ident{remote_file_ident}
    {
    }

    version_type find_history_entry(version_type begin_version, version_type end_version,
                                    HistoryEntry& entry) const noexcept override final
    {
        bool only_nonempty = true;
        return m_history.find_history_entry(begin_version, end_version, m_remote_file_ident, only_nonempty, entry);
    }

    ChunkedBinaryData get_reciprocal_transform(version_type version) const override final
    {
        return m_history.get_reciprocal_transform(m_remote_file_ident, version); // Throws
    }

    void set_reciprocal_transform(version_type version, BinaryData data) override final
    {
        m_history.set_reciprocal_transform(m_remote_file_ident, version, data); // Throws
    }

private:
    ShortCircuitHistory& m_history;
    const file_ident_type m_remote_file_ident;
};


class ShortCircuitHistory::TransformerImpl : public _impl::TransformerImpl {
public:
    TransformerImpl(TestDirNameGenerator* changeset_dump_dir_gen)
        : _impl::TransformerImpl()
        , m_changeset_dump_dir_gen{changeset_dump_dir_gen}
    {
    }

protected:
    void merge_changesets(file_ident_type local_file_ident, Changeset* their_changesets, std::size_t their_size,
                          Changeset** our_changesets, std::size_t our_size, Reporter* reporter,
                          util::Logger* logger) override final
    {
        std::string directory;
        if (m_changeset_dump_dir_gen) {
            directory = m_changeset_dump_dir_gen->next();
            util::try_make_dir(directory);

            encode_changesets(our_changesets, our_size, logger);
            write_changesets_to_file(directory + "/ours_original", our_size, logger);

            encode_changesets(their_changesets, their_size, logger);
            write_changesets_to_file(directory + "/theirs_original", their_size, logger);
        }

        _impl::TransformerImpl::merge_changesets(local_file_ident, their_changesets, their_size, our_changesets,
                                                 our_size, reporter, logger);

        if (m_changeset_dump_dir_gen) {
            encode_changesets(our_changesets, our_size, logger);
            write_changesets_to_file(directory + "/ours_transformed", our_size, logger);

            encode_changesets(their_changesets, their_size, logger);
            write_changesets_to_file(directory + "/theirs_transformed", their_size, logger);
        }
    }

private:
    using OutputBuffer = util::ResettableExpandableBufferOutputStream;

    void encode_changesets(Changeset* changesets, std::size_t num_changesets, util::Logger* logger)
    {
        sync::ChangesetEncoder::Buffer encode_buffer;
        for (size_t i = 0; i < num_changesets; ++i) {
            encode_changeset(changesets[i], encode_buffer); // Throws

            HistoryEntry entry;
            entry.remote_version = changesets[i].last_integrated_remote_version;
            entry.origin_file_ident = changesets[i].origin_file_ident;
            entry.origin_timestamp = changesets[i].origin_timestamp;
            entry.changeset = BinaryData{encode_buffer.data(), encode_buffer.size()};

            _impl::ServerProtocol::ChangesetInfo info{changesets[i].version, entry.remote_version, entry,
                                                      entry.changeset.size()};

            m_protocol.insert_single_changeset_download_message(m_history_entries_buffer, info, *logger); // Throws

            encode_buffer.clear();
        }
    }

    void encode_changesets(Changeset** changesets, std::size_t num_changesets, util::Logger* logger)
    {
        sync::ChangesetEncoder::Buffer encode_buffer;
        for (size_t i = 0; i < num_changesets; ++i) {
            encode_changeset(*changesets[i], encode_buffer); // Throws

            HistoryEntry entry;
            entry.remote_version = changesets[i]->last_integrated_remote_version;
            entry.origin_file_ident = changesets[i]->origin_file_ident;
            entry.origin_timestamp = changesets[i]->origin_timestamp;
            entry.changeset = BinaryData{encode_buffer.data(), encode_buffer.size()};

            _impl::ServerProtocol::ChangesetInfo info{changesets[i]->version, entry.remote_version, entry,
                                                      entry.changeset.size()};

            m_protocol.insert_single_changeset_download_message(m_history_entries_buffer, info, *logger); // Throws

            encode_buffer.clear();
        }
    }

    void write_changesets_to_file(const std::string& pathname, std::size_t num_changesets, util::Logger* logger)
    {
        m_protocol.make_download_message(sync::get_current_protocol_version(), m_download_message_buffer,
                                         file_ident_type(0), version_type(0), version_type(0), version_type(0), 0,
                                         version_type(0), version_type(0), 0, num_changesets,
                                         m_history_entries_buffer.data(), m_history_entries_buffer.size(), 0, false,
                                         *logger); // Throws

        m_history_entries_buffer.reset();

        std::ofstream file(pathname, std::ios::binary);
        file.write(m_download_message_buffer.data(), m_download_message_buffer.size());
        m_download_message_buffer.reset();
    }

    _impl::ServerProtocol m_protocol;
    TestDirNameGenerator* m_changeset_dump_dir_gen;

    OutputBuffer m_history_entries_buffer;
    OutputBuffer m_download_message_buffer;
};


inline auto ShortCircuitHistory::integrate_remote_changesets(file_ident_type remote_file_ident, DB& sg,
                                                             const Transformer::RemoteChangeset* incoming_changesets,
                                                             size_t num_changesets, util::Logger* logger)
    -> version_type
{
    REALM_ASSERT(num_changesets != 0);

    TempDisableReplication tdr(sg);
    TransactionRef transact = sg.start_write(); // Throws
    version_type local_version = transact->get_version_of_current_transaction().version;
    REALM_ASSERT(local_version == s_initial_version + m_entries.size());

    std::vector<Changeset> changesets;
    changesets.resize(num_changesets);

    for (size_t i = 0; i < num_changesets; ++i) {
        REALM_ASSERT(incoming_changesets[i].last_integrated_local_version <= local_version);
        sync::parse_remote_changeset(incoming_changesets[i], changesets[i]); // Throws
    }

    if (!m_disable_compaction) {
        _impl::compact_changesets(changesets.data(), changesets.size());
    }

    TransformHistoryImpl transform_hist{*this, remote_file_ident};
    Transformer::Reporter* reporter = nullptr;
    m_transformer->transform_remote_changesets(transform_hist, m_local_file_ident, local_version, changesets.data(),
                                               changesets.size(), reporter, logger);

    sync::ChangesetEncoder::Buffer assembled_transformed_changeset;

    for (size_t i = 0; i < num_changesets; ++i) {
        sync::InstructionApplier applier{*transact};
        applier.apply(changesets[i], logger);

        transact->verify();

        sync::encode_changeset(changesets[i], assembled_transformed_changeset);
    }

    auto& last_changeset = changesets.back();
    HistoryEntry entry;
    entry.origin_timestamp = last_changeset.origin_timestamp;
    entry.origin_file_ident = last_changeset.origin_file_ident;
    entry.remote_version = last_changeset.version;
    entry.changeset = BinaryData(assembled_transformed_changeset.data(), assembled_transformed_changeset.size());
    m_entries.push_back(entry); // Throws
    REALM_ASSERT(m_entries.size() == m_core_entries.size() + 1);
    m_entries_data_owner.push_back(
        assembled_transformed_changeset.release().release()); // Ownership successfully handed over
    return transact->commit();                                // Throws
}


class Peer {
public:
    using file_ident_type = sync::file_ident_type;
    using version_type = sync::version_type;
    using timestamp_type = sync::timestamp_type;

    file_ident_type local_file_ident;
    DBTestPathGuard path_guard;
    util::Logger& logger;
    ShortCircuitHistory history;
    DBRef shared_group;
    TransactionRef group; // Null when no transaction is in progress
    TableRef selected_table;
    LnkLstPtr selected_link_list;
    LstBasePtr selected_array;
    version_type current_version = 0;
    std::map<file_ident_type, version_type> last_remote_versions_integrated;

    // FIXME: Remove the dependency on the unit_test namespace.
    static std::unique_ptr<Peer> create_server(const unit_test::TestContext& test_context,
                                               TestDirNameGenerator* changeset_dump_dir_gen,
                                               const std::string path_add_on = "")
    {
        file_ident_type client_file_ident = ShortCircuitHistory::servers_file_ident();
        std::ostringstream out;
        out << ".server" << path_add_on << ".realm";
        std::string suffix = out.str();
        std::string test_path = get_test_path(test_context.get_test_name(), suffix);
        util::Logger& logger = test_context.logger;
        return std::unique_ptr<Peer>(new Peer(client_file_ident, test_path, changeset_dump_dir_gen, logger));
    }

    // FIXME: Remove the dependency on the unit_test namespace.
    static std::unique_ptr<Peer> create_client(const unit_test::TestContext& test_context,
                                               file_ident_type client_file_ident,
                                               TestDirNameGenerator* changeset_dump_dir_gen,
                                               const std::string path_add_on = "")
    {
        REALM_ASSERT(client_file_ident != 0);
        REALM_ASSERT(client_file_ident != ShortCircuitHistory::servers_file_ident());
        std::ostringstream out;
        out << ".client_" << client_file_ident << path_add_on << ".realm";
        std::string suffix = out.str();
        std::string test_path = get_test_path(test_context.get_test_name(), suffix);
        util::Logger& logger = test_context.logger;
        return std::unique_ptr<Peer>(new Peer(client_file_ident, test_path, changeset_dump_dir_gen, logger));
    }

    template <class F>
    void create_schema(F lambda)
    {
        WriteTransaction transaction(shared_group);
        lambda(transaction);
        current_version = transaction.commit();
    }

    void start_transaction()
    {
        group = shared_group->start_write(); // Throws
    }

    version_type commit()
    {
        current_version = group->commit();
        group = nullptr;
        selected_table = TableRef();
        selected_link_list.reset(nullptr);
        return current_version;
    }

    TableRef table(StringData name)
    {
        REALM_ASSERT(group); // Must be in transaction
        return group->get_table(name);
    }

    template <class F>
    void transaction(F lambda)
    {
        start_transaction();
        lambda(*this);
        commit();
    }

    size_t get_num_rows_via_read_transaction()
    {
        TransactionRef tr = shared_group->start_read(); // Throws
        return tr->get_table("foo")->size();
    }

    bool integrate_next_changeset_from(const Peer& remote)
    {
        return integrate_next_changesets_from(remote, 1);
    }

    bool integrate_next_changesets_from(const Peer& remote, size_t num_changesets)
    {
        if (num_changesets == 0)
            return false; // Nothing to do.

        REALM_ASSERT(local_file_ident != remote.local_file_ident);
        // Star shaped topology required
        REALM_ASSERT((local_file_ident == ShortCircuitHistory::servers_file_ident()) !=
                     (remote.local_file_ident == ShortCircuitHistory::servers_file_ident()));
        REALM_ASSERT(!group); // A transaction must not be in progress
        version_type& last_remote_version = last_remote_versions_integrated[remote.local_file_ident];
        if (last_remote_version == 0)
            last_remote_version = 1;
        std::unique_ptr<Transformer::RemoteChangeset[]> changesets{new Transformer::RemoteChangeset[num_changesets]};
        remote.get_next_changesets_for_remote(local_file_ident, last_remote_version, changesets.get(),
                                              num_changesets);
        /*
        std::cerr << "\nintegrate_remote_changeset: local_file_ident=" << local_file_ident
                  << " remote_version=" << changesets.get()->remote_version
                  << " last_intgerated_local_version=" << changesets.get()->last_integrated_local_version
                  << " origin_timestamp=" << changesets.get()->origin_timestamp
                  << " origin_file_ident=" << changesets.get()->origin_file_ident << std::endl;
        */
        file_ident_type remote_file_ident = remote.local_file_ident;
        util::Logger* replay_logger = &logger;
        version_type new_version =
            history.integrate_remote_changesets(remote_file_ident, *shared_group, changesets.get(), num_changesets,
                                                replay_logger); // Throws
        current_version = new_version;
        last_remote_version = changesets[num_changesets - 1].remote_version;
        return false;
    }

    size_t count_outstanding_changesets_from(const Peer& remote) const
    {
        REALM_ASSERT(local_file_ident != remote.local_file_ident);
        // Star shaped topology required
        REALM_ASSERT((local_file_ident == ShortCircuitHistory::servers_file_ident()) !=
                     (remote.local_file_ident == ShortCircuitHistory::servers_file_ident()));
        version_type last_remote_version = 0;
        auto i = last_remote_versions_integrated.find(remote.local_file_ident);
        if (i != last_remote_versions_integrated.end())
            last_remote_version = i->second;
        return remote.count_outstanding_changesets_for_remote(local_file_ident, last_remote_version);
    }

private:
    Peer(file_ident_type file_ident, const std::string& test_path, TestDirNameGenerator* changeset_dump_dir_gen,
         util::Logger& l)
        : local_file_ident(file_ident)
        , path_guard(test_path) // Throws
        , logger(l)
        , history(file_ident, changeset_dump_dir_gen)  // Throws
        , shared_group(DB::create(history, test_path)) // Throws
    {
    }

    void get_next_changesets_for_remote(file_ident_type remote_file_ident,
                                        version_type last_version_integrated_by_remote,
                                        Transformer::RemoteChangeset* out_changesets, size_t num_changesets) const
    {
        // At least one transaction can be assumed to have been performed
        REALM_ASSERT(current_version != 0);

        // Find next changeset not received from the remote
        version_type version = last_version_integrated_by_remote + 1;
        for (size_t i = 0; i < num_changesets; ++i) {
            HistoryEntry entry;
            for (;;) {
                history.get_history_entry(version, entry);
                if (!was_entry_received_from(entry, remote_file_ident))
                    break;
                ++version;
            }

            // Find the last remote version already integrated into the next
            // local version to be integrated by the remote
            version_type last_remote_version_integrated = 0;
            for (version_type version_2 = version - 1; version_2 >= 2; --version_2) {
                HistoryEntry entry_2;
                history.get_history_entry(version_2, entry_2);
                if (was_entry_received_from(entry_2, remote_file_ident)) {
                    last_remote_version_integrated = entry_2.remote_version;
                    break;
                }
            }

            out_changesets[i].data = entry.changeset;
            out_changesets[i].origin_timestamp = entry.origin_timestamp;
            out_changesets[i].origin_file_ident = entry.origin_file_ident;
            if (entry.origin_file_ident == 0)
                out_changesets[i].origin_file_ident = local_file_ident;
            out_changesets[i].last_integrated_local_version = last_remote_version_integrated;
            out_changesets[i].remote_version = version;

            ++version;
        }
    }

    size_t count_outstanding_changesets_for_remote(file_ident_type remote_file_ident,
                                                   version_type last_version_integrated_by_remote) const
    {
        size_t n = 0;
        version_type prev_version = last_version_integrated_by_remote;
        HistoryEntry entry;
        for (;;) {
            bool only_nonempty = false; // Don't skip empty changesets
            version_type version =
                history.find_history_entry(prev_version, current_version, remote_file_ident, only_nonempty, entry);
            if (version == 0)
                break;
            ++n;
            prev_version = version;
        }
        return n;
    }

    bool was_entry_received_from(const HistoryEntry& entry, file_ident_type remote_file_ident) const
    {
        bool is_server = (local_file_ident == ShortCircuitHistory::servers_file_ident());
        if (is_server)
            return (entry.origin_file_ident == remote_file_ident);
        return (entry.origin_file_ident != 0);
    }
};


template <class C>
inline void synchronize(Peer* server, C&& clients)
{
    for (Peer* client : clients) {
        size_t n = server->count_outstanding_changesets_from(*client);
        // FIXME: Server cannot integrate multiple changesets at a time because
        // if they get assembled, they will seem as a single changeset to other clients.
        for (size_t i = 0; i < n; ++i) {
            server->integrate_next_changeset_from(*client);
        }
    }
    for (Peer* client : clients) {
        size_t n = client->count_outstanding_changesets_from(*server);
        client->integrate_next_changesets_from(*server, n);
    }
}

inline void synchronize(Peer* server, std::initializer_list<Peer*> clients)
{
    synchronize(server, std::vector<Peer*>(clients));
}

/// Unit test helper for testing associativity of merge rules.
///
/// Calling `for_each_permutation()` with a lambda function that performs some
/// kind of test between a server and multiple clients, the function is invoked
/// separately for each permutation of clients. The lambda takes one argument -
/// an instance of `Associativity::Iteration` - which provides access to the
/// server and individual clients, as well as a `sync_all()` methods which
/// synchronizes the clients with the server in the order indicated by the
/// current permutation.
///
/// At the end of each iteration, the state on that iteration's server is
/// compared with the state on the first iteration's server to test that the
/// servers converge on the same state regardless of the order in which clients
/// sync.
///
/// Note that `for_each_permutation()` expects the lambda function to fully
/// synchronize all clients by calling `sync_all()` before returning.
struct Associativity {
    using TestContext = unit_test::TestContext;

    struct Iteration {
        TestContext& test_context;
        std::unique_ptr<Peer> server;
        std::vector<std::unique_ptr<Peer>> clients;
        std::vector<size_t> sync_order;
        std::unique_ptr<TestDirNameGenerator> dump_dir_gen;

        Iteration(TestContext& test_context, size_t num_clients, std::vector<size_t> sync_order,
                  TestDirNameGenerator* dump_dir_gen, std::string path_add_on = "")
            : test_context(test_context)
            , sync_order(std::move(sync_order))
        {
            REALM_ASSERT(this->sync_order.size() == num_clients);
            server = Peer::create_server(test_context, dump_dir_gen, path_add_on);
            for (size_t i = 0; i < num_clients; ++i) {
                clients.push_back(Peer::create_client(test_context, 2 + i, dump_dir_gen, path_add_on));
            }
        }

        void sync_all()
        {
            // Upload all changes from clients to the server.
            for (size_t i = 0; i < sync_order.size(); ++i) {
                size_t index = sync_order[i];
                Peer& client = *clients[index];
                size_t outstanding = server->count_outstanding_changesets_from(client);
                for (size_t j = 0; j < outstanding; ++j) {
                    server->integrate_next_changeset_from(client);
                }
            }

            // Download all changes on the server to the clients.
            //
            // Note: We don't particularly care about the order of downloads on
            // the clients, because they are already intrinsically representing
            // the outcome of applying changesets in different orders.
            for (size_t i = 0; i < sync_order.size(); ++i) {
                size_t index = sync_order[i];
                Peer& client = *clients[index];
                size_t outstanding = client.count_outstanding_changesets_from(*server);
                client.integrate_next_changesets_from(*server, outstanding);
            }
        }

        bool check_convergent()
        {
            ReadTransaction read_server{server->shared_group};
            for (auto& client : clients) {
                ReadTransaction read_client{client->shared_group};
                if (!CHECK(compare_groups(read_server, read_client, test_context.logger))) {
                    return false;
                }
            }
            return true;
        }
    };

    Associativity(TestContext& test_context, size_t num_clients, TestDirNameGenerator* dump_dir_gen = nullptr)
        : test_context(test_context)
        , num_clients(num_clients)
        , dump_dir_gen(std::move(dump_dir_gen))
    {
        REALM_ASSERT(num_clients != 0);
    }

    template <class F>
    bool for_each_permutation(F&& func)
    {
        sync_order.resize(num_clients, 0);
        std::iota(begin(sync_order), end(sync_order), 0);

        // Note: We are only dumping changesets for the first iteration.
        auto first = Iteration{test_context, num_clients, sync_order, dump_dir_gen};
        func(first);
        if (!first.check_convergent()) {
            return false;
        }

        size_t i = 1;

        while (std::next_permutation(begin(sync_order), end(sync_order))) {
            // Generate unique file names for each iteration.
            std::stringstream path_add_on;
            path_add_on << "permutation-" << i++;

            auto iter = Iteration{test_context, num_clients, sync_order, nullptr, path_add_on.str()};
            func(iter);
            if (!first.check_convergent()) {
                return false;
            }

            // Check that all permutations converge on the same state.
            ReadTransaction read_first{first.server->shared_group};
            ReadTransaction read_current{iter.server->shared_group};
            if (!CHECK(compare_groups(read_first, read_current, test_context.logger))) {
                return false;
            }
        }

        return true;
    }

    TestContext& test_context;
    size_t num_clients;
    std::vector<size_t> sync_order;
    TestDirNameGenerator* dump_dir_gen;
};

} // namespace test_util
} // namespace realm

#endif // REALM_TEST_PEER_HPP
