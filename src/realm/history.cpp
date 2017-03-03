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

#include <realm/impl/continuous_transactions_history.hpp>
#include <realm/binary_data.hpp>
#include <realm/group_shared.hpp>
#include <realm/replication.hpp>
#include <realm/history.hpp>

using namespace realm;


namespace {

// As new schema versions come into existsnece, describe them here.
constexpr int g_history_schema_version = 0;


/// This class is a basis for implementing the Replication API for the purpose
/// of supporting continuous transactions.
///
/// By ensuring that the root node of the history is correctly configured with
/// Group::m_top as its parent, this class allows for modifications of the
/// history as long as those modifications happen after the remainder of the
/// Group accessor is updated to reflect the new snapshot (see
/// History::update_early_from_top_ref()).
class InRealmHistory : public _impl::History {
public:
    void initialize(Group&);

    /// Must never be called more than once per transaction. Returns the version
    /// produced by the added changeset.
    version_type add_changeset(BinaryData);

    void update_early_from_top_ref(version_type, size_t, ref_type) override;
    void update_from_parent(version_type) override;
    void get_changesets(version_type, version_type, BinaryIterator*) const noexcept override;
    void set_oldest_bound_version(version_type) override;

    void verify() const override;

private:
    Group* m_group = nullptr;

    /// Version on which the first changeset in the history is based, or if the
    /// history is empty, the version associatede with currently bound
    /// snapshot. In general, the version associatede with currently bound
    /// snapshot is equal to `m_base_version + m_size`, but after
    /// add_changeset() is called, it is equal to one minus that.
    version_type m_base_version;

    /// Current number of entries in the history. A cache of
    /// `m_changesets->size()`.
    size_t m_size;

    /// A list of changesets, one for each entry in the history. If null, the
    /// history is empty.
    ///
    /// FIXME: Ideally, the B+tree accessor below should have been just
    /// Bptree<BinaryData>, but Bptree<BinaryData> seems to not allow that yet.
    ///
    /// FIXME: The memory-wise indirection is an unfortunate consequence of the
    /// fact that it is impossible to construct a BinaryColumn without already
    /// having a ref to a valid underlying node structure. This, in turn, is an
    /// unfortunate consequence of the fact that a column accessor contains a
    /// dynamically allocated root node accessor, and the type of the required
    /// root node accessor depends on the size of the B+-tree.
    std::unique_ptr<BinaryColumn> m_changesets;

    void update_from_ref(ref_type, version_type);
};


void InRealmHistory::initialize(Group& group)
{
    m_group = &group;
    m_base_version = 0;
    m_size = 0;
    m_changesets = nullptr;
}


InRealmHistory::version_type InRealmHistory::add_changeset(BinaryData changeset)
{
    if (!m_changesets) {
        using gf = _impl::GroupFriend;
        Allocator& alloc = gf::get_alloc(*m_group);
        size_t size = 0;
        bool nullable = false;
        ref_type hist_ref = BinaryColumn::create(alloc, size, nullable); // Throws
        _impl::DeepArrayRefDestroyGuard dg(hist_ref, alloc);
        m_changesets = std::make_unique<BinaryColumn>(alloc, hist_ref, nullable); // Throws
        gf::prepare_history_parent(*m_group, *m_changesets->get_root_array(),
                                   Replication::hist_InRealm,
                                   g_history_schema_version); // Throws
        // Note: gf::prepare_history_parent() also ensures the the root array
        // has a slot for the history ref.
        m_changesets->get_root_array()->update_parent(); // Throws
        dg.release();
    }
    // FIXME: BinaryColumn::set() currently interprets BinaryData{} as
    // null. It should probably be changed such that BinaryData{} is always
    // interpreted as the empty string. For the purpose of setting null values,
    // BinaryColumn::set() should accept values of type Optional<BinaryData>().
    if (changeset.is_null()) {
        m_changesets->add(BinaryData("", 0)); // Throws
    }
    else {
        m_changesets->add(changeset); // Throws
    }
    ++m_size;
    version_type new_version = m_base_version + m_size;
    return new_version;
}


void InRealmHistory::update_early_from_top_ref(version_type new_version, size_t new_file_size,
                                               ref_type new_top_ref)
{
    using gf = _impl::GroupFriend;
    gf::remap(*m_group, new_file_size); // Throws
    Allocator& alloc = gf::get_alloc(*m_group);
    ref_type hist_ref = gf::get_history_ref(alloc, new_top_ref);
    update_from_ref(hist_ref, new_version); // Throws
}


void InRealmHistory::update_from_parent(version_type version)
{
    using gf = _impl::GroupFriend;
    ref_type ref = gf::get_history_ref(*m_group);
    update_from_ref(ref, version); // Throws
}


void InRealmHistory::get_changesets(version_type begin_version, version_type end_version,
                                    BinaryIterator* buffer) const noexcept
{
    REALM_ASSERT(begin_version <= end_version);
    REALM_ASSERT(begin_version >= m_base_version);
    REALM_ASSERT(end_version <= m_base_version + m_size);
    version_type n_version_type = end_version - begin_version;
    version_type offset_version_type = begin_version - m_base_version;
    REALM_ASSERT(!util::int_cast_has_overflow<size_t>(n_version_type) &&
                 !util::int_cast_has_overflow<size_t>(offset_version_type));
    size_t n = size_t(n_version_type);
    size_t offset = size_t(offset_version_type);
    for (size_t i = 0; i < n; ++i)
        buffer[i] = BinaryIterator(m_changesets.get(), offset + i);
}


void InRealmHistory::set_oldest_bound_version(version_type version)
{
    REALM_ASSERT(version >= m_base_version);
    if (version > m_base_version) {
        REALM_ASSERT(m_changesets);
        size_t num_entries_to_erase = size_t(version - m_base_version);
        // The new changeset is always added before set_oldest_bound_version()
        // is called. Therefore, the trimming operation can never leave the
        // history empty.
        REALM_ASSERT(num_entries_to_erase < m_size);
        for (size_t i = 0; i < num_entries_to_erase; ++i)
            m_changesets->erase(0); // Throws
        m_base_version += num_entries_to_erase;
        m_size -= num_entries_to_erase;
    }
}


void InRealmHistory::verify() const
{
#ifdef REALM_DEBUG
    if (m_changesets)
        m_changesets->verify();
#endif
}


void InRealmHistory::update_from_ref(ref_type ref, version_type version)
{
    using gf = _impl::GroupFriend;
    if (ref == 0) {
        // No history
        m_base_version = version;
        m_size = 0;
        m_changesets = nullptr;
    }
    else {
        if (REALM_LIKELY(m_changesets)) {
            m_changesets->update_from_ref(ref); // Throws
        }
        else {
            Allocator& alloc = gf::get_alloc(*m_group);
            bool nullable = false;
            m_changesets = std::make_unique<BinaryColumn>(alloc, ref, nullable); // Throws
            gf::set_history_parent(*m_group, *m_changesets->get_root_array());
        }
        m_size = m_changesets->size();
        m_base_version = version - m_size;
    }
}


class InRealmHistoryImpl : public TrivialReplication, private InRealmHistory {
public:
    using version_type = TrivialReplication::version_type;

    InRealmHistoryImpl(std::string realm_path)
        : TrivialReplication(realm_path)
    {
    }

    void initialize(SharedGroup& sg) override
    {
        TrivialReplication::initialize(sg); // Throws
        using sgf = _impl::SharedGroupFriend;
        InRealmHistory::initialize(sgf::get_group(sg)); // Throws
    }

    void initiate_session(version_type) override
    {
        // No-op
    }

    void terminate_session() noexcept override
    {
        // No-op
    }

    version_type prepare_changeset(const char* data, size_t size, version_type orig_version) override
    {
        if (!is_history_updated())
            update_from_parent(orig_version); // Throws
        BinaryData changeset(data, size);
        version_type new_version = add_changeset(changeset); // Throws
        return new_version;
    }

    void finalize_changeset() noexcept override
    {
        // Since the history is in the Realm, the added changeset is
        // automatically finalized as part of the commit operation.
    }

    HistoryType get_history_type() const noexcept override
    {
        return hist_InRealm;
    }

    int get_history_schema_version() const noexcept override
    {
        return g_history_schema_version;
    }

    bool is_upgradable_history_schema(int stored_schema_version) const noexcept override
    {
        // Never called because only one schema version exists so far.
        static_cast<void>(stored_schema_version);
        REALM_ASSERT(false);
        return false;
    }

    void upgrade_history_schema(int stored_schema_version) override
    {
        // Never called because only one schema version exists so far.
        static_cast<void>(stored_schema_version);
        REALM_ASSERT(false);
    }

    _impl::History* get_history() override
    {
        return this;
    }

    BinaryData get_uncommitted_changes() noexcept override
    {
        return TrivialReplication::get_uncommitted_changes();
    }
};

} // unnamed namespace


namespace realm {

std::unique_ptr<Replication> make_in_realm_history(const std::string& realm_path)
{
    return std::unique_ptr<InRealmHistoryImpl>(new InRealmHistoryImpl(realm_path)); // Throws
}

} // namespace realm
