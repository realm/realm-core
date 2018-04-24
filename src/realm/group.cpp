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

#include <new>
#include <algorithm>
#include <set>
#include <fstream>

#ifdef REALM_DEBUG
#include <iostream>
#include <iomanip>
#endif

#include <realm/util/file_mapper.hpp>
#include <realm/util/memory_stream.hpp>
#include <realm/util/miscellaneous.hpp>
#include <realm/util/thread.hpp>
#include <realm/impl/destroy_guard.hpp>
#include <realm/utilities.hpp>
#include <realm/exceptions.hpp>
#include <realm/group_writer.hpp>
#include <realm/db.hpp>
#include <realm/replication.hpp>

using namespace realm;
using namespace realm::util;

namespace {

class Initialization {
public:
    Initialization()
    {
        realm::cpuid_init();
    }
};

Initialization initialization;

} // anonymous namespace


Group::Group()
    : m_local_alloc(new SlabAlloc)
    , m_alloc(*m_local_alloc) // Throws
    , m_top(m_alloc)
    , m_tables(m_alloc)
    , m_table_names(m_alloc)
    , m_is_shared(false)
{
    init_array_parents();
    m_alloc.attach_empty(); // Throws
    m_file_format_version = get_target_file_format_version_for_session(0, Replication::hist_None);
    ref_type top_ref = 0; // Instantiate a new empty group
    bool create_group_when_missing = true;
    attach(top_ref, create_group_when_missing); // Throws
}


Group::Group(const std::string& file, const char* key, OpenMode mode)
    : m_local_alloc(new SlabAlloc) // Throws
    , m_alloc(*m_local_alloc)
    , m_top(m_alloc)
    , m_tables(m_alloc)
    , m_table_names(m_alloc)
    , m_is_shared(false)
    , m_total_rows(0)
{
    init_array_parents();

    open(file, key, mode); // Throws
}


Group::Group(BinaryData buffer, bool take_ownership)
    : m_local_alloc(new SlabAlloc) // Throws
    , m_alloc(*m_local_alloc)
    , m_top(m_alloc)
    , m_tables(m_alloc)
    , m_table_names(m_alloc)
    , m_is_shared(false)
    , m_total_rows(0)
{
    init_array_parents();
    open(buffer, take_ownership); // Throws
}

Group::Group(unattached_tag) noexcept
    : m_local_alloc(new SlabAlloc) // Throws
    , m_alloc(*m_local_alloc)
    , // Throws
    m_top(m_alloc)
    , m_tables(m_alloc)
    , m_table_names(m_alloc)
    , m_is_shared(false)
    , m_total_rows(0)
{
    init_array_parents();
}

Group::Group(shared_tag) noexcept
    : m_local_alloc(new SlabAlloc) // Throws
    , m_alloc(*m_local_alloc)
    , // Throws
    m_top(m_alloc)
    , m_tables(m_alloc)
    , m_table_names(m_alloc)
    , m_is_shared(true)
    , m_total_rows(0)
{
    init_array_parents();
}

Group::Group(SlabAlloc* alloc) noexcept
    : m_alloc(*alloc)
    , // Throws
    m_top(m_alloc)
    , m_tables(m_alloc)
    , m_table_names(m_alloc)
    , m_is_shared(true)
    , m_total_rows(0)
{
    init_array_parents();
}


Group::TableRecycler Group::g_table_recycler_1;
Group::TableRecycler Group::g_table_recycler_2;
std::mutex Group::g_table_recycler_mutex;


TableKeyIterator& TableKeyIterator::operator++()
{
    m_pos++;
    m_index_in_group++;
    load_key();
    return *this;
}

TableKey TableKeyIterator::operator*()
{
    if (!bool(m_table_key)) {
        load_key();
    }
    return m_table_key;
}

void TableKeyIterator::load_key()
{
    const Group& g = *m_group;
    while (m_index_in_group < m_max_index_in_group) {
        RefOrTagged rot = g.m_tables.get_as_ref_or_tagged(m_index_in_group);
        if (rot.is_ref()) {
            if (m_index_in_group < g.m_table_accessors.size() && g.m_table_accessors[m_index_in_group]) {
                m_table_key = g.m_table_accessors[m_index_in_group]->get_key();
            }
            else {
                m_table_key = Table::get_key_direct(g.m_tables.get_alloc(), rot.get_as_ref());
            }
            return;
        }
        m_index_in_group++;
    }
    m_table_key = TableKey();
}

TableKey TableKeys::operator[](size_t p) const
{
    if (p < m_iter.m_pos) {
        m_iter = TableKeyIterator(m_iter.m_group, 0);
    }
    while (m_iter.m_pos < p) {
        ++m_iter;
    }
    return *m_iter;
}

size_t Group::size() const noexcept
{
    return m_num_tables;
}


void Group::set_size() const noexcept
{
    int retval = 0;
    if (is_attached() && m_table_names.is_attached()) {
        size_t max_index = m_tables.size();
        REALM_ASSERT(max_index < (1 << 16));
        for (size_t j = 0; j < max_index; ++j) {
            RefOrTagged rot = m_tables.get_as_ref_or_tagged(j);
            if (rot.is_ref() && rot.get_as_ref()) {
                ++retval;
            }
        }
    }
    m_num_tables = retval;
}


TableKey Group::ndx2key(size_t ndx) const
{
    REALM_ASSERT(is_attached());
    Table* accessor = m_table_accessors[ndx];
    if (accessor)
        return accessor->get_key(); // fast path

    // slow path:
    RefOrTagged rot = m_tables.get_as_ref_or_tagged(ndx);
    if (rot.is_tagged())
        throw InvalidKey("No such table");
    ref_type ref = rot.get_as_ref();
    REALM_ASSERT(ref);
    return Table::get_key_direct(m_tables.get_alloc(), ref);
}


size_t Group::key2ndx(TableKey key) const
{
    size_t idx = key.value & 0xFFFF;
    return idx;
}


size_t Group::key2ndx_checked(TableKey key) const
{
    // FIXME: This is a temporary hack we should revisit.
    // The notion of a const group as it is now, is not really
    // useful. It is linked to a distinction between a read
    // and a write transaction. This distinction is likely to
    // be moved from compile time to run time.
    Allocator* alloc = const_cast<SlabAlloc*>(&m_alloc);
    size_t idx = key2ndx(key);
    if (m_tables.is_attached() && idx < m_tables.size()) {
        RefOrTagged rot = m_tables.get_as_ref_or_tagged(idx);
        if (rot.is_ref() && rot.get_as_ref() && (Table::get_key_direct(*alloc, rot.get_as_ref()) == key)) {

            return idx;
        }
    }
    throw InvalidKey("No corresponding table");
}


int Group::get_file_format_version() const noexcept
{
    return m_file_format_version;
}


void Group::set_file_format_version(int file_format) noexcept
{
    m_file_format_version = file_format;
}


int Group::get_committed_file_format_version() const noexcept
{
    return m_alloc.get_committed_file_format_version();
}


int Group::get_target_file_format_version_for_session(int /* current_file_format_version */,
                                                      int /* requested_history_type */) noexcept
{
    // Note: This function is responsible for choosing the target file format
    // for a sessions. If it selects a file format that is different from
    // `current_file_format_version`, it will trigger a file format upgrade
    // process.

    // Note: `current_file_format_version` may be zero at this time, which means
    // that the file format it is not yet decided (only possible for empty
    // Realms where top-ref is zero).

    // Please see Group::get_file_format_version() for information about the
    // individual file format versions.

    return 10;
}


void Transaction::upgrade_file_format(int target_file_format_version)
{
    REALM_ASSERT(is_attached());

    // Be sure to revisit the following upgrade logic when a new file format
    // version is introduced. The following assert attempt to help you not
    // forget it.
    REALM_ASSERT_EX(target_file_format_version == 10, target_file_format_version);

    int current_file_format_version = get_file_format_version();
    REALM_ASSERT(current_file_format_version < target_file_format_version);

    // SharedGroup::do_open() must ensure this. Be sure to revisit the
    // following upgrade logic when SharedGroup::do_open() is changed (or
    // vice versa).
    REALM_ASSERT_EX(current_file_format_version >= 5 && current_file_format_version <= 9,
                    current_file_format_version);


    // Upgrade from version prior to 7 (new history schema version in top array)
    if (current_file_format_version <= 6 && target_file_format_version >= 7) {
        // If top array size is 9, then add the missing 10th element containing
        // the history schema version.
        std::size_t top_size = m_top.size();
        REALM_ASSERT(top_size <= 9);
        if (top_size == 9) {
            int initial_history_schema_version = 0;
            m_top.add(initial_history_schema_version); // Throws
        }
    }

    // NOTE: Additional future upgrade steps go here.
    if (current_file_format_version <= 9 && target_file_format_version >= 10) {
        bool changes = false;
        std::vector<TableKey> table_keys;
        for (size_t t = 0; t < m_table_names.size(); t++) {
            StringData name = m_table_names.get(t);
            auto table = get_table(name);
            table_keys.push_back(table->get_key());
            changes |= table->convert_columns();
        }

        if (changes) {
            commit_and_continue_as_read();
            promote_to_write();
        }

        for (auto k : table_keys) {
            auto table = get_table(k);
            if (table->create_objects()) {
                commit_and_continue_as_read();
                promote_to_write();
            }
        }

        for (auto k : table_keys) {
            auto table = get_table(k);
            const Spec& spec = _impl::TableFriend::get_spec(*table);
            size_t nb_cols = spec.get_column_count();
            for (size_t col_ndx = 0; col_ndx < nb_cols; col_ndx++) {
                if (table->copy_content_from_columns(col_ndx)) {
                    commit_and_continue_as_read();
                    promote_to_write();
                    table = get_table(k);
                }
            }
        }
    }

    set_file_format_version(target_file_format_version);
}

void Group::open(ref_type top_ref, const std::string& file_path)
{
    SlabAlloc::DetachGuard dg(m_alloc);

    // Select file format if it is still undecided.
    m_file_format_version = m_alloc.get_committed_file_format_version();

    bool file_format_ok = false;
    // It is not possible to open prior file format versions without an upgrade.
    // Since a Realm file cannot be upgraded when opened in this mode
    // (we may be unable to write to the file), no earlier versions can be opened.
    // Please see Group::get_file_format_version() for information about the
    // individual file format versions.
    switch (m_file_format_version) {
        case 0:
            file_format_ok = (top_ref == 0);
            break;
        case 10:
            file_format_ok = true;
            break;
    }
    if (REALM_UNLIKELY(!file_format_ok))
        throw InvalidDatabase("Unsupported Realm file format version", file_path);

    Replication::HistoryType history_type = Replication::hist_None;
    int target_file_format_version = get_target_file_format_version_for_session(m_file_format_version, history_type);
    if (m_file_format_version == 0) {
        set_file_format_version(target_file_format_version);
    }
    else {
        // From a technical point of view, we could upgrade the Realm file
        // format in memory here, but since upgrading can be expensive, it is
        // currently disallowed.
        REALM_ASSERT(target_file_format_version == m_file_format_version);
    }

    // Make all dynamically allocated memory (space beyond the attached file) as
    // available free-space.
    reset_free_space_tracking(); // Throws

    bool create_group_when_missing = true;
    attach(top_ref, create_group_when_missing); // Throws
    dg.release();                               // Do not detach after all
}

void Group::open(const std::string& file_path, const char* encryption_key, OpenMode mode)
{
    if (is_attached() || m_is_shared)
        throw LogicError(LogicError::wrong_group_state);

    SlabAlloc::Config cfg;
    cfg.read_only = mode == mode_ReadOnly;
    cfg.no_create = mode == mode_ReadWriteNoCreate;
    cfg.encryption_key = encryption_key;
    ref_type top_ref = m_alloc.attach_file(file_path, cfg); // Throws

    open(top_ref, file_path);
}


void Group::open(BinaryData buffer, bool take_ownership)
{
    REALM_ASSERT(buffer.data());

    if (is_attached() || m_is_shared)
        throw LogicError(LogicError::wrong_group_state);

    ref_type top_ref = m_alloc.attach_buffer(buffer.data(), buffer.size()); // Throws

    open(top_ref, {});

    if (take_ownership)
        m_alloc.own_buffer();
}


Group::~Group() noexcept
{
    // If this group accessor is detached at this point in time, it is either
    // because it is SharedGroup::m_group (m_is_shared), or it is a free-stading
    // group accessor that was never successfully opened.
    if (!m_top.is_attached())
        return;

    // Free-standing group accessor
    detach();

    // if a local allocator is set in m_local_alloc, then the destruction
    // of m_local_alloc will trigger destruction of the allocator, which will
    // verify that the allocator has been detached, so....
    if (m_local_alloc)
        m_local_alloc->detach();
}


void Group::remap(size_t new_file_size)
{
    m_alloc.update_reader_view(new_file_size); // Throws
    update_allocator_wrappers(m_is_writable);
}


void Group::remap_and_update_refs(ref_type new_top_ref, size_t new_file_size, bool writable)
{
    size_t old_baseline = m_alloc.get_baseline();

    m_alloc.update_reader_view(new_file_size); // Throws
    update_allocator_wrappers(writable);

    // force update of all ref->ptr translations if the mapping has changed
    auto mapping_version = m_alloc.get_mapping_version();
    if (mapping_version != m_last_seen_mapping_version) {
        // force re-translation of all refs
        old_baseline = 0;
        m_last_seen_mapping_version = mapping_version;
    }
    update_refs(new_top_ref, old_baseline);
}


void Group::attach(ref_type top_ref, bool create_group_when_missing)
{
    REALM_ASSERT(!m_top.is_attached());

    // If this function throws, it must leave the group accesor in a the
    // unattached state.

    m_tables.detach();
    m_table_names.detach();
    m_is_writable = create_group_when_missing;

    if (top_ref != 0) {
        m_top.init_from_ref(top_ref);
        size_t top_size = m_top.size();
        static_cast<void>(top_size);

        if (top_size < 8) {
            REALM_ASSERT_EX(top_size == 3 || top_size == 5 || top_size == 7, top_size);
        }
        else {
            REALM_ASSERT_EX(top_size == 9 || top_size == 10, top_size);
        }

        m_table_names.init_from_parent();
        m_tables.init_from_parent();

        // The 3rd slot in m_top is
        // `RefOrTagged::make_tagged(logical_file_size)`, and the logical file
        // size must never exceed actual file size.
        REALM_ASSERT_3(m_top.get_as_ref_or_tagged(2).get_as_int(), <=, m_alloc.get_baseline());
    }
    else if (create_group_when_missing) {
        create_empty_group(); // Throws
    }
    m_attached = true;
    set_size();

    size_t sz = m_tables.is_attached() ? m_tables.size() : 0;
    while (m_table_accessors.size() > sz) {
        if (Table* t = m_table_accessors.back()) {
            t->detach();
            recycle_table_accessor(t);
        }
        m_table_accessors.pop_back();
    }
    while (m_table_accessors.size() < sz) {
        m_table_accessors.emplace_back();
    }
#if REALM_METRICS
    update_num_objects();
#endif // REALM_METRICS
}


void Group::detach() noexcept
{
    detach_table_accessors();
    m_table_accessors.clear();

    m_table_names.detach();
    m_tables.detach();
    m_top.detach();

    m_attached = false;
}

void Group::update_num_objects()
{
#if REALM_METRICS
    if (m_metrics) {
        // FIXME: this is quite invasive and completely defeats the lazy loading mechanism
        // where table accessors are only instantiated on demand, because they are all created here.

        m_total_rows = 0;
        auto keys = get_table_keys();
        for (auto key : keys) {
            ConstTableRef t = get_table(key);
            m_total_rows += t->size();
        }
    }
#endif // REALM_METRICS
}


void Group::attach_shared(ref_type new_top_ref, size_t new_file_size, bool writable)
{
    REALM_ASSERT_3(new_top_ref, <, new_file_size);
    REALM_ASSERT(!is_attached());

    // update readers view of memory
    m_alloc.update_reader_view(new_file_size); // Throws
    update_allocator_wrappers(writable);

    // When `new_top_ref` is null, ask attach() to create a new node structure
    // for an empty group, but only during the initiation of write
    // transactions. When the transaction being initiated is a read transaction,
    // we instead have to leave array accessors m_top, m_tables, and
    // m_table_names in their detached state, as there are no underlying array
    // nodes to attached them to. In the case of write transactions, the nodes
    // have to be created, as they have to be ready for being modified.
    bool create_group_when_missing = writable;
    attach(new_top_ref, create_group_when_missing); // Throws
}


void Group::detach_table_accessors() noexcept
{
    for (auto& table_accessor : m_table_accessors) {
        if (Table* t = table_accessor) {
            t->detach();
            recycle_table_accessor(t);
            table_accessor = nullptr;
        }
    }
}


void Group::create_empty_group()
{
    m_top.create(Array::type_HasRefs); // Throws
    _impl::DeepArrayDestroyGuard dg_top(&m_top);
    {
        m_table_names.create(); // Throws
        _impl::DestroyGuard<ArrayStringShort> dg(&m_table_names);
        m_top.add(m_table_names.get_ref()); // Throws
        dg.release();
    }
    {
        m_tables.create(Array::type_HasRefs); // Throws
        _impl::DestroyGuard<Array> dg(&m_tables);
        m_top.add(m_tables.get_ref()); // Throws
        dg.release();
    }
    size_t initial_logical_file_size = sizeof(SlabAlloc::Header);
    m_top.add(RefOrTagged::make_tagged(initial_logical_file_size)); // Throws
    dg_top.release();
}


Table* Group::do_get_table(size_t table_ndx, DescMatcher desc_matcher)
{
    REALM_ASSERT(m_table_accessors.size() == m_tables.size());
    // Get table accessor from cache if it exists, else create
    Table* table = m_table_accessors[table_ndx];
    if (!table)
        table = create_table_accessor(table_ndx); // Throws

    if (desc_matcher) {
        typedef _impl::TableFriend tf;
        if (desc_matcher && !(*desc_matcher)(tf::get_spec(*table)))
            throw DescriptorMismatch();
    }

    return table;
}


Table* Group::do_get_table(StringData name, DescMatcher desc_matcher)
{
    if (!m_table_names.is_attached())
        return 0;
    size_t table_ndx = m_table_names.find_first(name);
    if (table_ndx == not_found)
        return 0;

    Table* table = do_get_table(table_ndx, desc_matcher); // Throws
    return table;
}


Table* Group::do_get_table(TableKey key, DescMatcher desc_matcher)
{
    if (!m_table_names.is_attached())
        return 0;
    size_t table_ndx = key2ndx(key);
    if (table_ndx >= m_tables.size())
        return 0;

    Table* table = do_get_table(table_ndx, desc_matcher); // Throws
    if (table->get_key() != key) {
        throw InvalidKey("no such key");
    }
    return table;
}


Table* Group::do_add_table(StringData name, DescSetter desc_setter, bool require_unique_name)
{
    if (!m_is_writable)
        throw LogicError(LogicError::wrong_transact_state);
    // get new key and index
    if (!m_table_names.is_attached())
        return 0;
    if (require_unique_name) {
        size_t table_ndx = m_table_names.find_first(name);
        if (table_ndx != not_found)
            throw TableNameInUse();
    }
    // find first empty spot:
    // FIXME: Optimize with rowing ptr or free list of some sort
    size_t j;
    RefOrTagged rot = RefOrTagged::make_tagged(0);
    for (j = 0; j < m_tables.size(); ++j) {
        rot = m_tables.get_as_ref_or_tagged(j);
        if (!rot.is_ref())
            break;
    }
    bool gen_null_tag = (j == m_tables.size()); // new tags start at zero
    uint64_t tag = gen_null_tag ? 0 : rot.get_as_int();
    TableKey key = TableKey((tag << 16) | j);
    create_and_insert_table(key, name);
    Table* table = create_table_accessor(j);
    if (desc_setter)
        (*desc_setter)(*table);
    return table;
}


Table* Group::do_add_table(TableKey key, StringData name, DescSetter desc_setter, bool require_unique_name)
{
    if (!m_table_names.is_attached())
        return 0;
    auto ndx = key2ndx(key);
    if (m_tables.is_attached() && m_tables.size() > ndx) {
        auto rot = m_tables.get_as_ref_or_tagged(ndx);
        // validate that no such table is already there
        if (rot.is_ref())
            throw InvalidKey("Key already in use");
        REALM_ASSERT(m_table_accessors[ndx] == nullptr);
    }
    // validate name if required
    if (require_unique_name) {
        size_t table_ndx = m_table_names.find_first(name);
        if (table_ndx != not_found)
            throw TableNameInUse();
    }
    create_and_insert_table(key, name);
    Table* table = create_table_accessor(ndx);
    if (desc_setter)
        (*desc_setter)(*table);
    return table;
}

Table* Group::do_get_or_add_table(StringData name, DescMatcher desc_matcher, DescSetter desc_setter, bool* was_added)
{
    REALM_ASSERT(m_table_names.is_attached());
    auto table = do_get_table(name, desc_matcher);
    if (table) {
        if (was_added)
            *was_added = false;
        return table;
    }
    else {
        table = do_add_table(name, desc_setter, false);
        if (was_added)
            *was_added = true;
        return table;
    }
}


Table* Group::do_get_or_add_table(TableKey key, StringData name, DescMatcher desc_matcher, DescSetter desc_setter,
                                  bool* was_added)
{
    REALM_ASSERT(m_table_names.is_attached());
    auto table = do_get_table(key, desc_matcher);
    if (table) {
        if (m_table_names.get(key2ndx(key)) != name) {
            throw InvalidKey("Key and name does not match");
        }
        if (was_added)
            *was_added = false;
        return table;
    }
    else {
        table = do_add_table(key, name, desc_setter, false);
        if (was_added)
            *was_added = true;
        return table;
    }
}


void Group::create_and_insert_table(TableKey key, StringData name)
{
    if (REALM_UNLIKELY(name.size() > max_table_name_length))
        throw LogicError(LogicError::table_name_too_long);

    using namespace _impl;
    typedef TableFriend tf;
    size_t table_ndx = key2ndx(key);
    ref_type ref = tf::create_empty_table(m_alloc, key); // Throws
    REALM_ASSERT_3(m_tables.size(), ==, m_table_names.size());
    size_t prior_num_tables = m_tables.size();
    RefOrTagged rot = RefOrTagged::make_ref(ref);
    REALM_ASSERT(m_table_accessors.size() == m_tables.size());

    if (table_ndx == m_tables.size()) {
        m_tables.add(rot);
        m_table_names.add(name);
        // Need new slot for table accessor
        m_table_accessors.push_back(nullptr);
    }
    else {
        m_tables.set(table_ndx, rot);       // Throws
        m_table_names.set(table_ndx, name); // Throws
    }

    if (Replication* repl = m_alloc.get_replication())
        repl->insert_group_level_table(key, prior_num_tables, name); // Throws
    ++m_num_tables;
}


Table* Group::create_table_accessor(size_t table_ndx)
{
    REALM_ASSERT(m_tables.size() == m_table_accessors.size());
    REALM_ASSERT(table_ndx < m_table_accessors.size());

    typedef _impl::TableFriend tf;
    RefOrTagged rot = m_tables.get_as_ref_or_tagged(table_ndx);
    ref_type ref = rot.get_as_ref();
    if (ref == 0) {
        throw InvalidKey("No such table");
    }
    Table* table = 0;
    {
        std::lock_guard<std::mutex> lg(g_table_recycler_mutex);
        if (g_table_recycler_2.empty()) {
            while (!g_table_recycler_1.empty()) {
                auto t = g_table_recycler_1.back();
                g_table_recycler_1.pop_back();
                g_table_recycler_2.push_back(t);
            }
        }
        if (g_table_recycler_2.size() + g_table_recycler_1.size() > g_table_recycling_delay) {
            table = g_table_recycler_2.back();
            table->fully_detach();
            g_table_recycler_2.pop_back();
        }
    }
    if (table) {
        table->revive(m_alloc, m_is_writable);
        table->init(ref, this, table_ndx, m_is_writable);
    }
    else {
        table = tf::create_accessor(m_alloc, ref, this, table_ndx, m_is_writable); // Throws
    }
    m_table_accessors[table_ndx] = table;
    tf::complete_accessor(*table);
    return table;
}


void Group::recycle_table_accessor(Table* to_be_recycled)
{
    std::lock_guard<std::mutex> lg(g_table_recycler_mutex);
    g_table_recycler_1.push_back(to_be_recycled);
}

void Group::remove_table(StringData name)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    size_t table_ndx = m_table_names.find_first(name);
    if (table_ndx == not_found)
        throw NoSuchTable();
    auto key = ndx2key(table_ndx);
    remove_table(table_ndx, key); // Throws
}


void Group::remove_table(TableKey key)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);

    size_t table_ndx = key2ndx_checked(key);
    remove_table(table_ndx, key);
}


void Group::remove_table(size_t table_ndx, TableKey key)
{
    if (!m_is_writable)
        throw LogicError(LogicError::wrong_transact_state);
    REALM_ASSERT_3(m_tables.size(), ==, m_table_names.size());
    if (table_ndx >= m_tables.size())
        throw LogicError(LogicError::table_index_out_of_range);
    TableRef table = get_table(key);

    // In principle we could remove a table even if it is the target of link
    // columns of other tables, however, to do that, we would have to
    // automatically remove the "offending" link columns from those other
    // tables. Such a behaviour is deemed too obscure, and we shall therefore
    // require that a removed table does not contain foreigh origin backlink
    // columns.
    typedef _impl::TableFriend tf;
    if (tf::is_cross_table_link_target(*table))
        throw CrossTableLinkTarget();

    // There is no easy way for Group::TransactAdvancer to handle removal of
    // tables that contain foreign target table link columns, because that
    // involves removal of the corresponding backlink columns. For that reason,
    // we start by removing all columns, which will generate individual
    // replication instructions for each column removal with sufficient
    // information for Group::TransactAdvancer to handle them.
    size_t n = table->get_column_count();
    for (size_t i = n; i > 0; --i) {
        ColKey col_key = table->ndx2colkey(i - 1);
        table->remove_column(col_key);
    }

    size_t prior_num_tables = m_tables.size();
    if (Replication* repl = m_alloc.get_replication())
        repl->erase_group_level_table(key, prior_num_tables); // Throws

    int64_t ref_64 = m_tables.get(table_ndx);
    REALM_ASSERT(!int_cast_has_overflow<ref_type>(ref_64));
    ref_type ref = ref_type(ref_64);

    // Replace entry in m_tables with next tag to use:
    RefOrTagged rot = RefOrTagged::make_tagged(1 + (key.value >> 16));
    // Remove table
    m_tables.set(table_ndx, rot);     // Throws
    m_table_names.set(table_ndx, {}); // Throws
    m_table_accessors[table_ndx] = nullptr;
    --m_num_tables;

    table->detach();
    // Destroy underlying node structure
    Array::destroy_deep(ref, m_alloc);
    recycle_table_accessor(table);
}


void Group::rename_table(StringData name, StringData new_name, bool require_unique_name)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    size_t table_ndx = m_table_names.find_first(name);
    if (table_ndx == not_found)
        throw NoSuchTable();
    rename_table(ndx2key(table_ndx), new_name, require_unique_name); // Throws
}


void Group::rename_table(TableKey key, StringData new_name, bool require_unique_name)
{
    if (REALM_UNLIKELY(!is_attached()))
        throw LogicError(LogicError::detached_accessor);
    if (!m_is_writable)
        throw LogicError(LogicError::wrong_transact_state);
    REALM_ASSERT_3(m_tables.size(), ==, m_table_names.size());
    if (require_unique_name && has_table(new_name))
        throw TableNameInUse();
    size_t table_ndx = key2ndx_checked(key);
    m_table_names.set(table_ndx, new_name);
    if (Replication* repl = m_alloc.get_replication())
        repl->rename_group_level_table(key, new_name); // Throws
}


class Group::DefaultTableWriter : public Group::TableWriter {
public:
    DefaultTableWriter(const Group& group)
        : m_group(group)
    {
    }
    ref_type write_names(_impl::OutputStream& out) override
    {
        bool deep = true;                                                // Deep
        bool only_if_modified = false;                                   // Always
        return m_group.m_table_names.write(out, deep, only_if_modified); // Throws
    }
    ref_type write_tables(_impl::OutputStream& out) override
    {
        bool deep = true;                                           // Deep
        bool only_if_modified = false;                              // Always
        return m_group.m_tables.write(out, deep, only_if_modified); // Throws
    }

private:
    const Group& m_group;
};

void Group::write(std::ostream& out, bool pad) const
{
    write(out, pad, 0);
}

void Group::write(std::ostream& out, bool pad_for_encryption, uint_fast64_t version_number) const
{
    REALM_ASSERT(is_attached());
    DefaultTableWriter table_writer(*this);
    bool no_top_array = !m_top.is_attached();
    write(out, m_file_format_version, table_writer, no_top_array, pad_for_encryption, version_number); // Throws
}

void Group::write(const std::string& path, const char* encryption_key) const
{
    write(path, encryption_key, 0);
}

void Group::write(const std::string& path, const char* encryption_key, uint_fast64_t version_number) const
{
    File file;
    int flags = 0;
    file.open(path, File::access_ReadWrite, File::create_Must, flags);
    write(file, encryption_key, version_number);
}

void Group::write(File& file, const char* encryption_key, uint_fast64_t version_number) const
{
    REALM_ASSERT(file.get_size() == 0);

    file.set_encryption_key(encryption_key);
    File::Streambuf streambuf(&file);
    std::ostream out(&streambuf);
    out.exceptions(std::ios_base::failbit | std::ios_base::badbit);
    write(out, encryption_key != 0, version_number);
    int sync_status = streambuf.pubsync();
    REALM_ASSERT(sync_status == 0);
}

BinaryData Group::write_to_mem() const
{
    REALM_ASSERT(is_attached());

    // Get max possible size of buffer
    //
    // FIXME: This size could potentially be vastly bigger that what
    // is actually needed.
    size_t max_size = m_alloc.get_total_size();

    char* buffer = static_cast<char*>(malloc(max_size)); // Throws
    if (!buffer)
        throw std::bad_alloc();
    try {
        MemoryOutputStream out; // Throws
        out.set_buffer(buffer, buffer + max_size);
        write(out); // Throws
        size_t buffer_size = out.size();
        return BinaryData(buffer, buffer_size);
    }
    catch (...) {
        free(buffer);
        throw;
    }
}


void Group::write(std::ostream& out, int file_format_version, TableWriter& table_writer, bool no_top_array,
                  bool pad_for_encryption, uint_fast64_t version_number)
{
    _impl::OutputStream out_2(out);

    // Write the file header
    SlabAlloc::Header streaming_header;
    if (no_top_array) {
        file_format_version = 0;
    }
    else if (file_format_version == 0) {
        // Use current file format version
        file_format_version = get_target_file_format_version_for_session(0, Replication::hist_None);
    }
    SlabAlloc::init_streaming_header(&streaming_header, file_format_version);
    out_2.write(reinterpret_cast<const char*>(&streaming_header), sizeof streaming_header);

    ref_type top_ref = 0;
    size_t final_file_size = sizeof streaming_header;
    if (no_top_array) {
        // Accept version number 1 as that number is (unfortunately) also used
        // to denote the empty initial state of a Realm file.
        REALM_ASSERT(version_number == 0 || version_number == 1);
    }
    else {
        // Because we need to include the total logical file size in the
        // top-array, we have to start by writing everything except the
        // top-array, and then finally compute and write a correct version of
        // the top-array. The free-space information of the group will only be
        // included if a non-zero version number is given as parameter,
        // indicating that versioning info is to be saved. This is used from
        // SharedGroup to compact the database by writing only the live data
        // into a separate file.
        ref_type names_ref = table_writer.write_names(out_2);   // Throws
        ref_type tables_ref = table_writer.write_tables(out_2); // Throws
        SlabAlloc new_alloc;
        new_alloc.attach_empty(); // Throws
        Array top(new_alloc);
        top.create(Array::type_HasRefs); // Throws
        _impl::ShallowArrayDestroyGuard dg_top(&top);
        // FIXME: We really need an alternative to Array::truncate() that is able to expand.
        int_fast64_t value_1 = from_ref(names_ref);
        int_fast64_t value_2 = from_ref(tables_ref);
        top.add(value_1); // Throws
        top.add(value_2); // Throws
        top.add(0);       // Throws

        int top_size = 3;
        if (version_number) {
            Array free_list(new_alloc);
            Array size_list(new_alloc);
            Array version_list(new_alloc);
            free_list.create(Array::type_Normal); // Throws
            _impl::DeepArrayDestroyGuard dg_1(&free_list);
            size_list.create(Array::type_Normal); // Throws
            _impl::DeepArrayDestroyGuard dg_2(&size_list);
            version_list.create(Array::type_Normal); // Throws
            _impl::DeepArrayDestroyGuard dg_3(&version_list);
            bool deep = true;              // Deep
            bool only_if_modified = false; // Always
            ref_type free_list_ref = free_list.write(out_2, deep, only_if_modified);
            ref_type size_list_ref = size_list.write(out_2, deep, only_if_modified);
            ref_type version_list_ref = version_list.write(out_2, deep, only_if_modified);
            top.add(RefOrTagged::make_ref(free_list_ref));     // Throws
            top.add(RefOrTagged::make_ref(size_list_ref));     // Throws
            top.add(RefOrTagged::make_ref(version_list_ref));  // Throws
            top.add(RefOrTagged::make_tagged(version_number)); // Throws
            top_size = 7;
        }
        top_ref = out_2.get_ref_of_next_array();

        // Produce a preliminary version of the top array whose
        // representation is guaranteed to be able to hold the final file
        // size
        size_t max_top_byte_size = Array::get_max_byte_size(top_size);
        size_t max_final_file_size = size_t(top_ref) + max_top_byte_size;
        top.ensure_minimum_width(RefOrTagged::make_tagged(max_final_file_size)); // Throws

        // Finalize the top array by adding the projected final file size
        // to it
        size_t top_byte_size = top.get_byte_size();
        final_file_size = size_t(top_ref) + top_byte_size;
        top.set(2, RefOrTagged::make_tagged(final_file_size)); // Throws

        // Write the top array
        bool deep = false;                        // Shallow
        bool only_if_modified = false;            // Always
        top.write(out_2, deep, only_if_modified); // Throws
        REALM_ASSERT_3(size_t(out_2.get_ref_of_next_array()), ==, final_file_size);

        dg_top.reset(nullptr); // Destroy now
    }

    // encryption will pad the file to a multiple of the page, so ensure the
    // footer is aligned to the end of a page
    if (pad_for_encryption) {
#if REALM_ENABLE_ENCRYPTION
        size_t unrounded_size = final_file_size + sizeof(SlabAlloc::StreamingFooter);
        size_t rounded_size = round_up_to_page_size(unrounded_size);
        if (rounded_size != unrounded_size) {
            std::unique_ptr<char[]> buffer(new char[rounded_size - unrounded_size]());
            out_2.write(buffer.get(), rounded_size - unrounded_size);
        }
#endif
    }

    // Write streaming footer
    SlabAlloc::StreamingFooter footer;
    footer.m_top_ref = top_ref;
    footer.m_magic_cookie = SlabAlloc::footer_magic_cookie;
    out_2.write(reinterpret_cast<const char*>(&footer), sizeof footer);
}


void Group::commit()
{
    if (!is_attached())
        throw LogicError(LogicError::detached_accessor);
    if (m_is_shared)
        throw LogicError(LogicError::wrong_group_state);

    GroupWriter out(*this); // Throws

    // Recursively write all changed arrays to the database file. We
    // postpone the commit until we are sure that no exceptions can be
    // thrown.
    ref_type top_ref = out.write_group(); // Throws

    // Since the group is persisiting in single-thread (unshared)
    // mode we have to make sure that the group stays valid after
    // commit

    // Mark all managed space (beyond the attached file) as free.
    reset_free_space_tracking(); // Throws

    size_t old_baseline = m_alloc.get_baseline();

    // Update view of the file
    size_t new_file_size = out.get_file_size();
    m_alloc.update_reader_view(new_file_size); // Throws
    update_allocator_wrappers(true);

    out.commit(top_ref); // Throws

    // Recursively update refs in all active tables (columns, arrays..)
    auto mapping_version = m_alloc.get_mapping_version();
    if (mapping_version != m_last_seen_mapping_version) {
        // force re-translation of all refs
        old_baseline = 0;
        m_last_seen_mapping_version = mapping_version;
    }
    update_refs(top_ref, old_baseline);
}


void Group::update_refs(ref_type top_ref, size_t old_baseline) noexcept
{
    old_baseline = 0; // force update of all accessors
    // After Group::commit() we will always have free space tracking
    // info.
    REALM_ASSERT_3(m_top.size(), >=, 5);

    // Array nodes that are part of the previous version of the
    // database will not be overwritten by Group::commit(). This is
    // necessary for robustness in the face of abrupt termination of
    // the process. It also means that we can be sure that an array
    // remains unchanged across a commit if the new ref is equal to
    // the old ref and the ref is below the previous baseline.

    if (top_ref < old_baseline && m_top.get_ref() == top_ref)
        return;

    m_top.init_from_ref(top_ref);

    // Now we can update it's child arrays
    m_table_names.update_from_parent(old_baseline);

    // If m_tables has not been modfied we don't
    // need to update attached table accessors
    if (!m_tables.update_from_parent(old_baseline))
        return;

    // Update all attached table accessors.
    for (auto& table_accessor : m_table_accessors) {
        if (table_accessor) {
            table_accessor->update_from_parent(old_baseline);
        }
    }
}

bool Group::operator==(const Group& g) const
{
    auto keys_this = get_table_keys();
    auto keys_g = g.get_table_keys();
    size_t n = keys_this.size();
    if (n != keys_g.size())
        return false;
    for (size_t i = 0; i < n; ++i) {
        const StringData& table_name_1 = get_table_name(keys_this[i]);
        const StringData& table_name_2 = g.get_table_name(keys_g[i]);
        if (table_name_1 != table_name_2)
            return false;

        ConstTableRef table_1 = get_table(keys_this[i]);
        ConstTableRef table_2 = g.get_table(keys_g[i]);
        if (*table_1 != *table_2)
            return false;
    }
    return true;
}


size_t Group::compute_aggregated_byte_size() const noexcept
{
    if (!is_attached())
        return 0;
    MemStats stats_2;
    m_top.stats(stats_2);
    return stats_2.allocated;
}


void Group::to_string(std::ostream& out) const
{
    // Calculate widths
    size_t index_width = 16;
    size_t name_width = 10;
    size_t rows_width = 6;

    auto keys = get_table_keys();
    for (auto key : keys) {
        StringData name = get_table_name(key);
        if (name_width < name.size())
            name_width = name.size();

        ConstTableRef table = get_table(name);
        size_t row_count = table->size();
        if (rows_width < row_count) { // FIXME: should be the number of digits in row_count: floor(log10(row_count+1))
            rows_width = row_count;
        }
    }


    // Print header
    out << std::setw(int(index_width + 1)) << std::left << " ";
    out << std::setw(int(name_width + 1)) << std::left << "tables";
    out << std::setw(int(rows_width)) << std::left << "rows" << std::endl;

    // Print tables
    for (auto key : keys) {
        StringData name = get_table_name(key);
        ConstTableRef table = get_table(name);
        size_t row_count = table->size();

        out << std::setw(int(index_width)) << std::right << key.value << " ";
        out << std::setw(int(name_width)) << std::left << std::string(name) << " ";
        out << std::setw(int(rows_width)) << std::left << row_count << std::endl;
    }
}



// In general, this class cannot assume more than minimal accessor consistency
// (See AccessorConsistencyLevels., it can however assume that replication
// instruction arguments are meaningfull with respect to the current state of
// the accessor hierarchy. For example, a column index argument of `i` is known
// to refer to the `i`'th entry of Table::m_cols.
//
// FIXME: There is currently no checking on valid instruction arguments such as
// column index within bounds. Consider whether we can trust the contents of the
// transaction log enough to skip these checks.
class Group::TransactAdvancer {
public:
    TransactAdvancer(Group&, bool& schema_changed)
        : m_schema_changed(schema_changed)
    {
    }

    bool insert_group_level_table(TableKey, size_t, StringData) noexcept
    {
        m_schema_changed = true;

        return true;
    }

    bool erase_group_level_table(TableKey, size_t) noexcept
    {
        m_schema_changed = true;

        return true;
    }

    bool rename_group_level_table(TableKey, StringData) noexcept
    {
        // No-op since table names are properties of the group, and the group
        // accessor is always refreshed
        m_schema_changed = true;
        return true;
    }

    bool select_table(TableKey) noexcept
    {
        return true;
    }

    bool create_object(ObjKey) noexcept
    {
        return true;
    }

    bool remove_object(ObjKey) noexcept
    {
        return true;
    }

    bool clear_table(size_t) noexcept
    {
        return true;
    }

    bool set_int(ColKey, ObjKey, int_fast64_t, _impl::Instruction, size_t) noexcept
    {
        return true; // No-op
    }

    bool add_int(ColKey, ObjKey, int_fast64_t) noexcept
    {
        return true; // No-op
    }

    bool set_bool(ColKey, ObjKey, bool, _impl::Instruction) noexcept
    {
        return true; // No-op
    }

    bool set_float(ColKey, ObjKey, float, _impl::Instruction) noexcept
    {
        return true; // No-op
    }

    bool set_double(ColKey, ObjKey, double, _impl::Instruction) noexcept
    {
        return true; // No-op
    }

    bool set_string(ColKey, ObjKey, StringData, _impl::Instruction, size_t) noexcept
    {
        return true; // No-op
    }

    bool set_binary(ColKey, ObjKey, BinaryData, _impl::Instruction) noexcept
    {
        return true; // No-op
    }

    bool set_timestamp(ColKey, ObjKey, Timestamp, _impl::Instruction) noexcept
    {
        return true; // No-op
    }

    bool set_null(ColKey, ObjKey, _impl::Instruction, size_t) noexcept
    {
        return true; // No-op
    }

    bool set_link(ColKey, ObjKey, ObjKey, TableKey, _impl::Instruction) noexcept
    {
        return true;
    }

    bool insert_substring(ColKey, ObjKey, size_t, StringData)
    {
        return true; // No-op
    }

    bool erase_substring(ColKey, ObjKey, size_t, size_t)
    {
        return true; // No-op
    }

    bool list_set_int(size_t, int64_t)
    {
        return true;
    }

    bool list_set_bool(size_t, bool)
    {
        return true;
    }

    bool list_set_float(size_t, float)
    {
        return true;
    }

    bool list_set_double(size_t, double)
    {
        return true;
    }

    bool list_set_string(size_t, StringData)
    {
        return true;
    }

    bool list_set_binary(size_t, BinaryData)
    {
        return true;
    }

    bool list_set_timestamp(size_t, Timestamp)
    {
        return true;
    }

    bool list_insert_int(size_t, int64_t, size_t)
    {
        return true;
    }

    bool list_insert_bool(size_t, bool, size_t)
    {
        return true;
    }

    bool list_insert_float(size_t, float, size_t)
    {
        return true;
    }

    bool list_insert_double(size_t, double, size_t)
    {
        return true;
    }

    bool list_insert_string(size_t, StringData, size_t)
    {
        return true;
    }

    bool list_insert_binary(size_t, BinaryData, size_t)
    {
        return true;
    }

    bool list_insert_timestamp(size_t, Timestamp, size_t)
    {
        return true;
    }

    bool enumerate_string_column(ColKey)
    {
        return true; // No-op
    }

    bool insert_column(ColKey, DataType, StringData, bool, bool)
    {
        m_schema_changed = true;

        return true;
    }

    bool insert_link_column(ColKey, DataType, StringData, TableKey, ColKey)
    {
        m_schema_changed = true;

        return true;
    }

    bool erase_column(ColKey)
    {
        m_schema_changed = true;

        return true;
    }

    bool erase_link_column(ColKey, TableKey, ColKey)
    {
        m_schema_changed = true;

        return true;
    }

    bool rename_column(ColKey, StringData) noexcept
    {
        m_schema_changed = true;
        return true; // No-op
    }

    bool add_search_index(ColKey) noexcept
    {
        return true; // No-op
    }

    bool remove_search_index(ColKey) noexcept
    {
        return true; // No-op
    }

    bool add_primary_key(size_t) noexcept
    {
        return true; // No-op
    }

    bool remove_primary_key() noexcept
    {
        return true; // No-op
    }

    bool set_link_type(ColKey, LinkType) noexcept
    {
        return true; // No-op
    }

    bool select_list(ColKey, ObjKey) noexcept
    {
        return true; // No-op
    }

    bool list_set_link(size_t, ObjKey) noexcept
    {
        return true; // No-op
    }

    bool list_insert_link(size_t, ObjKey, size_t) noexcept
    {
        return true; // No-op
    }

    bool list_insert_null(size_t, size_t)
    {
        return true;
    }

    bool list_move(size_t, size_t) noexcept
    {
        return true; // No-op
    }

    bool list_swap(size_t, size_t) noexcept
    {
        return true; // No-op
    }

    bool list_erase(size_t, size_t) noexcept
    {
        return true; // No-op
    }

    bool list_clear(size_t) noexcept
    {
        return true; // No-op
    }

    bool nullify_link(ColKey, ObjKey, TableKey)
    {
        return true; // No-op
    }

    bool link_list_nullify(size_t, size_t)
    {
        return true; // No-op
    }

private:
    bool& m_schema_changed;
};


void Group::update_allocator_wrappers(bool writable)
{
    m_is_writable = writable;
    // FIXME: We can't write protect at group level as the allocator is shared: m_alloc.set_read_only(!writable);
    for (size_t i = 0; i < m_table_accessors.size(); ++i) {
        auto table_accessor = m_table_accessors[i];
        if (table_accessor) {
            table_accessor->update_allocator_wrapper(writable);
        }
    }
}


void Group::refresh_dirty_accessors()
{
    if (!m_tables.is_attached()) {
        m_table_accessors.clear();
        return;
    }

    // The array of Tables cannot have shrunk:
    REALM_ASSERT(m_tables.size() >= m_table_accessors.size());

    // but it may have grown - and if so, we must resize the accessor array to match
    if (m_tables.size() > m_table_accessors.size()) {
        m_table_accessors.resize(m_tables.size());
    }

    // Update all attached table accessors.
    for (size_t i = 0; i < m_table_accessors.size(); ++i) {
        auto& table_accessor = m_table_accessors[i];
        if (table_accessor) {
            // If the table has changed it's key in the file, it's a
            // new table. This will detach the old accessor and remove it.
            RefOrTagged rot = m_tables.get_as_ref_or_tagged(i);
            bool same_table = false;
            if (rot.is_ref()) {
                auto ref = rot.get_as_ref();
                TableKey new_key = Table::get_key_direct(m_alloc, ref);
                if (new_key == table_accessor->get_key())
                    same_table = true;
            }
            if (same_table) {
                table_accessor->refresh_accessor_tree();
                // FIXME: Move these into table::refresh_accessor_tree ??
                table_accessor->bump_storage_version();
                table_accessor->bump_content_version();
            }
            else {
                table_accessor->detach();
                recycle_table_accessor(table_accessor);
                m_table_accessors[i] = nullptr;
            }
        }
    }
}


void Group::advance_transact(ref_type new_top_ref, size_t new_file_size, _impl::NoCopyInputStream& in, bool writable)
{
    REALM_ASSERT(is_attached());
    // REALM_ASSERT(false); // FIXME: accessor updates need to be handled differently

    // Exception safety: If this function throws, the group accessor and all of
    // its subordinate accessors are left in a state that may not be fully
    // consistent. Only minimal consistency is guaranteed (see
    // AccessorConsistencyLevels). In this case, the application is required to
    // either destroy the Group object, forcing all subordinate accessors to
    // become detached, or take some other equivalent action that involves a
    // call to Group::detach(), such as terminating the transaction in progress.
    // such actions will also lead to the detachment of all subordinate
    // accessors. Until then it is an error, and unsafe if the application
    // attempts to access the group one of its subordinate accessors.
    //
    //
    // The purpose of this function is to refresh all attached accessors after
    // the underlying node structure has undergone arbitrary change, such as
    // when a read transaction has been advanced to a later snapshot of the
    // database.
    //
    // Initially, when this function is invoked, we cannot assume any
    // correspondance between the accessor state and the underlying node
    // structure. We can assume that the hierarchy is in a state of minimal
    // consistency, and that it can be brought to a state of structural
    // correspondace using information in the transaction logs. When structural
    // correspondace is achieved, we can reliably refresh the accessor hierarchy
    // (Table::refresh_accessor_tree()) to bring it back to a fully concsistent
    // state. See AccessorConsistencyLevels.
    //
    // Much of the information in the transaction logs is not used in this
    // process, because the changes have already been applied to the underlying
    // node structure. All we need to do here is to bring the accessors back
    // into a state where they correctly reflect the underlying structure (or
    // detach them if the underlying object has been removed.)
    //
    // The consequences of the changes in the transaction logs can be divided
    // into two types; those that need to be applied to the accessors
    // immediately (Table::adj_insert_column()), and those that can be "lumped
    // together" and deduced during a final accessor refresh operation
    // (Table::refresh_accessor_tree()).
    //
    // Most transaction log instructions have consequences of both types. For
    // example, when an "insert column" instruction is seen, we must immediately
    // shift the positions of all existing columns accessors after the point of
    // insertion. For practical reasons, and for efficiency, we will just insert
    // a null pointer into `Table::m_cols` at this time, and then postpone the
    // creation of the column accessor to the final per-table accessor refresh
    // operation.
    //
    // The final per-table refresh operation visits each table accessor
    // recursively starting from the roots (group-level tables). It relies on
    // the the per-table accessor dirty flags (Table::m_dirty) to prune the
    // traversal to the set of accessors that were touched by the changes in the
    // transaction logs.
    // Update memory mapping if database file has grown

    // FIXME: When called from Transaction::internal_advance_read(), a previous
    // call has already updated mappings and wrappers to the new state. By aligning
    // other callers, we could remove the 2 calls below:
    m_alloc.update_reader_view(new_file_size); // Throws
    update_allocator_wrappers(writable);

    // This is no longer needed in Core, but we need to compute "schema_changed",
    // for the benefit of ObjectStore.
    bool schema_changed = false;
    _impl::TransactLogParser parser; // Throws
    TransactAdvancer advancer(*this, schema_changed);
    parser.parse(in, advancer); // Throws

    m_top.detach();                                 // Soft detach
    bool create_group_when_missing = false;         // See Group::attach_shared().
    attach(new_top_ref, create_group_when_missing); // Throws
    refresh_dirty_accessors();                      // Throws

    if (schema_changed)
        send_schema_change_notification();
}


void Group::prepare_history_parent(BPlusTreeBase& history_root, int history_type, int history_schema_version)
{
    REALM_ASSERT(m_file_format_version >= 7);
    if (m_top.size() < 10) {
        REALM_ASSERT(m_top.size() <= 7);
        while (m_top.size() < 7) {
            m_top.add(0); // Throws
        }
        ref_type history_ref = 0; // No history yet
        m_top.add(RefOrTagged::make_tagged(history_type)); // Throws
        m_top.add(RefOrTagged::make_ref(history_ref)); // Throws
        m_top.add(RefOrTagged::make_tagged(history_schema_version)); // Throws
    }
    else {
        int stored_history_type = int(m_top.get_as_ref_or_tagged(7).get_as_int());
        int stored_history_schema_version = int(m_top.get_as_ref_or_tagged(9).get_as_int());
        if (stored_history_type != Replication::hist_None) {
            REALM_ASSERT(stored_history_type == history_type);
            REALM_ASSERT(stored_history_schema_version == history_schema_version);
        }
        m_top.set(7, RefOrTagged::make_tagged(history_type)); // Throws
        m_top.set(9, RefOrTagged::make_tagged(history_schema_version)); // Throws
    }
    set_history_parent(history_root);
}


#ifdef REALM_DEBUG // LCOV_EXCL_START ignore debug functions

namespace {

class MemUsageVerifier : public Array::MemUsageHandler {
public:
    MemUsageVerifier(ref_type ref_begin, ref_type immutable_ref_end, ref_type mutable_ref_end, ref_type baseline)
        : m_ref_begin(ref_begin)
        , m_immutable_ref_end(immutable_ref_end)
        , m_mutable_ref_end(mutable_ref_end)
        , m_baseline(baseline)
    {
    }
    void add_immutable(ref_type ref, size_t size)
    {
        REALM_ASSERT_3(ref % 8, ==, 0);  // 8-byte alignment
        REALM_ASSERT_3(size % 8, ==, 0); // 8-byte alignment
        REALM_ASSERT_3(size, >, 0);
        REALM_ASSERT_3(ref, >=, m_ref_begin);
        REALM_ASSERT_3(size, <=, m_immutable_ref_end - ref);
        Chunk chunk;
        chunk.ref = ref;
        chunk.size = size;
        m_chunks.push_back(chunk);
    }
    void add_mutable(ref_type ref, size_t size)
    {
        REALM_ASSERT_3(ref % 8, ==, 0);  // 8-byte alignment
        REALM_ASSERT_3(size % 8, ==, 0); // 8-byte alignment
        REALM_ASSERT_3(size, >, 0);
        REALM_ASSERT_3(ref, >=, m_immutable_ref_end);
        REALM_ASSERT_3(size, <=, m_mutable_ref_end - ref);
        Chunk chunk;
        chunk.ref = ref;
        chunk.size = size;
        m_chunks.push_back(chunk);
    }
    void add(ref_type ref, size_t size)
    {
        REALM_ASSERT_3(ref % 8, ==, 0);  // 8-byte alignment
        REALM_ASSERT_3(size % 8, ==, 0); // 8-byte alignment
        REALM_ASSERT_3(size, >, 0);
        REALM_ASSERT_3(ref, >=, m_ref_begin);
        REALM_ASSERT(size <= (ref < m_baseline ? m_immutable_ref_end : m_mutable_ref_end) - ref);
        Chunk chunk;
        chunk.ref = ref;
        chunk.size = size;
        m_chunks.push_back(chunk);
    }
    void add(const MemUsageVerifier& verifier)
    {
        m_chunks.insert(m_chunks.end(), verifier.m_chunks.begin(), verifier.m_chunks.end());
    }
    void handle(ref_type ref, size_t allocated, size_t) override
    {
        add(ref, allocated);
    }
    void canonicalize()
    {
        // Sort the chunks in order of increasing ref, then merge adjacent
        // chunks while checking that there is no overlap
        typedef std::vector<Chunk>::iterator iter;
        iter i_1 = m_chunks.begin(), end = m_chunks.end();
        iter i_2 = i_1;
        sort(i_1, end);
        if (i_1 != end) {
            while (++i_2 != end) {
                ref_type prev_ref_end = i_1->ref + i_1->size;
                REALM_ASSERT_3(prev_ref_end, <=, i_2->ref);
                if (i_2->ref == prev_ref_end) {
                    i_1->size += i_2->size; // Merge
                }
                else {
                    *++i_1 = *i_2;
                }
            }
            m_chunks.erase(i_1 + 1, end);
        }
    }
    void clear()
    {
        m_chunks.clear();
    }
    void check_total_coverage()
    {
        REALM_ASSERT_3(m_chunks.size(), ==, 1);
        REALM_ASSERT_3(m_chunks.front().ref, ==, m_ref_begin);
        REALM_ASSERT_3(m_chunks.front().size, ==, m_mutable_ref_end - m_ref_begin);
    }

private:
    struct Chunk {
        ref_type ref;
        size_t size;
        bool operator<(const Chunk& c) const
        {
            return ref < c.ref;
        }
    };
    std::vector<Chunk> m_chunks;
    ref_type m_ref_begin, m_immutable_ref_end, m_mutable_ref_end, m_baseline;
};

} // anonymous namespace

#endif

void Group::verify() const
{
#ifdef REALM_DEBUG
    REALM_ASSERT(is_attached());

    m_alloc.verify();

    if (!m_top.is_attached()) {
        REALM_ASSERT(m_alloc.is_free_space_clean());
        return;
    }

    // Verify tables
    {
        auto keys = get_table_keys();
        for (auto key : keys) {
            ConstTableRef table = get_table(key);
            REALM_ASSERT_3(table->get_key().value, ==, key.value);
            table->verify();
        }
    }

    // Verify history if present
    if (Replication* repl = m_alloc.get_replication()) {
        if (auto hist = repl->get_history_read()) {
            _impl::History::version_type version = 0;
            int history_type = 0;
            int history_schema_version = 0;
            get_version_and_history_info(m_top, version, history_type, history_schema_version);
            REALM_ASSERT(history_type != Replication::hist_None || history_schema_version == 0);
            ref_type hist_ref = get_history_ref(m_top);
            hist->update_from_ref(hist_ref, version);
            hist->verify();
        }
    }

    size_t logical_file_size = to_size_t(m_top.get_as_ref_or_tagged(2).get_as_int());
    size_t ref_begin = sizeof(SlabAlloc::Header);
    ref_type real_immutable_ref_end = logical_file_size;
    ref_type real_mutable_ref_end = m_alloc.get_total_size();
    ref_type real_baseline = m_alloc.get_baseline();
    // Fake that any empty area between the file and slab is part of the file (immutable):
    ref_type immutable_ref_end = m_alloc.align_size_to_section_boundary(real_immutable_ref_end);
    ref_type mutable_ref_end = m_alloc.align_size_to_section_boundary(real_mutable_ref_end);
    ref_type baseline = m_alloc.align_size_to_section_boundary(real_baseline);

    // Check the consistency of the allocation of used memory
    MemUsageVerifier mem_usage_1(ref_begin, immutable_ref_end, mutable_ref_end, baseline);
    m_top.report_memory_usage(mem_usage_1);
    mem_usage_1.canonicalize();

    // Check concistency of the allocation of the immutable memory that was
    // marked as free before the file was opened.
    MemUsageVerifier mem_usage_2(ref_begin, immutable_ref_end, mutable_ref_end, baseline);
    {
        REALM_ASSERT_EX(m_top.size() == 3 || m_top.size() == 5 || m_top.size() == 7 ||
                        m_top.size() == 10, m_top.size());
        Allocator& alloc = m_top.get_alloc();
        ArrayInteger pos(alloc), len(alloc), ver(alloc);
        size_t pos_ndx = 3, len_ndx = 4, ver_ndx = 5;
        pos.set_parent(const_cast<Array*>(&m_top), pos_ndx);
        len.set_parent(const_cast<Array*>(&m_top), len_ndx);
        ver.set_parent(const_cast<Array*>(&m_top), ver_ndx);
        if (m_top.size() > pos_ndx) {
            if (ref_type ref = m_top.get_as_ref(pos_ndx))
                pos.init_from_ref(ref);
        }
        if (m_top.size() > len_ndx) {
            if (ref_type ref = m_top.get_as_ref(len_ndx))
                len.init_from_ref(ref);
        }
        if (m_top.size() > ver_ndx) {
            if (ref_type ref = m_top.get_as_ref(ver_ndx))
                ver.init_from_ref(ref);
        }
        REALM_ASSERT(pos.is_attached() == len.is_attached());
        REALM_ASSERT(pos.is_attached() || !ver.is_attached()); // pos.is_attached() <== ver.is_attached()
        if (pos.is_attached()) {
            size_t n = pos.size();
            REALM_ASSERT_3(n, ==, len.size());
            if (ver.is_attached())
                REALM_ASSERT_3(n, ==, ver.size());
            for (size_t i = 0; i != n; ++i) {
                ref_type ref = to_ref(pos.get(i));
                size_t size_of_i = to_size_t(len.get(i));
                mem_usage_2.add_immutable(ref, size_of_i);
            }
            mem_usage_2.canonicalize();
            mem_usage_1.add(mem_usage_2);
            mem_usage_1.canonicalize();
            mem_usage_2.clear();
        }
    }

    // Check the concistency of the allocation of the immutable memory that has
    // been marked as free after the file was opened
    for (const auto& free_block : m_alloc.m_free_read_only) {
        mem_usage_2.add_immutable(free_block.ref, free_block.size);
    }
    mem_usage_2.canonicalize();
    mem_usage_1.add(mem_usage_2);
    mem_usage_1.canonicalize();
    mem_usage_2.clear();

    // Check the concistency of the allocation of the mutable memory that has
    // been marked as free
    for (const auto& free_block : m_alloc.m_free_space) {
        mem_usage_2.add_mutable(free_block.ref, free_block.size);
    }
    mem_usage_2.canonicalize();
    mem_usage_1.add(mem_usage_2);
    mem_usage_1.canonicalize();
    mem_usage_2.clear();

    // There may be a hole between the end of file and the beginning of the slab area.
    // We need to take that into account here.
    REALM_ASSERT_3(real_immutable_ref_end, <=, real_baseline);
    auto slab_start = immutable_ref_end;
    if (real_immutable_ref_end < slab_start) {
        ref_type ref = real_immutable_ref_end;
        size_t corrected_size = slab_start - real_immutable_ref_end;
        mem_usage_1.add_immutable(ref, corrected_size);
        mem_usage_1.canonicalize();
    }

    // At this point we have accounted for all memory managed by the slab
    // allocator
    mem_usage_1.check_total_coverage();
#endif
}

#ifdef REALM_DEBUG

MemStats Group::stats()
{
    MemStats mem_stats;
    m_top.stats(mem_stats);

    return mem_stats;
}


void Group::print() const
{
    m_alloc.print();
}


void Group::print_free() const
{
    Allocator& alloc = m_top.get_alloc();
    ArrayInteger pos(alloc), len(alloc), ver(alloc);
    size_t pos_ndx = 3, len_ndx = 4, ver_ndx = 5;
    pos.set_parent(const_cast<Array*>(&m_top), pos_ndx);
    len.set_parent(const_cast<Array*>(&m_top), len_ndx);
    ver.set_parent(const_cast<Array*>(&m_top), ver_ndx);
    if (m_top.size() > pos_ndx) {
        if (ref_type ref = m_top.get_as_ref(pos_ndx))
            pos.init_from_ref(ref);
    }
    if (m_top.size() > len_ndx) {
        if (ref_type ref = m_top.get_as_ref(len_ndx))
            len.init_from_ref(ref);
    }
    if (m_top.size() > ver_ndx) {
        if (ref_type ref = m_top.get_as_ref(ver_ndx))
            ver.init_from_ref(ref);
    }

    if (!pos.is_attached()) {
        std::cout << "none\n";
        return;
    }
    bool has_versions = ver.is_attached();

    size_t n = pos.size();
    for (size_t i = 0; i != n; ++i) {
        size_t offset = to_size_t(pos[i]);
        size_t size_of_i = to_size_t(len[i]);
        std::cout << i << ": " << offset << " " << size_of_i;

        if (has_versions) {
            size_t version = to_size_t(ver[i]);
            std::cout << " " << version;
        }
        std::cout << "\n";
    }
    std::cout << "\n";
}


void Group::to_dot(std::ostream& out) const
{
    out << "digraph G {" << std::endl;

    out << "subgraph cluster_group {" << std::endl;
    out << " label = \"Group\";" << std::endl;

    m_top.to_dot(out, "group_top");
    m_table_names.to_dot(out, "table_names");
    m_tables.to_dot(out, "tables");

    // Tables
    auto keys = get_table_keys();
    for (auto key : keys) {
        ConstTableRef table = get_table(key);
        StringData name = get_table_name(key);
        table->to_dot(out, name);
    }

    out << "}" << std::endl;
    out << "}" << std::endl;
}


void Group::to_dot() const
{
    to_dot(std::cerr);
}


void Group::to_dot(const char* file_path) const
{
    std::ofstream out(file_path);
    to_dot(out);
}

#endif

std::pair<ref_type, size_t> Group::get_to_dot_parent(size_t ndx_in_parent) const
{
    return std::make_pair(m_tables.get_ref(), ndx_in_parent);
}

// LCOV_EXCL_STOP ignore debug functions
