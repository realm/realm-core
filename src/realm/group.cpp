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
#include <realm/transaction.hpp>
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

constexpr char Group::g_class_name_prefix[];
constexpr size_t Group::g_class_name_prefix_len;

Group::Group()
    : m_local_alloc(new SlabAlloc)
    , m_alloc(*m_local_alloc) // Throws
    , m_top(m_alloc)
    , m_tables(m_alloc)
    , m_table_names(m_alloc)
{
    init_array_parents();
    m_alloc.attach_empty(); // Throws
    m_file_format_version = get_target_file_format_version_for_session(0, Replication::hist_None);
    ref_type top_ref = 0; // Instantiate a new empty group
    bool create_group_when_missing = true;
    bool writable = create_group_when_missing;
    attach(top_ref, writable, create_group_when_missing); // Throws
}


Group::Group(const std::string& file_path, const char* encryption_key)
    : m_local_alloc(new SlabAlloc) // Throws
    , m_alloc(*m_local_alloc)
    , m_top(m_alloc)
    , m_tables(m_alloc)
    , m_table_names(m_alloc)
    , m_total_rows(0)
{
    init_array_parents();

    SlabAlloc::Config cfg;
    cfg.read_only = true;
    cfg.no_create = true;
    cfg.encryption_key = encryption_key;
    ref_type top_ref = m_alloc.attach_file(file_path, cfg); // Throws
    // Non-Transaction Groups always allow writing and simply don't allow
    // committing when opened in read-only mode
    m_alloc.set_read_only(false);

    open(top_ref, file_path);
}


Group::Group(BinaryData buffer, bool take_ownership)
    : m_local_alloc(new SlabAlloc) // Throws
    , m_alloc(*m_local_alloc)
    , m_top(m_alloc)
    , m_tables(m_alloc)
    , m_table_names(m_alloc)
    , m_total_rows(0)
{
    REALM_ASSERT(buffer.data());

    init_array_parents();
    ref_type top_ref = m_alloc.attach_buffer(buffer.data(), buffer.size()); // Throws

    open(top_ref, {});

    if (take_ownership)
        m_alloc.own_buffer();
}

Group::Group(SlabAlloc* alloc) noexcept
    : m_alloc(*alloc)
    , // Throws
    m_top(m_alloc)
    , m_tables(m_alloc)
    , m_table_names(m_alloc)
    , m_total_rows(0)
{
    init_array_parents();
}

namespace {

class TableRecycler : public std::vector<Table*> {
public:
    ~TableRecycler()
    {
        REALM_UNREACHABLE();
        // if ever enabled, remember to release Tables:
        // for (auto t : *this) {
        //    delete t;
        //}
    }
};

// We use the classic approach to construct a FIFO from two LIFO's,
// insertion is done into recycler_1, removal is done from recycler_2,
// and when recycler_2 is empty, recycler_1 is reversed into recycler_2.
// this i O(1) for each entry.
auto& g_table_recycler_1 = *new TableRecycler;
auto& g_table_recycler_2 = *new TableRecycler;
// number of tables held back before being recycled. We hold back recycling
// the latest to increase the probability of detecting race conditions
// without crashing.
const static int g_table_recycling_delay = 100;
auto& g_table_recycler_mutex = *new std::mutex;

} // namespace

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
    size_t max_index_in_group = g.m_table_names.size();
    while (m_index_in_group < max_index_in_group) {
        RefOrTagged rot = g.m_tables.get_as_ref_or_tagged(m_index_in_group);
        if (rot.is_ref()) {
            Table* t;
            if (m_index_in_group < g.m_table_accessors.size() &&
                (t = load_atomic(g.m_table_accessors[m_index_in_group], std::memory_order_acquire))) {
                m_table_key = t->get_key();
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

std::map<TableRef, ColKey> Group::get_primary_key_columns_from_pk_table(TableRef pk_table)
{
    std::map<TableRef, ColKey> ret;
    REALM_ASSERT(pk_table);
    ColKey col_table = pk_table->get_column_key("pk_table");
    ColKey col_prop = pk_table->get_column_key("pk_property");
    for (auto pk_obj : *pk_table) {
        auto object_type = pk_obj.get<String>(col_table);
        auto name = std::string(g_class_name_prefix) + std::string(object_type);
        auto table = get_table(name);
        auto pk_col_name = pk_obj.get<String>(col_prop);
        auto pk_col = table->get_column_key(pk_col_name);
        ret.emplace(table, pk_col);
    }

    return ret;
}

TableKey Group::ndx2key(size_t ndx) const
{
    REALM_ASSERT(is_attached());
    Table* accessor = load_atomic(m_table_accessors[ndx], std::memory_order_acquire);
    if (accessor)
        return accessor->get_key(); // fast path

    // slow path:
    RefOrTagged rot = m_tables.get_as_ref_or_tagged(ndx);
    if (rot.is_tagged())
        throw NoSuchTable();
    ref_type ref = rot.get_as_ref();
    REALM_ASSERT(ref);
    return Table::get_key_direct(m_tables.get_alloc(), ref);
}

size_t Group::key2ndx_checked(TableKey key) const
{
    size_t idx = key2ndx(key);
    // early out
    // note: don't lock when accessing m_table_accessors, because if we miss a concurrently introduced table
    // accessor, we'll just fall through to the slow path. Table accessors can be introduced concurrently,
    // but never removed. The following is only safe because 'm_table_accessors' will not be relocated
    // concurrently. (We aim to be safe in face of concurrent access to a frozen transaction, where tables
    // cannot be added or removed. All other races are undefined behaviour)
    if (idx < m_table_accessors.size()) {
        Table* tbl = load_atomic(m_table_accessors[idx], std::memory_order_acquire);
        if (tbl && tbl->get_key() == key)
            return idx;
    }
    // The notion of a const group as it is now, is not really
    // useful. It is linked to a distinction between a read
    // and a write transaction. This distinction is no longer
    // a compile time aspect (it's not const anymore)
    Allocator* alloc = const_cast<SlabAlloc*>(&m_alloc);
    if (m_tables.is_attached() && idx < m_tables.size()) {
        RefOrTagged rot = m_tables.get_as_ref_or_tagged(idx);
        if (rot.is_ref() && rot.get_as_ref() && (Table::get_key_direct(*alloc, rot.get_as_ref()) == key)) {

            return idx;
        }
    }
    throw NoSuchTable();
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

std::optional<int> Group::fake_target_file_format;

void _impl::GroupFriend::fake_target_file_format(const std::optional<int> format) noexcept
{
    Group::fake_target_file_format = format;
}

int Group::get_target_file_format_version_for_session(int current_file_format_version,
                                                      int requested_history_type) noexcept
{
    if (Group::fake_target_file_format) {
        return *Group::fake_target_file_format;
    }
    // Note: This function is responsible for choosing the target file format
    // for a sessions. If it selects a file format that is different from
    // `current_file_format_version`, it will trigger a file format upgrade
    // process.

    // Note: `current_file_format_version` may be zero at this time, which means
    // that the file format it is not yet decided (only possible for empty
    // Realms where top-ref is zero).

    // Please see Group::get_file_format_version() for information about the
    // individual file format versions.

    if (requested_history_type == Replication::hist_None) {
        if (current_file_format_version == 22) {
            // We are able to open these file formats in RO mode
            return current_file_format_version;
        }
    }

    return g_current_file_format_version;
}

void Group::get_version_and_history_info(const Array& top, _impl::History::version_type& version, int& history_type,
                                         int& history_schema_version) noexcept
{
    using version_type = _impl::History::version_type;
    version_type version_2 = 0;
    int history_type_2 = 0;
    int history_schema_version_2 = 0;
    if (top.is_attached()) {
        if (top.size() > s_version_ndx) {
            version_2 = version_type(top.get_as_ref_or_tagged(s_version_ndx).get_as_int());
        }
        if (top.size() > s_hist_type_ndx) {
            history_type_2 = int(top.get_as_ref_or_tagged(s_hist_type_ndx).get_as_int());
        }
        if (top.size() > s_hist_version_ndx) {
            history_schema_version_2 = int(top.get_as_ref_or_tagged(s_hist_version_ndx).get_as_int());
        }
    }
    // Version 0 is not a legal initial version, so it has to be set to 1
    // instead.
    if (version_2 == 0)
        version_2 = 1;
    version = version_2;
    history_type = history_type_2;
    history_schema_version = history_schema_version_2;
}

int Group::get_history_schema_version() noexcept
{
    bool history_schema_version = (m_top.is_attached() && m_top.size() > s_hist_version_ndx);
    if (history_schema_version) {
        return int(m_top.get_as_ref_or_tagged(s_hist_version_ndx).get_as_int());
    }
    return 0;
}

uint64_t Group::get_sync_file_id() const noexcept
{
    if (m_top.is_attached() && m_top.size() > s_sync_file_id_ndx) {
        return uint64_t(m_top.get_as_ref_or_tagged(s_sync_file_id_ndx).get_as_int());
    }
    auto repl = get_replication();
    if (repl && repl->get_history_type() == Replication::hist_SyncServer) {
        return 1;
    }
    return 0;
}

int Group::read_only_version_check(SlabAlloc& alloc, ref_type top_ref, const std::string& path)
{
    // Select file format if it is still undecided.
    auto file_format_version = alloc.get_committed_file_format_version();

    bool file_format_ok = false;
    // It is not possible to open prior file format versions without an upgrade.
    // Since a Realm file cannot be upgraded when opened in this mode
    // (we may be unable to write to the file), no earlier versions can be opened.
    // Please see Group::get_file_format_version() for information about the
    // individual file format versions.
    switch (file_format_version) {
        case 0:
            file_format_ok = (top_ref == 0);
            break;
        case 11:
        case 20:
        case 21:
        case g_current_file_format_version:
            file_format_ok = true;
            break;
    }
    if (REALM_UNLIKELY(!file_format_ok))
        throw FileFormatUpgradeRequired("Realm file needs upgrade before opening in RO mode", path);
    return file_format_version;
}

void Group::open(ref_type top_ref, const std::string& file_path)
{
    SlabAlloc::DetachGuard dg(m_alloc);
    m_file_format_version = read_only_version_check(m_alloc, top_ref, file_path);

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
    bool writable = create_group_when_missing;
    attach(top_ref, writable, create_group_when_missing); // Throws
    dg.release();                                         // Do not detach after all
}

Group::~Group() noexcept
{
    // If this group accessor is detached at this point in time, it is either
    // because it is DB::m_group (m_is_shared), or it is a free-stading
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
    m_alloc.update_reader_view(new_file_size); // Throws
    update_allocator_wrappers(writable);

    // force update of all ref->ptr translations if the mapping has changed
    auto mapping_version = m_alloc.get_mapping_version();
    if (mapping_version != m_last_seen_mapping_version) {
        m_last_seen_mapping_version = mapping_version;
    }
    update_refs(new_top_ref);
}

void Group::validate_top_array(const Array& arr, const SlabAlloc& alloc)
{
    size_t top_size = arr.size();
    ref_type top_ref = arr.get_ref();

    switch (top_size) {
        // These are the valid sizes
        case 3:
        case 5:
        case 7:
        case 9:
        case 10:
        case 11: {
            ref_type table_names_ref = arr.get_as_ref_or_tagged(s_table_name_ndx).get_as_ref();
            ref_type tables_ref = arr.get_as_ref_or_tagged(s_table_refs_ndx).get_as_ref();
            auto logical_file_size = arr.get_as_ref_or_tagged(s_file_size_ndx).get_as_int();

            // Logical file size must never exceed actual file size.
            auto file_size = alloc.get_baseline();
            if (logical_file_size > file_size) {
                std::string err = "Invalid logical file size: " + util::to_string(logical_file_size) +
                                  ", actual file size: " + util::to_string(file_size);
                throw InvalidDatabase(err, "");
            }
            // First two entries must be valid refs pointing inside the file
            auto invalid_ref = [logical_file_size](ref_type ref) {
                return ref == 0 || (ref & 7) || ref > logical_file_size;
            };
            if (invalid_ref(table_names_ref) || invalid_ref(tables_ref)) {
                std::string err = "Invalid top array (top_ref, [0], [1]): " + util::to_string(top_ref) + ", " +
                                  util::to_string(table_names_ref) + ", " + util::to_string(tables_ref);
                throw InvalidDatabase(err, "");
            }
            break;
        }
        default: {
            std::string err = "Invalid top array size (ref: " + util::to_string(top_ref) +
                              ", size: " + util::to_string(top_size) + ")";
            throw InvalidDatabase(err, "");
            break;
        }
    }
}

void Group::attach(ref_type top_ref, bool writable, bool create_group_when_missing)
{
    REALM_ASSERT(!m_top.is_attached());
    if (create_group_when_missing)
        REALM_ASSERT(writable);

    // If this function throws, it must leave the group accesor in a the
    // unattached state.

    m_tables.detach();
    m_table_names.detach();
    m_is_writable = writable;

    if (top_ref != 0) {
        m_top.init_from_ref(top_ref);
        validate_top_array(m_top, m_alloc);
        m_table_names.init_from_parent();
        m_tables.init_from_parent();
    }
    else if (create_group_when_missing) {
        create_empty_group(); // Throws
    }
    m_attached = true;
    set_size();

    size_t sz = m_tables.is_attached() ? m_tables.size() : 0;
    while (m_table_accessors.size() > sz) {
        if (Table* t = m_table_accessors.back()) {
            t->detach(Table::cookie_void);
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
        // This is quite invasive and completely defeats the lazy loading mechanism
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
    attach(new_top_ref, writable, create_group_when_missing); // Throws
}


void Group::detach_table_accessors() noexcept
{
    for (auto& table_accessor : m_table_accessors) {
        if (Table* t = table_accessor) {
            t->detach(Table::cookie_transaction_ended);
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


Table* Group::do_get_table(size_t table_ndx)
{
    REALM_ASSERT(m_table_accessors.size() == m_tables.size());
    // Get table accessor from cache if it exists, else create
    Table* table = load_atomic(m_table_accessors[table_ndx], std::memory_order_acquire);
    if (!table) {
        // double-checked locking idiom
        std::lock_guard<std::mutex> lock(m_accessor_mutex);
        table = m_table_accessors[table_ndx];
        if (!table)
            table = create_table_accessor(table_ndx); // Throws
    }
    return table;
}


Table* Group::do_get_table(StringData name)
{
    if (!m_table_names.is_attached())
        return 0;
    size_t table_ndx = m_table_names.find_first(name);
    if (table_ndx == not_found)
        return 0;

    Table* table = do_get_table(table_ndx); // Throws
    return table;
}

TableRef Group::add_table_with_primary_key(StringData name, DataType pk_type, StringData pk_name, bool nullable,
                                           Table::Type table_type)
{
    if (!is_attached())
        throw LogicError(LogicError::detached_accessor);
    check_table_name_uniqueness(name);

    auto table = do_add_table(name, table_type, false);

    // Add pk column - without replication
    ColumnAttrMask attr;
    if (nullable)
        attr.set(col_attr_Nullable);
    ColKey pk_col = table->generate_col_key(ColumnType(pk_type), attr);
    table->do_insert_root_column(pk_col, ColumnType(pk_type), pk_name);
    table->do_set_primary_key_column(pk_col);

    if (Replication* repl = *get_repl())
        repl->add_class_with_primary_key(table->get_key(), name, pk_type, pk_name, nullable, table_type);

    return TableRef(table, table->m_alloc.get_instance_version());
}

Table* Group::do_add_table(StringData name, Table::Type table_type, bool do_repl)
{
    if (!m_is_writable)
        throw LogicError(LogicError::wrong_transact_state);

    // get new key and index
    // find first empty spot:
    uint32_t j;
    RefOrTagged rot = RefOrTagged::make_tagged(0);
    for (j = 0; j < m_tables.size(); ++j) {
        rot = m_tables.get_as_ref_or_tagged(j);
        if (!rot.is_ref())
            break;
    }
    bool gen_null_tag = (j == m_tables.size()); // new tags start at zero
    uint32_t tag = gen_null_tag ? 0 : uint32_t(rot.get_as_int());
    TableKey key = TableKey((tag << 16) | j);

    if (REALM_UNLIKELY(name.size() > max_table_name_length))
        throw LogicError(LogicError::table_name_too_long);

    using namespace _impl;
    size_t table_ndx = key2ndx(key);
    ref_type ref = Table::create_empty_table(m_alloc, key); // Throws
    REALM_ASSERT_3(m_tables.size(), ==, m_table_names.size());

    rot = RefOrTagged::make_ref(ref);
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

    Replication* repl = *get_repl();
    if (do_repl && repl)
        repl->add_class(key, name, table_type);

    ++m_num_tables;

    Table* table = create_table_accessor(j);
    table->do_set_table_type(table_type);

    return table;
}


Table* Group::create_table_accessor(size_t table_ndx)
{
    REALM_ASSERT(m_tables.size() == m_table_accessors.size());
    REALM_ASSERT(table_ndx < m_table_accessors.size());

    RefOrTagged rot = m_tables.get_as_ref_or_tagged(table_ndx);
    ref_type ref = rot.get_as_ref();
    if (ref == 0) {
        throw NoSuchTable();
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
        table->revive(get_repl(), m_alloc, m_is_writable);
        table->init(ref, this, table_ndx, m_is_writable, is_frozen());
    }
    else {
        std::unique_ptr<Table> new_table(new Table(get_repl(), m_alloc));  // Throws
        new_table->init(ref, this, table_ndx, m_is_writable, is_frozen()); // Throws
        table = new_table.release();
    }
    table->refresh_index_accessors();
    // must be atomic to allow concurrent probing of the m_table_accessors vector.
    store_atomic(m_table_accessors[table_ndx], table, std::memory_order_release);
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
    if (table->is_cross_table_link_target())
        throw CrossTableLinkTarget();

    // There is no easy way for Group::TransactAdvancer to handle removal of
    // tables that contain foreign target table link columns, because that
    // involves removal of the corresponding backlink columns. For that reason,
    // we start by removing all columns, which will generate individual
    // replication instructions for each column removal with sufficient
    // information for Group::TransactAdvancer to handle them.
    size_t n = table->get_column_count();
    Replication* repl = *get_repl();
    if (repl) {
        // This will prevent sync instructions for column removals to be generated
        repl->prepare_erase_class(key);
    }
    for (size_t i = n; i > 0; --i) {
        ColKey col_key = table->spec_ndx2colkey(i - 1);
        table->remove_column(col_key);
    }

    size_t prior_num_tables = m_tables.size();
    if (repl)
        repl->erase_class(key, prior_num_tables); // Throws

    int64_t ref_64 = m_tables.get(table_ndx);
    REALM_ASSERT(!int_cast_has_overflow<ref_type>(ref_64));
    ref_type ref = ref_type(ref_64);

    // Replace entry in m_tables with next tag to use:
    RefOrTagged rot = RefOrTagged::make_tagged((1 + (key.value >> 16)) & 0x7FFF);
    // Remove table
    m_tables.set(table_ndx, rot);     // Throws
    m_table_names.set(table_ndx, {}); // Throws
    m_table_accessors[table_ndx] = nullptr;
    --m_num_tables;

    table->detach(Table::cookie_removed);
    // Destroy underlying node structure
    Array::destroy_deep(ref, m_alloc);
    recycle_table_accessor(table.unchecked_ptr());
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
    if (Replication* repl = *get_repl())
        repl->rename_class(key, new_name); // Throws
}

Obj Group::get_object(ObjLink link)
{
    auto target_table = get_table(link.get_table_key());
    ObjKey key = link.get_obj_key();
    TableClusterTree* ct = key.is_unresolved() ? target_table->m_tombstones.get() : &target_table->m_clusters;
    return ct->get(key);
}

void Group::validate(ObjLink link) const
{
    if (auto tk = link.get_table_key()) {
        auto target_key = link.get_obj_key();
        auto target_table = get_table(tk);
        const ClusterTree* ct =
            target_key.is_unresolved() ? target_table->m_tombstones.get() : &target_table->m_clusters;
        if (!ct->is_valid(target_key)) {
            throw LogicError(LogicError::target_row_index_out_of_range);
        }
        if (target_table->is_embedded()) {
            throw LogicError(LogicError::wrong_kind_of_table);
        }
        if (target_table->is_asymmetric()) {
            throw LogicError(LogicError::wrong_kind_of_table);
        }
    }
}

ref_type Group::DefaultTableWriter::write_names(_impl::OutputStream& out)
{
    bool deep = true;                                                 // Deep
    bool only_if_modified = false;                                    // Always
    return m_group->m_table_names.write(out, deep, only_if_modified); // Throws
}
ref_type Group::DefaultTableWriter::write_tables(_impl::OutputStream& out)
{
    bool deep = true;                                            // Deep
    bool only_if_modified = false;                               // Always
    return m_group->m_tables.write(out, deep, only_if_modified); // Throws
}

auto Group::DefaultTableWriter::write_history(_impl::OutputStream& out) -> HistoryInfo
{
    bool deep = true;              // Deep
    bool only_if_modified = false; // Always
    ref_type history_ref = _impl::GroupFriend::get_history_ref(*m_group);
    HistoryInfo info;
    if (history_ref) {
        _impl::History::version_type version;
        int history_type, history_schema_version;
        _impl::GroupFriend::get_version_and_history_info(_impl::GroupFriend::get_alloc(*m_group),
                                                         m_group->m_top.get_ref(), version, history_type,
                                                         history_schema_version);
        REALM_ASSERT(history_type != Replication::hist_None);
        if (!m_should_write_history ||
            (history_type != Replication::hist_SyncClient && history_type != Replication::hist_SyncServer)) {
            return info; // Only sync history should be preserved when writing to a new file
        }
        info.type = history_type;
        info.version = history_schema_version;
        Array history{const_cast<Allocator&>(_impl::GroupFriend::get_alloc(*m_group))};
        history.init_from_ref(history_ref);
        info.ref = history.write(out, deep, only_if_modified); // Throws
    }
    info.sync_file_id = m_group->get_sync_file_id();
    return info;
}

void Group::write(std::ostream& out, bool pad) const
{
    DefaultTableWriter table_writer;
    write(out, pad, 0, table_writer);
}

void Group::write(std::ostream& out, bool pad_for_encryption, uint_fast64_t version_number, TableWriter& writer) const
{
    REALM_ASSERT(is_attached());
    writer.set_group(this);
    bool no_top_array = !m_top.is_attached();
    write(out, m_file_format_version, writer, no_top_array, pad_for_encryption, version_number); // Throws
}

void Group::write(File& file, const char* encryption_key, uint_fast64_t version_number, TableWriter& writer) const
{
    REALM_ASSERT(file.get_size() == 0);

    file.set_encryption_key(encryption_key);

    // The aim is that the buffer size should be at least 1/256 of needed size but less than 64 Mb
    constexpr size_t upper_bound = 64 * 1024 * 1024;
    size_t min_space = std::min(get_used_space() >> 8, upper_bound);
    size_t buffer_size = 4096;
    while (buffer_size < min_space) {
        buffer_size <<= 1;
    }
    File::Streambuf streambuf(&file, buffer_size);

    std::ostream out(&streambuf);
    out.exceptions(std::ios_base::failbit | std::ios_base::badbit);
    write(out, encryption_key != 0, version_number, writer);
    int sync_status = streambuf.pubsync();
    REALM_ASSERT(sync_status == 0);
}

void Group::write(const std::string& path, const char* encryption_key, uint64_t version_number,
                  bool write_history) const
{
    File file;
    int flags = 0;
    file.open(path, File::access_ReadWrite, File::create_Must, flags);
    DefaultTableWriter table_writer(write_history);
    write(file, encryption_key, version_number, table_writer);
}


BinaryData Group::write_to_mem() const
{
    REALM_ASSERT(is_attached());

    // Get max possible size of buffer
    size_t max_size = m_alloc.get_total_size();

    auto buffer = std::unique_ptr<char[]>(new (std::nothrow) char[max_size]);
    if (!buffer)
        throw util::bad_alloc();
    MemoryOutputStream out; // Throws
    out.set_buffer(buffer.get(), buffer.get() + max_size);
    write(out); // Throws
    size_t buffer_size = out.size();
    return BinaryData(buffer.release(), buffer_size);
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
        // DB to compact the database by writing only the live data
        // into a separate file.
        ref_type names_ref = table_writer.write_names(out_2);   // Throws
        ref_type tables_ref = table_writer.write_tables(out_2); // Throws
        SlabAlloc new_alloc;
        new_alloc.attach_empty(); // Throws
        Array top(new_alloc);
        top.create(Array::type_HasRefs); // Throws
        _impl::ShallowArrayDestroyGuard dg_top(&top);
        int_fast64_t value_1 = from_ref(names_ref);
        int_fast64_t value_2 = from_ref(tables_ref);
        top.add(value_1); // Throws
        top.add(value_2); // Throws
        top.add(0);       // Throws

        int top_size = 3;
        if (version_number) {
            TableWriter::HistoryInfo history_info = table_writer.write_history(out_2); // Throws

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

            if (history_info.type != Replication::hist_None) {
                top.add(RefOrTagged::make_tagged(history_info.type));
                top.add(RefOrTagged::make_ref(history_info.ref));
                top.add(RefOrTagged::make_tagged(history_info.version));
                top.add(RefOrTagged::make_tagged(history_info.sync_file_id));
                top_size = s_group_max_size;
            }
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


void Group::update_refs(ref_type top_ref) noexcept
{
    // After Group::commit() we will always have free space tracking
    // info.
    REALM_ASSERT_3(m_top.size(), >=, 5);

    m_top.init_from_ref(top_ref);

    // Now we can update it's child arrays
    m_table_names.update_from_parent();
    m_tables.update_from_parent();

    // Update all attached table accessors.
    for (auto& table_accessor : m_table_accessors) {
        if (table_accessor) {
            table_accessor->update_from_parent();
        }
    }
}

bool Group::operator==(const Group& g) const
{
    for (auto tk : get_table_keys()) {
        const StringData& table_name = get_table_name(tk);

        ConstTableRef table_1 = get_table(tk);
        ConstTableRef table_2 = g.get_table(table_name);
        if (!table_2)
            return false;
        if (table_1->get_primary_key_column().get_type() != table_2->get_primary_key_column().get_type()) {
            return false;
        }
        if (table_1->is_embedded() != table_2->is_embedded())
            return false;
        if (table_1->is_embedded())
            continue;

        if (*table_1 != *table_2)
            return false;
    }
    return true;
}
void Group::schema_to_json(std::ostream& out, std::map<std::string, std::string>* opt_renames) const
{
    if (!is_attached())
        throw LogicError(LogicError::detached_accessor);

    std::map<std::string, std::string> renames;
    if (opt_renames) {
        renames = *opt_renames;
    }

    out << "[" << std::endl;

    auto keys = get_table_keys();
    int sz = int(keys.size());
    for (int i = 0; i < sz; ++i) {
        auto key = keys[i];
        ConstTableRef table = get_table(key);

        table->schema_to_json(out, renames);
        if (i < sz - 1)
            out << ",";
        out << std::endl;
    }

    out << "]" << std::endl;
}

void Group::to_json(std::ostream& out, size_t link_depth, std::map<std::string, std::string>* opt_renames,
                    JSONOutputMode output_mode) const
{
    if (!is_attached())
        throw LogicError(LogicError::detached_accessor);

    std::map<std::string, std::string> renames;
    if (opt_renames) {
        renames = *opt_renames;
    }

    out << "{" << std::endl;

    auto keys = get_table_keys();
    bool first = true;
    for (size_t i = 0; i < keys.size(); ++i) {
        auto key = keys[i];
        StringData name = get_table_name(key);
        if (renames[name] != "")
            name = renames[name];

        ConstTableRef table = get_table(key);

        if (!table->is_embedded()) {
            if (!first)
                out << ",";
            out << "\"" << name << "\"";
            out << ":";
            table->to_json(out, link_depth, renames, output_mode);
            out << std::endl;
            first = false;
        }
    }

    out << "}" << std::endl;
}

namespace {

size_t size_of_tree_from_ref(ref_type ref, Allocator& alloc)
{
    if (ref) {
        Array a(alloc);
        a.init_from_ref(ref);
        MemStats stats;
        a.stats(stats);
        return stats.allocated;
    }
    else
        return 0;
}
} // namespace

size_t Group::compute_aggregated_byte_size(SizeAggregateControl ctrl) const noexcept
{
    SlabAlloc& alloc = *const_cast<SlabAlloc*>(&m_alloc);
    if (!m_top.is_attached())
        return 0;
    size_t used = 0;
    if (ctrl & SizeAggregateControl::size_of_state) {
        MemStats stats;
        m_table_names.stats(stats);
        m_tables.stats(stats);
        used = stats.allocated + m_top.get_byte_size();
        used += sizeof(SlabAlloc::Header);
    }
    if (ctrl & SizeAggregateControl::size_of_freelists) {
        if (m_top.size() >= 6) {
            auto ref = m_top.get_as_ref_or_tagged(3).get_as_ref();
            used += size_of_tree_from_ref(ref, alloc);
            ref = m_top.get_as_ref_or_tagged(4).get_as_ref();
            used += size_of_tree_from_ref(ref, alloc);
            ref = m_top.get_as_ref_or_tagged(5).get_as_ref();
            used += size_of_tree_from_ref(ref, alloc);
        }
    }
    if (ctrl & SizeAggregateControl::size_of_history) {
        if (m_top.size() >= 9) {
            auto ref = m_top.get_as_ref_or_tagged(8).get_as_ref();
            used += size_of_tree_from_ref(ref, alloc);
        }
    }
    return used;
}

size_t Group::get_used_space() const noexcept
{
    if (!m_top.is_attached())
        return 0;

    size_t used_space = (size_t(m_top.get(2)) >> 1);

    if (m_top.size() > 4) {
        Array free_lengths(const_cast<SlabAlloc&>(m_alloc));
        free_lengths.init_from_ref(ref_type(m_top.get(4)));
        used_space -= size_t(free_lengths.get_sum());
    }

    return used_space;
}


class Group::TransactAdvancer {
public:
    TransactAdvancer(Group&, bool& schema_changed)
        : m_schema_changed(schema_changed)
    {
    }

    bool insert_group_level_table(TableKey) noexcept
    {
        m_schema_changed = true;
        return true;
    }

    bool erase_class(TableKey) noexcept
    {
        m_schema_changed = true;
        return true;
    }

    bool rename_class(TableKey) noexcept
    {
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

    bool modify_object(ColKey, ObjKey) noexcept
    {
        return true; // No-op
    }

    bool list_set(size_t)
    {
        return true;
    }

    bool list_insert(size_t)
    {
        return true;
    }

    bool enumerate_string_column(ColKey)
    {
        return true; // No-op
    }

    bool insert_column(ColKey)
    {
        m_schema_changed = true;
        return true;
    }

    bool erase_column(ColKey)
    {
        m_schema_changed = true;
        return true;
    }

    bool rename_column(ColKey) noexcept
    {
        m_schema_changed = true;
        return true; // No-op
    }

    bool set_link_type(ColKey) noexcept
    {
        return true; // No-op
    }

    bool select_collection(ColKey, ObjKey) noexcept
    {
        return true; // No-op
    }

    bool list_move(size_t, size_t) noexcept
    {
        return true; // No-op
    }

    bool list_erase(size_t) noexcept
    {
        return true; // No-op
    }

    bool list_clear(size_t) noexcept
    {
        return true; // No-op
    }

    bool dictionary_insert(size_t, Mixed)
    {
        return true; // No-op
    }
    bool dictionary_set(size_t, Mixed)
    {
        return true; // No-op
    }
    bool dictionary_erase(size_t, Mixed)
    {
        return true; // No-op
    }

    bool set_insert(size_t)
    {
        return true; // No-op
    }
    bool set_erase(size_t)
    {
        return true; // No-op
    }
    bool set_clear(size_t)
    {
        return true; // No-op
    }
    bool typed_link_change(ColKey, TableKey)
    {
        return true; // No-op
    }

private:
    bool& m_schema_changed;
};


void Group::update_allocator_wrappers(bool writable)
{
    m_is_writable = writable;
    for (size_t i = 0; i < m_table_accessors.size(); ++i) {
        auto table_accessor = m_table_accessors[i];
        if (table_accessor) {
            table_accessor->update_allocator_wrapper(writable);
        }
    }
}

void Group::flush_accessors_for_commit()
{
    for (auto& acc : m_table_accessors)
        if (acc)
            acc->flush_for_commit();
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
            }
            else {
                table_accessor->detach(Table::cookie_removed);
                recycle_table_accessor(table_accessor);
                m_table_accessors[i] = nullptr;
            }
        }
    }
}


void Group::advance_transact(ref_type new_top_ref, util::NoCopyInputStream& in, bool writable)
{
    REALM_ASSERT(is_attached());
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
    // The purpose of this function is to refresh all attached accessors after
    // the underlying node structure has undergone arbitrary change, such as
    // when a read transaction has been advanced to a later snapshot of the
    // database.
    //
    // Initially, when this function is invoked, we cannot assume any
    // correspondence between the accessor state and the underlying node
    // structure. We can assume that the hierarchy is in a state of minimal
    // consistency, and that it can be brought to a state of structural
    // correspondence using information in the transaction logs. When structural
    // correspondence is achieved, we can reliably refresh the accessor hierarchy
    // (Table::refresh_accessor_tree()) to bring it back to a fully consistent
    // state. See AccessorConsistencyLevels.
    //
    // Much of the information in the transaction logs is not used in this
    // process, because the changes have already been applied to the underlying
    // node structure. All we need to do here is to bring the accessors back
    // into a state where they correctly reflect the underlying structure (or
    // detach them if the underlying object has been removed.)
    //
    // This is no longer needed in Core, but we need to compute "schema_changed",
    // for the benefit of ObjectStore.
    bool schema_changed = false;
    if (has_schema_change_notification_handler()) {
        _impl::TransactLogParser parser; // Throws
        TransactAdvancer advancer(*this, schema_changed);
        parser.parse(in, advancer); // Throws
    }

    m_top.detach();                                           // Soft detach
    bool create_group_when_missing = false;                   // See Group::attach_shared().
    attach(new_top_ref, writable, create_group_when_missing); // Throws
    refresh_dirty_accessors();                                // Throws

    if (schema_changed)
        send_schema_change_notification();
}

void Group::prepare_top_for_history(int history_type, int history_schema_version, uint64_t file_ident)
{
    REALM_ASSERT(m_file_format_version >= 7);
    if (m_top.size() < s_group_max_size) {
        REALM_ASSERT(m_top.size() <= s_hist_type_ndx);
        while (m_top.size() < s_hist_type_ndx) {
            m_top.add(0); // Throws
        }
        ref_type history_ref = 0;                                    // No history yet
        m_top.add(RefOrTagged::make_tagged(history_type));           // Throws
        m_top.add(RefOrTagged::make_ref(history_ref));               // Throws
        m_top.add(RefOrTagged::make_tagged(history_schema_version)); // Throws
        m_top.add(RefOrTagged::make_tagged(file_ident));             // Throws
    }
    else {
        int stored_history_type = int(m_top.get_as_ref_or_tagged(s_hist_type_ndx).get_as_int());
        int stored_history_schema_version = int(m_top.get_as_ref_or_tagged(s_hist_version_ndx).get_as_int());
        if (stored_history_type != Replication::hist_None) {
            REALM_ASSERT(stored_history_type == history_type);
            REALM_ASSERT(stored_history_schema_version == history_schema_version);
        }
        m_top.set(s_hist_type_ndx, RefOrTagged::make_tagged(history_type));              // Throws
        m_top.set(s_hist_version_ndx, RefOrTagged::make_tagged(history_schema_version)); // Throws
    }
}

void Group::clear_history()
{
    bool has_history = (m_top.is_attached() && m_top.size() > s_hist_type_ndx);
    if (has_history) {
        auto hist_ref = m_top.get_as_ref(s_hist_ref_ndx);
        Array::destroy_deep(hist_ref, m_top.get_alloc());
        m_top.set(s_hist_type_ndx, RefOrTagged::make_tagged(Replication::hist_None)); // Throws
        m_top.set(s_hist_version_ndx, RefOrTagged::make_tagged(0));                   // Throws
        m_top.set(s_hist_ref_ndx, 0);                                                 // Throws
    }
}

#ifdef REALM_DEBUG // LCOV_EXCL_START ignore debug functions

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
                if (i_2->ref == prev_ref_end) { // in-file
                    i_1->size += i_2->size;     // Merge
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

#endif

void Group::verify() const
{
#ifdef REALM_DEBUG
    REALM_ASSERT(is_attached());

    m_alloc.verify();

    if (!m_top.is_attached()) {
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
    if (Replication* repl = *get_repl()) {
        if (auto hist = repl->_create_history_read()) {
            hist->set_group(const_cast<Group*>(this), false);
            _impl::History::version_type version = 0;
            int history_type = 0;
            int history_schema_version = 0;
            get_version_and_history_info(m_top, version, history_type, history_schema_version);
            REALM_ASSERT(history_type != Replication::hist_None || history_schema_version == 0);
            ref_type hist_ref = get_history_ref(m_top);
            hist->update_from_ref_and_version(hist_ref, version);
            hist->verify();
        }
    }

    if (auto tr = dynamic_cast<const Transaction*>(this)) {
        // This is a transaction
        if (tr->get_transact_stage() == DB::TransactStage::transact_Reading) {
            // Verifying the memory cannot be done from a read transaction
            // There might be a write transaction running that has freed some
            // memory that is seen as being in use in this transaction
            return;
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
        REALM_ASSERT_EX(m_top.size() == 3 || m_top.size() == 5 || m_top.size() == 7 || m_top.size() == 10 ||
                            m_top.size() == 11,
                        m_top.size());
        Allocator& alloc = m_top.get_alloc();
        Array pos(alloc), len(alloc), ver(alloc);
        pos.set_parent(const_cast<Array*>(&m_top), s_free_pos_ndx);
        len.set_parent(const_cast<Array*>(&m_top), s_free_size_ndx);
        ver.set_parent(const_cast<Array*>(&m_top), s_free_version_ndx);
        if (m_top.size() > s_free_pos_ndx) {
            if (ref_type ref = m_top.get_as_ref(s_free_pos_ndx))
                pos.init_from_ref(ref);
        }
        if (m_top.size() > s_free_size_ndx) {
            if (ref_type ref = m_top.get_as_ref(s_free_size_ndx))
                len.init_from_ref(ref);
        }
        if (m_top.size() > s_free_version_ndx) {
            if (ref_type ref = m_top.get_as_ref(s_free_version_ndx))
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
        mem_usage_2.add_immutable(free_block.first, free_block.second);
    }
    mem_usage_2.canonicalize();
    mem_usage_1.add(mem_usage_2);
    mem_usage_1.canonicalize();
    mem_usage_2.clear();

    // Check the consistency of the allocation of the mutable memory that has
    // been marked as free
    m_alloc.for_all_free_entries([&](ref_type ref, size_t sz) {
        mem_usage_2.add_mutable(ref, sz);
    });
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

void Group::validate_primary_columns()
{
    auto table_keys = this->get_table_keys();
    for (auto tk : table_keys) {
        auto table = get_table(tk);
        table->validate_primary_column();
    }
}

#ifdef REALM_DEBUG

MemStats Group::get_stats()
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
    Array pos(alloc), len(alloc), ver(alloc);
    pos.set_parent(const_cast<Array*>(&m_top), s_free_pos_ndx);
    len.set_parent(const_cast<Array*>(&m_top), s_free_size_ndx);
    ver.set_parent(const_cast<Array*>(&m_top), s_free_version_ndx);
    if (m_top.size() > s_free_pos_ndx) {
        if (ref_type ref = m_top.get_as_ref(s_free_pos_ndx))
            pos.init_from_ref(ref);
    }
    if (m_top.size() > s_free_size_ndx) {
        if (ref_type ref = m_top.get_as_ref(s_free_size_ndx))
            len.init_from_ref(ref);
    }
    if (m_top.size() > s_free_version_ndx) {
        if (ref_type ref = m_top.get_as_ref(s_free_version_ndx))
            ver.init_from_ref(ref);
    }

    if (!pos.is_attached()) {
        std::cout << "none\n";
        return;
    }
    bool has_versions = ver.is_attached();

    size_t n = pos.size();
    for (size_t i = 0; i != n; ++i) {
        size_t offset = to_size_t(pos.get(i));
        size_t size_of_i = to_size_t(len.get(i));
        std::cout << i << ": " << offset << " " << size_of_i;

        if (has_versions) {
            size_t version = to_size_t(ver.get(i));
            std::cout << " " << version;
        }
        std::cout << "\n";
    }
    std::cout << "\n";
}
#endif

// LCOV_EXCL_STOP ignore debug functions
