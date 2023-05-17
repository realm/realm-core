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

#include <realm/impl/destroy_guard.hpp>
#include <realm/spec.hpp>
#include <realm/replication.hpp>
#include <realm/util/to_string.hpp>
#include <realm/group.hpp>
#include <iostream>

using namespace realm;

// Uninitialized Spec (call init() to init)
Spec::Spec(Allocator& alloc) noexcept
    : m_top(alloc)
    , m_types(alloc)
    , m_names(alloc)
    , m_attr(alloc)
    , m_enumkeys(alloc)
    , m_keys(alloc)
{
    m_types.set_parent(&m_top, 0);
    m_names.set_parent(&m_top, 1);
    m_attr.set_parent(&m_top, 2);
    m_enumkeys.set_parent(&m_top, 4);
    m_keys.set_parent(&m_top, 5);
}


Spec::~Spec() noexcept {}

void Spec::detach() noexcept
{
    m_top.detach();
}

bool Spec::init(ref_type ref) noexcept
{
    MemRef mem(ref, get_alloc());
    init(mem);
    return true;
}

void Spec::init(MemRef mem) noexcept
{
    m_top.init_from_mem(mem);
    size_t top_size = m_top.size();
    REALM_ASSERT(top_size > 2 && top_size <= 6);

    m_types.init_from_ref(m_top.get_as_ref(0));
    m_names.init_from_ref(m_top.get_as_ref(1));
    m_attr.init_from_ref(m_top.get_as_ref(2));

    while (m_top.size() < 6) {
        m_top.add(0);
    }

    // Enumkeys array is only there when there are StringEnum columns
    if (auto ref = m_top.get_as_ref(4)) {
        m_enumkeys.init_from_ref(ref);
    }
    else {
        m_enumkeys.detach();
    }

    if (m_top.get_as_ref(5) == 0) {
        // This is an upgrade - create column key array
        MemRef mem_ref = Array::create_empty_array(Array::type_Normal, false, m_top.get_alloc()); // Throws
        m_keys.init_from_mem(mem_ref);
        m_keys.update_parent();
        size_t num_cols = m_types.size();
        for (size_t i = 0; i < num_cols; i++) {
            m_keys.add(i);
        }
    }
    else {
        m_keys.init_from_parent();
    }


    update_internals();
}

void Spec::update_internals() noexcept
{
    m_num_public_columns = 0;
    size_t n = m_types.size();
    for (size_t i = 0; i < n; ++i) {
        if (ColumnType(int(m_types.get(i))) == col_type_BackLink) {
            // Now we have no more public columns
            return;
        }
        m_num_public_columns++;
    }
}

void Spec::update_from_parent() noexcept
{
    m_top.update_from_parent();
    m_types.update_from_parent();
    m_names.update_from_parent();
    m_attr.update_from_parent();

    if (m_top.get_as_ref(4) != 0) {
        m_enumkeys.update_from_parent();
    }
    else {
        m_enumkeys.detach();
    }

    m_keys.update_from_parent();

    update_internals();
}


MemRef Spec::create_empty_spec(Allocator& alloc)
{
    // The 'spec_set' contains the specification (types and names) of
    // all columns and sub-tables
    Array spec_set(alloc);
    _impl::DeepArrayDestroyGuard dg(&spec_set);
    spec_set.create(Array::type_HasRefs); // Throws

    _impl::DeepArrayRefDestroyGuard dg_2(alloc);
    {
        // One type for each column
        bool context_flag = false;
        MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v(from_ref(mem.get_ref()));
        spec_set.add(v); // Throws
        dg_2.release();
    }
    {
        size_t size = 0;
        // One name for each column
        MemRef mem = ArrayStringShort::create_array(size, alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v = from_ref(mem.get_ref());
        spec_set.add(v); // Throws
        dg_2.release();
    }
    {
        // One attrib set for each column
        bool context_flag = false;
        MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v = from_ref(mem.get_ref());
        spec_set.add(v); // Throws
        dg_2.release();
    }
    spec_set.add(0); // Subspecs array
    spec_set.add(0); // Enumkeys array
    {
        // One key for each column
        bool context_flag = false;
        MemRef mem = Array::create_empty_array(Array::type_Normal, context_flag, alloc); // Throws
        dg_2.reset(mem.get_ref());
        int_fast64_t v = from_ref(mem.get_ref());
        spec_set.add(v); // Throws
        dg_2.release();
    }

    dg.release();
    return spec_set.get_mem();
}

ColKey Spec::update_colkey(ColKey existing_key, size_t spec_ndx, TableKey table_key)
{
    auto attr = get_column_attr(spec_ndx);
    // index and uniqueness are not passed on to the key, so clear them
    attr.reset(col_attr_Indexed);
    attr.reset(col_attr_Unique);
    auto type = get_column_type(spec_ndx);
    if (existing_key.get_type() != type || existing_key.get_attrs() != attr) {
        unsigned upper = unsigned(table_key.value);

        return ColKey(ColKey::Idx{existing_key.get_index().val}, type, attr, upper);
    }
    // Existing key is valid
    return existing_key;
}

bool Spec::convert_column_attributes()
{
    bool changes = false;
    size_t enumkey_ndx = 0;

    for (size_t column_ndx = 0; column_ndx < m_types.size(); column_ndx++) {
        if (column_ndx < m_names.size()) {
            StringData name = m_names.get(column_ndx);
            if (name.size() == 0) {
                auto new_name = std::string("col_") + util::to_string(column_ndx);
                m_names.set(column_ndx, new_name);
                changes = true;
            }
            else if (m_names.find_first(name) != column_ndx) {
                auto new_name = std::string(name.data()) + '_' + util::to_string(column_ndx);
                m_names.set(column_ndx, new_name);
                changes = true;
            }
        }
        ColumnType type = ColumnType(int(m_types.get(column_ndx)));
        ColumnAttrMask attr = ColumnAttrMask(m_attr.get(column_ndx));
        switch (type) {
            case col_type_Link:
                if (!attr.test(col_attr_Nullable)) {
                    attr.set(col_attr_Nullable);
                    m_attr.set(column_ndx, attr.m_value);
                    changes = true;
                }
                break;
            case col_type_LinkList:
                if (!attr.test(col_attr_List)) {
                    attr.set(col_attr_List);
                    m_attr.set(column_ndx, attr.m_value);
                    changes = true;
                }
                break;
            default:
                if (type == col_type_OldTable) {
                    Array subspecs(m_top.get_alloc());
                    subspecs.set_parent(&m_top, 3);
                    subspecs.init_from_parent();

                    Spec sub_spec(get_alloc());
                    size_t subspec_ndx = get_subspec_ndx(column_ndx);
                    ref_type ref = to_ref(subspecs.get(subspec_ndx)); // Throws
                    sub_spec.init(ref);
                    REALM_ASSERT(sub_spec.get_column_count() == 1);
                    m_types.set(column_ndx, int(sub_spec.get_column_type(0)));
                    m_attr.set(column_ndx, m_attr.get(column_ndx) | sub_spec.m_attr.get(0) | col_attr_List);
                    sub_spec.destroy();

                    subspecs.erase(subspec_ndx);
                    changes = true;
                }
                else if (type == col_type_OldStringEnum) {
                    m_types.set(column_ndx, int(col_type_String));
                    // We need to padd zeroes into the m_enumkeys so that the index in
                    // m_enumkeys matches the column index.
                    for (size_t i = enumkey_ndx; i < column_ndx; i++) {
                        m_enumkeys.insert(i, 0);
                    }
                    enumkey_ndx = column_ndx + 1;
                    changes = true;
                }
                else {
                    REALM_ASSERT_RELEASE(type.is_valid());
                }
                break;
        }
    }
    if (m_enumkeys.is_attached()) {
        while (m_enumkeys.size() < m_num_public_columns) {
            m_enumkeys.add(0);
        }
    }
    return changes;
}

bool Spec::convert_column_keys(TableKey table_key)
{
    // This step will ensure that the column keys has right attribute and type info
    bool changes = false;
    auto sz = m_types.size();
    for (size_t ndx = 0; ndx < sz; ndx++) {
        ColKey existing_key = ColKey{m_keys.get(ndx)};
        ColKey col_key = update_colkey(existing_key, ndx, table_key);
        if (col_key != existing_key) {
            m_keys.set(ndx, col_key.value);
            changes = true;
        }
    }
    return changes;
}

void Spec::fix_column_keys(TableKey table_key)
{
    if (get_column_name(m_num_public_columns - 1) == "!ROW_INDEX") {
        unsigned idx = unsigned(m_types.size()) - 1;
        size_t ndx = m_num_public_columns - 1;
        // Fixing "!ROW_INDEX" column
        {
            ColKey col_key(ColKey::Idx{idx}, col_type_Int, ColumnAttrMask(), table_key.value);
            m_keys.set(ndx, col_key.value);
        }
        // Fix backlink columns
        idx = unsigned(m_num_public_columns) - 1;
        for (ndx = m_num_public_columns; ndx < m_types.size(); ndx++, idx++) {
            ColKey col_key(ColKey::Idx{idx}, col_type_BackLink, ColumnAttrMask(), table_key.value);
            m_keys.set(ndx, col_key.value);
        }
    }
}

void Spec::insert_column(size_t column_ndx, ColKey col_key, ColumnType type, StringData name, int attr)
{
    REALM_ASSERT(column_ndx <= m_types.size());

    if (REALM_UNLIKELY(name.size() > Table::max_column_name_length)) {
        throw LogicError(LogicError::column_name_too_long);
    }
    if (get_column_index(name) != npos) {
        throw LogicError(LogicError::column_name_in_use);
    }

    if (type != col_type_BackLink) {
        m_names.insert(column_ndx, name); // Throws
        m_num_public_columns++;
    }

    m_types.insert(column_ndx, int(type)); // Throws
    // FIXME: So far, attributes are never reported to the replication system
    m_attr.insert(column_ndx, attr); // Throws
    m_keys.insert(column_ndx, col_key.value);

    if (m_enumkeys.is_attached() && type != col_type_BackLink) {
        m_enumkeys.insert(column_ndx, 0);
    }
    if (column_ndx > m_interners.size()) {
        m_interners.resize(column_ndx + 1);
    }
    REALM_ASSERT(column_ndx <= m_interners.size());
    m_interners.insert(m_interners.begin() + column_ndx, nullptr);
    update_internals();
}

void Spec::erase_column(size_t column_ndx)
{
    REALM_ASSERT(column_ndx < m_types.size());

    if (ColumnType(int(m_types.get(column_ndx))) != col_type_BackLink) {
        if (is_string_enum_type(column_ndx)) {
            // Enum columns do also have a separate key list
            ref_type keys_ref = m_enumkeys.get_as_ref(column_ndx);
            Array::destroy_deep(keys_ref, m_top.get_alloc());
            m_enumkeys.set(column_ndx, 0);
        }

        // Remove this column from the enum keys lookup and clean it up if it's now empty
        if (m_enumkeys.is_attached()) {
            m_enumkeys.erase(column_ndx); // Throws
            bool all_empty = true;
            for (size_t i = 0; i < m_enumkeys.size(); i++) {
                if (m_enumkeys.get(i) != 0) {
                    all_empty = false;
                    break;
                }
            }
            if (all_empty) {
                m_enumkeys.destroy_deep();
                m_top.set(4, 0);
            }
        }
        m_num_public_columns--;
        m_names.erase(column_ndx); // Throws
    }

    // Delete the entries common for all columns
    m_types.erase(column_ndx); // Throws
    m_attr.erase(column_ndx);  // Throws
    m_keys.erase(column_ndx);
    m_interners.erase(m_interners.begin() + column_ndx);
    update_internals();
}


// For link and link list columns the old subspec array contain an entry which
// is the group-level table index of the target table, and for backlink
// columns the first entry is the group-level table index of the origin
// table, and the second entry is the index of the origin column in the
// origin table.
size_t Spec::get_subspec_ndx(size_t column_ndx) const noexcept
{
    REALM_ASSERT(column_ndx == get_column_count() || get_column_type(column_ndx) == col_type_Link ||
                 get_column_type(column_ndx) == col_type_LinkList ||
                 get_column_type(column_ndx) == col_type_BackLink ||
                 // col_type_OldTable is used when migrating from file format 9 to 10.
                 get_column_type(column_ndx) == col_type_OldTable);

    size_t subspec_ndx = 0;
    for (size_t i = 0; i != column_ndx; ++i) {
        ColumnType type = ColumnType(int(m_types.get(i)));
        if (type == col_type_Link || type == col_type_LinkList) {
            subspec_ndx += 1; // index of dest column
        }
        else if (type == col_type_BackLink) {
            subspec_ndx += 2; // index of table and index of linked column
        }
    }
    return subspec_ndx;
}


void Spec::upgrade_string_to_enum(size_t column_ndx, ref_type keys_ref)
{
    REALM_ASSERT(get_column_type(column_ndx) == col_type_String);

    // Create the enumkeys list if needed
    if (!m_enumkeys.is_attached()) {
        m_enumkeys.create(Array::type_HasRefs, false, m_num_public_columns);
        m_top.set(4, m_enumkeys.get_ref());
        m_enumkeys.set_parent(&m_top, 4);
    }

    // Insert the new key list
    m_enumkeys.set(column_ndx, keys_ref);
}

bool Spec::is_string_enum_type(size_t column_ndx) const noexcept
{
    return true;
    // return m_enumkeys.is_attached() ? (m_enumkeys.get(column_ndx) != 0) : false;
}

#ifdef OLD_STRINGS
ref_type Spec::get_enumkeys_ref(size_t column_ndx, ArrayParent*& keys_parent) noexcept
{
    // We also need to return parent info
    keys_parent = &m_enumkeys;

    return m_enumkeys.get_as_ref(column_ndx);
}
#endif

TableKey Spec::get_opposite_link_table_key(size_t column_ndx) const noexcept
{
    REALM_ASSERT(column_ndx < get_column_count());
    REALM_ASSERT(get_column_type(column_ndx) == col_type_Link || get_column_type(column_ndx) == col_type_LinkList ||
                 get_column_type(column_ndx) == col_type_BackLink);

    // Key of opposite table is stored as tagged int in the
    // subspecs array
    size_t subspec_ndx = get_subspec_ndx(column_ndx);
    Array subspecs(m_top.get_alloc());
    subspecs.init_from_ref(m_top.get_as_ref(3));
    int64_t tagged_value = subspecs.get(subspec_ndx);
    REALM_ASSERT(tagged_value != 0); // can't retrieve it if never set

    uint64_t table_ref = uint64_t(tagged_value) >> 1;

    REALM_ASSERT(!util::int_cast_has_overflow<uint32_t>(table_ref));
    return TableKey(uint32_t(table_ref));
}

size_t Spec::get_origin_column_ndx(size_t backlink_col_ndx) const noexcept
{
    REALM_ASSERT(backlink_col_ndx < get_column_count());
    REALM_ASSERT(get_column_type(backlink_col_ndx) == col_type_BackLink);

    // Origin column is stored as second tagged int in the subspecs array
    size_t subspec_ndx = get_subspec_ndx(backlink_col_ndx);
    Array subspecs(m_top.get_alloc());
    subspecs.init_from_ref(m_top.get_as_ref(3));
    int64_t tagged_value = subspecs.get(subspec_ndx + 1);
    REALM_ASSERT(tagged_value != 0); // can't retrieve it if never set
    return size_t(tagged_value) >> 1;
}

ColKey Spec::find_backlink_column(TableKey origin_table_key, size_t spec_ndx) const noexcept
{
    size_t backlinks_column_start = m_num_public_columns;
    size_t backlinks_start = get_subspec_ndx(backlinks_column_start);
    Array subspecs(m_top.get_alloc());
    subspecs.init_from_ref(m_top.get_as_ref(3));
    size_t count = subspecs.size();

    int64_t tagged_table_ndx = (origin_table_key.value << 1) + 1;
    int64_t tagged_column_ndx = (spec_ndx << 1) + 1;

    size_t col_ndx = realm::npos;
    for (size_t i = backlinks_start; i < count; i += 2) {
        if (subspecs.get(i) == tagged_table_ndx && subspecs.get(i + 1) == tagged_column_ndx) {
            size_t pos = (i - backlinks_start) / 2;
            col_ndx = backlinks_column_start + pos;
            break;
        }
    }
    REALM_ASSERT(col_ndx != realm::npos);
    return ColKey{m_keys.get(col_ndx)};
}

namespace {

template <class T>
bool compare(const T& a, const T& b)
{
    if (a.size() != b.size())
        return false;

    for (size_t i = 0; i < a.size(); ++i) {
        if (b.get(i) != a.get(i))
            return false;
    }

    return true;
}

} // namespace

bool Spec::operator==(const Spec& spec) const noexcept
{
    if (!compare(m_attr, spec.m_attr))
        return false;
    if (!m_names.compare_string(spec.m_names))
        return false;

    // check each column's type
    const size_t column_count = get_column_count();
    for (size_t col_ndx = 0; col_ndx < column_count; ++col_ndx) {
        ColumnType col_type = ColumnType(int(m_types.get(col_ndx)));
        switch (col_type) {
            case col_type_Link:
            case col_type_TypedLink:
            case col_type_LinkList: {
                // In addition to name and attributes, the link target table must also be compared
                REALM_ASSERT(false); // We can no longer compare specs - in fact we don't want to
                /*
                auto lhs_table_key = get_opposite_link_table_key(col_ndx);
                auto rhs_table_key = spec.get_opposite_link_table_key(col_ndx);
                if (lhs_table_key != rhs_table_key)
                    return false;
                */
                break;
            }
            case col_type_Int:
            case col_type_Bool:
            case col_type_Binary:
            case col_type_String:
            case col_type_Mixed:
            case col_type_Timestamp:
            case col_type_Float:
            case col_type_Double:
            case col_type_Decimal:
            case col_type_BackLink:
            case col_type_ObjectId:
            case col_type_UUID:
                // All other column types are compared as before
                if (m_types.get(col_ndx) != spec.m_types.get(col_ndx))
                    return false;
                break;
        }
    }

    return true;
}


ColKey Spec::get_key(size_t column_ndx) const
{
    auto key = ColKey(m_keys.get(column_ndx));
    REALM_ASSERT(key.get_type().is_valid());
    return key;
}

void Spec::verify() const
{
#ifdef REALM_DEBUG
    REALM_ASSERT(m_names.size() == get_public_column_count());
    REALM_ASSERT(m_types.size() == get_column_count());
    REALM_ASSERT(m_attr.size() == get_column_count());

    REALM_ASSERT(m_types.get_ref() == m_top.get_as_ref(0));
    REALM_ASSERT(m_names.get_ref() == m_top.get_as_ref(1));
    REALM_ASSERT(m_attr.get_ref() == m_top.get_as_ref(2));
#endif
}

// Code for pair compression integrated with interning:
// ----------
// Compression is done by first expanding 8-bit chars to 16-bit symbols.
// these symbols are then pairwise compressed in multiple rounds, each
// round potentially halving the storage requirement.
template <>
struct std::hash<std::vector<uint16_t>> {
    std::size_t operator()(const std::vector<uint16_t>& c) const noexcept
    {
        auto seed = c.size();
        for (auto& x : c) {
            seed = (seed + 3) * (x + 7);
        }
        return seed;
    }
};

struct encoding_entry {
    uint16_t exp_a;
    uint16_t exp_b;
    uint16_t symbol = 0; // unused symbol 0.
};

int hash(uint16_t a, uint16_t b)
{
    // range of return value must match size of encoding table
    uint32_t tmp = a + 3;
    tmp *= b + 7;
    return (tmp ^ (tmp >> 16)) & 0xFFFF;
}

#define INTERN_ONLY 0
#define COMPRESS_BEFORE_INTERNING 1

class Spec::string_interner {
public:
#if INTERN_ONLY
    std::vector<std::string> strings;
    std::unordered_map<std::string, int> string_map;
#else
    std::vector<std::vector<uint16_t>> symbols;
    std::vector<encoding_entry> encoding_table;
    std::vector<encoding_entry> decoding_table;
#if COMPRESS_BEFORE_INTERNING
    std::unordered_map<std::vector<uint16_t>, int> symbol_map;
#else
    std::vector<std::string> strings;
    std::unordered_map<std::string, int> string_map;
#endif
#endif
#if !INTERN_ONLY
    bool separators[256];
    uint16_t symbol_buffer[8192];
    string_interner()
    {
        encoding_table.resize(65536);
        for (int j = 0; j < 0x20; ++j)
            separators[j] = true;
        for (int j = 0x20; j < 0x100; ++j)
            separators[j] = false;
        separators['/'] = true;
        separators[':'] = true;
        separators['?'] = true;
        separators['<'] = true;
        separators['>'] = true;
        separators['['] = true;
        separators[']'] = true;
        separators['{'] = true;
        separators['}'] = true;
    }
    int compress_symbols(uint16_t symbols[], int size, int max_runs, int breakout_limit = 1)
    {
        bool table_full = decoding_table.size() >= 65536 - 256;
        // std::cout << "Input: ";
        // for (int i = 0; i < size; ++i)
        //     std::cout << symbols[i] << " ";
        // std::cout << std::endl;
        for (int runs = 0; runs < max_runs; ++runs) {
            uint16_t* to = symbols;
            int p;
            uint16_t* from = symbols;
            for (p = 0; p < size - 1;) {
                uint16_t a = from[0];
                uint16_t b = from[1];
                auto index = hash(a, b);
                auto& e = encoding_table[index];
                if (e.symbol && e.exp_a == a && e.exp_b == b) {
                    // existing matching entry -> compress
                    *to++ = e.symbol;
                    p += 2;
                }
                else if (e.symbol || table_full) {
                    // existing conflicting entry or at capacity -> don't compress
                    *to++ = a;
                    p++;
                    // trying to stay aligned yields slightly worse results, so disable for now:
                    // *to++ = from[1];
                    // p++;
                }
                else {
                    // no matching entry yet, create new one -> compress
                    e.symbol = decoding_table.size() + 256;
                    e.exp_a = a;
                    e.exp_b = b;
                    // std::cout << "             new symbol " << e.symbol << " -> " << e.exp_a << " " << e.exp_b
                    //           << std::endl;
                    decoding_table.push_back({a, b, e.symbol});
                    table_full = decoding_table.size() >= 65536 - 256;
                    *to++ = e.symbol;
                    p += 2;
                }
                from = symbols + p;
            }
            // potentially move last unpaired symbol over
            if (p < size) {
                *to++ = *from++; // need to increment for early out check below
            }
            size = to - symbols;
            // std::cout << " -- Round " << runs << " -> ";
            // for (int i = 0; i < size; ++i)
            //     std::cout << symbols[i] << " ";
            // std::cout << std::endl;
            if (size <= breakout_limit)
                break; // early out, gonna use at least one chunk anyway
            if (from == to)
                break; // early out, no symbols were compressed on last run
        }
        return size;
    }
    void decompress_and_verify(uint16_t symbols[], int size, const char* first, const char* past)
    {
        uint16_t decompressed[8192];
        uint16_t* from = symbols;
        uint16_t* to = decompressed;

        auto decompress = [&](uint16_t symbol, auto& recurse) -> void {
            if (symbol < 256)
                *to++ = symbol;
            else {
                auto& e = decoding_table[symbol - 256];
                recurse(e.exp_a, recurse);
                recurse(e.exp_b, recurse);
            }
        };

        while (size--) {
            decompress(*from++, decompress);
        }
        // walk back on any trailing zeroes:
        while (to[-1] == 0 && to > decompressed) {
            --to;
        }
        size = to - decompressed;
        // std::cout << "reverse -> ";
        // for (int i = 0; i < size; ++i) {
        //     std::cout << decompressed[i] << " ";
        // }
        // std::cout << std::endl;
        REALM_ASSERT(size == past - first);
        uint16_t* checked = decompressed;
        while (first < past) {
            REALM_ASSERT((0xFF & *first++) == *checked++);
        }
    }
    int compress(uint16_t symbols[], const char* first, const char* past)
    {
        // expand into 16 bit symbols:
        int size = past - first;
        REALM_ASSERT(size < 8180);
        uint16_t* to = symbols;
        int out_size = 0;
        for (const char* p = first; p < past;) {
            // form group from !seps followed by seps
            uint16_t* group_start = to;
            while (p < past && !separators[0xFFUL & *p])
                *to++ = *p++ & 0xFF;
            while (p < past && separators[0xFFUL & *p])
                *to++ = *p++ & 0xFF;
            int group_size = to - group_start;
            // compress the group
            group_size = compress_symbols(group_start, group_size, 2);
            to = group_start + group_size;
            out_size += group_size;
        }
        // compress all groups together
        // size = out_size;
        size = compress_symbols(symbols, out_size, 2, 4);
        compressed_symbols += size;
        return size;
    }
    int symbol_table_size()
    {
        return decoding_table.size();
    }
#endif
    int handle(const StringData value)
    {
        const char* _first = value.data();
        int size = value.size();
        const char* _past = value.data() + size;
        total_chars += size;
#if INTERN_ONLY
        std::string s(_first, _past);
        auto it = string_map.find(s);
        if (it == string_map.end()) {
            auto id = strings.size();
            strings.push_back(s);
            string_map[s] = id;
            unique_symbol_size += size;
            return id;
        }
        else {
            return it->second;
        }
#else
#if COMPRESS_BEFORE_INTERNING
        size = compress(symbol_buffer, _first, _past);
        decompress_and_verify(symbol_buffer, size, _first, _past);

        std::vector<uint16_t> symbol(size);
        for (int j = 0; j < size; ++j)
            symbol[j] = symbol_buffer[j];
        auto it = symbol_map.find(symbol);
        if (it == symbol_map.end()) {
            auto id = symbols.size();
            symbols.push_back(symbol);
            symbol_map[symbol] = id;
            unique_symbol_size += 2 * size;
            return id;
        }
        else {
            return it->second;
        }
#else // INTERN BEFORE COMPRESSING:
        std::string s(_first, _past);
        auto it = string_map.find(s);
        if (it == string_map.end()) {
            auto id = strings.size();
            strings.push_back(s);
            string_map[s] = id;
            auto size = compress(symbol_buffer, _first, _past);
            std::vector<uint16_t> symbol(size);
            for (int j = 0; j < size; ++j)
                symbol[j] = symbol_buffer[j];
            symbols.push_back(symbol);
            unique_symbol_size += 2 * size;
            return id;
        }
        else {
            return it->second;
        }
#endif
#endif
    }
    size_t search(StringData value)
    {
        return npos;
    }
    int next_id = 0;
    int next_prefix_id = -1;
    int64_t total_chars = 0;
    int64_t compressed_symbols = 0;
    int64_t unique_symbol_size = 0;
    size_t num_unique_values()
    {
#if INTERN_ONLY
        return strings.size();
#else
        return symbols.size();
#endif
    }
    void dump_interning_stats()
    {
        std::cout << " Interning " << total_chars << " bytes of input strings into " << num_unique_values()
                  << " unique strings in " << unique_symbol_size << " bytes" << std::endl;
    }
};

void Spec::dump_interning_stats()
{
    for (auto col = 0UL; col < m_interners.size(); ++col) {
        auto& e = m_interners[col];
        if (e) {
            std::cout << "Column " << col;
            e->dump_interning_stats();
        }
    }
}


size_t Spec::add_insert_enum_string(size_t column_ndx, StringData value)
{
    // REALM_ASSERT(value.data());
    if (value.data() == nullptr) {
        return 0;
    }
    if (column_ndx >= m_interners.size()) {
        m_interners.resize(column_ndx + 1);
    }
    if (!m_interners[column_ndx]) {
        m_interners[column_ndx] = std::make_unique<string_interner>();
    }
    return m_interners[column_ndx]->handle(value);
}

size_t Spec::search_enum_string(size_t column_ndx, StringData value)
{
    // REALM_ASSERT(value.data());
    if (value.data() == nullptr) {
        return 0;
    }
    REALM_ASSERT(column_ndx < m_interners.size());
    auto& interner = m_interners[column_ndx];
    return interner->search(value);
}

size_t Spec::get_num_unique_values(size_t column_ndx) const
{
    REALM_ASSERT(column_ndx < m_interners.size());
    const auto& interner = m_interners[column_ndx];
    return interner->num_unique_values();
}

StringData Spec::get_enum_string(size_t column_ndx, size_t id)
{
    // Returning as StringData is not really possible for a compressed string
    return {nullptr, 0};
    // if (!id) {
    //     return {nullptr, 0};
    // }
    // REALM_ASSERT(column_ndx < m_interners.size());
    // auto& interner = m_interners[column_ndx];
    //  return interner->strings[id + 1];
}

bool Spec::is_null_enum_string(size_t column_ndx, size_t id)
{
    return id == 0;
}
