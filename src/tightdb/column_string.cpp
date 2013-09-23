#include <cstdlib>
#include <cstring>
#include <cstdio> // debug
#include <iostream>
#include <iomanip>

#ifdef _WIN32
#  include <win32\types.h>
#endif

#include <tightdb/query_conditions.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/index_string.hpp>

using namespace std;
using namespace tightdb;


namespace {

const size_t short_string_max_size = 15;

Array::Type get_type_from_ref(ref_type ref, Allocator& alloc)
{
    const char* header = alloc.translate(ref);
    return Array::get_type_from_header(header);
}

// Getter function for string index
StringData get_string(void* column, size_t ndx)
{
    return static_cast<AdaptiveStringColumn*>(column)->get(ndx);
}

void copy_leaf(const ArrayString& from, ArrayStringLong& to)
{
    size_t n = from.size();
    for (size_t i = 0; i < n; ++i)
        to.add(from.get(i)); // Throws
}

} // anonymous namespace



AdaptiveStringColumn::AdaptiveStringColumn(Allocator& alloc): m_index(0)
{
    m_array = new ArrayString(0, 0, alloc);
}

AdaptiveStringColumn::AdaptiveStringColumn(ref_type ref, ArrayParent* parent, size_t pndx,
                                           Allocator& alloc): m_index(0)
{
    Array::Type type = get_type_from_ref(ref, alloc);
    switch (type) {
        case Array::type_InnerColumnNode:
            m_array = new Array(ref, parent, pndx, alloc);
            break;
        case Array::type_HasRefs:
            m_array = new ArrayStringLong(ref, parent, pndx, alloc);
            break;
        case Array::type_Normal:
            m_array = new ArrayString(ref, parent, pndx, alloc);
            break;
    }
}

AdaptiveStringColumn::~AdaptiveStringColumn() TIGHTDB_NOEXCEPT
{
    delete m_array;
    delete m_index;
}

void AdaptiveStringColumn::destroy() TIGHTDB_NOEXCEPT
{
    ColumnBase::destroy();
    if (m_index)
        m_index->destroy();
}

StringIndex& AdaptiveStringColumn::create_index()
{
    TIGHTDB_ASSERT(!m_index);

    // Create new index
    m_index = new StringIndex(this, &get_string, m_array->get_alloc());

    // Populate the index
    const size_t count = size();
    for (size_t i = 0; i < count; ++i) {
        StringData value = get(i);
        bool is_last = true;
        m_index->insert(i, value, is_last);
    }

    return *m_index;
}

void AdaptiveStringColumn::set_index_ref(ref_type ref, ArrayParent* parent, size_t ndx_in_parent)
{
    TIGHTDB_ASSERT(!m_index);
    m_index = new StringIndex(ref, parent, ndx_in_parent, this, &get_string, m_array->get_alloc());
}

void AdaptiveStringColumn::clear()
{
    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (long_strings) {
            static_cast<ArrayStringLong*>(m_array)->clear();
        }
        else {
            static_cast<ArrayString*>(m_array)->clear();
        }
    }

    // Revert to string array
    m_array->destroy();
    Array* array = new ArrayString(m_array->get_parent(), m_array->get_ndx_in_parent(), m_array->get_alloc());
    delete m_array;
    m_array = array;

    if (m_index)
        m_index->clear();
}

void AdaptiveStringColumn::resize(size_t ndx)
{
    TIGHTDB_ASSERT(root_is_leaf()); // currently only available on leaf level (used by b-tree code)

    bool long_strings = m_array->has_refs();
    if (long_strings) {
        static_cast<ArrayStringLong*>(m_array)->resize(ndx);
    }
    else {
        static_cast<ArrayString*>(m_array)->resize(ndx);
    }
}


namespace {

struct SetLeafElem: Array::UpdateHandler {
    Allocator& m_alloc;
    const StringData m_value;
    SetLeafElem(Allocator& alloc, StringData value) TIGHTDB_NOEXCEPT:
        m_alloc(alloc), m_value(value) {}
    void update(MemRef mem, ArrayParent* parent, size_t ndx_in_parent,
                size_t elem_ndx_in_leaf) TIGHTDB_OVERRIDE
    {
        bool long_strings = Array::get_hasrefs_from_header(mem.m_addr);
        if (!long_strings) {
            ArrayString leaf(mem, parent, ndx_in_parent, m_alloc);
            if (m_value.size() <= short_string_max_size)
                return leaf.set(elem_ndx_in_leaf, m_value); // Throws
            // Upgrade leaf from short to long strings
            ArrayStringLong new_leaf(parent, ndx_in_parent, m_alloc); // Throws
            copy_leaf(leaf, new_leaf); // Throws
            leaf.destroy();
            return new_leaf.set(elem_ndx_in_leaf, m_value); // Throws
        }
        ArrayStringLong leaf(mem, parent, ndx_in_parent, m_alloc);
        return leaf.set(elem_ndx_in_leaf, m_value); // Throws
    }
};

} // anonymous namespace

void AdaptiveStringColumn::set(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx < size());

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        StringData old_val = get(ndx);
        m_index->set(ndx, old_val, value); // Throws
    }

    if (m_array->is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (!long_strings && short_string_max_size < value.size()) {
            // Upgrade root leaf from short to long strings
            ArrayString* leaf = static_cast<ArrayString*>(m_array);
            ArrayParent* parent = leaf->get_parent();
            std::size_t ndx_in_parent = leaf->get_ndx_in_parent();
            Allocator& alloc = leaf->get_alloc();
            UniquePtr<ArrayStringLong> new_leaf(new ArrayStringLong(parent, ndx_in_parent,
                                                                    alloc)); // Throws
            copy_leaf(*leaf, *new_leaf); // Throws
            leaf->destroy();
            delete leaf;
            m_array = new_leaf.release();
            long_strings = true;
        }
        if (long_strings) {
            ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array);
            leaf->set(ndx, value); // Throws
        }
        else {
            ArrayString* leaf = static_cast<ArrayString*>(m_array);
            leaf->set(ndx, value); // Throws
        }
        return;
    }

    SetLeafElem set_leaf_elem(m_array->get_alloc(), value);
    m_array->update_bptree_elem(ndx, set_leaf_elem); // Throws
}

void AdaptiveStringColumn::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());
    TIGHTDB_ASSERT(!m_index);

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i)
        add(StringData()); // Throws

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}


class AdaptiveStringColumn::EraseLeafElem: public ColumnBase::EraseHandlerBase {
public:
    EraseLeafElem(AdaptiveStringColumn& column) TIGHTDB_NOEXCEPT:
        EraseHandlerBase(column) {}
    bool erase_leaf_elem(MemRef leaf_mem, ArrayParent* parent,
                         size_t leaf_ndx_in_parent,
                         size_t elem_ndx_in_leaf) TIGHTDB_OVERRIDE
    {
        bool long_strings = Array::get_hasrefs_from_header(leaf_mem.m_addr);
        if (long_strings) {
            ArrayStringLong leaf(leaf_mem, parent, leaf_ndx_in_parent, get_alloc());
            TIGHTDB_ASSERT(leaf.size() >= 1);
            size_t last_ndx = leaf.size() - 1;
            if (last_ndx == 0)
                return true;
            size_t ndx = elem_ndx_in_leaf;
            if (ndx == npos)
                ndx = last_ndx;
            leaf.erase(ndx); // Throws
        }
        else {
            ArrayString leaf(leaf_mem, parent, leaf_ndx_in_parent, get_alloc());
            TIGHTDB_ASSERT(leaf.size() >= 1);
            size_t last_ndx = leaf.size() - 1;
            if (last_ndx == 0)
                return true;
            size_t ndx = elem_ndx_in_leaf;
            if (ndx == npos)
                ndx = last_ndx;
            leaf.erase(ndx); // Throws
        }
        return false;
    }
    void destroy_leaf(MemRef leaf_mem) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE
    {
        ArrayParent* parent = 0;
        size_t ndx_in_parent = 0;
        Array leaf(leaf_mem, parent, ndx_in_parent, get_alloc());
        leaf.destroy();
    }
    void replace_root_by_leaf(MemRef leaf_mem) TIGHTDB_OVERRIDE
    {
        ArrayParent* parent = 0;
        size_t ndx_in_parent = 0;
        UniquePtr<Array> leaf;
        bool long_strings = Array::get_hasrefs_from_header(leaf_mem.m_addr);
        if (long_strings) {
            leaf.reset(new ArrayStringLong(leaf_mem, parent, ndx_in_parent,
                                           get_alloc())); // Throws
        }
        else {
            leaf.reset(new ArrayString(leaf_mem, parent, ndx_in_parent,
                                       get_alloc())); // Throws
        }
        replace_root(leaf); // Throws
    }
    void replace_root_by_empty_leaf() TIGHTDB_OVERRIDE
    {
        ArrayParent* parent = 0;
        size_t ndx_in_parent = 0;
        UniquePtr<Array> leaf(new ArrayString(parent, ndx_in_parent,
                                              get_alloc())); // Throws
        replace_root(leaf); // Throws
    }
};

void AdaptiveStringColumn::erase(size_t ndx, bool is_last)
{
    TIGHTDB_ASSERT(ndx < size());
    TIGHTDB_ASSERT(is_last == (ndx == size()-1));

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        StringData old_val = get(ndx);
        // FIXME: This always evaluates to false. Alexander, what was
        // the intention? See also ColumnStringEnum::erase().
        bool is_last_2 = ndx == size();
        m_index->erase(ndx, old_val, is_last_2);
    }

    if (m_array->is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (long_strings) {
            ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array);
            leaf->erase(ndx); // Throws
        }
        else {
            ArrayString* leaf = static_cast<ArrayString*>(m_array);
            leaf->erase(ndx); // Throws
        }
        return;
    }

    size_t ndx_2 = is_last ? npos : ndx;
    EraseLeafElem erase_leaf_elem(*this);
    Array::erase_bptree_elem(m_array, ndx_2, erase_leaf_elem); // Throws
}


void AdaptiveStringColumn::move_last_over(size_t ndx)
{
    // FIXME: ExceptionSafety: The current implementation of this
    // function is not exception-safe, and it is hard to see how to
    // repair it.

    // FIXME: Consider doing two nested calls to
    // update_bptree_elem(). If the two leafs are not the same, no
    // copying is needed. If they are the same, call
    // Array::move_last_over() (does not yet
    // exist). Array::move_last_over() could be implemented in a way
    // that avoids the intermediate copy. This approach is also likely
    // to be necesseray for exception safety.

    TIGHTDB_ASSERT(ndx+1 < size());

    size_t last_ndx = size() - 1;
    StringData value = get(last_ndx);

    // Copying string data from a column to itself requires an
    // intermediate copy of the data (constr:bptree-copy-to-self).
    UniquePtr<char[]> buffer(new char[value.size()]);
    copy(value.data(), value.data()+value.size(), buffer.get());
    StringData copy_of_value(buffer.get(), value.size());

    if (m_index) {
        // remove the value to be overwritten from index
        StringData old_target_val = get(ndx);
        m_index->erase(ndx, old_target_val, true);

        // update index to point to new location
        m_index->update_ref(copy_of_value, last_ndx, ndx);
    }

    if (m_array->is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (long_strings) {
            ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array);
            leaf->set(ndx, copy_of_value); // Throws
            leaf->erase(last_ndx); // Throws
        }
        else {
            ArrayString* leaf = static_cast<ArrayString*>(m_array);
            leaf->set(ndx, copy_of_value); // Throws
            leaf->erase(last_ndx); // Throws
        }
        return;
    }

    SetLeafElem set_leaf_elem(m_array->get_alloc(), copy_of_value);
    m_array->update_bptree_elem(ndx, set_leaf_elem); // Throws

    EraseLeafElem erase_leaf_elem(*this);
    Array::erase_bptree_elem(m_array, npos, erase_leaf_elem); // Throws
}


size_t AdaptiveStringColumn::count(StringData value) const
{
    if (m_index)
        return m_index->count(value); // Throws

    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (long_strings)
            return static_cast<ArrayStringLong*>(m_array)->count(value);
        return static_cast<ArrayString*>(m_array)->count(value);
    }

    size_t num_matches = 0;

    // FIXME: It would be better to always require that 'end' is
    // specified explicitely, since Table has the size readily
    // available, and Array::get_bptree_size() is deprecated.
    size_t begin = 0, end = m_array->get_bptree_size();
    while (begin < end) {
        pair<MemRef, size_t> p = m_array->get_bptree_leaf(begin);
        MemRef leaf_mem = p.first;
        TIGHTDB_ASSERT(p.second == 0);
        bool long_strings = Array::get_hasrefs_from_header(leaf_mem.m_addr);
        if (long_strings) {
            ArrayStringLong leaf(leaf_mem, 0, 0, m_array->get_alloc());
            num_matches += leaf.count(value);
            begin += leaf.size();
        }
        else {
            ArrayString leaf(leaf_mem, 0, 0, m_array->get_alloc());
            num_matches += leaf.count(value);
            begin += leaf.size();
        }
    }

    return num_matches;
}


size_t AdaptiveStringColumn::find_first(StringData value, size_t begin, size_t end) const
{
    TIGHTDB_ASSERT(begin <= size());
    TIGHTDB_ASSERT(end == npos || (begin <= end && end <= size()));

    if (m_index && begin == 0 && end == npos)
        return m_index->find_first(value); // Throws

    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (long_strings) {
            return static_cast<ArrayStringLong*>(m_array)->
                find_first(value, begin, end); // Throws (maybe)
        }
        return static_cast<ArrayString*>(m_array)->
            find_first(value, begin, end);
    }

    // FIXME: It would be better to always require that 'end' is
    // specified explicitely, since Table has the size readily
    // available, and Array::get_bptree_size() is deprecated.
    if (end == npos)
        end = m_array->get_bptree_size();

    size_t ndx_in_tree = begin;
    while (ndx_in_tree < end) {
        pair<MemRef, size_t> p = m_array->get_bptree_leaf(ndx_in_tree);
        MemRef leaf_mem = p.first;
        size_t ndx_in_leaf = p.second, end_in_leaf;
        size_t leaf_offset = ndx_in_tree - ndx_in_leaf;
        bool long_strings = Array::get_hasrefs_from_header(leaf_mem.m_addr);
        if (long_strings) {
            ArrayStringLong leaf(leaf_mem, 0, 0, m_array->get_alloc());
            end_in_leaf = min(leaf.size(), end - leaf_offset);
            size_t ndx = leaf.find_first(value, ndx_in_leaf, end_in_leaf);
            if (ndx != not_found)
                return leaf_offset + ndx;
        }
        else {
            ArrayString leaf(leaf_mem, 0, 0, m_array->get_alloc());
            end_in_leaf = min(leaf.size(), end - leaf_offset);
            size_t ndx = leaf.find_first(value, ndx_in_leaf, end_in_leaf);
            if (ndx != not_found)
                return leaf_offset + ndx;
        }
        ndx_in_tree = leaf_offset + end_in_leaf;
    }

    return not_found;
}


void AdaptiveStringColumn::find_all(Array &result, StringData value, size_t begin, size_t end) const
{
    TIGHTDB_ASSERT(begin <= size());
    TIGHTDB_ASSERT(end == npos || (begin <= end && end <= size()));

    if (m_index && begin == 0 && end == npos)
        m_index->find_all(result, value); // Throws

    if (root_is_leaf()) {
        size_t leaf_offset = 0;
        bool long_strings = m_array->has_refs();
        if (long_strings) {
            static_cast<ArrayStringLong*>(m_array)->
                find_all(result, value, leaf_offset, begin, end); // Throws
        }
        else {
            static_cast<ArrayString*>(m_array)->
                find_all(result, value, leaf_offset, begin, end); // Throws
        }
        return;
    }

    // FIXME: It would be better to always require that 'end' is
    // specified explicitely, since Table has the size readily
    // available, and Array::get_bptree_size() is deprecated.
    if (end == npos)
        end = m_array->get_bptree_size();

    size_t ndx_in_tree = begin;
    while (ndx_in_tree < end) {
        pair<MemRef, size_t> p = m_array->get_bptree_leaf(ndx_in_tree);
        MemRef leaf_mem = p.first;
        size_t ndx_in_leaf = p.second, end_in_leaf;
        size_t leaf_offset = ndx_in_tree - ndx_in_leaf;
        bool long_strings = Array::get_hasrefs_from_header(leaf_mem.m_addr);
        if (long_strings) {
            ArrayStringLong leaf(leaf_mem, 0, 0, m_array->get_alloc());
            end_in_leaf = min(leaf.size(), end - leaf_offset);
            leaf.find_all(result, value, leaf_offset, ndx_in_leaf, end_in_leaf); // Throws
        }
        else {
            ArrayString leaf(leaf_mem, 0, 0, m_array->get_alloc());
            end_in_leaf = min(leaf.size(), end - leaf_offset);
            leaf.find_all(result, value, leaf_offset, ndx_in_leaf, end_in_leaf); // Throws
        }
        ndx_in_tree = leaf_offset + end_in_leaf;
    }
}


FindRes AdaptiveStringColumn::find_all_indexref(StringData value, size_t& dst) const
{
    TIGHTDB_ASSERT(value.data());
    TIGHTDB_ASSERT(m_index);

    return m_index->find_all(value, dst);
}


bool AdaptiveStringColumn::auto_enumerate(ref_type& keys_ref, ref_type& values_ref) const
{
    AdaptiveStringColumn keys(m_array->get_alloc());

    // Generate list of unique values (keys)
    size_t n = size();
    for (size_t i=0; i<n; ++i) {
        StringData v = get(i);

        // Insert keys in sorted order, ignoring duplicates
        size_t pos = keys.lower_bound_string(v);
        if (pos != keys.size() && keys.get(pos) == v)
            continue;

        // Don't bother auto enumerating if there are too few duplicates
        if (n/2 < keys.size()) {
            keys.destroy(); // cleanup
            return false;
        }

        keys.insert(pos, v);
    }

    // Generate enumerated list of entries
    Column values(m_array->get_alloc());
    for (size_t i=0; i<n; ++i) {
        StringData v = get(i);
        size_t pos = keys.lower_bound_string(v);
        TIGHTDB_ASSERT(pos != keys.size());
        values.add(pos);
    }

    keys_ref   = keys.get_ref();
    values_ref = values.get_ref();
    return true;
}

bool AdaptiveStringColumn::compare_string(const AdaptiveStringColumn& c) const
{
    size_t n = size();
    if (c.size() != n)
        return false;
    for (size_t i = 0; i != n; ++i) {
        if (get(i) != c.get(i))
            return false;
    }
    return true;
}


void AdaptiveStringColumn::do_insert(size_t ndx, StringData value)
{
    TIGHTDB_ASSERT(ndx == npos || ndx < size());
    ref_type new_sibling_ref;
    Array::TreeInsert<AdaptiveStringColumn> state;
    if (root_is_leaf()) {
        TIGHTDB_ASSERT(ndx == npos || ndx < TIGHTDB_MAX_LIST_SIZE);
        bool long_strings = m_array->has_refs();
        if (!long_strings && short_string_max_size < value.size()) {
            // Upgrade root leaf from short to long strings
            ArrayString* leaf = static_cast<ArrayString*>(m_array);
            ArrayParent* parent = leaf->get_parent();
            std::size_t ndx_in_parent = leaf->get_ndx_in_parent();
            Allocator& alloc = leaf->get_alloc();
            UniquePtr<ArrayStringLong> new_leaf(new ArrayStringLong(parent, ndx_in_parent,
                                                                    alloc)); // Throws
            copy_leaf(*leaf, *new_leaf); // Throws
            leaf->destroy();
            delete leaf;
            m_array = new_leaf.release();
            long_strings = true;
        }
        if (long_strings) {
            ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array);
            new_sibling_ref = leaf->bptree_leaf_insert(ndx, value, state); // Throws
        }
        else {
            ArrayString* leaf = static_cast<ArrayString*>(m_array);
            new_sibling_ref = leaf->bptree_leaf_insert(ndx, value, state); // Throws
        }
    }
    else {
        state.m_value = value;
        if (ndx == npos) {
            new_sibling_ref = m_array->bptree_append(state); // Throws
        }
        else {
            new_sibling_ref = m_array->bptree_insert(ndx, state); // Throws
        }
    }

    if (TIGHTDB_UNLIKELY(new_sibling_ref)) {
        bool is_append = ndx == npos;
        introduce_new_root(new_sibling_ref, state, is_append); // Throws
    }

    // Update index
    if (m_index) {
        bool is_last = ndx == npos;
        size_t real_ndx = is_last ? size()-1 : ndx;
        m_index->insert(real_ndx, value, is_last); // Throws
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}


ref_type AdaptiveStringColumn::leaf_insert(MemRef leaf_mem, ArrayParent& parent,
                                           size_t ndx_in_parent, Allocator& alloc,
                                           size_t insert_ndx,
                                           Array::TreeInsert<AdaptiveStringColumn>& state)
{
    const char* leaf_header = leaf_mem.m_addr;
    bool long_strings = Array::get_hasrefs_from_header(leaf_header);
    if (!long_strings) {
        ArrayString leaf(leaf_mem, &parent, ndx_in_parent, alloc);
        if (state.m_value.size() <= short_string_max_size)
            return leaf.bptree_leaf_insert(insert_ndx, state.m_value, state); // Throws
        // Upgrade leaf from short to long strings
        ArrayStringLong new_leaf(&parent, ndx_in_parent, alloc); // Throws
        copy_leaf(leaf, new_leaf); // Throws
        leaf.destroy();
        return new_leaf.bptree_leaf_insert(insert_ndx, state.m_value, state); // Throws
    }
    ArrayStringLong leaf(leaf_mem, &parent, ndx_in_parent, alloc);
    return leaf.bptree_leaf_insert(insert_ndx, state.m_value, state); // Throws
}


#ifdef TIGHTDB_DEBUG

namespace {

size_t verify_leaf(MemRef mem, Allocator& alloc)
{
    size_t leaf_size;
    bool long_strings = Array::get_hasrefs_from_header(mem.m_addr);
    if (long_strings) {
        ArrayStringLong leaf(mem, 0, 0, alloc);
        leaf.Verify();
        leaf_size = leaf.size();
    }
    else {
        ArrayString leaf(mem, 0, 0, alloc);
        leaf.Verify();
        leaf_size = leaf.size();
    }
    return leaf_size;
}

} // anonymous namespace

void AdaptiveStringColumn::Verify() const
{
    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (long_strings) {
            static_cast<ArrayStringLong*>(m_array)->Verify();
        }
        else {
            static_cast<ArrayString*>(m_array)->Verify();
        }
    }
    else {
        m_array->verify_bptree(&verify_leaf);
    }

    if (m_index)
        m_index->verify_entries(*this);
}


void AdaptiveStringColumn::to_dot(ostream& out, StringData title) const
{
    ref_type ref = m_array->get_ref();
    out << "subgraph cluster_string_column" << ref << " {" << endl;
    out << " label = \"String column";
    if (title.size() != 0)
        out << "\\n'" << title << "'";
    out << "\";" << endl;
    tree_to_dot(out);
    out << "}" << endl;
}

void AdaptiveStringColumn::leaf_to_dot(MemRef leaf_mem, ArrayParent* parent, size_t ndx_in_parent,
                                       ostream& out) const
{
    bool long_strings = Array::get_hasrefs_from_header(leaf_mem.m_addr);
    if (long_strings) {
        ArrayStringLong leaf(leaf_mem, parent, ndx_in_parent, m_array->get_alloc());
        leaf.to_dot(out);
    }
    else {
        ArrayString leaf(leaf_mem, parent, ndx_in_parent, m_array->get_alloc());
        leaf.to_dot(out);
    }
}

namespace {

void leaf_dumper(MemRef mem, Allocator& alloc, ostream& out, int level)
{
    size_t leaf_size;
    const char* leaf_type;
    bool long_strings = Array::get_hasrefs_from_header(mem.m_addr);
    if (long_strings) {
        ArrayStringLong leaf(mem, 0, 0, alloc);
        leaf_size = leaf.size();
        leaf_type = "Long strings leaf";
    }
    else {
        ArrayString leaf(mem, 0, 0, alloc);
        leaf_size = leaf.size();
        leaf_type = "Short strings leaf";
    }
    int indent = level * 2;
    out << setw(indent) << "" << leaf_type << " (size: "<<leaf_size<<")\n";
}

} // anonymous namespace

void AdaptiveStringColumn::dump_node_structure(ostream& out, int level) const
{
    m_array->dump_bptree_structure(out, level, &leaf_dumper);
}

#endif // TIGHTDB_DEBUG
