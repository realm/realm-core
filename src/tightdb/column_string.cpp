#include <cstdlib>
#include <cstring>
#include <cstdio> // debug
#ifdef _MSC_VER
#  include <win32\types.h>
#endif

#include <tightdb/query_conditions.hpp>
#include <tightdb/column_string.hpp>
#include <tightdb/index_string.hpp>

using namespace std;
using namespace tightdb;


namespace {

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

bool AdaptiveStringColumn::is_empty() const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (long_strings) {
            return static_cast<ArrayStringLong*>(m_array)->is_empty();
        }
        return static_cast<ArrayString*>(m_array)->is_empty();
    }

    Array offsets = NodeGetOffsets();
    return offsets.is_empty();
}

size_t AdaptiveStringColumn::size() const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (long_strings) {
            return static_cast<ArrayStringLong*>(m_array)->size();
        }
        return static_cast<ArrayString*>(m_array)->size();
    }

    Array offsets = NodeGetOffsets();
    size_t size = offsets.is_empty() ? 0 : size_t(offsets.back());
    return size;
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

void AdaptiveStringColumn::move_last_over(size_t ndx)
{
    TIGHTDB_ASSERT(ndx+1 < size());

    size_t ndx_last = size() - 1;
    StringData v = get(ndx_last);

    if (m_index) {
        // remove the value to be overwritten from index
        StringData old_val = get(ndx);
        m_index->erase(ndx, old_val, true);

        // update index to point to new location
        m_index->update_ref(v, ndx_last, ndx);
    }

    TreeSet<StringData, AdaptiveStringColumn>(ndx, v);

    // If the copy happened within the same array
    // it might have moved the source data when making
    // room for the insert. In that case we wil have to
    // copy again from the new position
    // TODO: manual resize before copy
    StringData v2 = get(ndx_last);
    if (v != v2)
        TreeSet<StringData, AdaptiveStringColumn>(ndx, v2);

    TreeDelete<StringData, AdaptiveStringColumn>(ndx_last);
}

void AdaptiveStringColumn::set(size_t ndx, StringData str)
{
    TIGHTDB_ASSERT(ndx < size());

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        StringData old_val = get(ndx);
        m_index->set(ndx, old_val, str);
    }

    TreeSet<StringData, AdaptiveStringColumn>(ndx, str);
}

void AdaptiveStringColumn::fill(size_t count)
{
    TIGHTDB_ASSERT(is_empty());
    TIGHTDB_ASSERT(!m_index);

    // Fill column with default values
    // TODO: this is a very naive approach
    // we could speedup by creating full nodes directly
    for (size_t i = 0; i < count; ++i) {
        add(StringData());
    }

#ifdef TIGHTDB_DEBUG
    Verify();
#endif
}

void AdaptiveStringColumn::erase(size_t ndx)
{
    TIGHTDB_ASSERT(ndx < size());

    // Update index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_index) {
        StringData oldVal = get(ndx);
        const bool isLast = (ndx+1 == size());
        m_index->erase(ndx, oldVal, isLast);
    }

    TreeDelete<StringData, AdaptiveStringColumn>(ndx);
}

size_t AdaptiveStringColumn::count(StringData target) const
{
    if (m_index) {
        return m_index->count(target);
    }

    size_t count = 0;

    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (long_strings)
            count += static_cast<ArrayStringLong*>(m_array)->count(target);
        else
            count += static_cast<ArrayString*>(m_array)->count(target);
    }
    else {
        Array refs = NodeGetRefs();
        size_t n = refs.size();
        for (size_t i = 0; i < n; ++i) {
            ref_type ref = refs.get_as_ref(i);
            AdaptiveStringColumn col(ref, 0, 0, m_array->get_alloc());
            count += col.count(target);
        }
    }

    return count;
}

size_t AdaptiveStringColumn::find_first(StringData value, size_t begin, size_t end) const
{
    if (m_index && begin == 0 && end == size_t(-1)) {
        return m_index->find_first(value);
    }

    return TreeFind<StringData, AdaptiveStringColumn, Equal>(value, begin, end);
}


void AdaptiveStringColumn::find_all(Array &result, StringData value, size_t begin, size_t end) const
{
    if (m_index && begin == 0 && end == size_t(-1)) {
        return m_index->find_all(result, value);
    }

    TreeFindAll<StringData, AdaptiveStringColumn>(result, value, 0, begin, end);
}


FindRes AdaptiveStringColumn::find_all_indexref(StringData value, size_t& dst) const
{
    TIGHTDB_ASSERT(value.data());
    TIGHTDB_ASSERT(m_index);

    return m_index->find_all(value, dst);
}


void AdaptiveStringColumn::LeafSet(size_t ndx, StringData value)
{
    // Easy to set if the strings fit
    bool long_strings = m_array->has_refs();
    if (long_strings) {
        static_cast<ArrayStringLong*>(m_array)->set(ndx, value);
        return;
    }
    if (value.size() < 16) {
        static_cast<ArrayString*>(m_array)->set(ndx, value);
        return;
    }

    // Replace string array with long string array
    ArrayStringLong* const new_array =
        new ArrayStringLong(static_cast<Array*>(0), 0, m_array->get_alloc());

    // Copy strings to new array
    ArrayString* const oldarray = static_cast<ArrayString*>(m_array);
    for (size_t i = 0; i < oldarray->size(); ++i) {
        new_array->add(oldarray->get(i));
    }
    new_array->set(ndx, value);

    // Update parent to point to new array
    ArrayParent* parent = oldarray->get_parent();
    if (parent) {
        size_t pndx = oldarray->get_ndx_in_parent();
        parent->update_child_ref(pndx, new_array->get_ref());
        new_array->set_parent(parent, pndx);
    }

    // Replace string array with long string array
    m_array = new_array;
    oldarray->destroy();
    delete oldarray;
}

template<class> size_t AdaptiveStringColumn::LeafFind(StringData value, size_t begin, size_t end) const
{
    bool long_strings = m_array->has_refs();
    if (long_strings) {
        return static_cast<ArrayStringLong*>(m_array)->find_first(value, begin, end);
    }
    return static_cast<ArrayString*>(m_array)->find_first(value, begin, end);
}

void AdaptiveStringColumn::LeafFindAll(Array &result, StringData value, size_t add_offset, size_t begin, size_t end) const
{
    bool long_strings = m_array->has_refs();
    if (long_strings) {
        return static_cast<ArrayStringLong*>(m_array)->find_all(result, value, add_offset, begin, end);
    }
    return static_cast<ArrayString*>(m_array)->find_all(result, value, add_offset, begin, end);
}


void AdaptiveStringColumn::LeafDelete(size_t ndx)
{
    bool long_strings = m_array->has_refs();
    if (long_strings) {
        static_cast<ArrayStringLong*>(m_array)->erase(ndx);
    }
    else {
        static_cast<ArrayString*>(m_array)->erase(ndx);
    }
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
    const size_t n = size();
    if (c.size() != n)
        return false;
    for (size_t i=0; i<n; ++i) {
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
            UniquePtr<ArrayStringLong> new_leaf(new ArrayStringLong(parent, ndx_in_parent, alloc));
            copy_leaf(*leaf, *new_leaf);
            leaf->destroy();
            delete leaf;
            m_array = new_leaf.release();
            long_strings = true;
        }
        if (long_strings) {
            ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array);
            new_sibling_ref = leaf->btree_leaf_insert(ndx, value, state);
        }
        else {
            ArrayString* leaf = static_cast<ArrayString*>(m_array);
            new_sibling_ref = leaf->btree_leaf_insert(ndx, value, state);
        }
    }
    else {
        state.m_value = value;
        new_sibling_ref = m_array->btree_insert(ndx, state);
    }

    if (TIGHTDB_UNLIKELY(new_sibling_ref))
        introduce_new_root(new_sibling_ref, state);

    // Update index
    if (m_index) {
        bool is_last = ndx == npos;
        size_t real_ndx = is_last ? size()-1 : ndx;
        m_index->insert(real_ndx, value, is_last);
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
        if (state.m_value.size() <= short_string_max_size) {
            return leaf.btree_leaf_insert(insert_ndx, state.m_value, state);
        }
        // Upgrade leaf from short to long strings
        ArrayStringLong new_leaf(&parent, ndx_in_parent, alloc);
        copy_leaf(leaf, new_leaf);
        leaf.destroy();
        return new_leaf.btree_leaf_insert(insert_ndx, state.m_value, state);
    }
    ArrayStringLong leaf(leaf_mem, &parent, ndx_in_parent, alloc);
    return leaf.btree_leaf_insert(insert_ndx, state.m_value, state);
}


void AdaptiveStringColumn::copy_leaf(const ArrayString& from, ArrayStringLong& to)
{
    size_t n = from.size();
    for (size_t i=0; i<n; ++i) {
        to.add(from.get(i));
    }
}


#ifdef TIGHTDB_DEBUG

void AdaptiveStringColumn::Verify() const
{
    if (m_index) {
        m_index->verify_entries(*this);
    }
}

void AdaptiveStringColumn::leaf_to_dot(ostream& out, const Array& array) const
{
    bool long_strings = array.has_refs(); // has_refs() indicates long string array

    if (long_strings) {
        // ArrayStringLong has more members than Array, so we have to
        // really instantiate it (it is not enough with a cast)
        ref_type ref = array.get_ref();
        ArrayStringLong str_array(ref, 0, 0, array.get_alloc());
        str_array.to_dot(out);
    }
    else {
        static_cast<const ArrayString&>(array).to_dot(out);
    }
}

#endif // TIGHTDB_DEBUG
