#include <cstdlib>
#include <cstring>
#include <cstdio> // debug
#include <iostream>
#include <iomanip>

#ifdef _WIN32
#  include <win32\types.h>
#endif

#include <memory>

#include <realm/query_conditions.hpp>
#include <realm/column_string.hpp>
#include <realm/index_string.hpp>
#include <realm/table.hpp>

using namespace std;
using namespace realm;
using namespace realm::util;


namespace {

const size_t small_string_max_size  = 15; // ArrayString
const size_t medium_string_max_size = 63; // ArrayStringLong

// Getter function for index. For integer index, the caller must supply a buffer that we can store the
// extracted value in (it may be bitpacked, so we cannot return a pointer in to the Array as we do with
// String index).
StringData get_string(void* column, size_t ndx, char*)
{
    return static_cast<AdaptiveStringColumn*>(column)->get(ndx);
}

void copy_leaf(const ArrayString& from, ArrayStringLong& to)
{
    size_t n = from.size();
    for (size_t i = 0; i != n; ++i) {
        StringData str = from.get(i);
        to.add(from.get(i)); // Throws
    }
}

void copy_leaf(const ArrayString& from, ArrayBigBlobs& to)
{
    size_t n = from.size();
    for (size_t i = 0; i != n; ++i) {
        StringData str = from.get(i);
        to.add_string(str); // Throws
    }
}

void copy_leaf(const ArrayStringLong& from, ArrayBigBlobs& to)
{
    size_t n = from.size();
    for (size_t i = 0; i != n; ++i) {
        StringData str = from.get(i);
        to.add_string(str); // Throws
    }
}

} // anonymous namespace



AdaptiveStringColumn::AdaptiveStringColumn(Allocator& alloc, ref_type ref)
{
    char* header = alloc.translate(ref);
    MemRef mem(header, ref);

    // Within an AdaptiveStringColumn the leaves can be of different
    // type optimized for the lengths of the strings contained
    // therein.  The type is indicated by the combination of the
    // is_inner_bptree_node(N), has_refs(R) and context_flag(C):
    //
    //   N R C
    //   1 0 0   InnerBptreeNode (not leaf)
    //   0 0 0   ArrayString
    //   0 1 0   ArrayStringLong
    //   0 1 1   ArrayBigBlobs
    Array::Type type = Array::get_type_from_header(header);
    switch (type) {
        case Array::type_Normal: {
            // Small strings root leaf
            ArrayString* root = new ArrayString(alloc); // Throws
            root->init_from_mem(mem);
            m_array.reset(root);
            return;
        }
        case Array::type_HasRefs: {
            bool is_big = Array::get_context_flag_from_header(header);
            if (!is_big) {
                // Medium strings root leaf
                ArrayStringLong* root = new ArrayStringLong(alloc); // Throws
                root->init_from_mem(mem);
                m_array.reset(root);
                return;
            }
            // Big strings root leaf
            ArrayBigBlobs* root = new ArrayBigBlobs(alloc); // Throws
            root->init_from_mem(mem);
            m_array.reset(root);
            return;
        }
        case Array::type_InnerBptreeNode: {
            // Non-leaf root
            Array* root = new Array(alloc); // Throws
            root->init_from_mem(mem);
            m_array.reset(root);
            return;
        }
    }
    REALM_ASSERT_DEBUG(false);
}


AdaptiveStringColumn::~AdaptiveStringColumn() REALM_NOEXCEPT
{
}


void AdaptiveStringColumn::destroy() REALM_NOEXCEPT
{
    ColumnBase::destroy();
    if (m_search_index)
        m_search_index->destroy();
}


StringData AdaptiveStringColumn::get(size_t ndx) const REALM_NOEXCEPT
{
    REALM_ASSERT_DEBUG(ndx < size());

    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (!long_strings) {
            // Small strings root leaf
            ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
            return leaf->get(ndx);
        }
        bool is_big = m_array->get_context_flag();
        if (!is_big) {
            // Medium strings root leaf
            ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
            return leaf->get(ndx);
        }
        // Big strings root leaf
        ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array.get());
        return leaf->get_string(ndx);
    }

    // Non-leaf root
    pair<MemRef, size_t> p = m_array->get_bptree_leaf(ndx);
    const char* leaf_header = p.first.m_addr;
    size_t ndx_in_leaf = p.second;
    bool long_strings = Array::get_hasrefs_from_header(leaf_header);
    if (!long_strings) {
        // Small strings
        return ArrayString::get(leaf_header, ndx_in_leaf);
    }
    Allocator& alloc = m_array->get_alloc();
    bool is_big = Array::get_context_flag_from_header(leaf_header);
    if (!is_big) {
        // Medimum strings
        return ArrayStringLong::get(leaf_header, ndx_in_leaf, alloc);
    }
    // Big strings
    return ArrayBigBlobs::get_string(leaf_header, ndx_in_leaf, alloc);
}


StringIndex* AdaptiveStringColumn::create_search_index()
{
    REALM_ASSERT(!m_search_index);

    std::unique_ptr<StringIndex> index;
    index.reset(new StringIndex(this, &get_string, m_array->get_alloc())); // Throws

    // Populate the index
    size_t num_rows = size();
    for (size_t row_ndx = 0; row_ndx != num_rows; ++row_ndx) {
        StringData value = get(row_ndx);
        size_t num_rows = 1;
        bool is_append = true;
        index->insert(row_ndx, value, num_rows, is_append); // Throws
    }

    m_search_index = std::move(index);
    return m_search_index.get();
}


void AdaptiveStringColumn::destroy_search_index() REALM_NOEXCEPT
{
    m_search_index.reset();
}


std::unique_ptr<StringIndex> AdaptiveStringColumn::release_search_index() REALM_NOEXCEPT
{
    return std::move(m_search_index);
}


void AdaptiveStringColumn::set_search_index_ref(ref_type ref, ArrayParent* parent,
                                                size_t ndx_in_parent, bool allow_duplicate_valaues)
{
    REALM_ASSERT(!m_search_index);
    m_search_index.reset(new StringIndex(ref, parent, ndx_in_parent, this, &get_string,
                                         !allow_duplicate_valaues, m_array->get_alloc())); // Throws
}


void AdaptiveStringColumn::set_search_index_allow_duplicate_values(bool allow) REALM_NOEXCEPT
{
    m_search_index->set_allow_duplicate_values(allow);
}


void AdaptiveStringColumn::update_from_parent(std::size_t old_baseline) REALM_NOEXCEPT
{
    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (!long_strings) {
            // Small strings root leaf
            ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
            leaf->update_from_parent(old_baseline);
        }
        else {
            bool is_big = m_array->get_context_flag();
            if (!is_big) {
                // Medium strings root leaf
                ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
                leaf->update_from_parent(old_baseline);
            }
            else {
                // Big strings root leaf
                ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array.get());
                leaf->update_from_parent(old_baseline);
            }
        }
    }
    else {
        // Non-leaf root
        m_array->update_from_parent(old_baseline);
    }
    if (m_search_index)
        m_search_index->update_from_parent(old_baseline);
}


namespace {

class SetLeafElem: public Array::UpdateHandler {
public:
    Allocator& m_alloc;
    const StringData m_value;

    SetLeafElem(Allocator& alloc, StringData value) REALM_NOEXCEPT:
        m_alloc(alloc), m_value(value) {}

    void update(MemRef mem, ArrayParent* parent, size_t ndx_in_parent,
                size_t elem_ndx_in_leaf) override
    {
        bool long_strings = Array::get_hasrefs_from_header(mem.m_addr);
        if (long_strings) {
            bool is_big = Array::get_context_flag_from_header(mem.m_addr);
            if (is_big) {
                ArrayBigBlobs leaf(m_alloc);
                leaf.init_from_mem(mem);
                leaf.set_parent(parent, ndx_in_parent);
                leaf.set_string(elem_ndx_in_leaf, m_value); // Throws
                return;
            }
            ArrayStringLong leaf(m_alloc);
            leaf.init_from_mem(mem);
            leaf.set_parent(parent, ndx_in_parent);
            if (m_value.size() <= medium_string_max_size) {
                leaf.set(elem_ndx_in_leaf, m_value); // Throws
                return;
            }
            // Upgrade leaf from medium to big strings
            ArrayBigBlobs new_leaf(m_alloc);
            new_leaf.create(); // Throws
            new_leaf.set_parent(parent, ndx_in_parent); // Throws
            new_leaf.update_parent(); // Throws
            copy_leaf(leaf, new_leaf); // Throws
            leaf.destroy();
            new_leaf.set_string(elem_ndx_in_leaf, m_value); // Throws
            return;
        }
        ArrayString leaf(m_alloc);
        leaf.init_from_mem(mem);
        leaf.set_parent(parent, ndx_in_parent);
        if (m_value.size() <= small_string_max_size) {
            leaf.set(elem_ndx_in_leaf, m_value); // Throws
            return;
        }
        if (m_value.size() <= medium_string_max_size) {
            // Upgrade leaf from small to medium strings
            ArrayStringLong new_leaf(m_alloc);
            new_leaf.create(); // Throws
            new_leaf.set_parent(parent, ndx_in_parent);
            new_leaf.update_parent(); // Throws
            copy_leaf(leaf, new_leaf); // Throws
            leaf.destroy();
            new_leaf.set(elem_ndx_in_leaf, m_value); // Throws
            return;
        }
        // Upgrade leaf from small to big strings
        ArrayBigBlobs new_leaf(m_alloc);
        new_leaf.create(); // Throws
        new_leaf.set_parent(parent, ndx_in_parent);
        new_leaf.update_parent(); // Throws
        copy_leaf(leaf, new_leaf); // Throws
        leaf.destroy();
        new_leaf.set_string(elem_ndx_in_leaf, m_value); // Throws
    }
};

} // anonymous namespace

void AdaptiveStringColumn::set(size_t ndx, StringData value)
{
    REALM_ASSERT_DEBUG(ndx < size());

    // We must modify the search index before modifying the column, because we
    // need to be able to abort the operation if the modification of the search
    // index fails due to a unique constraint violation.

    // Update search index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_search_index) {
        m_search_index->set(ndx, value); // Throws
    }

    bool root_is_leaf = !m_array->is_inner_bptree_node();
    if (root_is_leaf) {
        LeafType leaf_type = upgrade_root_leaf(value.size()); // Throws
        switch (leaf_type) {
            case leaf_type_Small: {
                ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
                leaf->set(ndx, value); // Throws
                return;
            }
            case leaf_type_Medium: {
                ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
                leaf->set(ndx, value); // Throws
                return;
            }
            case leaf_type_Big: {
                ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array.get());
                leaf->set_string(ndx, value); // Throws
                return;
            }
        }
        REALM_ASSERT(false);
    }

    SetLeafElem set_leaf_elem(m_array->get_alloc(), value);
    m_array->update_bptree_elem(ndx, set_leaf_elem); // Throws
}


class AdaptiveStringColumn::EraseLeafElem: public ColumnBase::EraseHandlerBase {
public:
    EraseLeafElem(AdaptiveStringColumn& column) REALM_NOEXCEPT:
        EraseHandlerBase(column) {}
    bool erase_leaf_elem(MemRef leaf_mem, ArrayParent* parent,
                         size_t leaf_ndx_in_parent,
                         size_t elem_ndx_in_leaf) override
    {
        bool long_strings = Array::get_hasrefs_from_header(leaf_mem.m_addr);
        if (!long_strings) {
            // Small strings
            ArrayString leaf(get_alloc());
            leaf.init_from_mem(leaf_mem);
            leaf.set_parent(parent, leaf_ndx_in_parent);
            REALM_ASSERT_3(leaf.size(), >=, 1);
            size_t last_ndx = leaf.size() - 1;
            if (last_ndx == 0)
                return true;
            size_t ndx = elem_ndx_in_leaf;
            if (ndx == npos)
                ndx = last_ndx;
            leaf.erase(ndx); // Throws
            return false;
        }
        bool is_big = Array::get_context_flag_from_header(leaf_mem.m_addr);
        if (!is_big) {
            // Medium strings
            ArrayStringLong leaf(get_alloc());
            leaf.init_from_mem(leaf_mem);
            leaf.set_parent(parent, leaf_ndx_in_parent);
            REALM_ASSERT_3(leaf.size(), >=, 1);
            size_t last_ndx = leaf.size() - 1;
            if (last_ndx == 0)
                return true;
            size_t ndx = elem_ndx_in_leaf;
            if (ndx == npos)
                ndx = last_ndx;
            leaf.erase(ndx); // Throws
            return false;
        }
        // Big strings
        ArrayBigBlobs leaf(get_alloc());
        leaf.init_from_mem(leaf_mem);
        leaf.set_parent(parent, leaf_ndx_in_parent);
        REALM_ASSERT_3(leaf.size(), >=, 1);
        size_t last_ndx = leaf.size() - 1;
        if (last_ndx == 0)
            return true;
        size_t ndx = elem_ndx_in_leaf;
        if (ndx == npos)
            ndx = last_ndx;
        leaf.erase(ndx); // Throws
        return false;
    }
    void destroy_leaf(MemRef leaf_mem) REALM_NOEXCEPT override
    {
        Array::destroy_deep(leaf_mem, get_alloc());
    }
    void replace_root_by_leaf(MemRef leaf_mem) override
    {
        Array* leaf;
        bool long_strings = Array::get_hasrefs_from_header(leaf_mem.m_addr);
        if (!long_strings) {
            // Small strings
            ArrayString* leaf_2 = new ArrayString(get_alloc()); // Throws
            leaf_2->init_from_mem(leaf_mem);
            leaf = leaf_2;
        }
        else {
            bool is_big = Array::get_context_flag_from_header(leaf_mem.m_addr);
            if (!is_big) {
                // Medium strings
                ArrayStringLong* leaf_2 = new ArrayStringLong(get_alloc()); // Throws
                leaf_2->init_from_mem(leaf_mem);
                leaf = leaf_2;
            }
            else {
                // Big strings
                ArrayBigBlobs* leaf_2 = new ArrayBigBlobs(get_alloc()); // Throws
                leaf_2->init_from_mem(leaf_mem);
                leaf = leaf_2;
            }
        }
        replace_root(leaf); // Throws, but accessor ownership is passed to callee
    }
    void replace_root_by_empty_leaf() override
    {
        std::unique_ptr<ArrayString> leaf;
        leaf.reset(new ArrayString(get_alloc())); // Throws
        leaf->create(); // Throws
        replace_root(leaf.release()); // Throws, but accessor ownership is passed to callee
    }
};

void AdaptiveStringColumn::do_erase(size_t ndx, bool is_last)
{
    REALM_ASSERT_3(ndx, <, size());
    REALM_ASSERT_3(is_last, ==, (ndx == size() - 1));

    // Update search index
    // (it is important here that we do it before actually setting
    //  the value, or the index would not be able to find the correct
    //  position to update (as it looks for the old value))
    if (m_search_index) {
        m_search_index->erase<StringData>(ndx, is_last);
    }

    bool root_is_leaf = !m_array->is_inner_bptree_node();
    if (root_is_leaf) {
        bool long_strings = m_array->has_refs();
        if (!long_strings) {
            // Small strings root leaf
            ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
            leaf->erase(ndx); // Throws
            return;
        }
        bool is_big = m_array->get_context_flag();
        if (!is_big) {
            // Medium strings root leaf
            ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
            leaf->erase(ndx); // Throws
            return;
        }
        // Big strings root leaf
        ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array.get());
        leaf->erase(ndx); // Throws
        return;
    }

    // Non-leaf root
    size_t ndx_2 = is_last ? npos : ndx;
    EraseLeafElem erase_leaf_elem(*this);
    Array::erase_bptree_elem(m_array.get(), ndx_2, erase_leaf_elem); // Throws
}


void AdaptiveStringColumn::do_move_last_over(size_t row_ndx, size_t last_row_ndx)
{
    REALM_ASSERT_3(row_ndx, <=, last_row_ndx);
    REALM_ASSERT_3(last_row_ndx + 1, ==, size());

    // FIXME: ExceptionSafety: The current implementation of this
    // function is not exception-safe, and it is hard to see how to
    // repair it.

    // FIXME: Consider doing two nested calls to
    // update_bptree_elem(). If the two leaves are not the same, no
    // copying is needed. If they are the same, call
    // Array::move_last_over() (does not yet
    // exist). Array::move_last_over() could be implemented in a way
    // that avoids the intermediate copy. This approach is also likely
    // to be necesseray for exception safety.

    StringData value = get(last_row_ndx);

    // Copying string data from a column to itself requires an
    // intermediate copy of the data (constr:bptree-copy-to-self).
    std::unique_ptr<char[]> buffer(new char[value.size()]); // Throws
    copy(value.data(), value.data()+value.size(), buffer.get());
    StringData copy_of_value(buffer.get(), value.size());

    if (m_search_index) {
        // remove the value to be overwritten from index
        bool is_last = true; // This tells StringIndex::erase() to not adjust subsequent indexes
        m_search_index->erase<StringData>(row_ndx, is_last); // Throws

        // update index to point to new location
        if (row_ndx != last_row_ndx)
            m_search_index->update_ref(copy_of_value, last_row_ndx, row_ndx); // Throws
    }

    bool root_is_leaf = !m_array->is_inner_bptree_node();
    if (root_is_leaf) {
        bool long_strings = m_array->has_refs();
        if (!long_strings) {
            // Small strings root leaf
            ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
            leaf->set(row_ndx, copy_of_value); // Throws
            leaf->erase(last_row_ndx); // Throws
            return;
        }
        bool is_big = m_array->get_context_flag();
        if (!is_big) {
            // Medium strings root leaf
            ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
            leaf->set(row_ndx, copy_of_value); // Throws
            leaf->erase(last_row_ndx); // Throws
            return;
        }
        // Big strings root leaf
        ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array.get());
        leaf->set_string(row_ndx, copy_of_value); // Throws
        leaf->erase(last_row_ndx); // Throws
        return;
    }

    // Non-leaf root
    SetLeafElem set_leaf_elem(m_array->get_alloc(), copy_of_value);
    m_array->update_bptree_elem(row_ndx, set_leaf_elem); // Throws
    EraseLeafElem erase_leaf_elem(*this);
    Array::erase_bptree_elem(m_array.get(), realm::npos, erase_leaf_elem); // Throws
}


void AdaptiveStringColumn::do_clear()
{
    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (!long_strings) {
            // Small strings root leaf
            ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
            leaf->clear(); // Throws
        }
        else {
            bool is_big = m_array->get_context_flag();
            if (!is_big) {
                // Medium strings root leaf
                ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
                leaf->clear(); // Throws
            }
            else {
                // Big strings root leaf
                ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array.get());
                leaf->clear(); // Throws
            }
        }
    }
    else {
        // Non-leaf root - revert to small strings leaf
        Allocator& alloc = m_array->get_alloc();
        std::unique_ptr<ArrayString> array;
        array.reset(new ArrayString(alloc)); // Throws
        array->create(); // Throws
        array->set_parent(m_array->get_parent(), m_array->get_ndx_in_parent());
        array->update_parent(); // Throws

        // Remove original node
        m_array->destroy_deep();
        m_array = std::move(array);
    }

    if (m_search_index)
        m_search_index->clear(); // Throws
}


size_t AdaptiveStringColumn::count(StringData value) const
{
    if (m_search_index)
        return m_search_index->count(value); // Throws

    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (!long_strings) {
            // Small strings root leaf
            ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
            return leaf->count(value);
        }
        bool is_big = m_array->get_context_flag();
        if (!is_big) {
            // Medium strings root leaf
            ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
            return leaf->count(value);
        }
        // Big strings root leaf
        BinaryData bin(value.data(), value.size());
        bool is_string = true;
        ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array.get());
        return leaf->count(bin, is_string);
    }

    // Non-leaf root
    size_t num_matches = 0;

    // FIXME: It would be better to always require that 'end' is
    // specified explicitely, since Table has the size readily
    // available, and Array::get_bptree_size() is deprecated.
    size_t begin = 0, end = m_array->get_bptree_size();
    while (begin < end) {
        pair<MemRef, size_t> p = m_array->get_bptree_leaf(begin);
        MemRef leaf_mem = p.first;
        REALM_ASSERT_3(p.second, ==, 0);
        bool long_strings = Array::get_hasrefs_from_header(leaf_mem.m_addr);
        if (!long_strings) {
            // Small strings
            ArrayString leaf(m_array->get_alloc());
            leaf.init_from_mem(leaf_mem);
            num_matches += leaf.count(value);
            begin += leaf.size();
            continue;
        }
        bool is_big = Array::get_context_flag_from_header(leaf_mem.m_addr);
        if (!is_big) {
            // Medium strings
            ArrayStringLong leaf(m_array->get_alloc());
            leaf.init_from_mem(leaf_mem);
            num_matches += leaf.count(value);
            begin += leaf.size();
            continue;
        }
        // Big strings
        ArrayBigBlobs leaf(m_array->get_alloc());
        leaf.init_from_mem(leaf_mem);
        BinaryData bin(value.data(), value.size());
        bool is_string = true;
        num_matches += leaf.count(bin, is_string);
        begin += leaf.size();
    }

    return num_matches;
}


size_t AdaptiveStringColumn::find_first(StringData value, size_t begin, size_t end) const
{
    REALM_ASSERT_3(begin, <=, size());
    REALM_ASSERT(end == npos || (begin <= end && end <= size()));

    if (m_search_index && begin == 0 && end == npos)
        return m_search_index->find_first(value); // Throws

    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (!long_strings) {
            // Small strings root leaf
            ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
            return leaf->find_first(value, begin, end);
        }
        bool is_big = m_array->get_context_flag();
        if (!is_big) {
            // Medium strings root leaf
            ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
            return leaf->find_first(value, begin, end);
        }
        // Big strings root leaf
        ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array.get());
        BinaryData bin(value.data(), value.size());
        bool is_string = true;
        return leaf->find_first(bin, is_string, begin, end);
    }

    // Non-leaf root
    //
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
        if (!long_strings) {
            // Small strings
            ArrayString leaf(m_array->get_alloc());
            leaf.init_from_mem(leaf_mem);
            end_in_leaf = min(leaf.size(), end - leaf_offset);
            size_t ndx = leaf.find_first(value, ndx_in_leaf, end_in_leaf);
            if (ndx != not_found)
                return leaf_offset + ndx;
        }
        else {
            bool is_big = Array::get_context_flag_from_header(leaf_mem.m_addr);
            if (!is_big) {
                // Medium strings
                ArrayStringLong leaf(m_array->get_alloc());
                leaf.init_from_mem(leaf_mem);
                end_in_leaf = min(leaf.size(), end - leaf_offset);
                size_t ndx = leaf.find_first(value, ndx_in_leaf, end_in_leaf);
                if (ndx != not_found)
                    return leaf_offset + ndx;
            }
            else {
                // Big strings
                ArrayBigBlobs leaf(m_array->get_alloc());
                leaf.init_from_mem(leaf_mem);
                end_in_leaf = min(leaf.size(), end - leaf_offset);
                BinaryData bin(value.data(), value.size());
                bool is_string = true;
                size_t ndx = leaf.find_first(bin, is_string, ndx_in_leaf, end_in_leaf);
                if (ndx != not_found)
                    return leaf_offset + ndx;
            }
        }
        ndx_in_tree = leaf_offset + end_in_leaf;
    }

    return not_found;
}


void AdaptiveStringColumn::find_all(Column& result, StringData value, size_t begin, size_t end) const
{
    REALM_ASSERT_3(begin, <=, size());
    REALM_ASSERT(end == npos || (begin <= end && end <= size()));

    if (m_search_index && begin == 0 && end == npos) {
        m_search_index->find_all(result, value); // Throws
        return;
    }

    if (root_is_leaf()) {
        size_t leaf_offset = 0;
        bool long_strings = m_array->has_refs();
        if (!long_strings) {
            // Small strings root leaf
            ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
            leaf->find_all(result, value, leaf_offset, begin, end); // Throws
            return;
        }
        bool is_big = m_array->get_context_flag();
        if (!is_big) {
            // Medium strings root leaf
            ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
            leaf->find_all(result, value, leaf_offset, begin, end); // Throws
            return;
        }
        // Big strings root leaf
        ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array.get());
        BinaryData bin(value.data(), value.size());
        bool is_string = true;
        leaf->find_all(result, bin, is_string, leaf_offset, begin, end); // Throws
        return;
    }

    // Non-leaf root
    //
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
        if (!long_strings) {
            // Small strings
            ArrayString leaf(m_array->get_alloc());
            leaf.init_from_mem(leaf_mem);
            end_in_leaf = min(leaf.size(), end - leaf_offset);
            leaf.find_all(result, value, leaf_offset, ndx_in_leaf, end_in_leaf); // Throws
        }
        else {
            bool is_big = Array::get_context_flag_from_header(leaf_mem.m_addr);
            if (!is_big) {
                // Medium strings
                ArrayStringLong leaf(m_array->get_alloc());
                leaf.init_from_mem(leaf_mem);
                end_in_leaf = min(leaf.size(), end - leaf_offset);
                leaf.find_all(result, value, leaf_offset, ndx_in_leaf, end_in_leaf); // Throws
            }
            else {
                // Big strings
                ArrayBigBlobs leaf(m_array->get_alloc());
                leaf.init_from_mem(leaf_mem);
                end_in_leaf = min(leaf.size(), end - leaf_offset);
                BinaryData bin(value.data(), value.size());
                bool is_string = true;
                leaf.find_all(result, bin, is_string, leaf_offset, ndx_in_leaf,
                              end_in_leaf); // Throws
            }
        }
        ndx_in_tree = leaf_offset + end_in_leaf;
    }
}


namespace {

struct BinToStrAdaptor {
    typedef StringData value_type;
    const ArrayBigBlobs& m_big_blobs;
    BinToStrAdaptor(const ArrayBigBlobs& big_blobs) REALM_NOEXCEPT: m_big_blobs(big_blobs) {}
    ~BinToStrAdaptor() REALM_NOEXCEPT {}
    size_t size() const REALM_NOEXCEPT
    {
        return m_big_blobs.size();
    }
    StringData get(size_t ndx) const REALM_NOEXCEPT
    {
        return m_big_blobs.get_string(ndx);
    }
};

} // anonymous namespace

size_t AdaptiveStringColumn::lower_bound_string(StringData value) const REALM_NOEXCEPT
{
    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (!long_strings) {
            // Small strings root leaf
            ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
            return ColumnBase::lower_bound(*leaf, value);
        }
        bool is_big = m_array->get_context_flag();
        if (!is_big) {
            // Medium strings root leaf
            ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
            return ColumnBase::lower_bound(*leaf, value);
        }
        // Big strings root leaf
        ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array.get());
        BinToStrAdaptor adapt(*leaf);
        return ColumnBase::lower_bound(adapt, value);
    }
    // Non-leaf root
    return ColumnBase::lower_bound(*this, value);
}

size_t AdaptiveStringColumn::upper_bound_string(StringData value) const REALM_NOEXCEPT
{
    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (!long_strings) {
            // Small strings root leaf
            ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
            return ColumnBase::upper_bound(*leaf, value);
        }
        bool is_big = m_array->get_context_flag();
        if (!is_big) {
            // Medium strings root leaf
            ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
            return ColumnBase::upper_bound(*leaf, value);
        }
        // Big strings root leaf
        ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array.get());
        BinToStrAdaptor adapt(*leaf);
        return ColumnBase::upper_bound(adapt, value);
    }
    // Non-leaf root
    return ColumnBase::upper_bound(*this, value);
}


FindRes AdaptiveStringColumn::find_all_indexref(StringData value, size_t& dst) const
{
    REALM_ASSERT(value.data());
    REALM_ASSERT(m_search_index);

    return m_search_index->find_all(value, dst);
}


bool AdaptiveStringColumn::auto_enumerate(ref_type& keys_ref, ref_type& values_ref) const
{
    Allocator& alloc = m_array->get_alloc();
    ref_type keys_ref_2 = AdaptiveStringColumn::create(alloc); // Throws
    AdaptiveStringColumn keys(alloc, keys_ref_2); // Throws

    // Generate list of unique values (keys)
    size_t n = size();
    for (size_t i = 0; i != n; ++i) {
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

        keys.insert(pos, v); // Throws
    }

    // Generate enumerated list of entries
    ref_type values_ref_2 = Column::create(alloc); // Throws
    Column values(alloc, values_ref_2); // Throws
    for (size_t i = 0; i != n; ++i) {
        StringData v = get(i);
        size_t pos = keys.lower_bound_string(v);
        REALM_ASSERT_3(pos, !=, keys.size());
        values.add(pos); // Throws
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
        StringData v_1 = get(i);
        StringData v_2 = c.get(i);
        if (v_1 != v_2)
            return false;
    }
    return true;
}


void AdaptiveStringColumn::do_insert(size_t row_ndx, StringData value, size_t num_rows)
{
    bptree_insert(row_ndx, value, num_rows); // Throws

    if (m_search_index) {
        bool is_append = row_ndx == realm::npos;
        size_t row_ndx_2 = is_append ? size() - num_rows : row_ndx;
        m_search_index->insert(row_ndx_2, value, num_rows, is_append); // Throws
    }
}


void AdaptiveStringColumn::do_insert(size_t row_ndx, StringData value, size_t num_rows, bool is_append)
{
    size_t row_ndx_2 = is_append ? realm::npos : row_ndx;
    bptree_insert(row_ndx_2, value, num_rows); // Throws

    if (m_search_index)
        m_search_index->insert(row_ndx, value, num_rows, is_append); // Throws
}


void AdaptiveStringColumn::bptree_insert(size_t row_ndx, StringData value, size_t num_rows)
{
    REALM_ASSERT(row_ndx == realm::npos || row_ndx < size());
    ref_type new_sibling_ref;
    Array::TreeInsert<AdaptiveStringColumn> state;
    for (size_t i = 0; i != num_rows; ++i) {
        size_t row_ndx_2 = row_ndx == realm::npos ? realm::npos : row_ndx + i;
        if (root_is_leaf()) {
            REALM_ASSERT(row_ndx_2 == realm::npos || row_ndx_2 < REALM_MAX_BPNODE_SIZE);
            LeafType leaf_type = upgrade_root_leaf(value.size()); // Throws
            switch (leaf_type) {
                case leaf_type_Small: {
                    // Small strings root leaf
                    ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
                    new_sibling_ref = leaf->bptree_leaf_insert(row_ndx_2, value, state); // Throws
                    goto insert_done;
                }
                case leaf_type_Medium: {
                    // Medium strings root leaf
                    ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
                    new_sibling_ref = leaf->bptree_leaf_insert(row_ndx_2, value, state); // Throws
                    goto insert_done;
                }
                case leaf_type_Big: {
                    // Big strings root leaf
                    ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array.get());
                    new_sibling_ref =
                        leaf->bptree_leaf_insert_string(row_ndx_2, value, state); // Throws
                    goto insert_done;
                }
            }
            REALM_ASSERT(false);
        }

        // Non-leaf root
        state.m_value = value;
        if (row_ndx_2 == realm::npos) {
            new_sibling_ref = m_array->bptree_append(state); // Throws
        }
        else {
            new_sibling_ref = m_array->bptree_insert(row_ndx_2, state); // Throws
        }

      insert_done:
        if (REALM_UNLIKELY(new_sibling_ref)) {
            bool is_append = row_ndx_2 == realm::npos;
            introduce_new_root(new_sibling_ref, state, is_append); // Throws
        }
    }
}


ref_type AdaptiveStringColumn::leaf_insert(MemRef leaf_mem, ArrayParent& parent,
                                           size_t ndx_in_parent, Allocator& alloc,
                                           size_t insert_ndx,
                                           Array::TreeInsert<AdaptiveStringColumn>& state)
{
    bool long_strings = Array::get_hasrefs_from_header(leaf_mem.m_addr);
    if (long_strings) {
        bool is_big = Array::get_context_flag_from_header(leaf_mem.m_addr);
        if (is_big) {
            ArrayBigBlobs leaf(alloc);
            leaf.init_from_mem(leaf_mem);
            leaf.set_parent(&parent, ndx_in_parent);
            return leaf.bptree_leaf_insert_string(insert_ndx, state.m_value, state); // Throws
        }
        ArrayStringLong leaf(alloc);
        leaf.init_from_mem(leaf_mem);
        leaf.set_parent(&parent, ndx_in_parent);
        if (state.m_value.size() <= medium_string_max_size)
            return leaf.bptree_leaf_insert(insert_ndx, state.m_value, state); // Throws
        // Upgrade leaf from medium to big strings
        ArrayBigBlobs new_leaf(alloc);
        new_leaf.create(); // Throws
        new_leaf.set_parent(&parent, ndx_in_parent);
        new_leaf.update_parent(); // Throws
        copy_leaf(leaf, new_leaf); // Throws
        leaf.destroy();
        return new_leaf.bptree_leaf_insert_string(insert_ndx, state.m_value, state); // Throws
    }
    ArrayString leaf(alloc);
    leaf.init_from_mem(leaf_mem);
    leaf.set_parent(&parent, ndx_in_parent);
    if (state.m_value.size() <= small_string_max_size)
        return leaf.bptree_leaf_insert(insert_ndx, state.m_value, state); // Throws
    if (state.m_value.size() <= medium_string_max_size) {
        // Upgrade leaf from small to medium strings
        ArrayStringLong new_leaf(alloc);
        new_leaf.create(); // Throws
        new_leaf.set_parent(&parent, ndx_in_parent);
        new_leaf.update_parent(); // Throws
        copy_leaf(leaf, new_leaf); // Throws
        leaf.destroy();
        return new_leaf.bptree_leaf_insert(insert_ndx, state.m_value, state); // Throws
    }
    // Upgrade leaf from small to big strings
    ArrayBigBlobs new_leaf(alloc);
    new_leaf.create(); // Throws
    new_leaf.set_parent(&parent, ndx_in_parent);
    new_leaf.update_parent(); // Throws
    copy_leaf(leaf, new_leaf); // Throws
    leaf.destroy();
    return new_leaf.bptree_leaf_insert_string(insert_ndx, state.m_value, state); // Throws
}


AdaptiveStringColumn::LeafType AdaptiveStringColumn::upgrade_root_leaf(size_t value_size)
{
    REALM_ASSERT(root_is_leaf());

    bool long_strings = m_array->has_refs();
    if (long_strings) {
        bool is_big = m_array->get_context_flag();
        if (is_big)
            return leaf_type_Big;
        if (value_size <= medium_string_max_size)
            return leaf_type_Medium;
        // Upgrade root leaf from medium to big strings
        ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
        std::unique_ptr<ArrayBigBlobs> new_leaf;
        ArrayParent* parent = leaf->get_parent();
        size_t ndx_in_parent = leaf->get_ndx_in_parent();
        Allocator& alloc = leaf->get_alloc();
        new_leaf.reset(new ArrayBigBlobs(alloc)); // Throws
        new_leaf->create(); // Throws
        new_leaf->set_parent(parent, ndx_in_parent);
        new_leaf->update_parent(); // Throws
        copy_leaf(*leaf, *new_leaf); // Throws
        leaf->destroy();
        m_array = std::move(new_leaf);
        return leaf_type_Big;
    }
    if (value_size <= small_string_max_size)
        return leaf_type_Small;
    ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
    ArrayParent* parent = leaf->get_parent();
    size_t ndx_in_parent = leaf->get_ndx_in_parent();
    Allocator& alloc = leaf->get_alloc();
    if (value_size <= medium_string_max_size) {
        // Upgrade root leaf from small to medium strings
        std::unique_ptr<ArrayStringLong> new_leaf;
        new_leaf.reset(new ArrayStringLong(alloc)); // Throws
        new_leaf->create(); // Throws
        new_leaf->set_parent(parent, ndx_in_parent);
        new_leaf->update_parent(); // Throws
        copy_leaf(*leaf, *new_leaf); // Throws
        leaf->destroy();
        m_array = std::move(new_leaf);
        return leaf_type_Medium;
    }
    // Upgrade root leaf from small to big strings
    std::unique_ptr<ArrayBigBlobs> new_leaf;
    new_leaf.reset(new ArrayBigBlobs(alloc)); // Throws
    new_leaf->create(); // Throws
    new_leaf->set_parent(parent, ndx_in_parent);
    new_leaf->update_parent(); // Throws
    copy_leaf(*leaf, *new_leaf); // Throws
    leaf->destroy();
    m_array = std::move(new_leaf);
    return leaf_type_Big;
}


AdaptiveStringColumn::LeafType
AdaptiveStringColumn::GetBlock(size_t ndx, ArrayParent** ap, size_t& off, bool use_retval) const
{
    static_cast<void>(use_retval);
    REALM_ASSERT_3(use_retval, ==, false); // retval optimization not supported. See Array on how to implement

    Allocator& alloc = m_array->get_alloc();
    if (root_is_leaf()) {
        off = 0;
        bool long_strings = m_array->has_refs();
        if (long_strings) {
            if (m_array->get_context_flag()) {
                ArrayBigBlobs* asb2 = new ArrayBigBlobs(alloc); // Throws
                asb2->init_from_mem(m_array->get_mem());
                *ap = asb2;
                return leaf_type_Big;
            }
            ArrayStringLong* asl2 = new ArrayStringLong(alloc); // Throws
            asl2->init_from_mem(m_array->get_mem());
            *ap = asl2;
            return leaf_type_Medium;
        }
        ArrayString* as2 = new ArrayString(alloc); // Throws
        as2->init_from_mem(m_array->get_mem());
        *ap = as2;
        return leaf_type_Small;
    }

    pair<MemRef, size_t> p = m_array->get_bptree_leaf(ndx);
    off = ndx - p.second;
    bool long_strings = Array::get_hasrefs_from_header(p.first.m_addr);
    if (long_strings) {
        if (Array::get_context_flag_from_header(p.first.m_addr)) {
            ArrayBigBlobs* asb2 = new ArrayBigBlobs(alloc);
            asb2->init_from_mem(p.first);
            *ap = asb2;
            return leaf_type_Big;
        }
        ArrayStringLong* asl2 = new ArrayStringLong(alloc);
        asl2->init_from_mem(p.first);
        *ap = asl2;
        return leaf_type_Medium;
    }
    ArrayString* as2 = new ArrayString(alloc);
    as2->init_from_mem(p.first);
    *ap = as2;
    return leaf_type_Small;
}


class AdaptiveStringColumn::CreateHandler: public ColumnBase::CreateHandler {
public:
    CreateHandler(Allocator& alloc): m_alloc(alloc) {}
    ref_type create_leaf(size_t size) override
    {
        MemRef mem = ArrayString::create_array(size, m_alloc); // Throws
        return mem.m_ref;
    }
private:
    Allocator& m_alloc;
};

ref_type AdaptiveStringColumn::create(Allocator& alloc, size_t size)
{
    CreateHandler handler(alloc);
    return ColumnBase::create(alloc, size, handler);
}


class AdaptiveStringColumn::SliceHandler: public ColumnBase::SliceHandler {
public:
    SliceHandler(Allocator& alloc): m_alloc(alloc) {}
    MemRef slice_leaf(MemRef leaf_mem, size_t offset, size_t size,
                      Allocator& target_alloc) override
    {
        bool long_strings = Array::get_hasrefs_from_header(leaf_mem.m_addr);
        if (!long_strings) {
            // Small strings
            ArrayString leaf(m_alloc);
            leaf.init_from_mem(leaf_mem);
            return leaf.slice(offset, size, target_alloc); // Throws
        }
        bool is_big = Array::get_context_flag_from_header(leaf_mem.m_addr);
        if (!is_big) {
            // Medium strings
            ArrayStringLong leaf(m_alloc);
            leaf.init_from_mem(leaf_mem);
            return leaf.slice(offset, size, target_alloc); // Throws
        }
        // Big strings
        ArrayBigBlobs leaf(m_alloc);
        leaf.init_from_mem(leaf_mem);
        return leaf.slice(offset, size, target_alloc); // Throws
    }
private:
    Allocator& m_alloc;
};

ref_type AdaptiveStringColumn::write(size_t slice_offset, size_t slice_size,
                                     size_t table_size, _impl::OutputStream& out) const
{
    ref_type ref;
    if (root_is_leaf()) {
        Allocator& alloc = Allocator::get_default();
        MemRef mem;
        bool long_strings = m_array->has_refs();
        if (!long_strings) {
            // Small strings
            ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
            mem = leaf->slice(slice_offset, slice_size, alloc); // Throws
        }
        else {
            bool is_big = m_array->get_context_flag();
            if (!is_big) {
                // Medium strings
                ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
                mem = leaf->slice(slice_offset, slice_size, alloc); // Throws
            }
            else {
                // Big strings
                ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array.get());
                mem = leaf->slice(slice_offset, slice_size, alloc); // Throws
            }
        }
        Array slice(alloc);
        _impl::DeepArrayDestroyGuard dg(&slice);
        slice.init_from_mem(mem);
        size_t pos = slice.write(out); // Throws
        ref = pos;
    }
    else {
        SliceHandler handler(get_alloc());
        ref = ColumnBase::write(m_array.get(), slice_offset, slice_size,
                                table_size, handler, out); // Throws
    }
    return ref;
}


void AdaptiveStringColumn::refresh_accessor_tree(size_t col_ndx, const Spec& spec)
{
    refresh_root_accessor(); // Throws

    // Refresh search index
    if (m_search_index) {
        size_t ndx_in_parent = m_array->get_ndx_in_parent();
        m_search_index->get_root_array()->set_ndx_in_parent(ndx_in_parent + 1);
        m_search_index->refresh_accessor_tree(col_ndx, spec); // Throws
    }
}


void AdaptiveStringColumn::refresh_root_accessor()
{
    // The type of the cached root array accessor may no longer match the
    // underlying root node. In that case we need to replace it. Note that when
    // the root node is an inner B+-tree node, then only the top array accessor
    // of that node is cached. The top array accessor of an inner B+-tree node
    // is of type Array.

    ref_type root_ref = m_array->get_ref_from_parent();
    MemRef root_mem(root_ref, m_array->get_alloc());
    bool new_root_is_leaf   = !Array::get_is_inner_bptree_node_from_header(root_mem.m_addr);
    bool new_root_is_small  = !Array::get_hasrefs_from_header(root_mem.m_addr);
    bool new_root_is_medium = !Array::get_context_flag_from_header(root_mem.m_addr);
    bool old_root_is_leaf   = !m_array->is_inner_bptree_node();
    bool old_root_is_small  = !m_array->has_refs();
    bool old_root_is_medium = !m_array->get_context_flag();

    bool root_type_changed = old_root_is_leaf != new_root_is_leaf ||
        (old_root_is_leaf && (old_root_is_small != new_root_is_small ||
                              (!old_root_is_small && old_root_is_medium != new_root_is_medium)));
    if (!root_type_changed) {
        // Keep, but refresh old root accessor
        if (old_root_is_leaf) {
            if (old_root_is_small) {
                // Root is 'small strings' leaf
                ArrayString* root = static_cast<ArrayString*>(m_array.get());
                root->init_from_parent();
                return;
            }
            if (old_root_is_medium) {
                // Root is 'medium strings' leaf
                ArrayStringLong* root = static_cast<ArrayStringLong*>(m_array.get());
                root->init_from_parent();
                return;
            }
            // Root is 'big strings' leaf
            ArrayBigBlobs* root = static_cast<ArrayBigBlobs*>(m_array.get());
            root->init_from_parent();
            return;
        }
        // Root is inner node
        Array* root = m_array.get();
        root->init_from_parent();
        return;
    }

    // Create new root accessor
    Array* new_root;
    Allocator& alloc = m_array->get_alloc();
    if (new_root_is_leaf) {
        if (new_root_is_small) {
            // New root is 'small strings' leaf
            ArrayString* root = new ArrayString(alloc); // Throws
            root->init_from_mem(root_mem);
            new_root = root;
        }
        else if (new_root_is_medium) {
            // New root is 'medium strings' leaf
            ArrayStringLong* root = new ArrayStringLong(alloc); // Throws
            root->init_from_mem(root_mem);
            new_root = root;
        }
        else {
            // New root is 'big strings' leaf
            ArrayBigBlobs* root = new ArrayBigBlobs(alloc); // Throws
            root->init_from_mem(root_mem);
            new_root = root;
        }
    }
    else {
        // New root is inner node
        Array* root = new Array(alloc); // Throws
        root->init_from_mem(root_mem);
        new_root = root;
    }
    new_root->set_parent(m_array->get_parent(), m_array->get_ndx_in_parent());

    // Instate new root
    m_array.reset(new_root);
}


#ifdef REALM_DEBUG

namespace {

size_t verify_leaf(MemRef mem, Allocator& alloc)
{
    bool long_strings = Array::get_hasrefs_from_header(mem.m_addr);
    if (!long_strings) {
        // Small strings
        ArrayString leaf(alloc);
        leaf.init_from_mem(mem);
        leaf.Verify();
        return leaf.size();
    }
    bool is_big = Array::get_context_flag_from_header(mem.m_addr);
    if (!is_big) {
        // Medium strings
        ArrayStringLong leaf(alloc);
        leaf.init_from_mem(mem);
        leaf.Verify();
        return leaf.size();
    }
    // Big strings
    ArrayBigBlobs leaf(alloc);
    leaf.init_from_mem(mem);
    leaf.Verify();
    return leaf.size();
}

} // anonymous namespace

void AdaptiveStringColumn::Verify() const
{
    if (root_is_leaf()) {
        bool long_strings = m_array->has_refs();
        if (!long_strings) {
            // Small strings root leaf
            ArrayString* leaf = static_cast<ArrayString*>(m_array.get());
            leaf->Verify();
        }
        else {
            bool is_big = m_array->get_context_flag();
            if (!is_big) {
                // Medium strings root leaf
                ArrayStringLong* leaf = static_cast<ArrayStringLong*>(m_array.get());
                leaf->Verify();
            }
            else {
                // Big strings root leaf
                ArrayBigBlobs* leaf = static_cast<ArrayBigBlobs*>(m_array.get());
                leaf->Verify();
            }
        }
    }
    else {
        // Non-leaf root
        m_array->verify_bptree(&verify_leaf);
    }

    if (m_search_index) {
        m_search_index->Verify();
        m_search_index->verify_entries(*this);
    }
}


void AdaptiveStringColumn::Verify(const Table& table, size_t col_ndx) const
{
    ColumnBase::Verify(table, col_ndx);

    typedef _impl::TableFriend tf;
    const Spec& spec = tf::get_spec(table);
    ColumnAttr attr = spec.get_column_attr(col_ndx);
    bool has_search_index = (attr & col_attr_Indexed) != 0;
    REALM_ASSERT_3(has_search_index, ==, bool(m_search_index));
    if (has_search_index) {
        REALM_ASSERT(m_search_index->get_root_array()->get_ndx_in_parent() ==
                       m_array->get_ndx_in_parent() + 1);
    }
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
    if (!long_strings) {
        // Small strings
        ArrayString leaf(m_array->get_alloc());
        leaf.init_from_mem(leaf_mem);
        leaf.set_parent(parent, ndx_in_parent);
        leaf.to_dot(out);
        return;
    }
    bool is_big = Array::get_context_flag_from_header(leaf_mem.m_addr);
    if (!is_big) {
        // Medium strings
        ArrayStringLong leaf(m_array->get_alloc());
        leaf.init_from_mem(leaf_mem);
        leaf.set_parent(parent, ndx_in_parent);
        leaf.to_dot(out);
        return;
    }
    // Big strings
    ArrayBigBlobs leaf(m_array->get_alloc());
    leaf.init_from_mem(leaf_mem);
    leaf.set_parent(parent, ndx_in_parent);
    bool is_strings = true;
    leaf.to_dot(out, is_strings);
}


namespace {

void leaf_dumper(MemRef mem, Allocator& alloc, ostream& out, int level)
{
    size_t leaf_size;
    const char* leaf_type;
    bool long_strings = Array::get_hasrefs_from_header(mem.m_addr);
    if (!long_strings) {
        // Small strings
        ArrayString leaf(alloc);
        leaf.init_from_mem(mem);
        leaf_size = leaf.size();
        leaf_type = "Small strings leaf";
    }
    else {
        bool is_big = Array::get_context_flag_from_header(mem.m_addr);
        if (!is_big) {
            // Medium strings
            ArrayStringLong leaf(alloc);
            leaf.init_from_mem(mem);
            leaf_size = leaf.size();
            leaf_type = "Medimum strings leaf";
        }
        else {
            // Big strings
            ArrayBigBlobs leaf(alloc);
            leaf.init_from_mem(mem);
            leaf_size = leaf.size();
            leaf_type = "Big strings leaf";
        }
    }
    int indent = level * 2;
    out << setw(indent) << "" << leaf_type << " (size: "<<leaf_size<<")\n";
}

} // anonymous namespace

void AdaptiveStringColumn::do_dump_node_structure(ostream& out, int level) const
{
    m_array->dump_bptree_structure(out, level, &leaf_dumper);
    int indent = level * 2;
    out << setw(indent) << "" << "Search index\n";
    m_search_index->do_dump_node_structure(out, level+1);
}

#endif // REALM_DEBUG
