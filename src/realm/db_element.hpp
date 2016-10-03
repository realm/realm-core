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

#ifndef REALM_DB_ELEMENT_HPP
#define REALM_DB_ELEMENT_HPP

#include <realm/alloc.hpp>

namespace realm {

// Maximum number of bytes that the payload of an DbElement can be
const size_t max_array_payload = 0x00ffffffL;

/// Special index value. It has various meanings depending on
/// context. It is returned by some search functions to indicate 'not
/// found'. It is similar in function to std::string::npos.
const size_t npos = size_t(-1);

/// Alias for realm::npos.
const size_t not_found = npos;

struct TreeInsertBase {
    size_t m_split_offset;
    size_t m_split_size;
};

class ArrayParent {
public:
    virtual ~ArrayParent() noexcept
    {
    }

protected:
    virtual void update_child_ref(size_t child_ndx, ref_type new_ref) = 0;

    virtual ref_type get_child_ref(size_t child_ndx) const noexcept = 0;

    // Used only by Array::to_dot().
    virtual std::pair<ref_type, size_t> get_to_dot_parent(size_t ndx_in_parent) const = 0;

    friend class DbElement;
    friend class Array;
};

class DbElement {
public:
    enum Type {
        type_Normal,

        /// This array is the main array of an innner node of a B+-tree as used
        /// in table columns.
        type_InnerBptreeNode,

        /// This array may contain refs to subarrays. An element whose least
        /// significant bit is zero, is a ref pointing to a subarray. An element
        /// whose least significant bit is one, is just a value. It is the
        /// responsibility of the application to ensure that non-ref values have
        /// their least significant bit set. This will generally be done by
        /// shifting the desired vlue to the left by one bit position, and then
        /// setting the vacated bit to one.
        type_HasRefs
    };

    enum WidthType {
        wtype_Bits = 0,     // width indicates how many bits every element occupies
        wtype_Multiply = 1, // width indicates how many bytes every element occupies
        wtype_Ignore = 2,   // each element is 1 byte
    };

    static const int header_size = 8; // Number of bytes used by header

    // The encryption layer relies on headers always fitting within a single page.
    static_assert(header_size == 8, "Header must always fit in entirely on a page");

    // FIXME: Should not be public
    char* m_data = nullptr; // Points to first byte after header

    // The object will not be fully initialized when using this constructor
    explicit DbElement(Allocator& a) noexcept
        : m_alloc(a)
    {
    }

    explicit DbElement(ref_type ref, Allocator& a) noexcept
        : m_alloc(a)
    {
        init_from_ref(ref);
    }

    virtual ~DbElement()
    {
    }

    /*************************** Public virtuals *****************************/

    /// Construct a complete copy of this element (including its subelements)
    /// using the specified target allocator and return just the reference to
    /// the underlying memory.
    virtual MemRef clone_deep(Allocator& target_alloc) const
    {
        char* header = get_header_from_data(m_data);
        return clone(header, target_alloc);
    }

    /**************************** Initializers *******************************/

    /// Reinitialize this array accessor to point to the specified new
    /// underlying memory. This does not modify the parent reference information
    /// of this accessor.
    void init_from_ref(ref_type) noexcept;

    /// Same as init_from_ref(ref_type) but avoid the mapping of 'ref' to memory
    /// pointer.
    virtual void init_from_mem(MemRef) noexcept;

    /// Same as `init_from_ref(get_ref_from_parent())`.
    void init_from_parent() noexcept;

    /// Called in the context of Group::commit() to ensure that attached
    /// accessors stay valid across a commit. Please note that this works only
    /// for non-transactional commits. Accessors obtained during a transaction
    /// are always detached when the transaction ends.
    ///
    /// Returns true if, and only if the array has changed. If the array has not
    /// changed, then its children are guaranteed to also not have changed.
    bool update_from_parent(size_t old_baseline) noexcept;

    /************************** access functions *****************************/

    /// Get the address of the header of this array.
    char* get_header() const noexcept
    {
        return get_header_from_data(m_data);
    }

    /// Returns true if type is either type_HasRefs or type_InnerColumnNode.
    ///
    /// This information is guaranteed to be cached in the array accessor.
    bool has_refs() const noexcept
    {
        return m_has_refs;
    }

    /// This information is guaranteed to be cached in the array accessor.
    ///
    /// Columns and indexes can use the context bit to differentiate leaf types.
    bool get_context_flag() const noexcept
    {
        return m_context_flag;
    }

    ref_type get_ref() const noexcept
    {
        return m_ref;
    }

    MemRef get_mem() const noexcept
    {
        return MemRef(get_header_from_data(m_data), m_ref, m_alloc);
    }

    /// This information is guaranteed to be cached in the array accessor.
    bool is_inner_bptree_node() const noexcept
    {
        return m_is_inner_bptree_node;
    }

    Allocator& get_alloc() const noexcept
    {
        return m_alloc;
    }
    /// The meaning of 'width' depends on the context in which this
    /// array is used.
    uint_least8_t get_width() const noexcept
    {
        return m_width;
    }

    Type get_type() const noexcept;

    size_t size() const noexcept;

    /// Get the number of bytes currently in use by this array. This
    /// includes the array header, but it does not include allocated
    /// bytes corresponding to excess capacity. The result is
    /// guaranteed to be a multiple of 8 (i.e., 64-bit aligned).
    ///
    /// This number is exactly the number of bytes that will be
    /// written by a non-recursive invocation of write().
    size_t get_byte_size() const noexcept;

    bool is_empty() const noexcept
    {
        return size() == 0;
    }

    bool is_attached() const noexcept
    {
        return m_data != nullptr;
    }

    bool has_parent() const noexcept
    {
        return m_parent != nullptr;
    }

    ArrayParent* get_parent() const noexcept
    {
        return m_parent;
    }

    size_t get_ndx_in_parent() const noexcept
    {
        return m_ndx_in_parent;
    }

    /// Get the ref of this array as known to the parent. The caller must ensure
    /// that the parent information ('pointer to parent' and 'index in parent')
    /// is correct before calling this function.
    ref_type get_ref_from_parent() const noexcept
    {
        return m_parent->get_child_ref(m_ndx_in_parent);
    }

    /***************************** modifiers *********************************/

    void set_context_flag(bool value) noexcept
    {
        m_context_flag = value;
        set_header_context_flag(value);
    }

    /// Detach from the underlying array node. This method has no effect if the
    /// accessor is currently unattached (idempotency).
    void detach() noexcept
    {
        m_data = nullptr;
    }

    void truncate(size_t new_size);

    void destroy() noexcept;

    void destroy_deep() noexcept;

    /// Setting a new parent affects ownership of the attached array node, if
    /// any. If a non-null parent is specified, and there was no parent
    /// originally, then the caller passes ownership to the parent, and vice
    /// versa. This assumes, of course, that the change in parentship reflects a
    /// corresponding change in the list of children in the affected parents.
    void set_parent(ArrayParent* parent, size_t ndx_in_parent) noexcept
    {
        m_parent = parent;
        m_ndx_in_parent = ndx_in_parent;
    }

    void set_ndx_in_parent(size_t ndx) noexcept
    {
        m_ndx_in_parent = ndx;
    }

    void adjust_ndx_in_parent(int diff) noexcept
    {
        // Note that `diff` is promoted to an unsigned type, and that
        // C++03 still guarantees the expected result regardless of the
        // sizes of `int` and `decltype(m_ndx_in_parent)`.
        m_ndx_in_parent += diff;
    }

    /// Update the parents reference to this child. This requires, of course,
    /// that the parent information stored in this child is up to date. If the
    /// parent pointer is set to null, this function has no effect.
    void update_parent();

    /*********************** header access functions *************************/

    static char* get_data_from_header(char*) noexcept;
    static char* get_header_from_data(char*) noexcept;
    static const char* get_data_from_header(const char*) noexcept;

    static bool get_is_inner_bptree_node_from_header(const char*) noexcept;
    static bool get_hasrefs_from_header(const char*) noexcept;
    static bool get_context_flag_from_header(const char*) noexcept;
    static WidthType get_wtype_from_header(const char*) noexcept;
    static uint_least8_t get_width_from_header(const char*) noexcept;
    static size_t get_size_from_header(const char*) noexcept;

    static Type get_type_from_header(const char*) noexcept;

    /// Same as get_byte_size().
    static size_t get_byte_size_from_header(const char*) noexcept;

    // Undefined behavior if array is in immutable memory
    static size_t get_capacity_from_header(const char*) noexcept;

    /******************************* debug ***********************************/

#ifdef REALM_DEBUG
    void to_dot_parent_edge(std::ostream&) const;
#endif

protected:
    /// The total size in bytes (including the header) of a new empty
    /// db element. Must be a multiple of 8 (i.e., 64-bit aligned).
    static constexpr size_t initial_capacity = 128;

    size_t m_ref;
    uint_least8_t m_width = 0;   // Size of an element (meaning depend on type of array).
    bool m_is_inner_bptree_node; // This array is an inner node of B+-tree.
    bool m_has_refs;             // Elements whose first bit is zero are refs to subarrays.
    bool m_context_flag;         // Meaning depends on context.

    size_t m_capacity = 0; // Number of elements that fit inside the allocated memory.
    size_t m_size = 0;     // Number of elements currently stored.

    Allocator& m_alloc;

    static MemRef create_element(size_t size, Allocator& alloc, bool context_flag = false, Type type = type_Normal,
                                 WidthType width_type = wtype_Ignore, uint_least8_t width = 1);

    static void init_header(char* header, bool is_inner_bptree_node, bool has_refs, bool context_flag,
                            WidthType width_type, int width, size_t size, size_t capacity) noexcept;

    static MemRef clone(const char* header, Allocator& target_alloc);

    void alloc(size_t init_size, size_t width);
    void copy_on_write();

    // Includes array header. Not necessarily 8-byte aligned.
    virtual size_t calc_byte_len(size_t num_items, size_t width) const;
    virtual size_t calc_item_count(size_t bytes, size_t width) const noexcept;
    virtual void destroy_children() noexcept
    {
    }

    /********************** header modifier functions ************************/

    void set_header_is_inner_bptree_node(bool value) noexcept;
    void set_header_hasrefs(bool value) noexcept;
    void set_header_context_flag(bool value) noexcept;
    void set_header_wtype(WidthType value) noexcept;
    void set_header_width(int value) noexcept;
    void set_header_size(size_t value) noexcept;
    void set_header_capacity(size_t value) noexcept;

    static void set_header_is_inner_bptree_node(bool value, char* header) noexcept;
    static void set_header_hasrefs(bool value, char* header) noexcept;
    static void set_header_context_flag(bool value, char* header) noexcept;
    static void set_header_wtype(WidthType value, char* header) noexcept;
    static void set_header_width(int value, char* header) noexcept;
    static void set_header_size(size_t value, char* header) noexcept;
    static void set_header_capacity(size_t value, char* header) noexcept;

private:
    ArrayParent* m_parent = nullptr;
    size_t m_ndx_in_parent = 0; // Ignored if m_parent is null.

    static size_t calc_byte_size(WidthType wtype, size_t size, uint_least8_t width) noexcept;
};

inline void DbElement::init_from_ref(ref_type ref) noexcept
{
    REALM_ASSERT_DEBUG(ref);
    char* header = m_alloc.translate(ref);
    init_from_mem(MemRef(header, ref, m_alloc));
}

inline void DbElement::init_from_parent() noexcept
{
    ref_type ref = get_ref_from_parent();
    init_from_ref(ref);
}

/*****************************************************************************/

inline DbElement::Type DbElement::get_type() const noexcept
{
    if (m_is_inner_bptree_node) {
        REALM_ASSERT_DEBUG(m_has_refs);
        return type_InnerBptreeNode;
    }
    if (m_has_refs)
        return type_HasRefs;
    return type_Normal;
}

inline size_t DbElement::size() const noexcept
{
    REALM_ASSERT_DEBUG(is_attached());
    return m_size;
}

inline size_t DbElement::get_byte_size() const noexcept
{
    const char* header = get_header_from_data(m_data);
    WidthType wtype = get_wtype_from_header(header);
    size_t num_bytes = calc_byte_size(wtype, m_size, get_width());

    REALM_ASSERT_7(get_alloc().is_read_only(get_ref()), ==, true, ||, num_bytes, <=,
                   get_capacity_from_header(header));

    return num_bytes;
}

/*****************************************************************************/

inline void DbElement::destroy() noexcept
{
    if (!is_attached())
        return;
    char* header = get_header_from_data(m_data);
    get_alloc().free_(get_ref(), header);
    m_data = nullptr;
}

inline void DbElement::destroy_deep() noexcept
{
    if (!is_attached())
        return;

    if (has_refs())
        destroy_children();

    DbElement::destroy();
}

inline void DbElement::update_parent()
{
    if (m_parent)
        m_parent->update_child_ref(m_ndx_in_parent, m_ref);
}

/*****************************************************************************/

inline char* DbElement::get_data_from_header(char* header) noexcept
{
    return header + header_size;
}

inline char* DbElement::get_header_from_data(char* data) noexcept
{
    return data - header_size;
}

inline const char* DbElement::get_data_from_header(const char* header) noexcept
{
    return get_data_from_header(const_cast<char*>(header));
}

inline bool DbElement::get_is_inner_bptree_node_from_header(const char* header) noexcept
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return (int(h[4]) & 0x80) != 0;
}

inline bool DbElement::get_hasrefs_from_header(const char* header) noexcept
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return (int(h[4]) & 0x40) != 0;
}

inline bool DbElement::get_context_flag_from_header(const char* header) noexcept
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return (int(h[4]) & 0x20) != 0;
}

inline DbElement::WidthType DbElement::get_wtype_from_header(const char* header) noexcept
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return WidthType((int(h[4]) & 0x18) >> 3);
}

inline uint_least8_t DbElement::get_width_from_header(const char* header) noexcept
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return uint_least8_t((1 << (int(h[4]) & 0x07)) >> 1);
}

inline size_t DbElement::get_size_from_header(const char* header) noexcept
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return (size_t(h[5]) << 16) + (size_t(h[6]) << 8) + h[7];
}

inline DbElement::Type DbElement::get_type_from_header(const char* header) noexcept
{
    if (get_is_inner_bptree_node_from_header(header))
        return type_InnerBptreeNode;
    if (get_hasrefs_from_header(header))
        return type_HasRefs;
    return type_Normal;
}

inline size_t DbElement::get_byte_size_from_header(const char* header) noexcept
{
    size_t size = get_size_from_header(header);
    uint_least8_t width = get_width_from_header(header);
    WidthType wtype = get_wtype_from_header(header);
    size_t num_bytes = calc_byte_size(wtype, size, width);

    return num_bytes;
}

inline size_t DbElement::get_capacity_from_header(const char* header) noexcept
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return (size_t(h[0]) << 16) + (size_t(h[1]) << 8) + h[2];
}

/*****************************************************************************/

inline void DbElement::set_header_is_inner_bptree_node(bool value) noexcept
{
    set_header_is_inner_bptree_node(value, get_header_from_data(m_data));
}

inline void DbElement::set_header_hasrefs(bool value) noexcept
{
    set_header_hasrefs(value, get_header_from_data(m_data));
}

inline void DbElement::set_header_context_flag(bool value) noexcept
{
    set_header_context_flag(value, get_header_from_data(m_data));
}

inline void DbElement::set_header_wtype(WidthType value) noexcept
{
    set_header_wtype(value, get_header_from_data(m_data));
}

inline void DbElement::set_header_width(int value) noexcept
{
    set_header_width(value, get_header_from_data(m_data));
}

inline void DbElement::set_header_size(size_t value) noexcept
{
    set_header_size(value, get_header_from_data(m_data));
}

inline void DbElement::set_header_capacity(size_t value) noexcept
{
    set_header_capacity(value, get_header_from_data(m_data));
}

inline void DbElement::set_header_is_inner_bptree_node(bool value, char* header) noexcept
{
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[4] = uchar((int(h[4]) & ~0x80) | int(value) << 7);
}

inline void DbElement::set_header_hasrefs(bool value, char* header) noexcept
{
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[4] = uchar((int(h[4]) & ~0x40) | int(value) << 6);
}

inline void DbElement::set_header_context_flag(bool value, char* header) noexcept
{
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[4] = uchar((int(h[4]) & ~0x20) | int(value) << 5);
}

inline void DbElement::set_header_wtype(WidthType value, char* header) noexcept
{
    // Indicates how to calculate size in bytes based on width
    // 0: bits      (width/8) * size
    // 1: multiply  width * size
    // 2: ignore    1 * size
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[4] = uchar((int(h[4]) & ~0x18) | int(value) << 3);
}

inline void DbElement::set_header_width(int value, char* header) noexcept
{
    // Pack width in 3 bits (log2)
    int w = 0;
    while (value) {
        ++w;
        value >>= 1;
    }
    REALM_ASSERT_3(w, <, 8);

    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[4] = uchar((int(h[4]) & ~0x7) | w);
}

inline void DbElement::set_header_size(size_t value, char* header) noexcept
{
    REALM_ASSERT_3(value, <=, max_array_payload);
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[5] = uchar((value >> 16) & 0x000000FF);
    h[6] = uchar((value >> 8) & 0x000000FF);
    h[7] = uchar(value & 0x000000FF);
}

// Note: There is a copy of this function is test_alloc.cpp
inline void DbElement::set_header_capacity(size_t value, char* header) noexcept
{
    REALM_ASSERT_3(value, <=, max_array_payload);
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[0] = uchar((value >> 16) & 0x000000FF);
    h[1] = uchar((value >> 8) & 0x000000FF);
    h[2] = uchar(value & 0x000000FF);
}

/*****************************************************************************/

inline void DbElement::init_header(char* header, bool is_inner_bptree_node, bool has_refs, bool context_flag,
                                   WidthType width_type, int width, size_t size, size_t capacity) noexcept
{
    // Note: Since the header layout contains unallocated bit and/or
    // bytes, it is important that we put the entire header into a
    // well defined state initially.
    std::fill(header, header + header_size, 0);
    set_header_is_inner_bptree_node(is_inner_bptree_node, header);
    set_header_hasrefs(has_refs, header);
    set_header_context_flag(context_flag, header);
    set_header_wtype(width_type, header);
    set_header_width(width, header);
    set_header_size(size, header);
    set_header_capacity(capacity, header);
}

inline size_t DbElement::calc_byte_size(WidthType wtype, size_t size, uint_least8_t width) noexcept
{
    size_t num_bytes = 0;
    switch (wtype) {
        case wtype_Bits: {
            // Current assumption is that size is at most 2^24 and that width is at most 64.
            // In that case the following will never overflow. (Assuming that size_t is at least 32 bits)
            REALM_ASSERT_3(size, <, 0x1000000);
            size_t num_bits = size * width;
            num_bytes = (num_bits + 7) >> 3;
            break;
        }
        case wtype_Multiply: {
            num_bytes = size * width;
            break;
        }
        case wtype_Ignore:
            num_bytes = size;
            break;
    }

    // Ensure 8-byte alignment
    num_bytes = (num_bytes + 7) & ~size_t(7);

    num_bytes += header_size;

    return num_bytes;
}
}

#endif /* SRC_REALM_DB_ELEMENT_HPP_ */
