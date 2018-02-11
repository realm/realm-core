/*************************************************************************
 *
 * Copyright 2018 Realm Inc.
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

#ifndef REALM_NODE_HPP
#define REALM_NODE_HPP

#include <realm/alloc.hpp>

namespace realm {

/// Special index value. It has various meanings depending on
/// context. It is returned by some search functions to indicate 'not
/// found'. It is similar in function to std::string::npos.
const size_t npos = size_t(-1);

// Maximum number of bytes that the payload of a node can be
const size_t max_array_payload = 0x00ffffffL;
const size_t max_array_payload_aligned = 0x00fffff8L;

/// Alias for realm::npos.
const size_t not_found = npos;

/// All accessor classes that logically contains other objects must inherit
/// this class.
///
/// A database node accessor contains information about the parent of the
/// referenced node. This 'reverse' reference is not explicitly present in the
/// underlying node hierarchy, but it is needed when modifying an array. A
/// modification may lead to relocation of the underlying array node, and the
/// parent must be updated accordingly. Since this applies recursivly all the
/// way to the root node, it is essential that the entire chain of parent
/// accessors is constructed and propperly maintained when a particular array is
/// modified.
class ArrayParent {
public:
    virtual ~ArrayParent() noexcept {}

protected:
    virtual void update_child_ref(size_t child_ndx, ref_type new_ref) = 0;

    virtual ref_type get_child_ref(size_t child_ndx) const noexcept = 0;

    // Used only by Array::to_dot().
    virtual std::pair<ref_type, size_t> get_to_dot_parent(size_t ndx_in_parent) const = 0;

    friend class Node;
    friend class Array;
};

/// Provides access to individual array nodes of the database.
///
/// This class serves purely as an accessor, it assumes no ownership of the
/// referenced memory.
///
/// An node accessor can be in one of two states: attached or unattached. It is
/// in the attached state if, and only if is_attached() returns true. Most
/// non-static member functions of this class have undefined behaviour if the
/// accessor is in the unattached state. The exceptions are: is_attached(),
/// detach(), create(), init_from_ref(), init_from_mem(), init_from_parent(),
/// has_parent(), get_parent(), set_parent(), get_ndx_in_parent(),
/// set_ndx_in_parent(), adjust_ndx_in_parent(), and get_ref_from_parent().
///
/// An node accessor contains information about the parent of the referenced
/// node. This 'reverse' reference is not explicitly present in the
/// underlying node hierarchy, but it is needed when modifying a node. A
/// modification may lead to relocation of the underlying node, and the
/// parent must be updated accordingly. Since this applies recursively all the
/// way to the root node, it is essential that the entire chain of parent
/// accessors is constructed and properly maintained when a particular node is
/// modified.
///
/// The parent reference (`pointer to parent`, `index in parent`) is updated
/// independently from the state of attachment to an underlying node. In
/// particular, the parent reference remains valid and is unaffected by changes
/// in attachment. These two aspects of the state of the accessor is updated
/// independently, and it is entirely the responsibility of the caller to update
/// them such that they are consistent with the underlying node hierarchy before
/// calling any method that modifies the underlying node.
///
/// FIXME: This class currently has fragments of ownership, in particular the
/// constructors that allocate underlying memory. On the other hand, the
/// destructor never frees the memory. This is a problematic situation, because
/// it so easily becomes an obscure source of leaks. There are three options for
/// a fix of which the third is most attractive but hardest to implement: (1)
/// Remove all traces of ownership semantics, that is, remove the constructors
/// that allocate memory, but keep the trivial copy constructor. For this to
/// work, it is important that the constness of the accessor has nothing to do
/// with the constness of the underlying memory, otherwise constness can be
/// violated simply by copying the accessor. (2) Disallov copying but associate
/// the constness of the accessor with the constness of the underlying
/// memory. (3) Provide full ownership semantics like is done for Table
/// accessors, and provide a proper copy constructor that really produces a copy
/// of the node. For this to work, the class should assume ownership if, and
/// only if there is no parent. A copy produced by a copy constructor will not
/// have a parent. Even if the original was part of a database, the copy will be
/// free-standing, that is, not be part of any database. For intra, or inter
/// database copying, one would have to also specify the target allocator.
class Node {
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

    /*********************** Constructor / destructor ************************/

    // The object will not be fully initialized when using this constructor
    explicit Node(Allocator& alloc) noexcept : m_alloc(alloc) {}

    virtual ~Node() {}

    /**************************** Initializers *******************************/

    /// Same as init_from_ref(ref_type) but avoid the mapping of 'ref' to memory
    /// pointer.
    char* init_from_mem(MemRef mem) noexcept
    {
        char* header = mem.get_addr();
        m_ref = mem.get_ref();
        m_data = get_data_from_header(header);
        m_width = get_width_from_header(header);
        m_size = get_size_from_header(header);

        return header;
    }

    /************************** access functions *****************************/

    bool is_attached() const noexcept { return m_data != nullptr; }

    inline bool is_read_only() const noexcept
    {
        REALM_ASSERT_DEBUG(is_attached());
        return m_alloc.is_read_only(m_ref);
    }

    size_t size() const noexcept
    {
        REALM_ASSERT_DEBUG(is_attached());
        return m_size;
    }

    bool is_empty() const noexcept { return size() == 0; }

    ref_type get_ref() const noexcept { return m_ref; }
    MemRef get_mem() const noexcept { return MemRef(get_header_from_data(m_data), m_ref, m_alloc); }
    Allocator& get_alloc() const noexcept { return m_alloc; }
    /// Get the address of the header of this array.
    char* get_header() const noexcept { return get_header_from_data(m_data); }

    bool has_parent() const noexcept { return m_parent != nullptr; }
    ArrayParent* get_parent() const noexcept { return m_parent; }
    size_t get_ndx_in_parent() const noexcept { return m_ndx_in_parent; }
    /// Get the ref of this array as known to the parent. The caller must ensure
    /// that the parent information ('pointer to parent' and 'index in parent')
    /// is correct before calling this function.
    ref_type get_ref_from_parent() const noexcept
    {
        REALM_ASSERT_DEBUG(m_parent);
        ref_type ref = m_parent->get_child_ref(m_ndx_in_parent);
        return ref;
    }

    /***************************** modifiers *********************************/

    /// Detach from the underlying array node. This method has no effect if the
    /// accessor is currently unattached (idempotency).
    void detach() noexcept { m_data = nullptr; }

    /// Destroy only the array that this accessor is attached to, not the
    /// children of that array. See non-static destroy_deep() for an
    /// alternative. If this accessor is already in the detached state, this
    /// function has no effect (idempotency).
    void destroy() noexcept
    {
        if (!is_attached())
            return;
        char* header = get_header_from_data(m_data);
        m_alloc.free_(m_ref, header);
        m_data = nullptr;
    }

    /// Shorthand for `destroy(MemRef(ref, alloc), alloc)`.
    static void destroy(ref_type ref, Allocator& alloc) noexcept { destroy(MemRef(ref, alloc), alloc); }

    /// Destroy only the specified array node, not its children. See also
    /// destroy_deep(MemRef, Allocator&).
    static void destroy(MemRef mem, Allocator& alloc) noexcept { alloc.free_(mem); }


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
    void set_ndx_in_parent(size_t ndx) noexcept { m_ndx_in_parent = ndx; }

    /// Update the parents reference to this child. This requires, of course,
    /// that the parent information stored in this child is up to date. If the
    /// parent pointer is set to null, this function has no effect.
    void update_parent()
    {
        if (m_parent)
            m_parent->update_child_ref(m_ndx_in_parent, m_ref);
    }

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
    // Undefined behavior if array is in immutable memory
    static size_t get_capacity_from_header(const char*) noexcept;


    static Type get_type_from_header(const char*) noexcept;

protected:
    /// The total size in bytes (including the header) of a new empty
    /// array. Must be a multiple of 8 (i.e., 64-bit aligned).
    static const size_t initial_capacity = 128;

    size_t m_ref;
    Allocator& m_alloc;
    size_t m_size = 0;         // Number of elements currently stored.
    uint_least8_t m_width = 0; // Size of an element (meaning depend on type of array).

#if REALM_ENABLE_MEMDEBUG
    // If m_no_relocation is false, then copy_on_write() will always relocate this array, regardless if it's
    // required or not. If it's true, then it will never relocate, which is currently only expeted inside
    // GroupWriter::write_group() due to a unique chicken/egg problem (see description there).
    bool m_no_relocation = false;
#endif

    void alloc(size_t init_size, size_t new_width);
    void copy_on_write()
    {
#if REALM_ENABLE_MEMDEBUG
        // We want to relocate this array regardless if there is a need or not, in order to catch use-after-free bugs.
        // Only exception is inside GroupWriter::write_group() (see explanation at the definition of the
        // m_no_relocation
        // member)
        if (!m_no_relocation) {
#else
        if (is_read_only()) {
#endif
            do_copy_on_write();
        }
    }

    static MemRef create_element(size_t size, Allocator& alloc, bool context_flag = false, Type type = type_Normal,
                                 WidthType width_type = wtype_Ignore, int width = 1);

    void set_header_size(size_t value) noexcept { set_header_size(value, get_header()); }

    static void set_header_is_inner_bptree_node(bool value, char* header) noexcept;
    static void set_header_hasrefs(bool value, char* header) noexcept;
    static void set_header_context_flag(bool value, char* header) noexcept;
    static void set_header_wtype(WidthType value, char* header) noexcept;
    static void set_header_width(int value, char* header) noexcept;
    static void set_header_size(size_t value, char* header) noexcept;
    static void set_header_capacity(size_t value, char* header) noexcept;

    // Includes array header. Not necessarily 8-byte aligned.
    virtual size_t calc_byte_len(size_t num_items, size_t width) const;
    virtual size_t calc_item_count(size_t bytes, size_t width) const noexcept;
    static size_t calc_byte_size(WidthType wtype, size_t size, uint_least8_t width) noexcept;
    static void init_header(char* header, bool is_inner_bptree_node, bool has_refs, bool context_flag,
                            WidthType width_type, int width, size_t size, size_t capacity) noexcept;

private:
    ArrayParent* m_parent = nullptr;
    size_t m_ndx_in_parent = 0; // Ignored if m_parent is null.

    void do_copy_on_write(size_t minimum_size = 0);
};

/// Base class for all nodes holding user data
class ArrayPayload {
public:
    virtual ~ArrayPayload();
    virtual void init_from_ref(ref_type) noexcept = 0;
};

inline char* Node::get_data_from_header(char* header) noexcept
{
    return header + header_size;
}

inline char* Node::get_header_from_data(char* data) noexcept
{
    return data - header_size;
}

inline const char* Node::get_data_from_header(const char* header) noexcept
{
    return get_data_from_header(const_cast<char*>(header));
}

inline bool Node::get_is_inner_bptree_node_from_header(const char* header) noexcept
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return (int(h[4]) & 0x80) != 0;
}

inline bool Node::get_hasrefs_from_header(const char* header) noexcept
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return (int(h[4]) & 0x40) != 0;
}

inline bool Node::get_context_flag_from_header(const char* header) noexcept
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return (int(h[4]) & 0x20) != 0;
}

inline Node::WidthType Node::get_wtype_from_header(const char* header) noexcept
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return WidthType((int(h[4]) & 0x18) >> 3);
}

inline uint_least8_t Node::get_width_from_header(const char* header) noexcept
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return uint_least8_t((1 << (int(h[4]) & 0x07)) >> 1);
}

inline size_t Node::get_size_from_header(const char* header) noexcept
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return (size_t(h[5]) << 16) + (size_t(h[6]) << 8) + h[7];
}

inline size_t Node::get_capacity_from_header(const char* header) noexcept
{
    typedef unsigned char uchar;
    const uchar* h = reinterpret_cast<const uchar*>(header);
    return (size_t(h[0]) << 16) + (size_t(h[1]) << 8) + h[2];
}

inline Node::Type Node::get_type_from_header(const char* header) noexcept
{
    if (get_is_inner_bptree_node_from_header(header))
        return type_InnerBptreeNode;
    if (get_hasrefs_from_header(header))
        return type_HasRefs;
    return type_Normal;
}

inline void Node::set_header_is_inner_bptree_node(bool value, char* header) noexcept
{
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[4] = uchar((int(h[4]) & ~0x80) | int(value) << 7);
}

inline void Node::set_header_hasrefs(bool value, char* header) noexcept
{
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[4] = uchar((int(h[4]) & ~0x40) | int(value) << 6);
}

inline void Node::set_header_context_flag(bool value, char* header) noexcept
{
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[4] = uchar((int(h[4]) & ~0x20) | int(value) << 5);
}

inline void Node::set_header_wtype(WidthType value, char* header) noexcept
{
    // Indicates how to calculate size in bytes based on width
    // 0: bits      (width/8) * size
    // 1: multiply  width * size
    // 2: ignore    1 * size
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[4] = uchar((int(h[4]) & ~0x18) | int(value) << 3);
}

inline void Node::set_header_width(int value, char* header) noexcept
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

inline void Node::set_header_size(size_t value, char* header) noexcept
{
    REALM_ASSERT_3(value, <=, max_array_payload);
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[5] = uchar((value >> 16) & 0x000000FF);
    h[6] = uchar((value >> 8) & 0x000000FF);
    h[7] = uchar(value & 0x000000FF);
}

// Note: There is a copy of this function is test_alloc.cpp
inline void Node::set_header_capacity(size_t value, char* header) noexcept
{
    REALM_ASSERT_3(value, <=, max_array_payload);
    typedef unsigned char uchar;
    uchar* h = reinterpret_cast<uchar*>(header);
    h[0] = uchar((value >> 16) & 0x000000FF);
    h[1] = uchar((value >> 8) & 0x000000FF);
    h[2] = uchar(value & 0x000000FF);
}

inline size_t Node::calc_byte_size(WidthType wtype, size_t size, uint_least8_t width) noexcept
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

inline void Node::init_header(char* header, bool is_inner_bptree_node, bool has_refs, bool context_flag,
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
}

#endif /* REALM_NODE_HPP */
