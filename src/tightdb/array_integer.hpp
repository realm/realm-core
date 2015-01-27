/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2014] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
***************************************************************************/

#ifndef TIGHTDB_ARRAY_INTEGER_HPP
#define TIGHTDB_ARRAY_INTEGER_HPP

#include <tightdb/array.hpp>
#include <tightdb/util/safe_int_ops.hpp>

namespace tightdb {

class ArrayInteger: public Array {
public:
    typedef int64_t value_type;

    explicit ArrayInteger(no_prealloc_tag) TIGHTDB_NOEXCEPT;
    explicit ArrayInteger(Allocator&) TIGHTDB_NOEXCEPT;
    ~ArrayInteger() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE {}

    /// Construct an array of the specified type and size, and return just the
    /// reference to the underlying memory. All elements will be initialized to
    /// the specified value.
    static MemRef create_array(Type, bool context_flag, std::size_t size, int_fast64_t value,
                               Allocator&);

    void add(int64_t value);
    void set(std::size_t ndx, int64_t value);
    void set_uint(std::size_t ndx, uint64_t value);
    int64_t get(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    uint64_t get_uint(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    static int64_t get(const char* header, std::size_t ndx) TIGHTDB_NOEXCEPT;

    /// Add \a diff to the element at the specified index.
    void adjust(std::size_t ndx, int_fast64_t diff);

    /// Add \a diff to all the elements in the specified index range.
    void adjust(std::size_t begin, std::size_t end, int_fast64_t diff);

    /// Add signed \a diff to all elements that are greater than, or equal to \a
    /// limit.
    void adjust_ge(int_fast64_t limit, int_fast64_t diff);

    int64_t operator[](std::size_t ndx) const TIGHTDB_NOEXCEPT { return get(ndx); }
    int64_t front() const TIGHTDB_NOEXCEPT;
    int64_t back() const TIGHTDB_NOEXCEPT;

    std::size_t lower_bound(int64_t value) const TIGHTDB_NOEXCEPT;
    std::size_t upper_bound(int64_t value) const TIGHTDB_NOEXCEPT;

    void sort();

    std::vector<int64_t> ToVector() const;
};


class ArrayIntNull : public Array {
public:
    // FIXME: Express NULL in the value type.
    typedef int64_t value_type;

    explicit ArrayIntNull(no_prealloc_tag) TIGHTDB_NOEXCEPT;
    explicit ArrayIntNull(Allocator&) TIGHTDB_NOEXCEPT;

    /// Construct an array of the specified type and size, and return just the
    /// reference to the underlying memory. All elements will be initialized to
    /// the specified value.
    /// FIXME: Support initial NULL values.
    static MemRef create_array(Type, bool context_flag, std::size_t size, int_fast64_t value,
                               Allocator&);

    void add(int64_t);
    void set(std::size_t ndx, int64_t value);
    void set_uint(std::size_t ndx, uint64_t value);
    void set_null(std::size_t ndx); // Or nullify()?
    int64_t get(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    uint64_t get_uint(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    bool is_null(std::size_t ndx) const TIGHTDB_NOEXCEPT;

    static int64_t get(const char* header, std::size_t ndx) TIGHTDB_NOEXCEPT;
    static bool is_null(const char* header, std::size_t ndx) TIGHTDB_NOEXCEPT;

    void adjust(std::size_t ndx, int_fast64_t diff);
    void adjust(std::size_t begin, std::size_t end, int_fast64_t diff);
    void adjust_ge(int_fast64_t limit, int_fast64_t diff);

    // FIXME: Expression NULL in the value types.
    int64_t operator[](std::size_t ndx) const TIGHTDB_NOEXCEPT { return get(ndx); }
    int64_t front() const TIGHTDB_NOEXCEPT;
    int64_t back() const TIGHTDB_NOEXCEPT;

    std::size_t lower_bound(int64_t value) const TIGHTDB_NOEXCEPT;
    std::size_t upper_bound(int64_t value) const TIGHTDB_NOEXCEPT;

    /// Sorts the array, NULLs unspecified.
    /// FIXME: Provide option for NULLs first or last.
    void sort();

    // FIXME: Express NULL in the value type.
    std::vector<int64_t> ToVector() const;

    int64_t null_value() const TIGHTDB_NOEXCEPT;
private:
    // m_null indicates the "magic" value for NULL when the width is 64.
    // When width is <64, NULL is expressed with m_ubound.
    int64_t m_null;

    void ensure_non_null(int64_t value);
    void replace_nulls_with(int64_t new_null); // m_null must be set, even if the width is less than 64
    bool can_use_as_null(int64_t candidate) const TIGHTDB_NOEXCEPT; // ditto
};


// Implementation:

inline ArrayInteger::ArrayInteger(Array::no_prealloc_tag) TIGHTDB_NOEXCEPT:
    Array(Array::no_prealloc_tag())
{
}

inline ArrayInteger::ArrayInteger(Allocator& alloc) TIGHTDB_NOEXCEPT:
    Array(alloc)
{
}

inline MemRef ArrayInteger::create_array(Type type, bool context_flag, std::size_t size,
                                  int_fast64_t value, Allocator& alloc)
{
    return Array::create(type, context_flag, wtype_Bits, size, value, alloc); // Throws
}

inline void ArrayInteger::add(int64_t value)
{
    add_data(value);
}

inline int64_t ArrayInteger::get(size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get_data(ndx);
}

inline uint64_t ArrayInteger::get_uint(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get(ndx);
}

inline int64_t ArrayInteger::get(const char* header, size_t ndx) TIGHTDB_NOEXCEPT
{
    return Array::get_data(header, ndx);
}

inline void ArrayInteger::set(size_t ndx, int64_t value)
{
    set_data(ndx, value);
}

inline void ArrayInteger::set_uint(std::size_t ndx, uint_fast64_t value)
{
    // When a value of a signed type is converted to an unsigned type, the C++
    // standard guarantees that negative values are converted from the native
    // representation to 2's complement, but the effect of conversions in the
    // opposite direction is left unspecified by the
    // standard. `tightdb::util::from_twos_compl()` is used here to perform the
    // correct opposite unsigned-to-signed conversion, which reduces to a no-op
    // when 2's complement is the native representation of negative values.
    set(ndx, util::from_twos_compl<int_fast64_t>(value));
}


inline int64_t ArrayInteger::front() const TIGHTDB_NOEXCEPT
{
    return front_data();
}

inline int64_t ArrayInteger::back() const TIGHTDB_NOEXCEPT
{
    return back_data();
}

inline void ArrayInteger::adjust(std::size_t ndx, int_fast64_t diff)
{
    adjust_data(ndx, diff);
}

inline void ArrayInteger::adjust(std::size_t begin, std::size_t end, int_fast64_t diff)
{
    adjust_data(begin, end, diff);
}

inline void ArrayInteger::adjust_ge(int_fast64_t limit, int_fast64_t diff)
{
    adjust_data_ge(limit, diff);
}

inline std::size_t ArrayInteger::lower_bound(int64_t value) const TIGHTDB_NOEXCEPT
{
    return lower_bound_data(value);
}

inline std::size_t ArrayInteger::upper_bound(int64_t value) const TIGHTDB_NOEXCEPT
{
    return upper_bound_data(value);
}

inline void ArrayInteger::sort()
{
    sort_data();
}


inline ArrayIntNull::ArrayIntNull(Array::no_prealloc_tag) TIGHTDB_NOEXCEPT:
    Array(Array::no_prealloc_tag())
{
}

inline ArrayIntNull::ArrayIntNull(Allocator& alloc) TIGHTDB_NOEXCEPT:
    Array(alloc)
{
}



inline MemRef ArrayIntNull::create_array(Type type, bool context_flag, std::size_t size,
                                  int_fast64_t value, Allocator& alloc)
{
    return Array::create(type, context_flag, wtype_Bits, size, value, alloc); // Throws
}

inline void ArrayIntNull::add(int64_t value)
{
    ensure_non_null(value);
    add_data(value);
}

inline int64_t ArrayIntNull::get(size_t ndx) const TIGHTDB_NOEXCEPT
{
    int64_t v = get_data(ndx);
    if (v == null_value()) {
        return 0;
    }
    return v;
}

inline uint64_t ArrayIntNull::get_uint(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get(ndx);
}

inline bool ArrayIntNull::is_null(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    return get_data(ndx) == null_value();
}

inline int64_t ArrayIntNull::get(const char* header, size_t ndx) TIGHTDB_NOEXCEPT
{
    return Array::get_data(header, ndx);
}

inline void ArrayIntNull::set(size_t ndx, int64_t value)
{
    ensure_non_null(value);
    set_data(ndx, value);
}

inline void ArrayIntNull::set_uint(std::size_t ndx, uint_fast64_t value)
{
    // When a value of a signed type is converted to an unsigned type, the C++
    // standard guarantees that negative values are converted from the native
    // representation to 2's complement, but the effect of conversions in the
    // opposite direction is left unspecified by the
    // standard. `tightdb::util::from_twos_compl()` is used here to perform the
    // correct opposite unsigned-to-signed conversion, which reduces to a no-op
    // when 2's complement is the native representation of negative values.
    set(ndx, util::from_twos_compl<int_fast64_t>(value));
}

inline void ArrayIntNull::set_null(std::size_t ndx)
{
    set_data(ndx, null_value());
}


inline int64_t ArrayIntNull::front() const TIGHTDB_NOEXCEPT
{
    return front_data();
}

inline int64_t ArrayIntNull::back() const TIGHTDB_NOEXCEPT
{
    return back_data();
}

inline void ArrayIntNull::adjust(std::size_t ndx, int_fast64_t diff)
{
    adjust_data(ndx, diff);
}

inline void ArrayIntNull::adjust(std::size_t begin, std::size_t end, int_fast64_t diff)
{
    adjust_data(begin, end, diff);
}

inline void ArrayIntNull::adjust_ge(int_fast64_t limit, int_fast64_t diff)
{
    adjust_data_ge(limit, diff);
}

inline std::size_t ArrayIntNull::lower_bound(int64_t value) const TIGHTDB_NOEXCEPT
{
    return lower_bound_data(value);
}

inline std::size_t ArrayIntNull::upper_bound(int64_t value) const TIGHTDB_NOEXCEPT
{
    return upper_bound_data(value);
}

inline int64_t ArrayIntNull::null_value() const TIGHTDB_NOEXCEPT
{
    if (m_width == 64) {
        return m_null;
    }
    else {
        return m_ubound;
    }
}

inline void ArrayIntNull::replace_nulls_with(int64_t new_null)
{
    for (size_t i = 0; i < size(); ++i) {
        int64_t v = get_data(i);
        TIGHTDB_ASSERT(v != new_null);
        if (v == m_null) {
            set_data(i, new_null);
        }
    }
    m_null = new_null;
}

inline bool ArrayIntNull::can_use_as_null(int64_t candidate) const TIGHTDB_NOEXCEPT
{
    if (m_null == candidate)
        return true;

    for (size_t i = 0; i < size(); ++i) {
        int64_t v = get_data(i);
        if (v != m_null && v == candidate) {
            return false;
        }
    }
    return true;
}


}

#endif // TIGHTDB_ARRAY_INTEGER_HPP
