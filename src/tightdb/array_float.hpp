/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
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
 **************************************************************************/
#ifndef TIGHTDB_ARRAY_FLOAT_HPP
#define TIGHTDB_ARRAY_FLOAT_HPP

#include <tightdb/array.hpp>

namespace tightdb {

class ArrayFloat : public Array {
public:
    ArrayFloat(ArrayParent* parent=NULL, size_t pndx=0, Allocator& alloc=GetDefaultAllocator());
    ArrayFloat(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc=GetDefaultAllocator());
    ArrayFloat(Allocator& alloc);
    ~ArrayFloat();

    float Get(size_t ndx) const;
    void add(float value);
    void Set(size_t ndx, float value);
    void Insert(size_t ndx, float value);
    void Delete(size_t ndx);

    static size_t create_empty_float_array(Allocator& alloc);

private:
    virtual size_t CalcByteLen(size_t count, size_t width) const;
    virtual size_t CalcItemCount(size_t bytes, size_t width) const;
    virtual WidthType GetWidthType() const {return TDB_MULTIPLY;}
};


// Implementation:

inline size_t ArrayFloat::create_empty_float_array(Allocator& alloc) 
{
    const size_t capacity = Array::initial_capacity;
    const MemRef mem_ref = alloc.Alloc(capacity);
    if (!mem_ref.pointer) 
        return 0;

    init_header(mem_ref.pointer, false, false, TDB_MULTIPLY, sizeof(float), 0, capacity);

    return mem_ref.ref;
}

inline ArrayFloat::ArrayFloat(ArrayParent *parent, size_t ndx_in_parent, Allocator& alloc)
                        :Array(alloc)
{
    const size_t ref = create_empty_float_array(alloc);
    if (!ref) 
        throw_error(ERROR_OUT_OF_MEMORY); // FIXME: Check that this exception is handled properly in callers
    init_from_ref(ref);
    SetParent(parent, ndx_in_parent);
    update_ref_in_parent();
}


inline ArrayFloat::ArrayFloat(size_t ref, ArrayParent *parent, size_t ndx_in_parent,
                              Allocator& alloc): Array(alloc)
{
    // Manually create array as doing it in initializer list
    // will not be able to call correct virtual functions
    init_from_ref(ref);
    SetParent(const_cast<ArrayParent *>(parent), ndx_in_parent);
}

// Creates new array (but invalid, call UpdateRef to init)
inline ArrayFloat::ArrayFloat(Allocator& alloc): Array(alloc) 
{
}

inline ArrayFloat::~ArrayFloat() 
{
}


inline void ArrayFloat::add(float value)
{
    Insert(m_len, value);
}


} // namespace tightdb

#endif // TIGHTDB_ARRAY_FLOAT_HPP
