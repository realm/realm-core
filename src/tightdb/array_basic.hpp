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
#ifndef TIGHTDB_ARRAY_BASIC_HPP
#define TIGHTDB_ARRAY_BASIC_HPP

#include <tightdb/array.hpp>

namespace tightdb {

template<typename T> 
class ArrayBasic : public Array {
public:
    ArrayBasic(ArrayParent* parent=NULL, size_t pndx=0, Allocator& alloc=GetDefaultAllocator());
    ArrayBasic(size_t ref, ArrayParent* parent, size_t pndx, Allocator& alloc=GetDefaultAllocator());
    //ArrayBasic(Allocator& alloc);
    ~ArrayBasic();

    T Get(size_t ndx) const;
    void add(T value);
    void Set(size_t ndx, T value);
    void Insert(size_t ndx, T value);
    void Delete(size_t ndx);
    void Clear();

    size_t Find(T target, size_t start, size_t end) const;
    size_t find_first(T value, size_t start=0 , size_t end=-1) const;
    void find_all(Array& result, T value, size_t add_offset = 0, size_t start = 0, size_t end = -1);

    size_t count(T value, size_t start=0, size_t end=-1) const;
    double sum(size_t start=0, size_t end=-1) const;
    bool maximum(T& result, size_t start=0, size_t end=-1) const;
    bool minimum(T& result, size_t start=0, size_t end=-1) const;

    /// Compare two arrays for equality.
    bool Compare(const ArrayBasic<T>&) const;

    static size_t create_empty_basic_array(Allocator& alloc);

private:
    virtual size_t CalcByteLen(size_t count, size_t width) const;
    virtual size_t CalcItemCount(size_t bytes, size_t width) const;
    virtual WidthType GetWidthType() const {return TDB_MULTIPLY;}

    template <bool find_max> bool minmax(T& result, size_t start, size_t end) const;

};


#include <tightdb/array_basic_tpl.hpp>

#endif TIGHTDB_ARRAY_BASIC_HPP
