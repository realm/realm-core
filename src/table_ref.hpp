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
#ifndef __TIGHTDB_TABLE_REF_H
#define __TIGHTDB_TABLE_REF_H

#include <cstddef>
#include <algorithm>
#include <ostream>

namespace tightdb {


/**
 * A "smart" reference to a table.
 *
 * This kind of table reference is often needed when working with
 * subtables. For example:
 *
 * \code{.cpp}
 *
 *   void func(Table& table)
 *   {
 *     Table& sub1 = *table.get_subtable(0,0); // INVALID! (sub1 becomes 'dangeling')
 *     TableRef sub2 = table.get_subtable(0,0); // Safe!
 *   }
 *
 * \endcode
 *
 * \note When a top-level table is destroyed, all "smart" table
 * references obtained from it, or from any of its subtables, are
 * invalidated.
 */
template<class T> class BasicTableRef {
public:
    /**
     * Construct a null reference.
     */
    BasicTableRef(): m_table(0) {}

    /**
     * Copy a reference.
     */
    BasicTableRef(const BasicTableRef& r) { bind(r.m_table); }

    /**
     * Copy a reference from a pointer compatible table type.
     */
    template<class U> BasicTableRef(const BasicTableRef<U>& r) { bind(r.m_table); }

    ~BasicTableRef() { unbind(); }

    /**
     * Copy a reference.
     */
    BasicTableRef& operator=(const BasicTableRef& r) { reset(r.m_table); return *this; }

    /**
     * Copy a reference from a pointer compatible table type.
     */
    template<class U> BasicTableRef& operator=(const BasicTableRef<U>& r);

    /**
     * Allow comparison between related reference types.
     */
    template<class U> bool operator==(const BasicTableRef<U>&) const;
    template<class U> bool operator!=(const BasicTableRef<U>&) const;
    template<class U> bool operator<(const BasicTableRef<U>&) const;

    /**
     * Dereference this table reference.
     */
    T& operator*() const { return *m_table; }

    /**
     * Dereference this table reference for method invocation.
     */
    T* operator->() const { return m_table; }

    /**
     * Efficient swapping that avoids binding and unbinding.
     */
    void swap(BasicTableRef& r) { using std::swap; swap(m_table, r.m_table); }

private:
    typedef T* BasicTableRef::*unspecified_bool_type;
    typedef typename T::template Accessors<T>::Row RowAccessor;

public:
    /**
     * Test if this is a proper reference (ie. not a null reference.)
     *
     * \return True if, and only if this is a proper reference.
     */
    operator unspecified_bool_type() const;

    RowAccessor operator[](std::size_t i) const { return (*m_table)[i]; }

private:
    friend class ColumnSubtableParent;
    friend class Table;
    template<class> friend class BasicTable;
    template<class> friend class BasicTableRef;

    template<class U, class V> friend
    BasicTableRef<U> static_table_cast(const BasicTableRef<V>&);
    template<class U, class V> friend
    BasicTableRef<U> dynamic_table_cast(const BasicTableRef<V>&);
    template<class C, class U, class V> friend
    std::basic_ostream<C,U>& operator<<(std::basic_ostream<C,U>&, const BasicTableRef<V>&);

    T* m_table;

    explicit BasicTableRef(T* t) { bind(t); }

    void reset(T* = 0);
    void bind(T*);
    void unbind();
};


/**
 * Efficient swapping that avoids access to the referenced object,
 * in particular, its reference count.
 */
template<class T> inline void swap(BasicTableRef<T>&, BasicTableRef<T>&);

template<class T, class U> BasicTableRef<T> static_table_cast(const BasicTableRef<U>&);

template<class T, class U> BasicTableRef<T> dynamic_table_cast(const BasicTableRef<U>&);

template<class C, class T, class U>
std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>&, const BasicTableRef<U>&);


class Table;
typedef BasicTableRef<Table> TableRef;
typedef BasicTableRef<const Table> ConstTableRef;




// Implementation:

template<class T> template<class U>
inline BasicTableRef<T>& BasicTableRef<T>::operator=(const BasicTableRef<U>& r)
{
    reset(r.m_table);
    return *this;
}

template<class T> template<class U>
inline bool BasicTableRef<T>::operator==(const BasicTableRef<U>& r) const
{
    return m_table == r.m_table;
}

template<class T> template<class U>
inline bool BasicTableRef<T>::operator!=(const BasicTableRef<U>& r) const
{
    return m_table != r.m_table;
}

template<class T> template<class U>
inline bool BasicTableRef<T>::operator<(const BasicTableRef<U>& r) const
{
    return m_table < r.m_table;
}

template<class T>
inline BasicTableRef<T>::operator unspecified_bool_type() const
{
    return m_table ? &BasicTableRef::m_table : 0;
}

template<class T> inline void BasicTableRef<T>::reset(T* t)
{
    if(t == m_table) return;
    unbind();
    bind(t);
}

template<class T> inline void BasicTableRef<T>::bind(T* t)
{
    if (t) t->bind_ref();
    m_table = t;
}

template<class T> inline void BasicTableRef<T>::unbind()
{
    if (m_table) m_table->unbind_ref();
}

template<class T> inline void swap(BasicTableRef<T>& r, BasicTableRef<T>& s)
{
    r.swap(s);
}

template<class T, class U> BasicTableRef<T> static_table_cast(const BasicTableRef<U>& t)
{
    return BasicTableRef<T>(static_cast<T*>(t.m_table));
}

template<class T, class U> BasicTableRef<T> dynamic_table_cast(const BasicTableRef<U>& t)
{
    return BasicTableRef<T>(dynamic_cast<T*>(t.m_table));
}

template<class C, class T, class U>
std::basic_ostream<C,T>& operator<<(std::basic_ostream<C,T>& out, const BasicTableRef<U>& t)
{
    out << static_cast<void*>(t.m_table);
    return out;
}

}

#endif // __TIGHTDB_TABLE_REF_H
