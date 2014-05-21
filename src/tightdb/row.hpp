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
 **************************************************************************/
#ifndef TIGHTDB_ROW_HPP
#define TIGHTDB_ROW_HPP

#include <stdint.h>

#include <tightdb/mixed.hpp>
#include <tightdb/table_ref.hpp>

namespace tightdb {

template<class> class BasicRow;


template<class T, class R> class RowFuncs {
public:
    int_fast64_t get_int(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    bool get_bool(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    float get_float(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    double get_double(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    StringData get_string(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    BinaryData get_binary(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    DateTime get_datetime(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    BasicTableRef<T> get_subtable(std::size_t col_ndx);
    BasicTableRef<const T> get_subtable(std::size_t col_ndx) const;
    std::size_t get_subtable_size(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    Mixed get_mixed(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    DataType get_mixed_type(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;

    void set_int(std::size_t col_ndx, int_fast64_t value);
    void set_bool(std::size_t col_ndx, bool value);
    void set_float(std::size_t col_ndx, float value);
    void set_double(std::size_t col_ndx, double value);
    void set_string(std::size_t col_ndx, StringData value);
    void set_binary(std::size_t col_ndx, BinaryData value);
    void set_datetime(std::size_t col_ndx, DateTime value);
    void set_subtable(std::size_t col_ndx, const Table* value);
    void set_mixed(std::size_t col_ndx, Mixed value);
    void set_mixed_subtable(std::size_t col_ndx, const Table* value);

private:
    typedef T table_type;

    table_type& get_table() const TIGHTDB_NOEXCEPT;
    std::size_t get_row_ndx() const TIGHTDB_NOEXCEPT;
};


template<class T> class BasicRowExpr:
        public RowFuncs<T, BasicRowExpr<T> > {
public:
    typedef T table_type;

    template<class U> BasicRowExpr(const BasicRowExpr<U>&) TIGHTDB_NOEXCEPT;

private:
    T* m_table;
    std::size_t m_row_ndx;

    BasicRowExpr(T*, std::size_t row_ndx) TIGHTDB_NOEXCEPT;

    T& impl_get_table() const TIGHTDB_NOEXCEPT;
    std::size_t impl_get_row_ndx() const TIGHTDB_NOEXCEPT;

    // Make impl_get_table() and impl_get_row_ndx() accessible from
    // RowFuncs<T>::get_table() and RowFuncs<T>::get_row_ndx().
    friend class RowFuncs<T, BasicRowExpr<T> >;

    // Make m_table and m_col_ndx accessible from BasicRowExpr(const
    // BasicRowExpr<U>&) for any U.
    template<class> friend class BasicRowExpr;

    // Make m_table and m_col_ndx accessible from
    // BasicRow::BaicRow(BasicRowExpr<U>) for any U.
    template<class> friend class BasicRow;

    // Make BasicRowExpr(T*, std::size_t) accessible from Table.
    friend class Table;
};


class RowBase {
protected:
    TableRef m_table; // Null if detached.
    std::size_t m_row_ndx; // Undefined if detached.

    void attach(Table*, std::size_t row_ndx);
    void reattach(Table*, std::size_t row_ndx);
    void detach() TIGHTDB_NOEXCEPT;

    // Table needs to be able to call detach(), and to modify m_row_ndx.
    friend class Table;
};


template<class T> class BasicRow:
        private RowBase,
        public RowFuncs<T, BasicRow<T> > {
public:
    typedef T table_type;

    BasicRow() TIGHTDB_NOEXCEPT;

    template<class U> BasicRow(BasicRowExpr<U>);
    template<class U> BasicRow(const BasicRow<U>&);
    template<class U> BasicRow& operator=(BasicRowExpr<U>);
    template<class U> BasicRow& operator=(BasicRow<U>);

    ~BasicRow() TIGHTDB_NOEXCEPT;

    bool is_attached() const TIGHTDB_NOEXCEPT;

    void detach() TIGHTDB_NOEXCEPT;

    table_type& get_table() const TIGHTDB_NOEXCEPT;

    std::size_t get_index() const TIGHTDB_NOEXCEPT;

private:
    T& impl_get_table() const TIGHTDB_NOEXCEPT;
    std::size_t impl_get_row_ndx() const TIGHTDB_NOEXCEPT;

    // Make impl_get_table() and impl_get_row_ndx() accessible from
    // RowFuncs<T>::get_table() and RowFuncs<T>::get_row_ndx().
    friend class RowFuncs<T, BasicRow<T> >;
};

typedef BasicRow<Table> Row;
typedef BasicRow<const Table> ConstRow;




// Implementation

template<class T, class R>
inline int_fast64_t RowFuncs<T,R>::get_int(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_int(col_ndx, get_row_ndx());
}

template<class T, class R>
inline bool RowFuncs<T,R>::get_bool(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_bool(col_ndx, get_row_ndx());
}

template<class T, class R>
inline float RowFuncs<T,R>::get_float(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_float(col_ndx, get_row_ndx());
}

template<class T, class R>
inline double RowFuncs<T,R>::get_double(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_double(col_ndx, get_row_ndx());
}

template<class T, class R>
inline StringData RowFuncs<T,R>::get_string(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_string(col_ndx, get_row_ndx());
}

template<class T, class R>
inline BinaryData RowFuncs<T,R>::get_binary(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_binary(col_ndx, get_row_ndx());
}

template<class T, class R>
inline DateTime RowFuncs<T,R>::get_datetime(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_datetime(col_ndx, get_row_ndx());
}

template<class T, class R>
inline BasicTableRef<T> RowFuncs<T,R>::get_subtable(std::size_t col_ndx)
{
    return get_table().get_subtable(col_ndx, get_row_ndx()); // Throws
}

template<class T, class R>
inline BasicTableRef<const T> RowFuncs<T,R>::get_subtable(std::size_t col_ndx) const
{
    return get_table().get_subtable(col_ndx, get_row_ndx()); // Throws
}

template<class T, class R>
inline std::size_t RowFuncs<T,R>::get_subtable_size(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_subtable_size(col_ndx, get_row_ndx()); // Throws
}

template<class T, class R>
inline Mixed RowFuncs<T,R>::get_mixed(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_mixed(col_ndx, get_row_ndx());
}

template<class T, class R>
inline DataType RowFuncs<T,R>::get_mixed_type(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_mixed_type(col_ndx, get_row_ndx());
}

template<class T, class R>
inline void RowFuncs<T,R>::set_int(std::size_t col_ndx, int_fast64_t value)
{
    get_table().set_int(col_ndx, get_row_ndx(), value); // Throws
}

template<class T, class R>
inline void RowFuncs<T,R>::set_bool(std::size_t col_ndx, bool value)
{
    get_table().set_bool(col_ndx, get_row_ndx(), value); // Throws
}

template<class T, class R>
inline void RowFuncs<T,R>::set_float(std::size_t col_ndx, float value)
{
    get_table().set_float(col_ndx, get_row_ndx(), value); // Throws
}

template<class T, class R>
inline void RowFuncs<T,R>::set_double(std::size_t col_ndx, double value)
{
    get_table().set_double(col_ndx, get_row_ndx(), value); // Throws
}

template<class T, class R>
inline void RowFuncs<T,R>::set_string(std::size_t col_ndx, StringData value)
{
    get_table().set_string(col_ndx, get_row_ndx(), value); // Throws
}

template<class T, class R>
inline void RowFuncs<T,R>::set_binary(std::size_t col_ndx, BinaryData value)
{
    get_table().set_binary(col_ndx, get_row_ndx(), value); // Throws
}

template<class T, class R>
inline void RowFuncs<T,R>::set_datetime(std::size_t col_ndx, DateTime value)
{
    get_table().set_datetime(col_ndx, get_row_ndx(), value); // Throws
}

template<class T, class R>
inline void RowFuncs<T,R>::set_subtable(std::size_t col_ndx, const Table* value)
{
    get_table().set_subtable(col_ndx, get_row_ndx(), value); // Throws
}

template<class T, class R>
inline void RowFuncs<T,R>::set_mixed(std::size_t col_ndx, Mixed value)
{
    get_table().set_mixed(col_ndx, get_row_ndx(), value); // Throws
}

template<class T, class R>
inline void RowFuncs<T,R>::set_mixed_subtable(std::size_t col_ndx, const Table* value)
{
    get_table().set_mixed_subtable(col_ndx, get_row_ndx(), value); // Throws
}

template<class T, class R> inline T& RowFuncs<T,R>::get_table() const TIGHTDB_NOEXCEPT
{
    return static_cast<const R*>(this)->impl_get_table();
}

template<class T, class R> inline std::size_t RowFuncs<T,R>::get_row_ndx() const TIGHTDB_NOEXCEPT
{
    return static_cast<const R*>(this)->impl_get_row_ndx();
}


template<class T> template<class U>
inline BasicRowExpr<T>::BasicRowExpr(const BasicRowExpr<U>& expr) TIGHTDB_NOEXCEPT:
    m_table(expr.m_table),
    m_row_ndx(expr.m_row_ndx)
{
}

template<class T>
inline BasicRowExpr<T>::BasicRowExpr(T* table, std::size_t row_ndx) TIGHTDB_NOEXCEPT:
    m_table(table),
    m_row_ndx(row_ndx)
{
}

template<class T> inline T& BasicRowExpr<T>::impl_get_table() const TIGHTDB_NOEXCEPT
{
    return *m_table;
}

template<class T> inline std::size_t BasicRowExpr<T>::impl_get_row_ndx() const TIGHTDB_NOEXCEPT
{
    return m_row_ndx;
}


template<class T> inline BasicRow<T>::BasicRow() TIGHTDB_NOEXCEPT
{
}

template<class T> template<class U> inline BasicRow<T>::BasicRow(BasicRowExpr<U> expr)
{
    T& table = *expr.m_table;
    attach(const_cast<Table*>(&table), expr.m_row_ndx); // Throws
}

template<class T> template<class U> inline BasicRow<T>::BasicRow(const BasicRow<U>& row)
{
    T& table = *row.m_table;
    attach(const_cast<Table*>(&table), row.m_row_ndx); // Throws
}

template<class T> template<class U>
inline BasicRow<T>& BasicRow<T>::operator=(BasicRowExpr<U> expr)
{
    T& table = *expr.m_table;
    reattach(const_cast<Table*>(&table), expr.m_row_ndx); // Throws
    return *this;
}

template<class T> template<class U>
inline BasicRow<T>& BasicRow<T>::operator=(BasicRow<U> row)
{
    T& table = *row.m_table;
    reattach(const_cast<Table*>(&table), row.m_row_ndx); // Throws
    return *this;
}

template<class T> inline BasicRow<T>::~BasicRow() TIGHTDB_NOEXCEPT
{
    detach();
}

template<class T> inline bool BasicRow<T>::is_attached() const TIGHTDB_NOEXCEPT
{
    return bool(m_table);
}

template<class T> inline void BasicRow<T>::detach() TIGHTDB_NOEXCEPT
{
    RowBase::detach();
}

template<class T> inline T& BasicRow<T>::get_table() const TIGHTDB_NOEXCEPT
{
    return *m_table;
}

template<class T> inline std::size_t BasicRow<T>::get_index() const TIGHTDB_NOEXCEPT
{
    return m_row_ndx;
}

template<class T> inline T& BasicRow<T>::impl_get_table() const TIGHTDB_NOEXCEPT
{
    return *m_table;
}

template<class T> inline std::size_t BasicRow<T>::impl_get_row_ndx() const TIGHTDB_NOEXCEPT
{
    return m_row_ndx;
}

} // namespace tightdb

#endif // TIGHTDB_ROW_HPP
