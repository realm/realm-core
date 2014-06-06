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

#include <tightdb/util/type_traits.hpp>
#include <tightdb/mixed.hpp>
#include <tightdb/table_ref.hpp>
#include <tightdb/link_view_fwd.hpp>

namespace tightdb {

template<class> class BasicRow;


/// This class is a "mixin" and contains the common set of functions for several
/// distinct row-like classes.
///
/// There is a direct and natural correspondance between the functions in this
/// class and functions in Table of the same name. For example:
///
///     table[i].get_int(j) == table.get_int(i,j)
///
/// \tparam T A const or non-const table type (currently either `Table` or
/// `const Table`).
///
/// \tparam R A specific row accessor class (BasicRow or BasicRowExpr) providing
/// members `T& impl_get_table() const` and `std::size_t impl_get_row_ndx()
/// const`. Neither are allowed to throw.
///
/// \sa Table
/// \sa BasicRow
template<class T, class R> class RowFuncs {
public:
    typedef BasicTableRef<T> TableRef; // Same as ConstTableRef if `T` is 'const'
    typedef BasicTableRef<const T> ConstTableRef;

    typedef typename util::CopyConst<T, LinkView>::type L;
    typedef util::bind_ptr<L> LinkViewRef; // // Same as ConstLinkViewRef if `T` is 'const'
    typedef util::bind_ptr<const L> ConstLinkViewRef;

    int_fast64_t get_int(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    bool get_bool(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    float get_float(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    double get_double(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    StringData get_string(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    BinaryData get_binary(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    DateTime get_datetime(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    TableRef get_subtable(std::size_t col_ndx);
    ConstTableRef get_subtable(std::size_t col_ndx) const;
    std::size_t get_subtable_size(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    std::size_t get_link(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    bool is_null_link(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    LinkViewRef get_linklist(std::size_t col_ndx);
    ConstLinkViewRef get_linklist(std::size_t col_ndx) const;
    bool linklist_is_empty(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    std::size_t get_link_count(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
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
    void set_link(std::size_t col_ndx, std::size_t value);
    void nullify_link(std::size_t col_ndx);
    void set_mixed(std::size_t col_ndx, Mixed value);
    void set_mixed_subtable(std::size_t col_ndx, const Table* value);

    std::size_t get_backlink_count(const Table& src_table,
                                   std::size_t src_col_ndx) const TIGHTDB_NOEXCEPT;
    std::size_t get_backlink(const Table& src_table, std::size_t src_col_ndx,
                             std::size_t backlink_ndx) const TIGHTDB_NOEXCEPT;

    std::size_t get_column_count() const TIGHTDB_NOEXCEPT;
    DataType get_column_type(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    StringData get_column_name(std::size_t col_ndx) const TIGHTDB_NOEXCEPT;
    std::size_t get_column_index(StringData name) const TIGHTDB_NOEXCEPT;

private:
    T& get_table() TIGHTDB_NOEXCEPT;
    const T& get_table() const TIGHTDB_NOEXCEPT;
    std::size_t get_row_ndx() const TIGHTDB_NOEXCEPT;
};


/// This class is a special kind of row accessor. It differes from a real row
/// accessor (BasicRow) by having a trivial and fast copy constructor and
/// descructor. It is supposed to be used as the return type of functions such
/// as Table::operator[](), and then to be used as a basis for constructing a
/// real row accessor. Objects of this class are intended to only ever exist as
/// temporaries.
///
/// In contrast to a real row accessor, objects of this class do not keep the
/// parent table "alive", nor are they maintained (adjusted) across row
/// insertions and row removals like real row accessors are.
///
/// \sa BasicRow
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


/// An accessor class for table rows (a.k.a. a "row accessor").
///
/// For as long as it remains attached, a row accessor will keep the parent
/// table accessor alive. In case the lifetime of the parent table is not
/// managed by reference counting (such as when the table is an automatic
/// variable on the stack), the destruction of the table will cause all
/// remaining row accessors to be detached.
///
/// While attached, a row accessor is bound to a particular row of the parent
/// table. If that row is removed, the accesssor becomes detached. If rows are
/// inserted or removed before it (at lower row index), then the accessor is
/// automatically adjusted to account for the change in index of the row to
/// which the accessor is bound. In other words, a row accessor is bound to the
/// contents of a row, not to a row index. See also is_attached().
///
/// Row accessors are created and used as follows:
///
///     Row row       = table[7];  // 8th row of `table`
///     ConstRow crow = ctable[2]; // 3rd row of const `ctable`
///     Row first_row = table.front();
///     Row last_row  = table.back();
///
///     float v = row.get_float(1); // Get the float in the 2nd column
///     row.set_string(0, "foo");   // Update the string in the 1st column
///
///     Table* t = row.get_table();      // The parent table
///     std::size_t i = row.get_index(); // The current row index
///
/// \sa RowFuncs
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

    /// Returns true if, and only if this accessor is currently attached to a
    /// row.
    ///
    /// A row accesor may get detached from the underlying row for various
    /// reasons (see below). When it does, it no longer refers to anything, and
    /// can no longer be used, except for calling is_attached(). The
    /// consequences of calling other methods on a detached row accessor are
    /// undefined. Row accessors obtained by calling functions in the TightDB
    /// API are always in the 'attached' state immediately upon return from
    /// those functions.
    ///
    /// A row accessor becomes detached if the underlying row is removed, if the
    /// associated table accessor becomes detached, or if the detach() method is
    /// called. A row accessor does not become detached for any other reason.
    bool is_attached() const TIGHTDB_NOEXCEPT;

    /// Detach this accessor from whatever it was attached to. This function has
    /// no effect if the accessor was already detached (idempotency).
    void detach() TIGHTDB_NOEXCEPT;

    //// The table cotaining the row to which this accessor is currently
    /// bound. For a detached accessor, the returned value is null.
    table_type* get_table() const TIGHTDB_NOEXCEPT;

    /// The index of the row to which this accessor is currently bound. For a
    /// detached accessor, the returned value is unspecified.
    std::size_t get_index() const TIGHTDB_NOEXCEPT;

private:
    T& impl_get_table() const TIGHTDB_NOEXCEPT;
    std::size_t impl_get_row_ndx() const TIGHTDB_NOEXCEPT;

    // Make impl_get_table() and impl_get_row_ndx() accessible from
    // RowFuncs<T>::get_table() and RowFuncs<T>::get_row_ndx().
    friend class RowFuncs<T, BasicRow<T> >;

    // Make m_table and m_col_ndx accessible from BasicRow(const BasicRow<U>&)
    // for any U.
    template<class> friend class BasicRow;
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
inline typename RowFuncs<T,R>::TableRef RowFuncs<T,R>::get_subtable(std::size_t col_ndx)
{
    return get_table().get_subtable(col_ndx, get_row_ndx()); // Throws
}

template<class T, class R>
inline typename RowFuncs<T,R>::ConstTableRef RowFuncs<T,R>::get_subtable(std::size_t col_ndx) const
{
    return get_table().get_subtable(col_ndx, get_row_ndx()); // Throws
}

template<class T, class R>
inline std::size_t RowFuncs<T,R>::get_subtable_size(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_subtable_size(col_ndx, get_row_ndx());
}

template<class T, class R>
inline std::size_t RowFuncs<T,R>::get_link(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_link(col_ndx, get_row_ndx());
}

template<class T, class R>
inline bool RowFuncs<T,R>::is_null_link(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().is_null_link(col_ndx, get_row_ndx());
}

template<class T, class R>
inline typename RowFuncs<T,R>::LinkViewRef RowFuncs<T,R>::get_linklist(std::size_t col_ndx)
{
    return get_table().get_linklist(col_ndx, get_row_ndx()); // Throws
}

template<class T, class R> inline typename RowFuncs<T,R>::ConstLinkViewRef
RowFuncs<T,R>::get_linklist(std::size_t col_ndx) const
{
    return get_table().get_linklist(col_ndx, get_row_ndx()); // Throws
}

template<class T, class R>
inline bool RowFuncs<T,R>::linklist_is_empty(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().linklist_is_empty(col_ndx, get_row_ndx());
}

template<class T, class R>
inline std::size_t RowFuncs<T,R>::get_link_count(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_link_count(col_ndx, get_row_ndx());
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
inline void RowFuncs<T,R>::set_link(std::size_t col_ndx, std::size_t value)
{
    get_table().set_link(col_ndx, get_row_ndx(), value); // Throws
}

template<class T, class R>
inline void RowFuncs<T,R>::nullify_link(std::size_t col_ndx)
{
    get_table().nullify_link(col_ndx, get_row_ndx()); // Throws
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

template<class T, class R> inline std::size_t
RowFuncs<T,R>::get_backlink_count(const Table& src_table, std::size_t src_col_ndx) const
    TIGHTDB_NOEXCEPT
{
    return get_table().get_backlink_count(get_row_ndx(), src_table, src_col_ndx);
}

template<class T, class R>
inline std::size_t RowFuncs<T,R>::get_backlink(const Table& src_table, std::size_t src_col_ndx,
                                               std::size_t backlink_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_backlink(get_row_ndx(), src_table, src_col_ndx, backlink_ndx);
}

template<class T, class R>
inline std::size_t RowFuncs<T,R>::get_column_count() const TIGHTDB_NOEXCEPT
{
    return get_table().get_column_count();
}

template<class T, class R>
inline DataType RowFuncs<T,R>::get_column_type(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_column_type(col_ndx);
}

template<class T, class R>
inline StringData RowFuncs<T,R>::get_column_name(std::size_t col_ndx) const TIGHTDB_NOEXCEPT
{
    return get_table().get_column_name(col_ndx);
}

template<class T, class R>
inline std::size_t RowFuncs<T,R>::get_column_index(StringData name) const TIGHTDB_NOEXCEPT
{
    return get_table().get_column_index(name);
}

template<class T, class R> inline T& RowFuncs<T,R>::get_table() TIGHTDB_NOEXCEPT
{
    return static_cast<const R*>(this)->impl_get_table();
}

template<class T, class R> inline const T& RowFuncs<T,R>::get_table() const TIGHTDB_NOEXCEPT
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
    T* table = expr.m_table; // Check that pointer types are compatible
    attach(const_cast<Table*>(table), expr.m_row_ndx); // Throws
}

template<class T> template<class U> inline BasicRow<T>::BasicRow(const BasicRow<U>& row)
{
    T* table = row.m_table.get(); // Check that pointer types are compatible
    attach(const_cast<Table*>(table), row.m_row_ndx); // Throws
}

template<class T> template<class U>
inline BasicRow<T>& BasicRow<T>::operator=(BasicRowExpr<U> expr)
{
    T* table = expr.m_table; // Check that pointer types are compatible
    reattach(const_cast<Table*>(table), expr.m_row_ndx); // Throws
    return *this;
}

template<class T> template<class U>
inline BasicRow<T>& BasicRow<T>::operator=(BasicRow<U> row)
{
    T* table = row.m_table.get(); // Check that pointer types are compatible
    reattach(const_cast<Table*>(table), row.m_row_ndx); // Throws
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

template<class T> inline T* BasicRow<T>::get_table() const TIGHTDB_NOEXCEPT
{
    return &*m_table;
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
