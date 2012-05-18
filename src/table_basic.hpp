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
#ifndef TIGHTDB_BASIC_TABLE_HPP
#define TIGHTDB_BASIC_TABLE_HPP

#ifdef _MSC_VER
#include "win32/stdint.h"
#else
#include <stdint.h> // unint8_t etc
#endif

#include <cassert>
#include <cstddef>
#include <cstring> // strcmp()
#include <ctime>
#include <utility>

#include "static_assert.hpp"
#include "meta.hpp"
#include "tuple.hpp"
#include "table.hpp"
#include "column.hpp"
#include "query.hpp"
#include "table_accessors.hpp"
#include "table_view_basic.hpp"

namespace tightdb {


namespace _impl {
    template<class Type, int col_idx> struct AddCol;
    template<class Type, int col_idx> struct InsertIntoCol;
}



/**
 * This class is non-polymorphic, that is, it has no virtual
 * functions. Further more, it has no destructor, and it adds no new
 * data-members. These properties are important, because it ensures
 * that there is no run-time distinction between a Table instance and
 * an instance of any variation of this class, and therefore it is
 * valid to cast a pointer from Table to BasicTable<Spec> even when
 * the instance is constructed as a Table. Of couse, this also assumes
 * that Table is non-polymorphic. Further more, accessing the Table
 * via a poiter or reference to a BasicTable is not in violation of
 * the strict aliasing rule.
 */
template<class Spec> class BasicTable: private Table, public Spec::ConvenienceMethods {
public:
    typedef Spec spec_type;
    typedef typename Spec::Columns Columns;

    typedef BasicTableRef<BasicTable> Ref;
    typedef BasicTableRef<const BasicTable> ConstRef;

    typedef BasicTableView<BasicTable> View;
    typedef BasicTableView<const BasicTable> ConstView;

    using Table::is_empty;
    using Table::size;
    using Table::clear;
    using Table::remove;
    using Table::optimize;

    BasicTable(Allocator& alloc = GetDefaultAllocator()): Table(alloc)
    {
        tightdb::Spec& spec = get_spec();
        ForEachType<typename Spec::Columns, _impl::AddCol>::exec(&spec, Spec::dyn_col_names());
        update_from_spec();
    }

    static int get_column_count() { return TypeCount<typename Spec::Columns>::value; }

    BasicTableRef<BasicTable> get_table_ref()
    {
        return BasicTableRef<BasicTable>(this);
    }

    BasicTableRef<const BasicTable> get_table_ref() const
    {
        return BasicTableRef<const BasicTable>(this);
    }

private:
    template<int col_idx> struct Col {
        typedef typename TypeAt<typename Spec::Columns, col_idx>::type value_type;
        typedef _impl::ColumnAccessor<BasicTable, col_idx, value_type> type;
    };
    typedef typename Spec::template ColNames<Col, BasicTable*> ColsAccessor;

    template<int col_idx> struct ConstCol {
        typedef typename TypeAt<typename Spec::Columns, col_idx>::type value_type;
        typedef _impl::ColumnAccessor<const BasicTable, col_idx, value_type> type;
    };
    typedef typename Spec::template ColNames<ConstCol, const BasicTable*> ConstColsAccessor;

public:
    ColsAccessor cols() { return ColsAccessor(this); }
    ConstColsAccessor cols() const { return ConstColsAccessor(this); }

private:
    template<int col_idx> struct Field {
        typedef typename TypeAt<typename Spec::Columns, col_idx>::type value_type;
        typedef _impl::FieldAccessor<BasicTable, col_idx, value_type> type;
    };
    typedef std::pair<BasicTable*, std::size_t> FieldInit;
    typedef typename Spec::template ColNames<Field, FieldInit> RowAccessor;

    template<int col_idx> struct ConstField {
        typedef typename TypeAt<typename Spec::Columns, col_idx>::type value_type;
        typedef _impl::FieldAccessor<const BasicTable, col_idx, value_type> type;
    };
    typedef std::pair<const BasicTable*, std::size_t> ConstFieldInit;
    typedef typename Spec::template ColNames<ConstField, ConstFieldInit> ConstRowAccessor;

public:
    RowAccessor operator[](std::size_t row_idx)
    {
        return RowAccessor(std::make_pair(this, row_idx));
    }

    ConstRowAccessor operator[](std::size_t row_idx) const
    {
        return ConstRowAccessor(std::make_pair(this, row_idx));
    }

    RowAccessor front() { return RowAccessor(std::make_pair(this, 0)); }
    ConstRowAccessor front() const { return ConstRowAccessor(std::make_pair(this, 0)); }

    /**
     * Access the last row, or one of its predecessors.
     *
     * \param rel_idx An optional index of the row specified relative
     * to the end. Thus, <tt>table.back(rel_idx)</tt> is the same as
     * <tt>table[table.size() + rel_idx]</tt>.
     */
    RowAccessor back(int rel_idx = -1)
    {
        return RowAccessor(std::make_pair(this, m_size+rel_idx));
    }

    ConstRowAccessor back(int rel_idx = -1) const
    {
        return ConstRowAccessor(std::make_pair(this, m_size+rel_idx));
    }

    RowAccessor add() { return RowAccessor(std::make_pair(this, add_empty_row())); }

    // FIXME: Also insert() and set()
    template<class L> void add(const Tuple<L>& tuple)
    {
        TIGHTDB_STATIC_ASSERT(TypeCount<L>::value == TypeCount<Columns>::value);
        ForEachType<Columns, _impl::InsertIntoCol>::exec(this, size(), tuple);
        insert_done();
    }

    typedef RowAccessor Cursor; // FIXME: A cursor must be a distinct class that can be constructed from a RowAccessor
    typedef ConstRowAccessor ConstCursor;


    class Query;

    Query where() const { return Query(); } // FIXME: Bad thing to copy queries


    // FIXME: Get rid of all these!
    template<class T1>
    void add(const T1& v1)
    {
        Spec::insert(m_size, cols(), v1);
        insert_done();
    }

    template<class T1, class T2>
    void add(const T1& v1, const T2& v2)
    {
        Spec::insert(m_size, cols(), v1, v2);
        insert_done();
    }

    template<class T1, class T2, class T3>
    void add(const T1& v1, const T2& v2, const T3& v3)
    {
        Spec::insert(m_size, cols(), v1, v2, v3);
        insert_done();
    }

    template<class T1, class T2, class T3, class T4>
    void add(const T1& v1, const T2& v2, const T3& v3, const T4& v4)
    {
        Spec::insert(m_size, cols(), v1, v2, v3, v4);
        insert_done();
    }

    // FIXME: Add remaining add() methods up to 8 values.

    template<class T1>
    void insert(std::size_t i, const T1& v1)
    {
        Spec::insert(i, cols(), v1);
        insert_done();
    }

    template<class T1, class T2>
    void insert(std::size_t i, const T1& v1, const T2& v2)
    {
        Spec::insert(i, cols(), v1, v2);
        insert_done();
    }

    template<class T1, class T2, class T3>
    void insert(std::size_t i, const T1& v1, const T2& v2, const T3& v3)
    {
        Spec::insert(i, cols(), v1, v2, v3);
        insert_done();
    }

    template<class T1, class T2, class T3, class T4>
    void insert(std::size_t i, const T1& v1, const T2& v2, const T3& v3, const T4& v4)
    {
        Spec::insert(i, cols(), v1, v2, v3, v4);
        insert_done();
    }

    // FIXME: Add remaining insert() methods up to 8 values.

#ifdef _DEBUG
    using Table::verify;
    using Table::print;
    bool compare(const BasicTable& c) const { return Table::compare(c); }
#endif

private:
    template<class> friend class BasicTable;
    friend class BasicTableRef<BasicTable>;
    template<class, int, class> friend class _impl::FieldAccessor;
    template<class, int, class> friend class _impl::ColumnAccessorBase;
    template<class, int, class> friend class _impl::ColumnAccessor;
    friend class Group;

    template<int col_idx> struct QueryCol {
        typedef typename TypeAt<typename Spec::Columns, col_idx>::type value_type;
        typedef _impl::QueryColumn<BasicTable, col_idx, value_type> type;
    };

    Table* get_impl() { return this; }
    const Table* get_impl() const { return this; }
};




template<class Spec> class BasicTable<Spec>::Query:
    public Spec::template ColNames<QueryCol, Query*> {
public:
    template<class, int, class> friend class _impl::QueryColumnBase;
    template<class, int, class> friend class _impl::QueryColumn;

    Query(): Spec::template ColNames<QueryCol, Query*>(this) {}

    Query& group() { m_impl.group(); return *this; }

    Query& end_group() { m_impl.end_group(); return *this; }

    Query& parent() { m_impl.parent(); return *this; }

    Query& Or() { m_impl.Or(); return *this; }

    std::size_t find_next(const BasicTable<Spec>& table, std::size_t lastmatch=std::size_t(-1))
    {
        return m_impl.find_next(table, lastmatch);
    }

    typename BasicTable<Spec>::View find_all(BasicTable<Spec>& table, std::size_t start=0,
                                             std::size_t end=std::size_t(-1),
                                             std::size_t limit=std::size_t(-1))
    {
        return m_impl.find_all(table, start, end, limit);
    }

    typename BasicTable<Spec>::ConstView find_all(const BasicTable<Spec>& table,
                                                  std::size_t start=0,
                                                  std::size_t end=std::size_t(-1),
                                                  std::size_t limit=std::size_t(-1))
    {
        return m_impl.find_all(table, start, end, limit);
    }

    std::size_t count(const BasicTable<Spec>& table, std::size_t start=0,
                      std::size_t end=std::size_t(-1), std::size_t limit=std::size_t(-1)) const
    {
        return m_impl.count(table, start, end, limit);
    }

    std::size_t remove(BasicTable<Spec>& table, std::size_t start = 0,
                       std::size_t end = std::size_t(-1),
                       std::size_t limit = std::size_t(-1)) const
    {
        return m_impl.remove(table, start, end, limit);
    }

#ifdef _DEBUG
    std::string verify() { return m_impl.verify(); }
#endif

private:
    tightdb::Query m_impl;
};




namespace _impl
{
    template<class T> struct GetColumnTypeId;
    template<> struct GetColumnTypeId<int64_t> {
        static const ColumnType id = COLUMN_TYPE_INT;
    };
    template<> struct GetColumnTypeId<bool> {
        static const ColumnType id = COLUMN_TYPE_BOOL;
    };
    template<class E> struct GetColumnTypeId<SpecBase::Enum<E> > {
        static const ColumnType id = COLUMN_TYPE_INT;
    };
    template<> struct GetColumnTypeId<const char*> {
        static const ColumnType id = COLUMN_TYPE_STRING;
    };
    template<> struct GetColumnTypeId<Mixed> {
        static const ColumnType id = COLUMN_TYPE_MIXED;
    };


    template<class Type, int col_idx> struct AddCol {
        static void exec(Spec* spec, const char* const* col_names)
        {
            assert(col_idx == spec->get_column_count());
            spec->add_column(GetColumnTypeId<Type>::id, col_names[col_idx]);
        }
    };

    // AddCol specialization for subtables
    template<class Subspec, int col_idx> struct AddCol<BasicTable<Subspec>, col_idx> {
        static void exec(Spec* spec, const char* const* col_names)
        {
            assert(col_idx == spec->get_column_count());
            typedef typename Subspec::Columns SubcolTypes;
            Spec subspec = spec->add_subtable_column(col_names[col_idx]);
            ForEachType<SubcolTypes, _impl::AddCol>::exec(&subspec, Subspec::dyn_col_names());
        }
    };
}


} // namespace tightdb

#endif // TIGHTDB_BASIC_TABLE_HPP
