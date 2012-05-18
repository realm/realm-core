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
#ifndef TIGHTDB_BASIC_TABLE_VIEW_HPP
#define TIGHTDB_BASIC_TABLE_VIEW_HPP

#include "meta.hpp"
#include "table_view.hpp"
#include "table_accessors.hpp"

namespace tightdb {


/**
 * Common base class for BasicTableView<Tab> and BasicTableView<const
 * Tab>.
 */
template<class Tab, class View, class Impl> class BasicTableViewBase {
public:
    typedef typename Tab::spec_type spec_type;
    typedef Tab table_type;

    bool is_empty() const { return m_impl.is_empty(); }
    size_t size() const { return m_impl.size(); }

    // Get row index in the source table this view is "looking" at.
    size_t get_source_ndx(size_t row_ndx) const { return m_impl.get_source_ndx(row_ndx); }

    table_type& get_parent() const { return static_cast<table_type&>(m_impl.get_parent()); }

private:
    typedef typename Tab::spec_type Spec;

    template<int col_idx> struct Col {
        typedef typename TypeAt<typename Spec::Columns, col_idx>::type value_type;
        typedef _impl::ColumnAccessor<View, col_idx, value_type> type;
    };
    typedef typename Spec::template ColNames<Col, View*> ColsAccessor;

    template<int col_idx> struct ConstCol {
        typedef typename TypeAt<typename Spec::Columns, col_idx>::type value_type;
        typedef _impl::ColumnAccessor<const View, col_idx, value_type> type;
    };
    typedef typename Spec::template ColNames<ConstCol, const View*> ConstColsAccessor;

public:
    ColsAccessor cols() { return ColsAccessor(static_cast<View*>(this)); }

    ConstColsAccessor cols() const { return ConstColsAccessor(static_cast<const View*>(this)); }

private:
    template<int col_idx> struct Field {
        typedef typename TypeAt<typename Spec::Columns, col_idx>::type value_type;
        typedef _impl::FieldAccessor<View, col_idx, value_type> type;
    };
    typedef std::pair<View*, std::size_t> FieldInit;
    typedef typename Spec::template ColNames<Field, FieldInit> RowAccessor;

    template<int col_idx> struct ConstField {
        typedef typename TypeAt<typename Spec::Columns, col_idx>::type value_type;
        typedef _impl::FieldAccessor<const View, col_idx, value_type> type;
    };
    typedef std::pair<const View*, std::size_t> ConstFieldInit;
    typedef typename Spec::template ColNames<ConstField, ConstFieldInit> ConstRowAccessor;

public:
    RowAccessor operator[](std::size_t row_idx)
    {
        return RowAccessor(std::make_pair(static_cast<View*>(this), row_idx));
    }

    ConstRowAccessor operator[](std::size_t row_idx) const
    {
        return ConstRowAccessor(std::make_pair(static_cast<const View*>(this), row_idx));
    }

protected:
    template<class, int, class> friend class _impl::FieldAccessor;

    Impl m_impl;

    BasicTableViewBase() {}
    BasicTableViewBase(Impl i): m_impl(move(i)) {}

    Impl* get_impl() { return &m_impl; }
    const Impl* get_impl() const { return &m_impl; }
};




/**
 * A BasicTableView wraps a TableView and provides a type and
 * structure safe set of access methods. The TableView methods are no
 * available through a BasicTableView.
 *
 * Just like TableView, a BasicTableView has both copy and move
 * semantics. See TableView for more on this.
 *
 * \tparam Tab The parent table type. This will in general be an
 * instance of the BasicTable template. If the specified table type is
 * 'const' then this class only gives read access to the underlying
 * table.
 */
template<class Tab>
class BasicTableView: public BasicTableViewBase<Tab, BasicTableView<Tab>, TableView> {
private:
    typedef BasicTableViewBase<Tab, BasicTableView<Tab>, TableView> Base;

public:
    BasicTableView() {}
    BasicTableView& operator=(BasicTableView tv) { Base::m_impl = move(tv.m_impl); return *this; }
    friend BasicTableView move(BasicTableView& tv) { return BasicTableView(&tv); }

    // Deleting
    void clear() { Base::m_impl.clear(); }
    void remove(size_t ndx) { Base::m_impl.remove(ndx); }
    void remove_last() { Base::m_impl.remove_last(); }

private:
    template<class, int, class> friend class _impl::ColumnAccessorBase;
    template<class, int, class> friend class _impl::ColumnAccessor;
    friend class Tab::Query;
    BasicTableView(BasicTableView* tv): Base(move(tv->m_impl)) {}
    BasicTableView(TableView tv): Base(move(tv)) {}
};




/**
 * Specialization for const access to parent table.
 */
template<class Tab> class BasicTableView<const Tab>:
    public BasicTableViewBase<const Tab, BasicTableView<const Tab>, ConstTableView> {
private:
    typedef BasicTableViewBase<const Tab, BasicTableView<const Tab>, ConstTableView> Base;

public:
    BasicTableView() {}
    BasicTableView& operator=(BasicTableView tv) { Base::m_impl = move(tv.m_impl); return *this; }
    friend BasicTableView move(BasicTableView& tv) { return BasicTableView(&tv); }

    /**
     * Construct BasicTableView<const Tab> from BasicTableView<Tab>.
     */
    BasicTableView(BasicTableView<Tab> tv): Base(move(tv.m_impl)) {}

    /**
     * Assign BasicTableView<Tab> to BasicTableView<const Tab>.
     */
    BasicTableView& operator=(BasicTableView<Tab> tv)
    {
        Base::m_impl = move(tv.m_impl);
        return *this;
    }

private:
    template<class, int, class> friend class _impl::ColumnAccessorBase;
    template<class, int, class> friend class _impl::ColumnAccessor;
    friend class Tab::Query;
    BasicTableView(BasicTableView* tv): Base(move(tv->m_impl)) {}
    BasicTableView(ConstTableView tv): Base(move(tv)) {}
};


} // namespace tightdb

#endif // TIGHTDB_BASIC_TABLE_VIEW_HPP
