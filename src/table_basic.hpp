#ifndef TIGHTDB_BASIC_TABLE_H
#define TIGHTDB_BASIC_TABLE_H

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

#include "meta.hpp"
#include "table.hpp"
#include "query.hpp"

namespace tightdb {


template<class T> struct GetColumnTypeId;
template<int col_idx, class Type> class RegisterColumn;



/**
 * This class is non-polymorphic, that is, it has no virtual
 * functions. Further more, it has no destructor, and it adds no new
 * data-members. These properties are important, because it ensures
 * that there is no run-time distinction between a Table instance and
 * an instance of any variation of this class, and therefore it is
 * valid to cast a pointer from Table to basic_table<T> even when the
 * instance is constructed as a Table. Of couse, this also assumes
 * that Table is non-polymorphic.
 */
template<class Spec> class BasicTable: public Table {
private:
    template<class> friend class BasicTable;
    friend class BasicTableRef<BasicTable>;

    template<class Tab> class Accessors {
    public:
        class FieldBase;
        template<int col_idx, class Type> class Field;
        typedef std::pair<Tab*, std::size_t> FieldInit;
        typedef typename Spec::template Columns<Field, FieldInit> Row;

        template<int col_idx, class Type> class ColumnBase;
        template<int col_idx, class Type> class Column;
        typedef Tab* ColumnInit;
        typedef typename Spec::template Columns<Column, ColumnInit> Cols;

        class SubtabRow;
    };

    typedef BasicTable<Spec> ThisTable;

    typedef Accessors<ThisTable> Acc;
    typedef Accessors<const ThisTable> ConstAcc;

    typedef typename Acc::Cols ColsAccessor;
    typedef typename ConstAcc::Cols ConstColsAccessor;

    typedef typename Acc::Row RowAccessor;
    typedef typename ConstAcc::Row ConstRowAccessor;

    template<int col_idx, class Type> class QueryColumnBase;
    template<int col_idx, class Type> class QueryColumn;

public:
    class Query;

    Query GetQuery() const { return Query(); } // FIXME: Bad thing to copy queries

    typedef RowAccessor Cursor; // FIXME: Do we really neede a cursor
    typedef ConstRowAccessor ConstCursor; // FIXME: Do we really neede a cursor

    BasicTable(Allocator& alloc = GetDefaultAllocator()): Table(alloc)
    {
        tightdb::Spec& spec = get_spec();
        typename Spec::template Columns<RegisterColumn, tightdb::Spec*> c(&spec);
        update_from_spec();
    }

    BasicTableRef<BasicTable> get_table_ref() { return BasicTableRef<BasicTable>(this); }
    BasicTableRef<const BasicTable> get_table_ref() const { return BasicTableRef<const BasicTable>(this); }

    ColsAccessor cols() { return ColsAccessor(this); }

    ConstColsAccessor cols() const { return ConstColsAccessor(this); }

    RowAccessor operator[](std::size_t row_idx)
    {
        return RowAccessor(std::make_pair(this, row_idx));
    }

    ConstRowAccessor operator[](std::size_t row_idx) const
    {
        return ConstRowAccessor(std::make_pair(this, row_idx));
    }

    RowAccessor Front() { return RowAccessor(std::make_pair(this, 0)); }
    ConstRowAccessor Front() const { return ConstRowAccessor(std::make_pair(this, 0)); }

    /**
     * \param rel_idx The index of the row specified relative to the
     * end. Thus, <tt>table.Back(rel_idx)</tt> is the same as
     * <tt>table[table.size() + rel_idx]</tt>.
     */
    RowAccessor Back(int rel_idx = -1)
    {
        return RowAccessor(std::make_pair(this, m_size+rel_idx));
    }

    ConstRowAccessor Back(int rel_idx = -1) const
    {
        return ConstRowAccessor(std::make_pair(this, m_size+rel_idx));
    }

    RowAccessor Add() { return RowAccessor(std::make_pair(this, AddRow())); }

    template<class T1>
    void Add(const T1& v1)
    {
        Spec::insert(m_size, cols(), v1);
        InsertDone();
    }

    template<class T1, class T2>
    void Add(const T1& v1, const T2& v2)
    {
        Spec::insert(m_size, cols(), v1, v2);
        InsertDone();
    }

    template<class T1, class T2, class T3>
    void Add(const T1& v1, const T2& v2, const T3& v3)
    {
        Spec::insert(m_size, cols(), v1, v2, v3);
        InsertDone();
    }

    template<class T1, class T2, class T3, class T4>
    void Add(const T1& v1, const T2& v2, const T3& v3, const T4& v4)
    {
        Spec::insert(m_size, cols(), v1, v2, v3, v4);
        InsertDone();
    }

    // FIXME: Add remaining Add() methods up to 8 values.

    template<class T1>
    void Insert(std::size_t i, const T1& v1)
    {
        Spec::insert(i, cols(), v1);
        InsertDone();
    }

    template<class T1, class T2>
    void Insert(std::size_t i, const T1& v1, const T2& v2)
    {
        Spec::insert(i, cols(), v1, v2);
        InsertDone();
    }

    template<class T1, class T2, class T3>
    void Insert(std::size_t i, const T1& v1, const T2& v2, const T3& v3)
    {
        Spec::insert(i, cols(), v1, v2, v3);
        InsertDone();
    }

    template<class T1, class T2, class T3, class T4>
    void Insert(std::size_t i, const T1& v1, const T2& v2, const T3& v3, const T4& v4)
    {
        Spec::insert(i, cols(), v1, v2, v3, v4);
        InsertDone();
    }

    // FIXME: Add remaining Insert() methods up to 8 values.
};




// These types are meant to be used when specifying column types
// directly of via the TDB_TABLE_* macros.
struct SpecBase {
    typedef int64_t         Int;
    typedef bool            Bool;
    typedef const char*     String;
    typedef std::time_t     Date;
//    typedef tightdb::Binary Binary; // FIXME: Use tightdb::BinaryData here?
    typedef tightdb::Mixed  Mixed;
    template<class E> class Enum {
    public:
        Enum(E v) : m_value(v) {};
        operator E() const { return m_value; }
    private:
        E m_value;
    };
};




template<class Spec> class BasicTable<Spec>::Query:
    public Spec::template Columns<QueryColumn, Query*> {
public:
    template<int, class> friend class QueryColumnBase;
    template<int, class> friend class QueryColumn;
    Query(): Spec::template Columns<QueryColumn, Query*>(this) {}

    Query& Or() { m_impl.Or(); return *this; }

    Query& Group() { m_impl.LeftParan(); return *this; }

    Query& EndGroup() { m_impl.RightParan(); return *this; }

    std::size_t Delete(BasicTable<Spec>& table, size_t start = 0, size_t end = size_t(-1), // Should instead be 'table.erase(query);'
                       size_t limit = size_t(-1)) const
    {
        return m_impl.Delete(table, start, end, limit);
    }

    operator typename tightdb::Query() const { return m_impl; } // FIXME: Bad thing to copy queries

private:
    tightdb::Query m_impl;
};




template<class Spec> template<int col_idx, class Type> class BasicTable<Spec>::QueryColumnBase {
protected:
    typedef typename BasicTable<Spec>::Query Query;
    Query* const m_query;
    explicit QueryColumnBase(Query* q): m_query(q) {}

    Query& Equal(const Type& value) const
    {
        m_query->m_impl.Equal(col_idx, value);
        return *m_query;
    }

    Query& NotEqual(const Type& value) const
    {
        m_query->m_impl.NotEqual(col_idx, value);
        return *m_query;
    }
};

// QueryColumn specialization for integers
template<class Spec> template<int col_idx>
class BasicTable<Spec>::QueryColumn<col_idx, int64_t>: public QueryColumnBase<col_idx, int64_t> {
private:
    typedef typename BasicTable<Spec>::template QueryColumnBase<col_idx, int64_t> Base;
    typedef typename Base::Query Query;

public:
    explicit QueryColumn(Query* q, const char* = 0): Base(q) {}
    using Base::Equal;
    using Base::NotEqual;

    Query& Greater(int64_t value) const
    {
        Base::m_query->m_impl.Greater(col_idx, value);
        return *Base::m_query;
    }

    Query& GreaterEqual(int64_t value) const
    {
        Base::m_query->m_impl.GreaterEqual(col_idx, value);
        return *Base::m_query;
    }

    Query& Less(int64_t value) const
    {
        Base::m_query->m_impl.Less(col_idx, value);
        return *Base::m_query;
    }

    Query& LessEqual(int64_t value) const
    {
        Base::m_query->m_impl.LessEqual(col_idx, value);
        return *Base::m_query;
    }

    Query& Between(int64_t from, int64_t to) const
    {
        Base::m_query->m_impl.Between(col_idx, from, to);
        return *Base::m_query;
    };
};

// QueryColumn specialization for booleans
template<class Spec> template<int col_idx>
class BasicTable<Spec>::QueryColumn<col_idx, bool>: public QueryColumnBase<col_idx, bool> {
private:
    typedef typename BasicTable<Spec>::template QueryColumnBase<col_idx, bool> Base;
    typedef typename Base::Query Query;

public:
    explicit QueryColumn(Query* q, const char* = 0): Base(q) {}
    using Base::Equal;
    using Base::NotEqual;
};

// QueryColumn specialization for enumerations
template<class Spec> template<int col_idx, class E>
class BasicTable<Spec>::QueryColumn<col_idx, SpecBase::Enum<E> >:
    public QueryColumnBase<col_idx, SpecBase::Enum<E> > {
private:
    typedef typename BasicTable<Spec>::template QueryColumnBase<col_idx, SpecBase::Enum<E> > Base;
    typedef typename Base::Query Query;

public:
    explicit QueryColumn(Query* q, const char* = 0): Base(q) {}
    using Base::Equal;
    using Base::NotEqual;
};

// QueryColumn specialization for strings
template<class Spec> template<int col_idx>
class BasicTable<Spec>::QueryColumn<col_idx, const char*>: public QueryColumnBase<col_idx, const char*> {
private:
    typedef typename BasicTable<Spec>::template QueryColumnBase<col_idx, const char*> Base;
    typedef typename Base::Query Query;

public:
    explicit QueryColumn(Query* q, const char* = 0): Base(q) {}

    Query& Equal(const char* value, bool case_sensitive=true) const
    {
        Base::m_query->m_impl.Equal(col_idx, value, case_sensitive);
        return *Base::m_query;
    }

    Query& NotEqual(const char* value, bool case_sensitive=true) const
    {
        Base::m_query->m_impl.NotEqual(col_idx, value, case_sensitive);
        return *Base::m_query;
    }

    Query& BeginsWith(const char* value, bool case_sensitive=true) const
    {
        Base::m_query->m_impl.BeginsWith(col_idx, value, case_sensitive);
        return *Base::m_query;
    }

    Query& EndsWith(const char* value, bool case_sensitive=true) const
    {
        Base::m_query->m_impl.EndsWith(col_idx, value, case_sensitive);
        return *Base::m_query;
    }

    Query& Contains(const char* value, bool case_sensitive=true) const
    {
        Base::m_query->m_impl.Contains(col_idx, value, case_sensitive);
        return *Base::m_query;
    }
};

// QueryColumn specialization for mixed type
template<class Spec> template<int col_idx> class BasicTable<Spec>::QueryColumn<col_idx, Mixed> {
private:
    typedef typename BasicTable<Spec>::Query Query;

public:
    explicit QueryColumn(Query*, const char* = 0) {}
};

// QueryColumn specialization for subtables
template<class Spec> template<int col_idx, class Subspec>
class BasicTable<Spec>::QueryColumn<col_idx, BasicTable<Subspec> > {
private:
    typedef typename BasicTable<Spec>::Query Query;

public:
    explicit QueryColumn(Query*, const char* = 0) {}
};




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




template<int col_idx, class Type> class RegisterColumn {
public:
    RegisterColumn(tightdb::Spec* spec, const char* column_name)
    {
        assert(col_idx == spec->get_column_count());
        spec->add_column(GetColumnTypeId<Type>::id, column_name);
    }
};

// RegisterColumn specialization for subtables
template<int col_idx, class Subspec> class RegisterColumn<col_idx, BasicTable<Subspec> > {
public:
    RegisterColumn(tightdb::Spec* spec, const char* column_name)
    {
        assert(col_idx == spec->get_column_count());
        tightdb::Spec subspec = spec->add_subtable_column(column_name);
        typename Subspec::template Columns<tightdb::RegisterColumn, tightdb::Spec*> c(&subspec);
    }
};




template<class Spec> template<class Tab> class BasicTable<Spec>::Accessors<Tab>::FieldBase {
protected:
    Tab* const m_table;
    const std::size_t m_row_idx;
    FieldBase(FieldInit i): m_table(i.first), m_row_idx(i.second) {}
};

// Field accessor specialization for integers
template<class Spec> template<class Tab> template<int col_idx>
class BasicTable<Spec>::Accessors<Tab>::Field<col_idx, int64_t>: public FieldBase {
private:
    typedef typename BasicTable<Spec>::template Accessors<Tab> Acc;
    typedef typename Acc::FieldBase Base;

public:
    explicit Field(typename Acc::FieldInit i, const char* = 0): Base(i) {}
    operator int64_t() const { return Base::m_table->Get(col_idx, Base::m_row_idx); }
    const Field& operator=(int64_t value) const
    {
        Base::m_table->Set(col_idx, Base::m_row_idx, value);
        return *this;
    }
    const Field& operator+=(int64_t value) const
    {
        // FIXME: Should be optimized (can be both optimized and
        // generalized by using a form of expression templates).
        value = Base::m_table->Get(col_idx, Base::m_row_idx) + value;
        Base::m_table->Set(col_idx, Base::m_row_idx, value);
        return *this;
    }
};

// Field accessor specialization for booleans
template<class Spec> template<class Tab> template<int col_idx>
class BasicTable<Spec>::Accessors<Tab>::Field<col_idx, bool>: public FieldBase {
private:
    typedef typename BasicTable<Spec>::template Accessors<Tab> Acc;
    typedef typename Acc::FieldBase Base;

public:
    explicit Field(typename Acc::FieldInit i, const char* = 0): Base(i) {}
    operator bool() const { return Base::m_table->GetBool(col_idx, Base::m_row_idx); }
    const Field& operator=(bool value) const
    {
        Base::m_table->SetBool(col_idx, Base::m_row_idx, value);
        return *this;
    }
};

// Field accessor specialization for enumerations
template<class Spec> template<class Tab> template<int col_idx, class E>
class BasicTable<Spec>::Accessors<Tab>::Field<col_idx, SpecBase::Enum<E> >: public FieldBase {
private:
    typedef typename BasicTable<Spec>::template Accessors<Tab> Acc;
    typedef typename Acc::FieldBase Base;

public:
    explicit Field(typename Acc::FieldInit i, const char* = 0): Base(i) {}
    operator E() const { return static_cast<E>(Base::m_table->Get(col_idx, Base::m_row_idx)); }
    const Field& operator=(E value) const
    {
        Base::m_table->Set(col_idx, Base::m_row_idx, value);
        return *this;
    }
};

// Field accessor specialization for strings
template<class Spec> template<class Tab> template<int col_idx>
class BasicTable<Spec>::Accessors<Tab>::Field<col_idx, const char*>: public FieldBase {
private:
    typedef typename BasicTable<Spec>::template Accessors<Tab> Acc;
    typedef typename Acc::FieldBase Base;

public:
    explicit Field(typename Acc::FieldInit i, const char* = 0): Base(i) {}
    operator const char*() const { return Base::m_table->GetString(col_idx, Base::m_row_idx); }
    const Field& operator=(const char* value) const
    {
        Base::m_table->SetString(col_idx, Base::m_row_idx, value);
        return *this;
    }
    // FIXME: Not good to define operator==() here, beacuse it does
    // not have this semantic for char pointers in general. However,
    // if we choose to keep it, we should also have all the other
    // comparison operators, and many other operators need to be
    // disabled such that e.g. 't.foo - 10' is no longer possible (it
    // is now due to the conversion operator). A much better approach
    // would probably be to define a special tightdb::String type.
    bool operator==(const char* value) const
    {
        return std::strcmp(Base::m_table->GetString(col_idx, Base::m_row_idx), value) == 0;
    }
};

// Field accessor specialization for mixed type
template<class Spec> template<class Tab> template<int col_idx>
class BasicTable<Spec>::Accessors<Tab>::Field<col_idx, Mixed>: public FieldBase {
private:
    typedef typename BasicTable<Spec>::template Accessors<Tab> Acc;
    typedef typename Acc::FieldBase Base;

public:
    explicit Field(typename Acc::FieldInit i, const char* = 0): Base(i) {}
    operator Mixed() const { return Base::m_table->GetMixed(col_idx, Base::m_row_idx); }
    const Field& operator=(const Mixed& value) const
    {
        Base::m_table->SetMixed(col_idx, Base::m_row_idx, value);
        return *this;
    }
    ColumnType get_type() const { return Base::m_table->GetMixedType(col_idx, Base::m_row_idx); }
    int64_t get_int() const { return Mixed(*this).get_int(); }
    bool get_bool() const { return Mixed(*this).get_bool(); }
    std::time_t get_date() const { return Mixed(*this).get_date(); }
    const char* get_string() const { return Mixed(*this).get_string(); }
    BinaryData get_binary() const { return Mixed(*this).get_binary(); }
};

// Field accessor specialization for subtables
template<class Spec> template<class Tab> template<int col_idx, class Subspec>
class BasicTable<Spec>::Accessors<Tab>::Field<col_idx, BasicTable<Subspec> >: public FieldBase {
private:
    typedef typename BasicTable<Spec>::template Accessors<Tab> Acc;
    typedef typename Acc::FieldBase Base;
    typedef typename CopyConstness<Tab, BasicTable<Subspec> >::type Subtab;
    typedef typename BasicTable<Subspec>::template Accessors<Subtab> SubtabAcc;
    typedef typename SubtabAcc::SubtabRow RowAccessor;

public:
    explicit Field(typename Acc::FieldInit i, const char* = 0): Base(i) {}

    BasicTableRef<Subtab> operator->() const
    {
        Subtab* subtab =
            static_cast<Subtab*>(Base::m_table->get_subtable_ptr(col_idx, Base::m_row_idx));
        return Table::make_ref(subtab);
    }

    RowAccessor operator[](std::size_t row_idx) const
    {
        Subtab* subtab =
            static_cast<Subtab*>(Base::m_table->get_subtable_ptr(col_idx, Base::m_row_idx));
        return RowAccessor(subtab, row_idx);
    }
};




template<class Spec> template<class Tab> template<int col_idx, class Type>
class BasicTable<Spec>::Accessors<Tab>::ColumnBase {
public:
    explicit ColumnBase(Tab* t): m_table(t) {}

    Field<col_idx, Type> operator[](std::size_t row_idx) const
    {
        return Field<col_idx, Type>(std::make_pair(m_table, row_idx));
    }

protected:
    Tab* const m_table;
};

// Column accessor specialization for integers
template<class Spec> template<class Tab> template<int col_idx>
class BasicTable<Spec>::Accessors<Tab>::Column<col_idx, int64_t>:
    public ColumnBase<col_idx, int64_t> {
private:
    typedef typename BasicTable<Spec>::template Accessors<Tab>::template ColumnBase<col_idx, int64_t> Base;

public:
    explicit Column(Tab* t, const char* = 0): Base(t) {}

    std::size_t Find(int64_t value) const
    {
        return Base::m_table->Find(col_idx, value);
    }

    std::size_t FindPos(int64_t value) const
    {
        return Base::m_table->GetColumn(col_idx).FindPos(value);
    }

    TableView FindAll(int64_t value) const
    {
        TableView tv(*Base::m_table);
        Base::m_table->FindAll(tv, col_idx, value);
        return tv;
    }

    const Column& operator+=(int64_t value) const
    {
        Base::m_table->GetColumn(col_idx).Increment64(value);
        return *this;
    }

    void _insert(std::size_t row_idx, int64_t value) const // FIXME: Should not be public (maybe send specialized columns accessor to Spec::insert(), then in Spec::insert() do 'op(cols.name1, v1)')
    {
        Base::m_table->InsertInt(col_idx, row_idx, value);
    }
};

// Column accessor specialization for booleans
template<class Spec> template<class Tab> template<int col_idx>
class BasicTable<Spec>::Accessors<Tab>::Column<col_idx, bool>:
    public ColumnBase<col_idx, bool> {
private:
    typedef typename BasicTable<Spec>::template Accessors<Tab>::template ColumnBase<col_idx, bool> Base;

public:
    explicit Column(Tab* t, const char* = 0): Base(t) {}

    std::size_t Find(bool value) const
    {
        return Base::m_table->FindBool(col_idx, value);
    }

    TableView FindAll(bool value) const
    {
        TableView tv(*Base::m_table);
        Base::m_table->FindAllBool(tv, col_idx, value);
        return tv;
    }

    void _insert(std::size_t row_idx, bool value) const // FIXME: Should not be public (maybe send specialized columns accessor to Spec::insert(), then in Spec::insert() do 'op(cols.name1, v1)')
    {
        Base::m_table->InsertBool(col_idx, row_idx, value);
    }
};

// Column accessor specialization for enumerations
template<class Spec> template<class Tab> template<int col_idx, class E>
class BasicTable<Spec>::Accessors<Tab>::Column<col_idx, SpecBase::Enum<E> >:
    public ColumnBase<col_idx, SpecBase::Enum<E> > {
private:
    typedef typename BasicTable<Spec>::template Accessors<Tab>::template ColumnBase<col_idx, SpecBase::Enum<E> > Base;

public:
    explicit Column(Tab* t, const char* = 0): Base(t) {}

    std::size_t Find(E value) const
    {
        return Base::m_table->Find(col_idx, value);
    }

    TableView FindAll(E value) const
    {
        TableView tv(*Base::m_table);
        Base::m_table->FindAll(tv, col_idx, value);
        return tv;
    }

    void _insert(std::size_t row_idx, E value) const // FIXME: Should not be public (maybe send specialized columns accessor to Spec::insert(), then in Spec::insert() do 'op(cols.name1, v1)')
    {
        Base::m_table->InsertEnum(col_idx, row_idx, value);
    }
};

// Column accessor specialization for strings
template<class Spec> template<class Tab> template<int col_idx>
class BasicTable<Spec>::Accessors<Tab>::Column<col_idx, const char*>:
    public ColumnBase<col_idx, const char*> {
private:
    typedef typename BasicTable<Spec>::template Accessors<Tab>::template ColumnBase<col_idx, const char*> Base;

public:
    explicit Column(Tab* t, const char* = 0): Base(t) {}

    std::size_t Find(const char* value) const
    {
        return Base::m_table->FindString(col_idx, value);
    }

    TableView FindAll(const char* value) const
    {
        TableView tv(*Base::m_table);
        Base::m_table->FindAllString(tv, col_idx, value);
        return tv;
    }

    void _insert(std::size_t row_idx, const char* value) const // FIXME: Should not be public (maybe send specialized columns accessor to Spec::insert(), then in Spec::insert() do 'op(cols.name1, v1)')
    {
        Base::m_table->InsertString(col_idx, row_idx, value);
    }
};

// Column accessor specialization for mixed type
template<class Spec> template<class Tab> template<int col_idx>
class BasicTable<Spec>::Accessors<Tab>::Column<col_idx, Mixed>: public ColumnBase<col_idx, Mixed> {
private:
    typedef typename BasicTable<Spec>::template Accessors<Tab>::template ColumnBase<col_idx, Mixed> Base;

public:
    explicit Column(Tab* t, const char* = 0): Base(t) {}

    void _insert(std::size_t row_idx, const Mixed& value) const // FIXME: Should not be public (maybe send specialized columns accessor to Spec::insert(), then in Spec::insert() do 'op(cols.name1, v1)')
    {
        Base::m_table->InsertMixed(col_idx, row_idx, value);
    }
};

// Column accessor specialization for subtables
template<class Spec> template<class Tab> template<int col_idx, class Subspec>
class BasicTable<Spec>::Accessors<Tab>::Column<col_idx, BasicTable<Subspec> >:
    public ColumnBase<col_idx, BasicTable<Subspec> > {
private:
    typedef typename BasicTable<Spec>::template Accessors<Tab>::template ColumnBase<col_idx, BasicTable<Subspec> > Base;

public:
    explicit Column(Tab* t, const char* = 0): Base(t) {}
};




template<class Spec> template<class Tab>
class BasicTable<Spec>::Accessors<Tab>::SubtabRow: public Row {
public:
    SubtabRow(Tab* subtab, std::size_t row_idx):
        Row(std::make_pair(subtab, row_idx)), m_owner(Table::make_ref(subtab)) {}

private:
    BasicTableRef<Tab> const m_owner;
};


} // namespace tightdb

#endif // TIGHTDB_BASIC_TABLE_H
