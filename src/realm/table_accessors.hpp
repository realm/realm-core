/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/
#ifndef REALM_TABLE_ACCESSORS_HPP
#define REALM_TABLE_ACCESSORS_HPP

#include <cstring>
#include <utility>

#include <realm/mixed.hpp>
#include <realm/table.hpp>

#include <realm/query_engine.hpp>

namespace realm {


/// A convenience base class for Spec classes that are to be used with
/// BasicTable.
///
/// There are two reasons why you might want to derive your spec class
/// from this one. First, it offers short hand names for each of the
/// available column types. Second, it makes it easier when you do not
/// want to specify colum names or convenience methods, since suitable
/// fallbacks are defined here.
struct SpecBase {
    typedef int64_t             Int;
    typedef bool                Bool;
    typedef realm::DateTime   DateTime;
    typedef float               Float;
    typedef double              Double;
    typedef realm::StringData String;
    typedef realm::BinaryData Binary;
    typedef realm::Mixed      Mixed;

    template<class E> class Enum {
    public:
        typedef E enum_type;
        Enum(E v): m_value(v) {}
        operator E() const { return m_value; }
    private:
        E m_value;
    };

    template<class T> class Subtable {
    public:
        typedef T table_type;
        Subtable(T* t): m_table(t) {}
        operator T*() const { return m_table; }
    private:
        T* m_table;
    };

    /// By default, there are no static column names defined for a
    /// BasicTable. One may define a set of column mames as follows:
    ///
    /// \code{.cpp}
    ///
    ///   struct MyTableSpec: SpecBase {
    ///     typedef TypeAppend<void, int>::type Columns1;
    ///     typedef TypeAppend<Columns1, bool>::type Columns;
    ///
    ///     template<template<int> class Col, class Init> struct ColNames {
    ///       typename Col<0>::type foo;
    ///       typename Col<1>::type bar;
    ///       ColNames(Init i) REALM_NOEXCEPT: foo(i), bar(i) {}
    ///     };
    ///   };
    ///
    /// \endcode
    ///
    /// Note that 'i' in Col<i> links the name that you specify to a
    /// particular column index. You may specify the column names in
    /// any order. Multiple names may refer to the same column, and
    /// you do not have to specify a name for every column.
    template<template<int> class Col, class Init> struct ColNames {
        ColNames(Init) REALM_NOEXCEPT {}
    };

    /// FIXME: Currently we do not support absence of dynamic column
    /// names.
    static void dyn_column_names(StringData*) REALM_NOEXCEPT {}

    /// This is the fallback class that is used when no convenience
    /// methods are specified in the users Spec class.
    ///
    /// If you would like to add a more convenient add() method, here
    /// is how you could do it:
    ///
    /// \code{.cpp}
    ///
    ///   struct MyTableSpec: SpecBase {
    ///     typedef realm::TypeAppend<void, int>::type Columns1;
    ///     typedef realm::TypeAppend<Columns1, bool>::type Columns;
    ///
    ///     struct ConvenienceMethods {
    ///       void add(int foo, bool bar)
    ///       {
    ///         BasicTable<MyTableSpec>* const t = static_cast<BasicTable<MyTableSpec>*>(this);
    ///         t->add((tuple(), name1, name2));
    ///       }
    ///     };
    ///   };
    ///
    /// \endcode
    ///
    /// FIXME: ConvenienceMethods may not contain any virtual methods,
    /// nor may it contain any data memebers. We might want to check
    /// this by REALM_STATIC_ASSERT(sizeof(Derivative of
    /// ConvenienceMethods) == 1)), however, this would not be
    /// guaranteed by the standard, since even an empty class may add
    /// to the size of the derived class. Fortunately, as long as
    /// ConvenienceMethods is derived from, by BasicTable, after
    /// deriving from Table, this cannot become a problem, nor would
    /// it lead to a violation of the strict aliasing rule of C++03 or
    /// C++11.
    struct ConvenienceMethods {};
};


template<class> class BasicTable;
template<class> class BasicTableView;


namespace _impl {


/// Get the const qualified type of the table being accessed.
///
/// If T matches 'BasicTableView<T2>' or 'const BasicTableView<T2>',
/// then return T2, else simply return T.
template<class Tab> struct GetTableFromView { typedef Tab type; };
template<class Tab> struct GetTableFromView<BasicTableView<Tab>> { typedef Tab type; };
template<class Tab> struct GetTableFromView<const BasicTableView<Tab>> { typedef Tab type; };


/// Determine whether an accessor has const-only access to a table, so
/// that it is not allowed to modify fields, nor return non-const
/// subtable references.
///
/// Note that for Taboid = 'BasicTableView<const Tab>', a column
/// accessor is still allowed to reorder the rows of the view, as long
/// as it does not modify the contents of the table.
template<class Taboid> struct TableIsConst { static const bool value = false; };
template<class Taboid> struct TableIsConst<const Taboid> { static const bool value = true; };
template<class Tab> struct TableIsConst<BasicTableView<const Tab>> {
    static const bool value = true;
};



/// This class gives access to a field of a row of a table, or a table
/// view.
///
/// \tparam Taboid Either a table or a table view, that is, any of
/// 'BasicTable<S>', 'const BasicTable<S>',
/// 'BasicTableView<BasicTable<S>>', 'const
/// BasicTableView<BasicTable<S>>', 'BasicTableView<const
/// BasicTable<S>>', or 'const BasicTableView<const BasicTable<S>
/// >'. Note that the term 'taboid' is used here for something that is
/// table-like, i.e., either a table of a table view.
///
/// \tparam const_tab Indicates whether the accessor has const-only
/// access to the field, that is, if, and only if Taboid matches
/// 'const T' or 'BasicTableView<const T>' for any T.
template<class Taboid, int column_index, class Type, bool const_tab> class FieldAccessor;


/// Commmon base class for all field accessor specializations.
template<class Taboid> class FieldAccessorBase {
protected:
    typedef std::pair<Taboid*, std::size_t> Init;
    Taboid* const m_table;
    const std::size_t m_row_index;
    FieldAccessorBase(Init i) REALM_NOEXCEPT: m_table(i.first), m_row_index(i.second) {}
};


/// Field accessor specialization for integers.
template<class Taboid, int column_index, bool const_tab>
class FieldAccessor<Taboid, column_index, int64_t, const_tab>: public FieldAccessorBase<Taboid> {
private:
    typedef FieldAccessorBase<Taboid> Base;

public:
    int64_t get() const REALM_NOEXCEPT
    {
        return Base::m_table->get_impl()->get_int(column_index, Base::m_row_index);
    }

    void set(int64_t value) const
    {
        Base::m_table->get_impl()->set_int(column_index, Base::m_row_index, value);
    }
    operator int64_t() const REALM_NOEXCEPT { return get(); }
    const FieldAccessor& operator=(int64_t value) const { set(value); return *this; }

    const FieldAccessor& operator+=(int64_t value) const
    {
        // FIXME: Should be optimized (can be both optimized and
        // generalized by using a form of expression templates).
        set(get() + value);
        return *this;
    }

    const FieldAccessor& operator-=(int64_t value) const
    {
        // FIXME: Should be optimized (can be both optimized and
        // generalized by using a form of expression templates).
        set(get() - value);
        return *this;
    }

    const FieldAccessor& operator++() const { return *this += 1; }
    const FieldAccessor& operator--() const { return *this -= 1; }

    int64_t operator++(int) const
    {
        // FIXME: Should be optimized (can be both optimized and
        // generalized by using a form of expression templates).
        const int64_t value = get();
        set(value + 1);
        return value;
    }

    int64_t operator--(int) const
    {
        // FIXME: Should be optimized (can be both optimized and
        // generalized by using a form of expression templates).
        const int64_t value = get();
        set(value - 1);
        return value;
    }


    explicit FieldAccessor(typename Base::Init i) REALM_NOEXCEPT: Base(i) {}
};


/// Field accessor specialization for floats.
template<class Taboid, int column_index, bool const_tab>
class FieldAccessor<Taboid, column_index, float, const_tab>: public FieldAccessorBase<Taboid> {
private:
    typedef FieldAccessorBase<Taboid> Base;

public:
    float get() const REALM_NOEXCEPT
    {
        return Base::m_table->get_impl()->get_float(column_index, Base::m_row_index);
    }

    void set(float value) const
    {
        Base::m_table->get_impl()->set_float(column_index, Base::m_row_index, value);
    }

    operator float() const REALM_NOEXCEPT { return get(); }
    const FieldAccessor& operator=(float value) const { set(value); return *this; }

    const FieldAccessor& operator+=(float value) const
    {
        // FIXME: Should be optimized (can be both optimized and
        // generalized by using a form of expression templates).
        set(get() + value);
        return *this;
    }

    const FieldAccessor& operator-=(float value) const
    {
        // FIXME: Should be optimized (can be both optimized and
        // generalized by using a form of expression templates).
        set(get() - value);
        return *this;
    }


    explicit FieldAccessor(typename Base::Init i) REALM_NOEXCEPT: Base(i) {}
};


/// Field accessor specialization for doubles.
template<class Taboid, int column_index, bool const_tab>
class FieldAccessor<Taboid, column_index, double, const_tab>: public FieldAccessorBase<Taboid> {
private:
    typedef FieldAccessorBase<Taboid> Base;

public:
    double get() const REALM_NOEXCEPT
    {
        return Base::m_table->get_impl()->get_double(column_index, Base::m_row_index);
    }

    void set(double value) const
    {
        Base::m_table->get_impl()->set_double(column_index, Base::m_row_index, value);
    }

    operator double() const REALM_NOEXCEPT { return get(); }
    const FieldAccessor& operator=(double value) const { set(value); return *this; }

    const FieldAccessor& operator+=(double value) const
    {
        // FIXME: Should be optimized (can be both optimized and
        // generalized by using a form of expression templates).
        set(get() + value);
        return *this;
    }

    const FieldAccessor& operator-=(double value) const
    {
        // FIXME: Should be optimized (can be both optimized and
        // generalized by using a form of expression templates).
        set(get() - value);
        return *this;
    }


    explicit FieldAccessor(typename Base::Init i) REALM_NOEXCEPT: Base(i) {}
};


/// Field accessor specialization for booleans.
template<class Taboid, int column_index, bool const_tab>
class FieldAccessor<Taboid, column_index, bool, const_tab>: public FieldAccessorBase<Taboid> {
private:
    typedef FieldAccessorBase<Taboid> Base;

public:
    bool get() const REALM_NOEXCEPT
    {
        return Base::m_table->get_impl()->get_bool(column_index, Base::m_row_index);
    }

    void set(bool value) const
    {
        Base::m_table->get_impl()->set_bool(column_index, Base::m_row_index, value);
    }

    operator bool() const REALM_NOEXCEPT { return get(); }
    const FieldAccessor& operator=(bool value) const { set(value); return *this; }


    explicit FieldAccessor(typename Base::Init i) REALM_NOEXCEPT: Base(i) {}
};


/// Field accessor specialization for enumerations.
template<class Taboid, int column_index, class E, bool const_tab>
class FieldAccessor<Taboid, column_index, SpecBase::Enum<E>, const_tab>:
    public FieldAccessorBase<Taboid> {
private:
    typedef FieldAccessorBase<Taboid> Base;

public:
    E get() const REALM_NOEXCEPT
    {
        return static_cast<E>(Base::m_table->get_impl()->get_int(column_index, Base::m_row_index));
    }

    void set(E value) const
    {
        Base::m_table->get_impl()->set_int(column_index, Base::m_row_index, value);
    }

    operator E() const REALM_NOEXCEPT { return get(); }
    const FieldAccessor& operator=(E value) const { set(value); return *this; }


    explicit FieldAccessor(typename Base::Init i) REALM_NOEXCEPT: Base(i) {}
};


/// Field accessor specialization for dates.
template<class Taboid, int column_index, bool const_tab>
class FieldAccessor<Taboid, column_index, DateTime, const_tab>: public FieldAccessorBase<Taboid> {
private:
    typedef FieldAccessorBase<Taboid> Base;

public:
    DateTime get() const REALM_NOEXCEPT
    {
        return Base::m_table->get_impl()->get_datetime(column_index, Base::m_row_index);
    }

    void set(DateTime value) const
    {
        Base::m_table->get_impl()->set_datetime(column_index, Base::m_row_index, value);
    }

    operator DateTime() const REALM_NOEXCEPT { return get(); }
    const FieldAccessor& operator=(DateTime value) const { set(value); return *this; }


    explicit FieldAccessor(typename Base::Init i) REALM_NOEXCEPT: Base(i) {}
};


/// Field accessor specialization for strings.
template<class Taboid, int column_index, bool const_tab>
class FieldAccessor<Taboid, column_index, StringData, const_tab>: public FieldAccessorBase<Taboid> {
private:
    typedef FieldAccessorBase<Taboid> Base;

public:
    StringData get() const REALM_NOEXCEPT
    {
        return Base::m_table->get_impl()->get_string(column_index, Base::m_row_index);
    }

    void set(StringData value) const
    {
        Base::m_table->get_impl()->set_string(column_index, Base::m_row_index, value);
    }

    operator StringData() const REALM_NOEXCEPT { return get(); }
    const FieldAccessor& operator=(StringData value) const { set(value); return *this; }

    const char* data() const REALM_NOEXCEPT { return get().data(); }
    std::size_t size() const REALM_NOEXCEPT { return get().size(); }

    const char* c_str() const REALM_NOEXCEPT { return data(); }


    explicit FieldAccessor(typename Base::Init i) REALM_NOEXCEPT: Base(i) {}
};


/// Field accessor specialization for binary data.
template<class Taboid, int column_index, bool const_tab>
class FieldAccessor<Taboid, column_index, BinaryData, const_tab>: public FieldAccessorBase<Taboid> {
private:
    typedef FieldAccessorBase<Taboid> Base;

public:
    BinaryData get() const REALM_NOEXCEPT
    {
        return Base::m_table->get_impl()->get_binary(column_index, Base::m_row_index);
    }

    void set(const BinaryData& value) const
    {
        Base::m_table->get_impl()->set_binary(column_index, Base::m_row_index, value);
    }

    operator BinaryData() const REALM_NOEXCEPT { return get(); }
    const FieldAccessor& operator=(const BinaryData& value) const { set(value); return *this; }

    const char* data() const REALM_NOEXCEPT { return get().data(); }
    std::size_t size() const REALM_NOEXCEPT { return get().size(); }


    explicit FieldAccessor(typename Base::Init i) REALM_NOEXCEPT: Base(i) {}
};


/// Field accessor specialization for subtables of non-const parent.
template<class Taboid, int column_index, class Subtab>
class FieldAccessor<Taboid, column_index, SpecBase::Subtable<Subtab>, false>:
    public FieldAccessorBase<Taboid> {
private:
    typedef FieldAccessorBase<Taboid> Base;
    // FIXME: Dangerous slicing posibility as long as Cursor is same as RowAccessor.
    // FIXME: Accessors must not be publicly copyable. This requires that Spec::ColNames is made a friend of BasicTable.
    // FIXME: Need BasicTableView::Cursor and BasicTableView::ConstCursor if Cursors should exist at all.
    struct SubtabRowAccessor: Subtab::RowAccessor {
    public:
        SubtabRowAccessor(Subtab* subtab, std::size_t row_index):
            Subtab::RowAccessor(std::make_pair(subtab, row_index)),
            m_owner(subtab->get_table_ref()) {}

    private:
        typename Subtab::Ref const m_owner;
    };

public:
    operator typename Subtab::Ref() const
    {
        Subtab* subtab =
            Base::m_table->template get_subtable_ptr<Subtab>(column_index, Base::m_row_index);
        return subtab->get_table_ref();
    }

    operator typename Subtab::ConstRef() const
    {
        const Subtab* subtab =
            Base::m_table->template get_subtable_ptr<Subtab>(column_index, Base::m_row_index);
        return subtab->get_table_ref();
    }

    typename Subtab::Ref operator->() const
    {
        Subtab* subtab =
            Base::m_table->template get_subtable_ptr<Subtab>(column_index, Base::m_row_index);
        return subtab->get_table_ref();
    }

    SubtabRowAccessor operator[](std::size_t row_index) const
    {
        Subtab* subtab =
            Base::m_table->template get_subtable_ptr<Subtab>(column_index, Base::m_row_index);
        return SubtabRowAccessor(subtab, row_index);
    }


    explicit FieldAccessor(typename Base::Init i) REALM_NOEXCEPT: Base(i) {}
};


/// Field accessor specialization for subtables of const parent.
template<class Taboid, int column_index, class Subtab>
class FieldAccessor<Taboid, column_index, SpecBase::Subtable<Subtab>, true>:
    public FieldAccessorBase<Taboid> {
private:
    typedef FieldAccessorBase<Taboid> Base;
    // FIXME: Dangerous slicing posibility as long as Cursor is same as RowAccessor.
    struct SubtabRowAccessor: Subtab::ConstRowAccessor {
    public:
        SubtabRowAccessor(const Subtab* subtab, std::size_t row_index):
            Subtab::ConstRowAccessor(std::make_pair(subtab, row_index)),
            m_owner(subtab->get_table_ref()) {}

    private:
        typename Subtab::ConstRef const m_owner;
    };

public:
    explicit FieldAccessor(typename Base::Init i) REALM_NOEXCEPT: Base(i) {}

    operator typename Subtab::ConstRef() const
    {
        const Subtab* subtab =
            Base::m_table->template get_subtable_ptr<Subtab>(column_index, Base::m_row_index);
        return subtab->get_table_ref();
    }

    typename Subtab::ConstRef operator->() const
    {
        const Subtab* subtab =
            Base::m_table->template get_subtable_ptr<Subtab>(column_index, Base::m_row_index);
        return subtab->get_table_ref();
    }

    SubtabRowAccessor operator[](std::size_t row_index) const
    {
        const Subtab* subtab =
            Base::m_table->template get_subtable_ptr<Subtab>(column_index, Base::m_row_index);
        return SubtabRowAccessor(subtab, row_index);
    }
};


/// Base for field accessor specializations for mixed type.
template<class Taboid, int column_index, class FieldAccessor>
class MixedFieldAccessorBase: public FieldAccessorBase<Taboid> {
private:
    typedef FieldAccessorBase<Taboid> Base;

public:
    Mixed get() const REALM_NOEXCEPT
    {
        return Base::m_table->get_impl()->get_mixed(column_index, Base::m_row_index);
    }

    void set(const Mixed& value) const
    {
        Base::m_table->get_impl()->set_mixed(column_index, Base::m_row_index, value);
    }

    operator Mixed() const REALM_NOEXCEPT { return get(); }

    const FieldAccessor& operator=(const Mixed& value) const
    {
        set(value);
        return static_cast<FieldAccessor&>(*this);
    }

    DataType get_type() const REALM_NOEXCEPT
    {
        return Base::m_table->get_impl()->get_mixed_type(column_index, Base::m_row_index);
    }

    int64_t get_int() const REALM_NOEXCEPT { return get().get_int(); }

    bool get_bool() const REALM_NOEXCEPT { return get().get_bool(); }

    DateTime get_datetime() const REALM_NOEXCEPT { return get().get_datetime(); }

    float get_float() const REALM_NOEXCEPT { return get().get_float(); }

    double get_double() const REALM_NOEXCEPT { return get().get_double(); }

    StringData get_string() const REALM_NOEXCEPT { return get().get_string(); }

    BinaryData get_binary() const REALM_NOEXCEPT { return get().get_binary(); }

    bool is_subtable() const REALM_NOEXCEPT { return get_type() == type_Table; }

    /// Checks whether this value is a subtable of the specified type.
    ///
    /// FIXME: Consider deleting this function. It is mostly
    /// redundant, and it is inefficient if you want to also get a
    /// reference to the table, or if you want to check for multiple
    /// table types.
    template<class T> bool is_subtable() const
    {
        // FIXME: Conversion from TableRef to ConstTableRef is relatively expensive, or is it? Check whether it involves access to the reference count!
        ConstTableRef t = static_cast<const FieldAccessor*>(this)->get_subtable();
        return t && T::matches_dynamic_type(TableFriend::get_spec(*t));
    }

    /// Generally more efficient that get_subtable()->size().
    std::size_t get_subtable_size() const REALM_NOEXCEPT
    {
        return Base::m_table->get_impl()->get_subtable_size(column_index, Base::m_row_index);
    }

    template<class T> friend bool operator==(const FieldAccessor& a, const T& b) REALM_NOEXCEPT
    {
        return a.get() == b;
    }

    template<class T> friend bool operator!=(const FieldAccessor& a, const T& b) REALM_NOEXCEPT
    {
        return a.get() != b;
    }

    template<class T> friend bool operator==(const T& a, const FieldAccessor& b) REALM_NOEXCEPT
    {
        return a == b.get();
    }

    template<class T> friend bool operator!=(const T& a, const FieldAccessor& b) REALM_NOEXCEPT
    {
        return a != b.get();
    }

protected:
    MixedFieldAccessorBase(typename Base::Init i) REALM_NOEXCEPT: Base(i) {}
};


/// Field accessor specialization for mixed type of non-const parent.
template<class Taboid, int column_index>
class FieldAccessor<Taboid, column_index, Mixed, false>:
    public MixedFieldAccessorBase<Taboid, column_index, FieldAccessor<Taboid, column_index, Mixed, false>> {
private:
    typedef FieldAccessor<Taboid, column_index, Mixed, false> This;
    typedef MixedFieldAccessorBase<Taboid, column_index, This> Base;

public:
    /// Returns null if the current value is not a subtable.
    TableRef get_subtable() const
    {
        return Base::m_table->get_impl()->get_subtable(column_index, Base::m_row_index);
    }

    /// Overwrites the current value with an empty subtable and
    /// returns a reference to it.
    TableRef set_subtable() const
    {
        Base::m_table->get_impl()->clear_subtable(column_index, Base::m_row_index);
        return get_subtable();
    }

    /// Overwrites the current value with a copy of the specified
    /// table and returns a reference to the copy.
    TableRef set_subtable(const Table& t) const
    {
        t.set_into_mixed(Base::m_table->get_impl(), column_index, Base::m_row_index);
        return get_subtable();
    }

    /// This function makes the following assumption: If the current
    /// value is a subtable, then it is a subtable of the specified
    /// type. If this is not the case, your computer may catch fire.
    ///
    /// To safely and efficiently check whether the current value is a
    /// subtable of any of a set of specific table types, you may do
    /// as follows:
    ///
    /// \code{.cpp}
    ///
    ///   if (TableRef subtable = my_table[i].mixed.get_subtable()) {
    ///     if (subtable->is_a<MyFirstSubtable>()) {
    ///       MyFirstSubtable::Ref s = unchecked_cast<MyFirstSubtable>(move(subtable))) {
    ///       // ...
    ///     }
    ///     else if (subtable->is_a<MySecondSubtable>()) {
    ///       MySecondSubtable::Ref s = unchecked_cast<MySecondSubtable>(move(subtable))) {
    ///       // ...
    ///     }
    ///   }
    ///
    /// \endcode
    ///
    /// \return Null if the current value is not a subtable.
    ///
    /// \note This function is generally unsafe because it does not
    /// check that the specified table type matches the actual table
    /// type.
    ///
    /// FIXME: Consider deleting this function, since it is both
    /// unsafe and superfluous.
    template<class T> BasicTableRef<T> get_subtable() const
    {
        REALM_ASSERT(!Base::is_subtable() || Base::template is_subtable<T>());
        return unchecked_cast<T>(get_subtable());
    }

    /// Overwrites the current value with an empty subtable and
    /// returns a reference to it.
    ///
    /// \tparam T The subtable type. It must not be const-qualified.
    template<class T> BasicTableRef<T> set_subtable() const
    {
        BasicTableRef<T> t = unchecked_cast<T>(set_subtable());
        T::set_dynamic_type(*t);
        return t;
    }

    /// Overwrites the current value with a copy of the specified
    /// table and returns a reference to the copy.
    template<class T> typename T::Ref set_subtable(const T& t) const
    {
        t.set_into_mixed(Base::m_table->get_impl(), column_index, Base::m_row_index);
        return unchecked_cast<T>(get_subtable());
    }


    explicit FieldAccessor(typename Base::Init i) REALM_NOEXCEPT: Base(i) {}
};


/// Field accessor specialization for mixed type of const parent.
template<class Taboid, int column_index>
class FieldAccessor<Taboid, column_index, Mixed, true>:
    public MixedFieldAccessorBase<Taboid, column_index, FieldAccessor<Taboid, column_index, Mixed, true>> {
private:
    typedef FieldAccessor<Taboid, column_index, Mixed, true> This;
    typedef MixedFieldAccessorBase<Taboid, column_index, This> Base;

public:
    ConstTableRef get_subtable() const
    {
        return Base::m_table->get_impl()->get_subtable(column_index, Base::m_row_index);
    }

    /// FIXME: Consider deleting this function, since it is both
    /// unsafe and superfluous.
    template<class T> BasicTableRef<const T> get_subtable() const
    {
        REALM_ASSERT(!Base::is_subtable() || Base::template is_subtable<T>());
        return unchecked_cast<const T>(get_subtable());
    }


    explicit FieldAccessor(typename Base::Init i) REALM_NOEXCEPT: Base(i) {}
};




/// This class gives access to a column of a table.
///
/// \tparam Taboid Either a table or a table view. Constness of access
/// is controlled by what is allowed to be done with/on a 'Taboid*'.
template<class Taboid, int column_index, class Type> class ColumnAccessor;


/// Commmon base class for all column accessor specializations.
template<class Taboid, int column_index, class Type> class ColumnAccessorBase {
protected:
    typedef typename GetTableFromView<Taboid>::type RealTable;
    typedef FieldAccessor<Taboid, column_index, Type, TableIsConst<Taboid>::value> Field;

public:
    Field operator[](std::size_t row_index) const
    {
        return Field(std::make_pair(m_table, row_index));
    }

    bool has_search_index() const { return m_table->get_impl()->has_search_index(column_index); }
    void add_search_index() const { m_table->get_impl()->add_search_index(column_index); }
    void remove_search_index() const { m_table->get_impl()->remove_search_index(column_index); }

    BasicTableView<RealTable> get_sorted_view(bool ascending=true) const
    {
        return m_table->get_impl()->get_sorted_view(column_index, ascending);
    }

    void sort(bool ascending = true) const { m_table->get_impl()->sort(column_index, ascending); }

protected:
    Taboid* const m_table;

    explicit ColumnAccessorBase(Taboid* t) REALM_NOEXCEPT: m_table(t) {}
};


/// Column accessor specialization for integers.
template<class Taboid, int column_index>
class ColumnAccessor<Taboid, column_index, int64_t>:
    public ColumnAccessorBase<Taboid, column_index, int64_t>, public Columns<int64_t> {
private:
    typedef ColumnAccessorBase<Taboid, column_index, int64_t> Base;

public:
    explicit ColumnAccessor(Taboid* t) REALM_NOEXCEPT: Base(t) {
        // Columns store their own copy of m_table in order not to have too much class dependency/entanglement
        Columns<int64_t>::m_column = column_index;
        Columns<int64_t>::m_table = reinterpret_cast<const Table*>(Base::m_table->get_impl());
    }

    // fixme/todo, reinterpret_cast to make it compile with TableView which is not supported yet
    virtual Subexpr& clone() {
        return *new Columns<int64_t>(column_index, reinterpret_cast<const Table*>(Base::m_table->get_impl()));
    }

    std::size_t find_first(int64_t value) const
    {
        return Base::m_table->get_impl()->find_first_int(column_index, value);
    }

    BasicTableView<typename Base::RealTable> find_all(int64_t value) const
    {
        return Base::m_table->get_impl()->find_all_int(column_index, value);
    }

    BasicTableView<typename Base::RealTable> get_distinct_view() const
    {
        return Base::m_table->get_impl()->get_distinct_view(column_index);
    }

    size_t count(int64_t target) const
    {
        return Base::m_table->get_impl()->count_int(column_index, target);
    }

    int64_t sum() const
    {
        return Base::m_table->get_impl()->sum_int(column_index);
    }

    int64_t maximum(std::size_t* return_index = 0) const
    {
        return Base::m_table->get_impl()->maximum_int(column_index, return_index);
    }

    int64_t minimum(std::size_t* return_index = 0) const
    {
        return Base::m_table->get_impl()->minimum_int(column_index, return_index);
    }

    double average() const
    {
        return Base::m_table->get_impl()->average_int(column_index);
    }

    std::size_t lower_bound(int64_t value) const REALM_NOEXCEPT
    {
        return Base::m_table->lower_bound_int(column_index, value);
    }

    std::size_t upper_bound(int64_t value) const REALM_NOEXCEPT
    {
        return Base::m_table->upper_bound_int(column_index, value);
    }
};


/// Column accessor specialization for float
template<class Taboid, int column_index>
class ColumnAccessor<Taboid, column_index, float>:
    public ColumnAccessorBase<Taboid, column_index, float>, public Columns<float> {
private:
    typedef ColumnAccessorBase<Taboid, column_index, float> Base;

public:
    explicit ColumnAccessor(Taboid* t) REALM_NOEXCEPT: Base(t) {
        // Columns store their own copy of m_table in order not to have too much class dependency/entanglement
        Columns<float>::m_column = column_index;
        Columns<float>::m_table = reinterpret_cast<const Table*>(Base::m_table->get_impl());
    }

    // fixme/todo, reinterpret_cast to make it compile with TableView which is not supported yet
    virtual Subexpr& clone() {
        return *new Columns<float>(column_index, reinterpret_cast<const Table*>(Base::m_table->get_impl()));
    }

    std::size_t find_first(float value) const
    {
        return Base::m_table->get_impl()->find_first_float(column_index, value);
    }

    BasicTableView<typename Base::RealTable> find_all(float value) const
    {
        return Base::m_table->get_impl()->find_all_float(column_index, value);
    }

    BasicTableView<typename Base::RealTable> get_distinct_view() const
    {
        return Base::m_table->get_impl()->get_distinct_view(column_index);
    }

    size_t count(float target) const
    {
        return Base::m_table->get_impl()->count_float(column_index, target);
    }

    double sum() const
    {
        return Base::m_table->get_impl()->sum_float(column_index);
    }

    float maximum(std::size_t* return_index = 0) const
    {
        return Base::m_table->get_impl()->maximum_float(column_index, return_index);
    }

    float minimum(std::size_t* return_index = 0) const
    {
        return Base::m_table->get_impl()->minimum_float(column_index, return_index);
    }

    double average() const
    {
        return Base::m_table->get_impl()->average_float(column_index);
    }

    const ColumnAccessor& operator+=(float value) const
    {
        Base::m_table->get_impl()->add_float(column_index, value);
        return *this;
    }

    std::size_t lower_bound(float value) const REALM_NOEXCEPT
    {
        return Base::m_table->lower_bound_float(column_index, value);
    }

    std::size_t upper_bound(float value) const REALM_NOEXCEPT
    {
        return Base::m_table->upper_bound_float(column_index, value);
    }
};


/// Column accessor specialization for double
template<class Taboid, int column_index>
class ColumnAccessor<Taboid, column_index, double>:
    public ColumnAccessorBase<Taboid, column_index, double>, public Columns<double> {
private:
    typedef ColumnAccessorBase<Taboid, column_index, double> Base;

public:
    explicit ColumnAccessor(Taboid* t) REALM_NOEXCEPT: Base(t) {
        // Columns store their own copy of m_table in order not to have too much class dependency/entanglement
        Columns<double>::m_column = column_index;
        Columns<double>::m_table = reinterpret_cast<const Table*>(Base::m_table->get_impl());
    }

    // fixme/todo, reinterpret_cast to make it compile with TableView which is not supported yet
    virtual Subexpr& clone() {
        return *new Columns<double>(column_index, reinterpret_cast<const Table*>(Base::m_table->get_impl()));
    }

    std::size_t find_first(double value) const
    {
        return Base::m_table->get_impl()->find_first_double(column_index, value);
    }

    BasicTableView<typename Base::RealTable> find_all(double value) const
    {
        return Base::m_table->get_impl()->find_all_double(column_index, value);
    }

    BasicTableView<typename Base::RealTable> get_distinct_view() const
    {
        return Base::m_table->get_impl()->get_distinct_view(column_index);
    }

    size_t count(double target) const
    {
        return Base::m_table->get_impl()->count_double(column_index, target);
    }

    double sum() const
    {
        return Base::m_table->get_impl()->sum_double(column_index);
    }

    double maximum(std::size_t* return_index = 0) const
    {
        return Base::m_table->get_impl()->maximum_double(column_index, return_index);
    }

    double minimum(std::size_t* return_index = 0) const
    {
        return Base::m_table->get_impl()->minimum_double(column_index, return_index);
    }

    double average() const
    {
        return Base::m_table->get_impl()->average_double(column_index);
    }

    const ColumnAccessor& operator+=(double value) const
    {
        Base::m_table->get_impl()->add_double(column_index, value);
        return *this;
    }

    std::size_t lower_bound(float value) const REALM_NOEXCEPT
    {
        return Base::m_table->lower_bound_double(column_index, value);
    }

    std::size_t upper_bound(float value) const REALM_NOEXCEPT
    {
        return Base::m_table->upper_bound_double(column_index, value);
    }
};


/// Column accessor specialization for booleans.
template<class Taboid, int column_index>
class ColumnAccessor<Taboid, column_index, bool>: public ColumnAccessorBase<Taboid, column_index, bool> {
private:
    typedef ColumnAccessorBase<Taboid, column_index, bool> Base;

public:
    explicit ColumnAccessor(Taboid* t) REALM_NOEXCEPT: Base(t) {}

    std::size_t find_first(bool value) const
    {
        return Base::m_table->get_impl()->find_first_bool(column_index, value);
    }

    BasicTableView<typename Base::RealTable> find_all(bool value) const
    {
        return Base::m_table->get_impl()->find_all_bool(column_index, value);
    }

    std::size_t lower_bound(bool value) const REALM_NOEXCEPT
    {
        return Base::m_table->lower_bound_bool(column_index, value);
    }

    std::size_t upper_bound(bool value) const REALM_NOEXCEPT
    {
        return Base::m_table->upper_bound_bool(column_index, value);
    }

    BasicTableView<typename Base::RealTable> get_distinct_view() const
    {
        return Base::m_table->get_impl()->get_distinct_view(column_index);
    }
};


/// Column accessor specialization for enumerations.
template<class Taboid, int column_index, class E>
class ColumnAccessor<Taboid, column_index, SpecBase::Enum<E>>:
    public ColumnAccessorBase<Taboid, column_index, SpecBase::Enum<E>> {
private:
    typedef ColumnAccessorBase<Taboid, column_index, SpecBase::Enum<E>> Base;

public:
    explicit ColumnAccessor(Taboid* t) REALM_NOEXCEPT: Base(t) {}

    std::size_t find_first(E value) const
    {
        return Base::m_table->get_impl()->find_first_int(column_index, int64_t(value));
    }

    BasicTableView<typename Base::RealTable> find_all(E value) const
    {
        return Base::m_table->get_impl()->find_all_int(column_index, int64_t(value));
    }

    BasicTableView<typename Base::RealTable> get_distinct_view() const
    {
        return Base::m_table->get_impl()->get_distinct_view(column_index);
    }
};


/// Column accessor specialization for dates.
template<class Taboid, int column_index>
class ColumnAccessor<Taboid, column_index, DateTime>: public ColumnAccessorBase<Taboid, column_index, DateTime> {
private:
    typedef ColumnAccessorBase<Taboid, column_index, DateTime> Base;

public:
    explicit ColumnAccessor(Taboid* t) REALM_NOEXCEPT: Base(t) {}

    DateTime maximum(std::size_t* return_index = 0) const
    {
        return Base::m_table->get_impl()->maximum_datetime(column_index, return_index);
    }

    DateTime minimum(std::size_t* return_index = 0) const
    {
        return Base::m_table->get_impl()->minimum_datetime(column_index, return_index);
    }

    std::size_t find_first(DateTime value) const
    {
        return Base::m_table->get_impl()->find_first_datetime(column_index, value);
    }

    BasicTableView<typename Base::RealTable> find_all(DateTime value) const
    {
        return Base::m_table->get_impl()->find_all_datetime(column_index, value);
    }

    BasicTableView<typename Base::RealTable> get_distinct_view() const
    {
        return Base::m_table->get_impl()->get_distinct_view(column_index);
    }
};


/// Column accessor specialization for strings.
template<class Taboid, int column_index>
class ColumnAccessor<Taboid, column_index, StringData>:
    public ColumnAccessorBase<Taboid, column_index, StringData>, public Columns<StringData> {
private:
    typedef ColumnAccessorBase<Taboid, column_index, StringData> Base;
public:
    explicit ColumnAccessor(Taboid* t) REALM_NOEXCEPT: Base(t) {
        // Columns store their own copy of m_table in order not to have too much class dependency/entanglement
        Columns<StringData>::m_column = column_index;
        Columns<StringData>::m_table = reinterpret_cast<const Table*>(Base::m_table->get_impl());
    }

    size_t count(StringData value) const
    {
        return Base::m_table->get_impl()->count_string(column_index, value);
    }

    std::size_t find_first(StringData value) const
    {
        return Base::m_table->get_impl()->find_first_string(column_index, value);
    }

    BasicTableView<typename Base::RealTable> find_all(StringData value) const
    {
        return Base::m_table->get_impl()->find_all_string(column_index, value);
    }

    BasicTableView<typename Base::RealTable> get_distinct_view() const
    {
        return Base::m_table->get_impl()->get_distinct_view(column_index);
    }

    std::size_t lower_bound(StringData value) const REALM_NOEXCEPT
    {
        return Base::m_table->lower_bound_string(column_index, value);
    }

    std::size_t upper_bound(StringData value) const REALM_NOEXCEPT
    {
        return Base::m_table->upper_bound_string(column_index, value);
    }
};


/// Column accessor specialization for binary data.
template<class Taboid, int column_index>
class ColumnAccessor<Taboid, column_index, BinaryData>:
    public ColumnAccessorBase<Taboid, column_index, BinaryData> {
private:
    typedef ColumnAccessorBase<Taboid, column_index, BinaryData> Base;

public:
    explicit ColumnAccessor(Taboid* t) REALM_NOEXCEPT: Base(t) {}

    std::size_t find_first(const BinaryData &value) const
    {
        return Base::m_table->get_impl()->find_first_binary(column_index, value.data(), value.size());
    }

    BasicTableView<typename Base::RealTable> find_all(const BinaryData &value) const
    {
        return Base::m_table->get_impl()->find_all_binary(column_index, value.data(), value.size());
    }
};


/// Column accessor specialization for subtables.
template<class Taboid, int column_index, class Subtab>
class ColumnAccessor<Taboid, column_index, SpecBase::Subtable<Subtab>>:
    public ColumnAccessorBase<Taboid, column_index, SpecBase::Subtable<Subtab>> {
private:
    typedef ColumnAccessorBase<Taboid, column_index, SpecBase::Subtable<Subtab>> Base;

public:
    explicit ColumnAccessor(Taboid* t) REALM_NOEXCEPT: Base(t) {}
};


/// Column accessor specialization for mixed type.
template<class Taboid, int column_index>
class ColumnAccessor<Taboid, column_index, Mixed>: public ColumnAccessorBase<Taboid, column_index, Mixed> {
private:
    typedef ColumnAccessorBase<Taboid, column_index, Mixed> Base;

public:
    explicit ColumnAccessor(Taboid* t) REALM_NOEXCEPT: Base(t) {}
};



/// ***********************************************************************************************
/// This class implements a column of a table as used in a table query.
///
/// \tparam Taboid Matches either 'BasicTable<Spec>' or
/// 'BasicTableView<Tab>'. Neither may be const-qualified.
///
/// FIXME: These do not belong in this file!
template<class Taboid, int column_index, class Type> class QueryColumn;


/// Commmon base class for all query column specializations.
template<class Taboid, int column_index, class Type> class QueryColumnBase {
protected:
    typedef typename Taboid::Query Query;
    Query* const m_query;
    explicit QueryColumnBase(Query* q) REALM_NOEXCEPT: m_query(q) {}

    Query& equal(const Type& value) const
    {
        m_query->m_impl.equal(column_index, value);
        return *m_query;
    }

    Query& not_equal(const Type& value) const
    {
        m_query->m_impl.not_equal(column_index, value);
        return *m_query;
    }
};


/// QueryColumn specialization for integers.
template<class Taboid, int column_index>
class QueryColumn<Taboid, column_index, int64_t>: public QueryColumnBase<Taboid, column_index, int64_t> {
private:
    typedef QueryColumnBase<Taboid, column_index, int64_t> Base;
    typedef typename Taboid::Query Query;

public:
    explicit QueryColumn(Query* q) REALM_NOEXCEPT: Base(q) {}

    // Todo, these do not turn up in Visual Studio 2013 intellisense
    using Base::equal;
    using Base::not_equal;

    Query& greater(int64_t value) const
    {
        Base::m_query->m_impl.greater(column_index, value);
        return *Base::m_query;
    }

    Query& greater_equal(int64_t value) const
    {
        Base::m_query->m_impl.greater_equal(column_index, value);
        return *Base::m_query;
    }

    Query& less(int64_t value) const
    {
        Base::m_query->m_impl.less(column_index, value);
        return *Base::m_query;
    }

    Query& less_equal(int64_t value) const
    {
        Base::m_query->m_impl.less_equal(column_index, value);
        return *Base::m_query;
    }

    Query& between(int64_t from, int64_t to) const
    {
        Base::m_query->m_impl.between(column_index, from, to);
        return *Base::m_query;
    };

    int64_t sum(std::size_t* resultcount = 0, std::size_t start = 0,
                std::size_t end = std::size_t(-1), std::size_t limit=std::size_t(-1)) const
    {
        return Base::m_query->m_impl.sum_int(column_index, resultcount, start, end, limit);
    }

    int64_t maximum(std::size_t* resultcount = 0, std::size_t start = 0,
                    std::size_t end = std::size_t(-1), std::size_t limit=std::size_t(-1), 
                    std::size_t* return_index = 0) const
    {
        return Base::m_query->m_impl.maximum_int(column_index, resultcount, start, end, limit, return_index);
    }

    int64_t minimum(std::size_t* resultcount = 0, std::size_t start = 0,
                    std::size_t end = std::size_t(-1), std::size_t limit=std::size_t(-1),
                    std::size_t* return_index = 0) const
    {
        return Base::m_query->m_impl.minimum_int(column_index, resultcount, start, end, limit, return_index);
    }

    double average(std::size_t* resultcount = 0, std::size_t start = 0,
                   std::size_t end=std::size_t(-1), std::size_t limit=std::size_t(-1)) const
    {
        return Base::m_query->m_impl.average_int(column_index, resultcount, start, end, limit);
    }
};



/// QueryColumn specialization for floats.
template<class Taboid, int column_index>
class QueryColumn<Taboid, column_index, float>: public QueryColumnBase<Taboid, column_index, float> {
private:
    typedef QueryColumnBase<Taboid, column_index, float> Base;
    typedef typename Taboid::Query Query;

public:
    explicit QueryColumn(Query* q) REALM_NOEXCEPT: Base(q) {}
    using Base::equal;
    using Base::not_equal;

    Query& greater(float value) const
    {
        Base::m_query->m_impl.greater(column_index, value);
        return *Base::m_query;
    }

    Query& greater_equal(float value) const
    {
        Base::m_query->m_impl.greater_equal(column_index, value);
        return *Base::m_query;
    }

    Query& less(float value) const
    {
        Base::m_query->m_impl.less(column_index, value);
        return *Base::m_query;
    }

    Query& less_equal(float value) const
    {
        Base::m_query->m_impl.less_equal(column_index, value);
        return *Base::m_query;
    }

    Query& between(float from, float to) const
    {
        Base::m_query->m_impl.between(column_index, from, to);
        return *Base::m_query;
    };

    double sum(std::size_t* resultcount = 0, std::size_t start = 0,
               std::size_t end = std::size_t(-1), std::size_t limit=std::size_t(-1)) const
    {
        return Base::m_query->m_impl.sum_float(column_index, resultcount, start, end, limit);
    }

    float maximum(std::size_t* resultcount = 0, std::size_t start = 0,
                    std::size_t end = std::size_t(-1), std::size_t limit=std::size_t(-1),
                    std::size_t* return_index = 0) const
    {
        return Base::m_query->m_impl.maximum_float(column_index, resultcount, start, end, limit, return_index);
    }

    float minimum(std::size_t* resultcount = 0, std::size_t start = 0,
                    std::size_t end = std::size_t(-1), std::size_t limit=std::size_t(-1),
                    std::size_t* return_index = 0) const
    {
        return Base::m_query->m_impl.minimum_float(column_index, resultcount, start, end, limit, return_index);
    }

    double average(std::size_t* resultcount = 0, std::size_t start = 0,
                   std::size_t end=std::size_t(-1), std::size_t limit=std::size_t(-1)) const
    {
        return Base::m_query->m_impl.average_float(column_index, resultcount, start, end, limit);
    }
};



/// QueryColumn specialization for doubles.
template<class Taboid, int column_index>
class QueryColumn<Taboid, column_index, double>: public QueryColumnBase<Taboid, column_index, double> {
private:
    typedef QueryColumnBase<Taboid, column_index, double> Base;
    typedef typename Taboid::Query Query;

public:
    explicit QueryColumn(Query* q) REALM_NOEXCEPT: Base(q) {}
    using Base::equal;
    using Base::not_equal;

    Query& greater(double value) const
    {
        Base::m_query->m_impl.greater(column_index, value);
        return *Base::m_query;
    }

    Query& greater_equal(double value) const
    {
        Base::m_query->m_impl.greater_equal(column_index, value);
        return *Base::m_query;
    }

    Query& less(double value) const
    {
        Base::m_query->m_impl.less(column_index, value);
        return *Base::m_query;
    }

    Query& less_equal(double value) const
    {
        Base::m_query->m_impl.less_equal(column_index, value);
        return *Base::m_query;
    }

    Query& between(double from, double to) const
    {
        Base::m_query->m_impl.between(column_index, from, to);
        return *Base::m_query;
    };

    double sum(std::size_t* resultcount = 0, std::size_t start = 0,
               std::size_t end = std::size_t(-1), std::size_t limit=std::size_t(-1)) const
    {
        return Base::m_query->m_impl.sum_double(column_index, resultcount, start, end, limit);
    }

    double maximum(std::size_t* resultcount = 0, std::size_t start = 0,
                    std::size_t end = std::size_t(-1), std::size_t limit=std::size_t(-1),
                    std::size_t* return_index = 0) const
    {
        return Base::m_query->m_impl.maximum_double(column_index, resultcount, start, end, limit, return_index);
    }

    double minimum(std::size_t* resultcount = 0, std::size_t start = 0,
                    std::size_t end = std::size_t(-1), std::size_t limit=std::size_t(-1),
                    std::size_t* return_index = 0) const
    {
        return Base::m_query->m_impl.minimum_double(column_index, resultcount, start, end, limit, return_index);
    }

    double average(std::size_t* resultcount = 0, std::size_t start = 0,
                   std::size_t end=std::size_t(-1), std::size_t limit=std::size_t(-1)) const
    {
        return Base::m_query->m_impl.average_double(column_index, resultcount, start, end, limit);
    }
};



/// QueryColumn specialization for booleans.
template<class Taboid, int column_index>
class QueryColumn<Taboid, column_index, bool>: public QueryColumnBase<Taboid, column_index, bool> {
private:
    typedef QueryColumnBase<Taboid, column_index, bool> Base;
    typedef typename Taboid::Query Query;

public:
    explicit QueryColumn(Query* q) REALM_NOEXCEPT: Base(q) {}
    using Base::equal;
    using Base::not_equal;
};


/// QueryColumn specialization for enumerations.
template<class Taboid, int column_index, class E>
class QueryColumn<Taboid, column_index, SpecBase::Enum<E>>:
    public QueryColumnBase<Taboid, column_index, SpecBase::Enum<E>> {
private:
    typedef QueryColumnBase<Taboid, column_index, SpecBase::Enum<E>> Base;
    typedef typename Taboid::Query Query;

public:
    explicit QueryColumn(Query* q) REALM_NOEXCEPT: Base(q) {}
    using Base::equal;
    using Base::not_equal;
};


/// QueryColumn specialization for dates.
template<class Taboid, int column_index>
class QueryColumn<Taboid, column_index, DateTime>: public QueryColumnBase<Taboid, column_index, DateTime> {
private:
    typedef QueryColumnBase<Taboid, column_index, DateTime> Base;
    typedef typename Taboid::Query Query;

public:
    explicit QueryColumn(Query* q) REALM_NOEXCEPT: Base(q) {}

    Query& equal(DateTime value) const
    {
        Base::m_query->m_impl.equal_datetime(column_index, value);
        return *Base::m_query;
    }

    Query& not_equal(DateTime value) const
    {
        Base::m_query->m_impl.not_equal_datetime(column_index, value);
        return *Base::m_query;
    }

    Query& greater(DateTime value) const
    {
        Base::m_query->m_impl.greater_datetime(column_index, value);
        return *Base::m_query;
    }

    Query& greater_equal(DateTime value) const
    {
        Base::m_query->m_impl.greater_equal_datetime(column_index, value);
        return *Base::m_query;
    }

    Query& less(DateTime value) const
    {
        Base::m_query->m_impl.less_datetime(column_index, value);
        return *Base::m_query;
    }

    Query& less_equal(DateTime value) const
    {
        Base::m_query->m_impl.less_equal_datetime(column_index, value);
        return *Base::m_query;
    }

    Query& between(DateTime from, DateTime to) const
    {
        Base::m_query->m_impl.between_datetime(column_index, from, to);
        return *Base::m_query;
    };

    DateTime maximum(std::size_t* resultcount = 0, std::size_t start = 0,
                 std::size_t end = std::size_t(-1), std::size_t limit=std::size_t(-1),
                 std::size_t* return_index = 0) const
    {
        return Base::m_query->m_impl.maximum_datetime(column_index, resultcount, start, end, limit, return_index);
    }

    DateTime minimum(std::size_t* resultcount = 0, std::size_t start = 0,
                 std::size_t end = std::size_t(-1), std::size_t limit=std::size_t(-1),
                 std::size_t* return_index = 0) const
    {
        return Base::m_query->m_impl.minimum_datetime(column_index, resultcount, start, end, limit, return_index);
    }
};


/// QueryColumn specialization for strings.
template<class Taboid, int column_index>
class QueryColumn<Taboid, column_index, StringData>:
    public QueryColumnBase<Taboid, column_index, StringData> {
private:
    typedef QueryColumnBase<Taboid, column_index, StringData> Base;
    typedef typename Taboid::Query Query;

public:
    explicit QueryColumn(Query* q) REALM_NOEXCEPT: Base(q) {}

    Query& equal(StringData value, bool case_sensitive=true) const
    {
        Base::m_query->m_impl.equal(column_index, value, case_sensitive);
        return *Base::m_query;
    }

    Query& not_equal(StringData value, bool case_sensitive=true) const
    {
        Base::m_query->m_impl.not_equal(column_index, value, case_sensitive);
        return *Base::m_query;
    }

    Query& begins_with(StringData value, bool case_sensitive=true) const
    {
        Base::m_query->m_impl.begins_with(column_index, value, case_sensitive);
        return *Base::m_query;
    }

    Query& ends_with(StringData value, bool case_sensitive=true) const
    {
        Base::m_query->m_impl.ends_with(column_index, value, case_sensitive);
        return *Base::m_query;
    }

    Query& contains(StringData value, bool case_sensitive=true) const
    {
        Base::m_query->m_impl.contains(column_index, value, case_sensitive);
        return *Base::m_query;
    }
};


/// QueryColumn specialization for binary data.
template<class Taboid, int column_index>
class QueryColumn<Taboid, column_index, BinaryData>:
    public QueryColumnBase<Taboid, column_index, BinaryData> {
private:
    typedef QueryColumnBase<Taboid, column_index, BinaryData> Base;
    typedef typename Taboid::Query Query;

public:
    explicit QueryColumn(Query* q) REALM_NOEXCEPT: Base(q) {}

    Query& equal(BinaryData value) const
    {
        Base::m_query->m_impl.equal(column_index, value);
        return *Base::m_query;
    }

    Query& not_equal(BinaryData value) const
    {
        Base::m_query->m_impl.not_equal(column_index, value);
        return *Base::m_query;
    }

    Query& begins_with(BinaryData value) const
    {
        Base::m_query->m_impl.begins_with(column_index, value);
        return *Base::m_query;
    }

    Query& ends_with(BinaryData value) const
    {
        Base::m_query->m_impl.ends_with(column_index, value);
        return *Base::m_query;
    }

    Query& contains(BinaryData value) const
    {
        Base::m_query->m_impl.contains(column_index, value);
        return *Base::m_query;
    }
};


/// QueryColumn specialization for subtables.
template<class Taboid, int column_index, class Subtab>
class QueryColumn<Taboid, column_index, SpecBase::Subtable<Subtab>> {
private:
    typedef typename Taboid::Query Query;
    Query* const m_query;

public:
    explicit QueryColumn(Query* q) REALM_NOEXCEPT: m_query(q) {}

    Query& subtable()
    {
        m_query->m_impl.subtable(column_index);
        return *m_query;
    }
};


/// QueryColumn specialization for mixed type.
template<class Taboid, int column_index> class QueryColumn<Taboid, column_index, Mixed> {
private:
    typedef typename Taboid::Query Query;

public:
    explicit QueryColumn(Query*) REALM_NOEXCEPT {}
};


} // namespace _impl
} // namespaced realm

#endif // REALM_TABLE_ACCESSORS_HPP
