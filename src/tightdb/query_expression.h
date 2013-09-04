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

/*
This file lets you write queries in C++ syntax like: Expression* e = (first + 1 / second >= third + 12.3);

Type conversion/promotion semantics is the same as in the C++ expressions, e.g float + int > double == float + 
(float)int > double. Grammar and classes:
-----------------------------------------------------------------------------------------------------------------------
    Expression:         Subexpr2<T>  Compare<Cond, T>  Subexpr2<T>

    Subexpr2<T>:        Value<T>
                        Columns<T>
                        Subexpr2<T>  Operator<Oper<T>  Subexpr2<T>
              
    Value<T>:           T

    Operator<Oper<T>>:  +, -, *, /
              
    Compare<Cond, T>:   ==, !=, >=, <=, >, <

    T:                  int, int64_t, float, double

-----------------------------------------------------------------------------------------------------------------------
The Value<T> type can contain values and is used for both user-specified constants and internal intermediate results.

All Subexpr2<T> subclasses (Value, Columns and Operator) contain the method:

    void evaluate(size_t i, ValueBase* destination) 
    
which returns a Value<T> representing the value for table row i. This evaluate() is called recursively by Operator if 
you have syntax trees with multiple levels of Subexpr2.

Furthermore, Value<T> actually contains 8 concecutive values and all operations are based on these chunks. This is
to save overhead by virtual calls needed for evaluating a query that has been dynamically constructed at runtime.

The Compare class contains the method:

    size_t compare(size_t start, size_t end)

which performs the final query search. 
-----------------------------------------------------------------------------------------------------------------------

Todo:    
    * Use query_engine.hpp for execution of simple queries that are supported there, because it's faster
    * Support unary operators like power and sqrt
    * The name Columns for query-expression-columns can be confusing

*/


#ifndef TIGHTDB_QUERY_EXPRESSION_HPP
#define TIGHTDB_QUERY_EXPRESSION_HPP

// namespace tightdb {


template<class T>struct Plus { 
    T operator()(T v1, T v2) const {return v1 + v2;} 
    typedef T type;
};

template<class T>struct Minus { 
    T operator()(T v1, T v2) const {return v1 - v2;} 
    typedef T type;
};

template<class T>struct Div { 
    T operator()(T v1, T v2) const {return v1 / v2;} 
    typedef T type;
};

template<class T>struct Mul { 
    T operator()(T v1, T v2) const {return v1 * v2;} 
    typedef T type;
};

template<class T>struct Pow { 
    T operator()(T v) const {return v * v;} 
    typedef T type;
};

// Finds a common type for T1 and T2 according to C++ conversion/promotion in arithmetic (float + int => float, etc)
template<class T1, class T2,
    bool T1_is_int = std::numeric_limits<T1>::is_integer,
    bool T2_is_int = std::numeric_limits<T2>::is_integer,
    bool T1_is_widest = (sizeof(T1) > sizeof(T2)) > struct Common;

template<class T1, class T2, bool b> struct Common<T1, T2, b, b, true > { 
    typedef T1 type; 
};

template<class T1, class T2, bool b> struct Common<T1, T2, b, b, false> {
    typedef T2 type; 
};

template<class T1, class T2, bool b> struct Common<T1, T2, false, true , b> { 
    typedef T1 type; 
};

template<class T1, class T2, bool b> struct Common<T1, T2, true , false, b> { 
    typedef T2 type; 
};


class Expression : public Query
{
public:
    Expression() {

    }

    virtual size_t compare(size_t start, size_t end) = 0;
    virtual void set_table(const Table* table) = 0;
    virtual ~Expression() {
    
    
    }

    bool m_auto_delete;
};

class ValueBase 
{
public:
    static const size_t elements = 8;
    virtual void export_int(ValueBase& destination) const = 0;
    virtual void export_float(ValueBase& destination) const = 0;
    virtual void export_int64_t(ValueBase& destination) const = 0;
    virtual void export_double(ValueBase& destination) const = 0;
    virtual void import(ValueBase& destination) = 0;
};

class Subexpr
{
public:
    virtual ~Subexpr() {
    
    }

    virtual const Subexpr& get_qexp_column() const {
        return *this;
    }

    // Values need no table attached and have no children to call set_table() on either, so do nothing
    virtual void set_table(const Table*) { }    
    virtual Table* get_table() { return NULL; }

    TIGHTDB_FORCEINLINE virtual void evaluate(size_t, ValueBase&) { TIGHTDB_ASSERT(false); }

    bool m_auto_delete;
};

class ColumnsBase {

};

template <class T> class Columns;
template <class T> class Value;
template <class T> class Subexpr2;
template <class oper, class TLeft = Subexpr, class TRight = Subexpr> class Operator;
template <class TCond, class T, class TLeft = Subexpr, class TRight = Subexpr> class Compare;

// Stores 8 values of type T. Can also exchange data with other ValueBase of different types
template<class T> class Value : public ValueBase, public Subexpr2<T>
{
public:
    Value(T v, bool auto_delete) {
        std::fill(m_v, m_v + ValueBase::elements, v); 
        Subexpr::m_auto_delete = auto_delete;
    }

public:
    Value() { }

    Value(T v) {
        Subexpr::m_auto_delete = false;
        std::fill(m_v, m_v + ValueBase::elements, v); 
    }

    TIGHTDB_FORCEINLINE void evaluate(size_t, ValueBase& destination) {
        destination.import(*this);
    }

    template <class TOperator> TIGHTDB_FORCEINLINE void fun(Value* left, Value* right) {
        TOperator o;
        for(size_t t = 0; t < ValueBase::elements; t++)
            m_v[t] = o(left->m_v[t], right->m_v[t]);
    }

    template<class D> TIGHTDB_FORCEINLINE void export2(ValueBase& destination) const
    {
        Value<D>& d = static_cast<Value<D>&>(destination);
        for(size_t t = 0; t < ValueBase::elements; t++)
            d.m_v[t] = static_cast<D>(m_v[t]);
    }

    // Import and export methods are for type conversion of expression elements that have different types
    TIGHTDB_FORCEINLINE void export_int64_t(ValueBase& destination) const
    {
        export2<int64_t>(destination);
    }

    TIGHTDB_FORCEINLINE void export_float(ValueBase& destination) const
    {
        export2<float>(destination);
    }

    TIGHTDB_FORCEINLINE void export_int(ValueBase& destination) const
    {
        export2<int>(destination);
    }

    TIGHTDB_FORCEINLINE void export_double(ValueBase& destination) const
    {
        export2<double>(destination);
    }

    TIGHTDB_FORCEINLINE void import(ValueBase& source)
    {
        if(SameType<T, int>::value)
            source.export_int(*this);
        else if(SameType<T, float>::value)
            source.export_float(*this);
        else if(SameType<T, double>::value)
            source.export_double(*this);
        else if(SameType<T, int64_t>::value)
            source.export_int64_t(*this);
        else 
            TIGHTDB_ASSERT(false);
    }

    template <class TCond> TIGHTDB_FORCEINLINE static size_t compare(Value<T>* left, Value<T>* right)
    {        
        TCond c;

        // 665 ms unrolled vs 698 for loop (vs2010)
        if(c(left->m_v[0], right->m_v[0]))
            return 0;          
        else if(c(left->m_v[1], right->m_v[1]))
            return 1;          
        else if(c(left->m_v[2], right->m_v[2]))
            return 2;          
        else if(c(left->m_v[3], right->m_v[3]))
            return 3;          
        else if(c(left->m_v[4], right->m_v[4]))
            return 4;          
        else if(c(left->m_v[5], right->m_v[5]))
            return 5;          
        else if(c(left->m_v[6], right->m_v[6]))
            return 6;          
        else if(c(left->m_v[7], right->m_v[7]))
            return 7;            
    
        return ValueBase::elements; // no match
    }

    // Performance note: Declaring values as separately named members generates faster (10% or so) code in VS2010, 
    // compared to array, even if the array accesses elements individually instead of in for-loops.
    T m_v[elements];
};


// All overloads where left-hand-side is Subexpr2<L>:
//
// left-hand-side       operator                              right-hand-side
// Subexpr2<L>          +, -, *, /, <, >, ==, !=, <=, >=      R, Subexpr2<R>
//
// For L = R = {int, int64_t, float, double}:  

class ColumnAccessorBase;

template <class L, class R> class Overloads
{
    typedef typename Common<L, R>::type CommonType;
public:

    // Arithmetic, right side constant
    Operator<Plus<CommonType> >& operator + (R right) { 
       return *new Operator<Plus<CommonType> >(static_cast<Subexpr2<L>&>(*this), *new Value<R>(right, true), true); 
    }
    Operator<Minus<CommonType> >& operator - (R right) { 
       return *new Operator<Minus<CommonType> > (static_cast<Subexpr2<L>&>(*this), *new Value<R>(right, true), true); 
    }
    Operator<Mul<CommonType> >& operator * (R right) { 
       return *new Operator<Mul<CommonType> > (static_cast<Subexpr2<L>&>(*this), *new Value<R>(right, true), true); 
    }
    Operator<Div<CommonType> >& operator / (R right) { 
        return *new Operator<Div<CommonType> > (static_cast<Subexpr2<L>&>(*this), *new Value<R>(right, true), true); 
    }    

    // Arithmetic, right side subexpression
    Operator<Plus<CommonType> >& operator + (const Subexpr2<R>& right) { 
        return *new Operator<Plus<CommonType> > (static_cast<Subexpr2<L>&>(*this), right, true); 
    }
        
    Operator<Minus<CommonType> >& operator - (const Subexpr2<R>& right) { 
        return *new Operator<Minus<CommonType> > (static_cast<Subexpr2<L>&>(*this), right, true); 
    }
    Operator<Mul<CommonType> >& operator * (const Subexpr2<R>& right) { 
        return *new Operator<Mul<CommonType> > (static_cast<Subexpr2<L>&>(*this), right, true); 
    }
    Operator<Div<CommonType> >& operator / (const Subexpr2<R>& right) { 
        return *new Operator<Div<CommonType> > (static_cast<Subexpr2<L>&>(*this), right, true); 
    }

    // Compare, right side constant
    Query operator > (R right) {
        return *new Compare<Greater, CommonType>(static_cast<Subexpr2<L>&>(*this), *new Value<R>(right, true), true);
    }
    Query operator < (R right) {
        return *new Compare<Less, CommonType>(static_cast<Subexpr2<L>&>(*this), *new Value<R>(right, true), true);
    }
    Query operator >= (R right) {
        return *new Compare<GreaterEqual, CommonType>(static_cast<Subexpr2<L>&>(*this), *new Value<R>(right, true), true);
    }
    Query operator <= (R right) {
        return *new Compare<LessEqual, CommonType>(static_cast<Subexpr2<L>&>(*this), *new Value<R>(right, true), true);
    }
    Query operator == (R right) {
        return *new Compare<Equal, CommonType>(static_cast<Subexpr2<L>&>(*this), *new Value<R>(right, true), true);
    }
    Query operator != (R right) {
        return *new Compare<NotEqual, CommonType>(static_cast<Subexpr2<L>&>(*this), *new Value<R>(right, true), true);
    }

    // Compare, right side subexpression
    Query operator == (const Subexpr2<R>& right) { 
        return *new Compare<Equal, CommonType>(static_cast<Subexpr2<L>&>(*this), right, true); 
    }
    Query operator != (const Subexpr2<R>& right) { 
        return *new Compare<NotEqual, CommonType>(static_cast<Subexpr2<L>&>(*this), right, true); 
    }
    Query operator > (const Subexpr2<R>& right) { 
        return *new Compare<Greater, CommonType>(static_cast<Subexpr2<L>&>(*this), right, true); 
    }
    Query operator < (const Subexpr2<R>& right) { 
        return *new Compare<Less, CommonType>(static_cast<Subexpr2<L>&>(*this), right, true); 
    }
    Query operator >= (const Subexpr2<R>& right) { 
        return *new Compare<GreaterEqual, CommonType>(static_cast<Subexpr2<L>&>(*this), right, true); 
    }
    Query operator <= (const Subexpr2<R>& right) { 
        return *new Compare<LessEqual, CommonType>(static_cast<Subexpr2<L>&>(*this), right, true); 
    }
};

// With this wrapper class we can define just 20 overloads inside Overloads<L, R> instead of 4 * 20 = 80.
template <class T> class Subexpr2 : public Subexpr, public Overloads<T, int>, public Overloads<T, float>, 
                                    public Overloads<T, double>, public Overloads<T, int64_t> 
{
public:
    virtual ~Subexpr2() {
    
    };

    #define TDB_U2(t, o) using Overloads<T, t>::operator o;
    #define TDB_U(o) TDB_U2(int, o) TDB_U2(float, o) TDB_U2(double, o) TDB_U2(int64_t, o)
    TDB_U(+) TDB_U(-) TDB_U(*) TDB_U(/) TDB_U(>) TDB_U(<) TDB_U(==) TDB_U(!=) TDB_U(>=) TDB_U(<=)
};


// All overloads where left-hand-side is L:
// 
// left-hand-side       operator                              right-hand-side
// L                    +, -, *, /, <, >, ==, !=, <=, >=      Subexpr2<R>
// 
// For L = R = {int, int64_t, float, double}:

// Compare
template <class R> Query operator > (double left, const Subexpr2<R>& right) { 
    return *new Compare<Greater, typename Common<R, double>::type>(*new Value<double>(left, true), right, true); 
}
template <class R> Query operator > (float left, const Subexpr2<R>& right) {
    return *new Compare<Greater, typename Common<R, float>::type>(*new Value<float>(left, true), right, true);
}
template <class R> Query operator > (int left, const Subexpr2<R>& right) {
    return *new Compare<Greater, typename Common<R, int>::type>(*new Value<int>(left, true), right, true);
}
template <class R> Query operator > (int64_t left, const Subexpr2<R>& right) {
    return *new Compare<Greater, typename Common<R, int64_t>::type>(*new Value<int64_t>(left, true), right, true); 
}
template <class R> Query operator < (double left, const Subexpr2<R>& right) {
    return *new Compare<Less, typename Common<R, double>::type>(*new Value<double>(left, true), right, true); 
}
template <class R> Query operator < (float left, const Subexpr2<R>& right) { 
    return *new Compare<Less, typename Common<R, float>::type>(*new Value<float>(left, true), right, true); 
}
template <class R> Query operator < (int left, const Subexpr2<R>& right) {
    return *new Compare<Less, typename Common<R, int>::type>(*new Value<int>(left, true), right, true);
}
template <class R> Query operator < (int64_t left, const Subexpr2<R>& right) {
    return *new Compare<Less, typename Common<R, int64_t>::type>(*new Value<int64_t>(left, true), right, true); 
}
template <class R> Query operator == (double left, const Subexpr2<R>& right) { 
    return *new Compare<Equal, typename Common<R, double>::type>(*new Value<double>(left, true), right, true);
}
template <class R> Query operator == (float left, const Subexpr2<R>& right) {
    return *new Compare<Equal, typename Common<R, float>::type>(*new Value<float>(left, true), right, true); 
}
template <class R> Query operator == (int left, const Subexpr2<R>& right) {
    return *new Compare<Equal, typename Common<R, int>::type>(*new Value<int>(left, true), right, true); 
}
template <class R> Query operator == (int64_t left, const Subexpr2<R>& right) { 
    return *new Compare<Equal, typename Common<R, int64_t>::type>(*new Value<int64_t>(left, true), right, true); 
}
template <class R> Query operator >= (double left, const Subexpr2<R>& right) {
    return *new Compare<GreaterEqual, typename Common<R, double>::type>(*new Value<double>(left, true), right, true);
}
template <class R> Query operator >= (float left, const Subexpr2<R>& right) {
    return *new Compare<GreaterEqual, typename Common<R, float>::type>(*new Value<float>(left, true), right, true);
}
template <class R> Query operator >= (int left, const Subexpr2<R>& right) {
    return *new Compare<GreaterEqual, typename Common<R, int>::type>(*new Value<int>(left, true), right, true);
}
template <class R> Query operator >= (int64_t left, const Subexpr2<R>& right) {
    return *new Compare<GreaterEqual, typename Common<R, int64_t>::type>(*new Value<int64_t>(left, true), right, true); 
}
template <class R> Query operator <= (double left, const Subexpr2<R>& right) {
    return *new Compare<LessEqual, typename Common<R, double>::type>(*new Value<double>(left, true), right, true); 
}
template <class R> Query operator <= (float left, const Subexpr2<R>& right) {
    return *new Compare<LessEqual, typename Common<R, float>::type>(*new Value<float>(left, true), right, true); 
}
template <class R> Query operator <= (int left, const Subexpr2<R>& right) {
    return *new Compare<LessEqual, typename Common<R, int>::type>(*new Value<int>(left, true), right, true); 
}
template <class R> Query operator <= (int64_t left, const Subexpr2<R>& right) { 
    return *new Compare<LessEqual, typename Common<R, int64_t>::type>(*new Value<int64_t>(left, true), right, true);
}
template <class R> Query operator != (double left, const Subexpr2<R>& right) {
    return *new Compare<NotEqual, typename Common<R, double>::type>(*new Value<double>(left, true), right, true); 
}
template <class R> Query operator != (float left, const Subexpr2<R>& right) {
    return *new Compare<NotEqual, typename Common<R, float>::type>(*new Value<float>(left, true), right, true); 
}
template <class R> Query operator != (int left, const Subexpr2<R>& right) {
    return *new Compare<NotEqual, typename Common<R, int>::type>(*new Value<int>(left, true), right, true); 
}
template <class R> Query operator != (int64_t left, const Subexpr2<R>& right) { 
    return *new Compare<NotEqual, typename Common<R, int64_t>::type>(*new Value<int64_t>(left, true), right, true);
}

// Arithmetic
template <class R> Operator<Plus<typename Common<R, double>::type> >& operator + (double left, const Subexpr2<R>& right) { 
    return *new Operator<Plus<typename Common<R, double>::type> >(*new Value<double>(left, true), right, true); 
}
template <class R> Operator<Plus<typename Common<R, float>::type> >& operator + (float left, const Subexpr2<R>& right) {
    return *new Operator<Plus<typename Common<R, float>::type> >(*new Value<float>(left, true), right, true); 
}
template <class R> Operator<Plus<typename Common<R, int>::type> >& operator + (int left, const Subexpr2<R>& right) {
    return *new Operator<Plus<typename Common<R, int>::type> >(*new Value<int>(left, true), right, true);
}
template <class R> Operator<Plus<typename Common<R, int64_t>::type> >& operator + (int64_t left, const Subexpr2<R>& right) { 
    return *new Operator<Plus<typename Common<R, int64_t>::type> >(*new Value<int64_t>(left, true), right, true);
}
template <class R> Operator<Minus<typename Common<R, double>::type> >& operator - (double left, const Subexpr2<R>& right) {
    return *new Operator<Minus<typename Common<R, double>::type> >(*new Value<double>(left, true), right, true); 
}
template <class R> Operator<Minus<typename Common<R, float>::type> >& operator - (float left, const Subexpr2<R>& right) { 
    return *new Operator<Minus<typename Common<R, float>::type> >(*new Value<float>(left, true), right, true);
}
template <class R> Operator<Minus<typename Common<R, int>::type> >& operator - (int left, const Subexpr2<R>& right) { 
    return *new Operator<Minus<typename Common<R, int>::type> >(*new Value<int>(left, true), right, true);
}
template <class R> Operator<Minus<typename Common<R, int64_t>::type> >& operator - (int64_t left, const Subexpr2<R>& right) { 
    return *new Operator<Minus<typename Common<R, int64_t>::type> >(*new Value<int64_t>(left, true), right, true); 
}
template <class R> Operator<Mul<typename Common<R, double>::type> >& operator * (double left, const Subexpr2<R>& right) {
    return *new Operator<Mul<typename Common<R, double>::type> >(*new Value<double>(left, true), right, true); 
}
template <class R> Operator<Mul<typename Common<R, float>::type> >& operator * (float left, const Subexpr2<R>& right) { 
    return *new Operator<Mul<typename Common<R, float>::type> >(*new Value<float>(left, true), right, true); 
}
template <class R> Operator<Mul<typename Common<R, int>::type> >& operator * (int left, const Subexpr2<R>& right) { 
    return *new Operator<Mul<typename Common<R, int>::type> >(*new Value<int>(left, true), right, true);
}
template <class R> Operator<Mul<typename Common<R, int64_t>::type> >& operator * (int64_t left, const Subexpr2<R>& right) { 
    return *new Operator<Mul<typename Common<R, int64_t>::type> >(*new Value<int64_t>(left, true), right, true);
}
template <class R> Operator<Div<typename Common<R, double>::type> >& operator / (double left, const Subexpr2<R>& right) { 
    return *new Operator<Div<typename Common<R, double>::type> >(*new Value<double>(left, true), right, true);
}
template <class R> Operator<Div<typename Common<R, float>::type> >& operator / (float left, const Subexpr2<R>& right) { 
    return *new Operator<Div<typename Common<R, float>::type> >*(new Value<float>(left, true), right, true);
}
template <class R> Operator<Div<typename Common<R, int>::type> >& operator / (int left, const Subexpr2<R>& right) { 
    return *new Operator<Div<typename Common<R, int>::type> >(*new Value<int>(left, true), right, true);
}
template <class R> Operator<Div<typename Common<R, int64_t>::type> >& operator / (int64_t left, const Subexpr2<R>& right) { 
    return *new Operator<Div<typename Common<R, int64_t>::type> >(*new Value<int64_t>(left, true), right, true);
}


template <class T> class Columns : public Subexpr2<T>, public ColumnsBase
{
public:
    /*
    Columns(const Columns& other)
    {
        sg = other.sg;
        m_column = other.m_column;
        m_table2 = other.m_table2;
    }    
    */
    ~Columns()
    {
        delete sg;
        sg = NULL;
    }

    explicit Columns(size_t column, bool auto_delete) : m_table2(NULL), sg(NULL)
    {
        Subexpr::m_auto_delete = auto_delete;
        m_column = column;
    }

    explicit Columns(size_t column, Table* table, bool auto_delete) : m_table2(NULL), sg(NULL)
    {
        m_column = column;
        Subexpr::m_auto_delete = auto_delete;
        set_table(table);
    }

    explicit Columns() : m_table2(NULL), sg(NULL)
    {
        Subexpr::m_auto_delete = false;
    }

    explicit Columns(size_t column) : m_table2(NULL), sg(NULL)
    {
        Subexpr::m_auto_delete = false;
        m_column = column;
    }

    virtual void set_table(const Table* table) 
    {
        m_table2 = table;
        typedef typename ColumnTypeTraits<T>::column_type ColType;
        ColType* c;
        c = (ColType*)&table->GetColumnBase(m_column);
        if(sg == NULL)
            sg = new SequentialGetter<T>();
        sg->init(c);
    }

    virtual Table* get_table() 
    {
        return (Table*) m_table2;
    }

    TIGHTDB_FORCEINLINE void evaluate(size_t i, ValueBase& destination) {
        Value<T> v;            
        sg->cache_next(i);
        if(SameType<T, int64_t>::value) {
            // int64_t leafs have a get_chunk optimization that returns an 8 int64_t values at once
            sg->m_array_ptr->get_chunk(i - sg->m_leaf_start, static_cast<Value<int64_t>*>(static_cast<ValueBase*>(&v))->m_v);
        }
        else {
            for(size_t t = 0; t < ValueBase::elements && i + t < sg->m_leaf_end; t++)
                v.m_v[t] = sg->get_next(i + t);
        }
        destination.import(v);
    }

    const Table* m_table2;
    size_t m_column;
//private:
    SequentialGetter<T>* sg;
};

template <class oper, class TLeft, class TRight> class Operator : public Subexpr2<typename oper::type>
{
public:
    // todo: get_qexp_column was a very quick/dirty hack to get a non-temporary column from ColumnAccessor. Todo, fix
    Operator(TLeft& left, const TRight& right) : 
    m_left(const_cast<TLeft&>(left.get_qexp_column())), 
    m_right(const_cast<TRight&>(right.get_qexp_column()))
    {
        Subexpr::m_auto_delete = false;
    }

    // todo, make protected
    Operator(TLeft& left, const TRight& right, bool auto_delete) :
    m_left(const_cast<TLeft&>(left.get_qexp_column())),
    m_right(const_cast<TRight&>(right.get_qexp_column()))
    {
        Subexpr::m_auto_delete = auto_delete;
    }

    ~Operator() 
    {
        if(m_left.m_auto_delete)
            delete &m_left;

        if(m_right.m_auto_delete)
            delete &m_right;

    }

    void set_table(const Table* table)
    {
        m_left.set_table(table);
        m_right.set_table(table);
    }

    Table* get_table()
    {
        Table* l = m_left.get_table();
        Table* r = m_right.get_table();
        return l ? l : r;
    }

    TIGHTDB_FORCEINLINE void evaluate(size_t i, ValueBase& destination) {
        Value<T> result;
        Value<T> left;
        Value<T> right;
/*
        // Optimize for Constant <operator> Column and Column <operator> Constant cases
        if(SameType<TLeft, Value<T> >::value && SameType<TRight, Columns<T> >::value) {
            m_right->evaluate(i, &right);
            result.template fun<oper>(static_cast<Value<T>*>(static_cast<Subexpr*>(m_left)), &right);
        }
        else if(SameType<TRight, Value<T> >::value && SameType<TLeft, Columns<T> >::value) {
            m_left->evaluate(i, &left);
            result.template fun<oper>(static_cast<Value<T>*>(static_cast<Subexpr*>(m_right)), &left);
        }
        else {
            // Avoid vtable lookups. Qualifying is required even with 'final' keyword in C++11 (664 ms vs 734)
            if(!SameType<TLeft, Subexpr>::value)
                m_left->TLeft::evaluate(i, &left);
            else */
                m_left.evaluate(i, left);
    /*
            if(!SameType<TRight, Subexpr>::value)
                m_right->TRight::evaluate(i, &right);
            else*/
                m_right.evaluate(i, right);

            result.template fun<oper>(&left, &right);
//        }
        destination.import(result);
    }

private:
    typedef typename oper::type T;

    TLeft& m_left;
    TRight& m_right;
};

template <class TCond, class T, class TLeft, class TRight> class Compare : public Expression
{
public:
    ~Compare()
    {
        if(m_left.m_auto_delete)
            delete &m_left;

        if(m_right.m_auto_delete)
            delete &m_right;
            
    }

    // todo: get_qexp_column was a very quick/dirty hack to get a non-temporary column from ColumnAccessor. Todo, fix
    Compare(TLeft& left, const TRight& right) : 
    m_left(const_cast<TLeft&>(left.get_qexp_column())), 
    m_right(const_cast<TRight&>(right.get_qexp_column()))
    {
        m_auto_delete = false;
        Query::expression(this);
        Table* t = get_table();
        if(t)
            m_table = t->get_table_ref(); // todo, review, Lasse
    }

    // todo: get_qexp_column was a very quick/dirty hack to get a non-temporary column from ColumnAccessor. Todo, fix
    Compare(TLeft& left, const TRight& right, bool auto_delete) : 
    m_left(const_cast<TLeft&>(left.get_qexp_column())), 
    m_right(const_cast<TRight&>(right.get_qexp_column()))
    {
        m_auto_delete = auto_delete;
        Query::expression(this);
        Table* t = get_table();
        if(t)
            m_table = t->get_table_ref(); // todo, review, Lasse
    }


    void set_table(const Table* table) 
    {
        m_left.set_table(table);
        m_right.set_table(table);
    }

    Table* get_table()
    {
        Table* l = m_left.get_table();
        Table* r = m_right.get_table();
        return l ? l : r;
    }

    size_t compare(size_t start, size_t end) {
        size_t match;
        Value<T> right;
        Value<T> left;

        for(; start < end; start += ValueBase::elements) {

/*
            // Save time by avoid calling evaluate() for constants of the same type as compare object
            if(SameType<TLeft, Value<T> >::value) {
                m_right->evaluate(start, &right);
                match = Value<T>::template compare<TCond>(static_cast<Value<T>*>(static_cast<Subexpr*>(m_left)), &right); 
            }
            else if(SameType<TRight, Value<T> >::value) {
                m_left->evaluate(start, &left);
                match = Value<T>::template compare<TCond>(&left, static_cast<Value<T>*>(static_cast<Subexpr*>(m_right))); 
            }
            else {
                // General case. Again avoid vtable lookup when possible
                if(!SameType<TLeft, Subexpr>::value)               
                    static_cast<TLeft*>(m_left).TLeft::evaluate(start, &left);
                else */
                    m_left.evaluate(start, left);
                /*
                if(!SameType<TRight, Subexpr>::value)               
                    m_right.TRight::evaluate(start, &right);
                else
                    */
                    m_right.evaluate(start, right);

                match = Value<T>::template compare<TCond>(&left, &right);
 //           }

            // Note the second condition that tests if match position in chunk exceeds column length
            if(match != ValueBase::elements && start + match < end)
                return start + match;
        }

        return not_found; // no match
    }
    
private: 
    TLeft& m_left;
    TRight& m_right;
};

//}
#endif // TIGHTDB_QUERY_EXPRESSION_HPP

