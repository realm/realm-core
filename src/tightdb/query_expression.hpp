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
Todo: describe strings

This file lets you write queries in C++ syntax like: Expression* e = (first + 1 / second >= third + 12.3);

Type conversion/promotion semantics is the same as in the C++ expressions, e.g float + int > double == float + 
(float)int > double. 


Grammar:
-----------------------------------------------------------------------------------------------------------------------
    Expression:         Subexpr2<T>  Compare<Cond, T>  Subexpr2<T>

    Subexpr2<T>:        Value<T>
                        Columns<T>
                        Subexpr2<T>  Operator<Oper<T>  Subexpr2<T>
              
    Value<T>:           T

    Operator<Oper<T>>:  +, -, *, /
              
    Compare<Cond, T>:   ==, !=, >=, <=, >, <

    T:                  int, int64_t, float, double


Class diagram
-----------------------------------------------------------------------------------------------------------------------
Subexpr2
    void evaluate(size_t i, ValueBase* destination)

Compare: public Subexpr2
    size_t find_first(size_t start, size_t end)     // main method that executes query

    bool m_auto_delete
    Subexpr2& m_left;                               // left expression subtree
    Subexpr2& m_right;                              // right expression subtree

Operator: public Subexpr2
    bool m_auto_delete
    Subexpr2& m_left;                               // left expression subtree 
    Subexpr2& m_right;                              // right expression subtree

Value<T>: public Subexpr2
    T m_v[8];

Columns<T>: public Subexpr2
    SequentialGetter<T> sg;                         // class bound to a column, lets you read values in a fast way
    Table* m_table;

class ColumnAccessor<>: public Columns<double>


Flow chart:
-----------------------------------------------------------------------------------------------------------------------
Compare, Operator, Value and Columns have an evaluate() method which returns a Value<T> representing the value for 
table row i. 

Value<T> actually contains 8 concecutive values and all operations are based on these chunks. This is
to save overhead by virtual calls needed for evaluating a query that has been dynamically constructed at runtime.


Memory allocation: 
-----------------------------------------------------------------------------------------------------------------------
Operator and Compare contain a 'bool m_auto_delete' which tell if their subtrees were created by the query system or by the 
end-user. If created by query system, they are deleted upon destructed of Operator and Compare.

Value and Columns given to Operator or Compare constructors are cloned with 'new' and hence deleted unconditionally
by query system.


Caveats, notes and todos
-----------------------------------------------------------------------------------------------------------------------
    * Perhaps disallow columns from two different tables in same expression
    * Support unary operators like power and sqrt
    * The name Columns (with s) an be confusing because we also have Column (without s)
    * Memory allocation: Maybe clone Compare and Operator to get rid of m_auto_delete. However, this might become
      bloated, with non-trivial copy constructors instead of defaults
    * Hack: In compare operator overloads (==, !=, >, etc), Compare class is returned as Query class, resulting in object 
      slicing. Just be aware.
    * clone() some times new's, sometimes it just returns *this. Can be confusing. Rename method or copy always.
    * We have Columns::m_table, Query::m_table and ColumnAccessorBase::m_table that point at the same thing, even with 
      ColumnAccessor<> extending Columns. So m_table is redundant, but this is in order to keep class dependencies and
      entanglement low so that the design is flexible (if you perhaps later want a Columns class that is not dependent 
      on ColumnAccessor)
*/


#ifndef TIGHTDB_QUERY_EXPRESSION_HPP
#define TIGHTDB_QUERY_EXPRESSION_HPP

// namespace tightdb {

    // FIXME, this needs to exist elsewhere
    typedef int64_t             Int;
    typedef bool                Bool;
    typedef tightdb::DateTime   DateTime;
    typedef float               Float;
    typedef double              Double;
    typedef tightdb::StringData String;


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

// Unary not supported yet
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
    Expression() {}

    virtual size_t find_first(size_t start, size_t end) const = 0;
    virtual void set_table(const Table* table) = 0;
    virtual ~Expression() {}

};

class ValueBase 
{
public:
    static const size_t elements = 8;
    virtual void export_int(ValueBase& destination) const = 0;
    virtual void export_float(ValueBase& destination) const = 0;
    virtual void export_int64_t(ValueBase& destination) const = 0;
    virtual void export_double(ValueBase& destination) const = 0;
    virtual void import(const ValueBase& destination) = 0;
};

class Subexpr
{
public:
    virtual ~Subexpr() {}

    // todo, think about renaming, or actualy doing deep copy
    virtual Subexpr& clone() {
        return *this;
    }

    // Recursively set table pointers for all Columns object in the expression tree. Used for late binding of table
    virtual void set_table(const Table*) {}    

    // Recursively fetch tables of columns in expression tree. Used when user first builds a stand-alone expression and 
    // binds it to a Query at a later time
    virtual const Table* get_table() 
    { 
        return NULL; 
    }

    virtual void evaluate(size_t index, ValueBase& destination) = 0;
};

class ColumnsBase {};

template <class T> class Columns;
template <class T> class Value;
template <class T> class Subexpr2;
template <class oper, class TLeft = Subexpr, class TRight = Subexpr> class Operator;
template <class TCond, class T, class TLeft = Subexpr, class TRight = Subexpr> class Compare;

// Stores 8 values of type T. Can also exchange data with other ValueBase of different types
template<class T> class Value : public ValueBase, public Subexpr2<T>
{
public:
    Value() {}

    Value(T v) {
        std::fill(m_v, m_v + ValueBase::elements, v); 
    }

    void evaluate(size_t, ValueBase& destination) {
        destination.import(*this);
    }

    template <class TOperator> TIGHTDB_FORCEINLINE void fun(const Value* left, const Value* right) {
        TOperator o;
        for(size_t t = 0; t < ValueBase::elements; t++)
            m_v[t] = o(left->m_v[t], right->m_v[t]);
    }

    // Below import and export methods are for type conversion between float, double, int64_t, etc.
    template<class D> TIGHTDB_FORCEINLINE void export2(ValueBase& destination) const
    {
        Value<D>& d = static_cast<Value<D>&>(destination);
        for(size_t t = 0; t < ValueBase::elements; t++)
            d.m_v[t] = static_cast<D>(m_v[t]);
    }

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

    TIGHTDB_FORCEINLINE void import(const ValueBase& source)
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

    // Given a TCond (==, !=, >, <, >=, <=) and two Value<T>, return index of first match
    template <class TCond> TIGHTDB_FORCEINLINE size_t static compare(Value<T>* left, Value<T>* right)
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

    virtual Subexpr& clone()
    {
        Value<T>& n = *new Value<T>();
        n = *this;
        return n;
    }

    // Performance note: Declaring values as separately named members generates faster (10% or so) code in VS2010, 
    // compared to array, even if the array accesses elements individually instead of in for-loops.
    T m_v[elements];
};

class ColumnAccessorBase;


// Handle cases where left side is a constant (int, float, int64_t, double)
template <class L, class Cond, class R> Query create (L left, const Subexpr2<R>& right) 
{
    // Purpose of below code is to intercept the creation of a condition and test if it's supported by the old
    // query_engine.hpp which is faster. If it's supported, create a query_engine.hpp node, otherwise create a 
    // query_expression.hpp node.
    const Columns<R>* column = dynamic_cast<const Columns<R>*>(&right);
    if(column && (std::numeric_limits<L>::is_integer) && (std::numeric_limits<R>::is_integer)) {
        const Table* t = (const_cast<Columns<R>*>(column))->get_table();
        Query q = Query(*t);

        if(SameType<Cond, Less>::value)
            q.greater(column->m_column, left);
        else if(SameType<Cond, Greater>::value)
            q.less(column->m_column, left);        
        else if(SameType<Cond, Equal>::value)
            q.equal(column->m_column, left);        
        else if(SameType<Cond, NotEqual>::value)
            q.not_equal(column->m_column, left);        
        else if(SameType<Cond, LessEqual>::value)
            q.greater_equal(column->m_column, left);        
        else if(SameType<Cond, GreaterEqual>::value)
            q.less_equal(column->m_column, left);
        else {
            // query_engine.hpp does not support this Cond. Please either add support for it in query_engine.hpp or
            // fallback to using use 'return *new Compare<>' instead.
            TIGHTDB_ASSERT(false); 
        }
        return q;
    }
    else {
        return *new Compare<Cond, typename Common<R, float>::type>(*new Value<L>(left), const_cast<Subexpr2<R>&>(right).clone(), true);
    }
}




// All overloads where left-hand-side is Subexpr2<L>:
//
// left-hand-side       operator                              right-hand-side
// Subexpr2<L>          +, -, *, /, <, >, ==, !=, <=, >=      R, Subexpr2<R>
//
// For L = R = {int, int64_t, float, double}:  
template <class L, class R> class Overloads
{
    typedef typename Common<L, R>::type CommonType;
public:

    // Arithmetic, right side constant
    Operator<Plus<CommonType> >& operator + (R right) { 
       return *new Operator<Plus<CommonType> >(static_cast<Subexpr2<L>&>(*this).clone(), *new Value<R>(right), true); 
    }
    Operator<Minus<CommonType> >& operator - (R right) { 
       return *new Operator<Minus<CommonType> > (static_cast<Subexpr2<L>&>(*this).clone(), *new Value<R>(right), true); 
    }
    Operator<Mul<CommonType> >& operator * (R right) { 
       return *new Operator<Mul<CommonType> > (static_cast<Subexpr2<L>&>(*this).clone(), *new Value<R>(right), true); 
    }
    Operator<Div<CommonType> >& operator / (R right) { 
        return *new Operator<Div<CommonType> > (static_cast<Subexpr2<L>&>(*this).clone(), *new Value<R>(right), true); 
    }    

    // Arithmetic, right side subexpression
    Operator<Plus<CommonType> >& operator + (const Subexpr2<R>& right) { 
        return *new Operator<Plus<CommonType> > (static_cast<Subexpr2<L>&>(*this).clone(), const_cast<Subexpr2<R>&>(right).clone(), true); 
    }
    Operator<Minus<CommonType> >& operator - (const Subexpr2<R>& right) { 
        return *new Operator<Minus<CommonType> > (static_cast<Subexpr2<L>&>(*this).clone(), const_cast<Subexpr2<R>&>(right).clone(), true); 
    }
    Operator<Mul<CommonType> >& operator * (const Subexpr2<R>& right) { 
        return *new Operator<Mul<CommonType> > (static_cast<Subexpr2<L>&>(*this).clone(), const_cast<Subexpr2<R>&>(right).clone(), true); 
    }
    Operator<Div<CommonType> >& operator / (const Subexpr2<R>& right) { 
        return *new Operator<Div<CommonType> > (static_cast<Subexpr2<L>&>(*this).clone(), const_cast<Subexpr2<R>&>(right).clone(), true); 
    }

    // Compare, right side constant
    Query operator > (R right) {
        return create<R, Less, L>(right, static_cast<Subexpr2<L>&>(*this));
    }
    Query operator < (R right) {
        return create<R, Greater, L>(right, static_cast<Subexpr2<L>&>(*this));
    }
    Query operator >= (R right) {
        return create<R, LessEqual, L>(right, static_cast<Subexpr2<L>&>(*this));
    }
    Query operator <= (R right) {
        return create<R, GreaterEqual, L>(right, static_cast<Subexpr2<L>&>(*this));
    }
    Query operator == (R right) {
        return create<R, Equal, L>(right, static_cast<Subexpr2<L>&>(*this));
    }
    Query operator != (R right) {
        return create<R, NotEqual, L>(right, static_cast<Subexpr2<L>&>(*this));
    }


    // Purpose of this method is to intercept the creation of a condition and test if it's supported by the old
    // query_engine.hpp which is faster. If it's supported, create a query_engine.hpp node, otherwise create a 
    // query_expression.hpp node.
    template <class Cond> Query create2 (const Subexpr2<R>& right) 
    {
        const Columns<R>* left_col = dynamic_cast<const Columns<R>*>(      static_cast<Subexpr2<L>*>(this)    );
        const Columns<R>* right_col = dynamic_cast<const Columns<R>*>(&right);

        // query_engine supports 'T-column <op> <T-column>' for T = {int64_t, float, double}, op = {<, >, ==, !=, <=, >=}
        if(left_col && right_col && SameType<L, R>::value) {
            const Table* t = (const_cast<Columns<R>*>(left_col))->get_table();
            Query q = Query(*t);

            if(std::numeric_limits<L>::is_integer) {
                if(SameType<Cond, Less>::value)
                    q.less_int(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, Greater>::value)
                    q.greater_int(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, Equal>::value)
                    q.equal_int(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, NotEqual>::value)
                    q.not_equal_int(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, LessEqual>::value)
                    q.less_equal_int(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, GreaterEqual>::value)
                    q.greater_equal_int(left_col->m_column, right_col->m_column);
                else {
                    TIGHTDB_ASSERT(false); 
                }
            }
            else if(SameType<L, float>::value) {
                if(SameType<Cond, Less>::value)
                    q.less_float(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, Greater>::value)
                    q.greater_float(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, Equal>::value)
                    q.equal_float(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, NotEqual>::value)
                    q.not_equal_float(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, LessEqual>::value)
                    q.less_equal_float(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, GreaterEqual>::value)
                    q.greater_equal_float(left_col->m_column, right_col->m_column);
                else {
                    TIGHTDB_ASSERT(false); 
                }
            }
            else if(SameType<L, double>::value) {
                if(SameType<Cond, Less>::value)
                    q.less_double(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, Greater>::value)
                    q.greater_double(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, Equal>::value)
                    q.equal_double(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, NotEqual>::value)
                    q.not_equal_double(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, LessEqual>::value)
                    q.less_equal_double(left_col->m_column, right_col->m_column);
                else if(SameType<Cond, GreaterEqual>::value)
                    q.greater_equal_double(left_col->m_column, right_col->m_column);
                else {
                    TIGHTDB_ASSERT(false); 
                }
            }
            else {
                TIGHTDB_ASSERT(false); 
            }

            return q;
        }
        else {
            return *new Compare<Cond, typename Common<R, float>::type>(static_cast<Subexpr2<L>&>(*this).clone(), const_cast<Subexpr2<R>&>(right).clone(), true);
        }
    }

    // Compare, right side subexpression
    Query operator == (const Subexpr2<R>& right) { 
        return create2<Equal>(right); 
    }
    Query operator != (const Subexpr2<R>& right) { 
        return create2<NotEqual>(right); 
    }
    Query operator > (const Subexpr2<R>& right) { 
        return create2<Greater>(right); 
    }
    Query operator < (const Subexpr2<R>& right) { 
        return create2<Less>(right); 
    }
    Query operator >= (const Subexpr2<R>& right) { 
        return create2<GreaterEqual>(right); 
    }
    Query operator <= (const Subexpr2<R>& right) {    
        return create2<LessEqual>(right); 
    }
};

// With this wrapper class we can define just 20 overloads inside Overloads<L, R> instead of 4 * 20 = 80.
template <class T> class Subexpr2 : public Subexpr, public Overloads<T, const char*>, public Overloads<T, int>, public
                                    Overloads<T, float>, public Overloads<T, double>, public Overloads<T, int64_t> 
{
public:
    virtual ~Subexpr2() {};

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



// Compare numeric values
template <class R> Query operator > (double left, const Subexpr2<R>& right) {
    return create<double, Greater, R>(left, right);
}
template <class R> Query operator > (float left, const Subexpr2<R>& right) {
    return create<float, Greater, R>(left, right);
}
template <class R> Query operator > (int left, const Subexpr2<R>& right) {
    return create<int, Greater, R>(left, right);
}
template <class R> Query operator > (int64_t left, const Subexpr2<R>& right) {
    return create<int64_t, Greater, R>(left, right);
}
template <class R> Query operator < (double left, const Subexpr2<R>& right) {
    return create<float, Less, R>(left, right);
}
template <class R> Query operator < (float left, const Subexpr2<R>& right) { 
    return create<int, Less, R>(left, right);
}
template <class R> Query operator < (int left, const Subexpr2<R>& right) {
    return create<int, Less, R>(left, right);
}
template <class R> Query operator < (int64_t left, const Subexpr2<R>& right) {
    return create<int64_t, Less, R>(left, right);
}
template <class R> Query operator == (double left, const Subexpr2<R>& right) { 
    return create<double, Equal, R>(left, right);
}
template <class R> Query operator == (float left, const Subexpr2<R>& right) {
    return create<float, Equal, R>(left, right);
}
template <class R> Query operator == (int left, const Subexpr2<R>& right) {
    return create<int, Equal, R>(left, right);
}
template <class R> Query operator == (int64_t left, const Subexpr2<R>& right) { 
    return create<int64_t, Equal, R>(left, right);
}
template <class R> Query operator >= (double left, const Subexpr2<R>& right) {
    return create<double, GreaterEqual, R>(left, right);
}
template <class R> Query operator >= (float left, const Subexpr2<R>& right) {
    return create<float, GreaterEqual, R>(left, right);
}
template <class R> Query operator >= (int left, const Subexpr2<R>& right) {
    return create<int, GreaterEqual, R>(left, right);
}
template <class R> Query operator >= (int64_t left, const Subexpr2<R>& right) {
    return create<int64_t, GreaterEqual, R>(left, right);
}
template <class R> Query operator <= (double left, const Subexpr2<R>& right) {
    return create<double, LessEqual, R>(left, right);
}
template <class R> Query operator <= (float left, const Subexpr2<R>& right) {
    return create<float, LessEqual, R>(left, right);
}
template <class R> Query operator <= (int left, const Subexpr2<R>& right) {
    return create<int, LessEqual, R>(left, right);
}
template <class R> Query operator <= (int64_t left, const Subexpr2<R>& right) { 
    return create<int64_t, LessEqual, R>(left, right);
}
template <class R> Query operator != (double left, const Subexpr2<R>& right) {
    return create<double, NotEqual, R>(left, right);
}
template <class R> Query operator != (float left, const Subexpr2<R>& right) {
    return create<float, NotEqual, R>(left, right);
}
template <class R> Query operator != (int left, const Subexpr2<R>& right) {
    return create<int, NotEqual, R>(left, right);
}
template <class R> Query operator != (int64_t left, const Subexpr2<R>& right) { 
    return create<int64_t, NotEqual, R>(left, right);
}

// Arithmetic
template <class R> Operator<Plus<typename Common<R, double>::type> >& operator + (double left, const Subexpr2<R>& right) { 
    return *new Operator<Plus<typename Common<R, double>::type> >(*new Value<double>(left), const_cast<Subexpr2<R>&>(right).clone(), true); 
}
template <class R> Operator<Plus<typename Common<R, float>::type> >& operator + (float left, const Subexpr2<R>& right) {
    return *new Operator<Plus<typename Common<R, float>::type> >(*new Value<float>(left), const_cast<Subexpr2<R>&>(right).clone(), true); 
}
template <class R> Operator<Plus<typename Common<R, int>::type> >& operator + (int left, const Subexpr2<R>& right) {
    return *new Operator<Plus<typename Common<R, int>::type> >(*new Value<int>(left), const_cast<Subexpr2<R>&>(right).clone(), true);
}
template <class R> Operator<Plus<typename Common<R, int64_t>::type> >& operator + (int64_t left, const Subexpr2<R>& right) { 
    return *new Operator<Plus<typename Common<R, int64_t>::type> >(*new Value<int64_t>(left), const_cast<Subexpr2<R>&>(right).clone(), true);
}
template <class R> Operator<Minus<typename Common<R, double>::type> >& operator - (double left, const Subexpr2<R>& right) {
    return *new Operator<Minus<typename Common<R, double>::type> >(*new Value<double>(left), const_cast<Subexpr2<R>&>(right).clone(), true); 
}
template <class R> Operator<Minus<typename Common<R, float>::type> >& operator - (float left, const Subexpr2<R>& right) { 
    return *new Operator<Minus<typename Common<R, float>::type> >(*new Value<float>(left), const_cast<Subexpr2<R>&>(right).clone(), true);
}
template <class R> Operator<Minus<typename Common<R, int>::type> >& operator - (int left, const Subexpr2<R>& right) { 
    return *new Operator<Minus<typename Common<R, int>::type> >(*new Value<int>(left), const_cast<Subexpr2<R>&>(right).clone(), true);
}
template <class R> Operator<Minus<typename Common<R, int64_t>::type> >& operator - (int64_t left, const Subexpr2<R>& right) { 
    return *new Operator<Minus<typename Common<R, int64_t>::type> >(*new Value<int64_t>(left), const_cast<Subexpr2<R>&>(right).clone(), true); 
}
template <class R> Operator<Mul<typename Common<R, double>::type> >& operator * (double left, const Subexpr2<R>& right) {
    return *new Operator<Mul<typename Common<R, double>::type> >(*new Value<double>(left), const_cast<Subexpr2<R>&>(right).clone(), true); 
}
template <class R> Operator<Mul<typename Common<R, float>::type> >& operator * (float left, const Subexpr2<R>& right) { 
    return *new Operator<Mul<typename Common<R, float>::type> >(*new Value<float>(left), const_cast<Subexpr2<R>&>(right).clone(), true); 
}
template <class R> Operator<Mul<typename Common<R, int>::type> >& operator * (int left, const Subexpr2<R>& right) { 
    return *new Operator<Mul<typename Common<R, int>::type> >(*new Value<int>(left), const_cast<Subexpr2<R>&>(right).clone(), true);
}
template <class R> Operator<Mul<typename Common<R, int64_t>::type> >& operator * (int64_t left, const Subexpr2<R>& right) { 
    return *new Operator<Mul<typename Common<R, int64_t>::type> >(*new Value<int64_t>(left), const_cast<Subexpr2<R>&>(right).clone(), true);
}
template <class R> Operator<Div<typename Common<R, double>::type> >& operator / (double left, const Subexpr2<R>& right) { 
    return *new Operator<Div<typename Common<R, double>::type> >(*new Value<double>(left), const_cast<Subexpr2<R>&>(right).clone(), true);
}
template <class R> Operator<Div<typename Common<R, float>::type> >& operator / (float left, const Subexpr2<R>& right) { 
    return *new Operator<Div<typename Common<R, float>::type> >*(new Value<float>(left), const_cast<Subexpr2<R>&>(right).clone(), true);
}
template <class R> Operator<Div<typename Common<R, int>::type> >& operator / (int left, const Subexpr2<R>& right) { 
    return *new Operator<Div<typename Common<R, int>::type> >(*new Value<int>(left), const_cast<Subexpr2<R>&>(right).clone(), true);
}
template <class R> Operator<Div<typename Common<R, int64_t>::type> >& operator / (int64_t left, const Subexpr2<R>& right) { 
    return *new Operator<Div<typename Common<R, int64_t>::type> >(*new Value<int64_t>(left), const_cast<Subexpr2<R>&>(right).clone(), true);
}


template <> class Columns<StringData> : public Subexpr
{
public:
    explicit Columns(size_t column, const Table* table) : m_table(NULL)
    {
        m_column = column;
        set_table(table);
    }

    explicit Columns() : m_table(NULL) { }

    explicit Columns(size_t column) : m_table(NULL)
    {
        m_column = column;
    }

    virtual void set_table(const Table* table) 
    {
        m_table = table;
    }

    virtual const Table* get_table() 
    {
        return m_table;
    }

    virtual void evaluate(size_t index, ValueBase& destination) 
    {
        static_cast<void>(index);
        static_cast<void>(destination);
        // String column conditions use fallback to old query_engine.hpp, hence bypassing all the query_expression.hpp
        // pathways like this method.
        TIGHTDB_ASSERT(false);
    }

    const Table* m_table;
    size_t m_column;
};


template <class T> Query operator == (T left, const Columns<StringData>& right) {
    return operator==(right, left);
}

template <class T> Query operator != (T left, const Columns<StringData>& right) {
    return operator!=(right, left);
}

template <class T> Query operator == (const Columns<StringData>& left, T right) {
    const Table* t = const_cast<Columns<StringData>*>(&left)->get_table();
    Query q = Query(*t);
    q.equal(left.m_column, right);
    return q;
}

template <class T> Query operator != (const Columns<StringData>& left, T right) {
    const Table* t = const_cast<Columns<StringData>*>(&left)->get_table();
    Query q = Query(*t);
    q.not_equal(left.m_column, right);
    return q;
}


template <class T> class Columns : public Subexpr2<T>, public ColumnsBase
{
public:
    explicit Columns(size_t column, const Table* table) : m_table(NULL), sg(NULL)
    {
        m_column = column;
        set_table(table);
    }

    explicit Columns() : m_table(NULL), sg(NULL) { }

    explicit Columns(size_t column) : m_table(NULL), sg(NULL)
    {
        m_column = column;
    }

    ~Columns()
    {
        delete sg;
    }

    virtual Subexpr& clone() 
    {
        Columns<T>& n = *new Columns<T>();
        n = *this;
        SequentialGetter<T> *s = new SequentialGetter<T>();
        n.sg = s;
        return n;
    }

    // Recursively set table pointers for all Columns object in the expression tree. Used for late binding of table
    virtual void set_table(const Table* table) 
    {
        m_table = table;
        typedef typename ColumnTypeTraits<T>::column_type ColType;
        const ColType* c = static_cast<const ColType*>(&table->get_column_base(m_column));
        if(sg == NULL)
            sg = new SequentialGetter<T>();
        sg->init(c);
    }

    // Recursively fetch tables of columns in expression tree. Used when user first builds a stand-alone expression and 
    // binds it to a Query at a later time
    virtual const Table* get_table() 
    {
        return m_table;
    }

    void evaluate(size_t index, ValueBase& destination) {  
        Value<T> v;          
        sg->cache_next(index);
        if(SameType<T, int64_t>::value && index + ValueBase::elements < sg->m_leaf_end) {
            // int64_t leafs have a get_chunk optimization that returns multiple int64_t values at once
            sg->m_array_ptr->get_chunk(index - sg->m_leaf_start, static_cast<Value<int64_t>*>(static_cast<ValueBase*>(&v))->m_v);
        }
        else {
            // To make Valgrind happy we must initialize all elements in v even if Column ends earlier. Todo, benchmark
            // if an unconditional zero out is faster
            if(index + ValueBase::elements >= sg->m_leaf_end)
                v = Value<T>(static_cast<T>(0));              
            for(size_t t = 0; t < ValueBase::elements && index + t < sg->m_leaf_end; t++)
                v.m_v[t] = sg->get_next(index + t);
        }
        destination.import(v);
    }

    // m_table is redundant with ColumnAccessorBase<>::m_table, but is in order to decrease class dependency/entanglement
    const Table* m_table;
    size_t m_column;
private:
    SequentialGetter<T>* sg;
};


template <class oper, class TLeft, class TRight> class Operator : public Subexpr2<typename oper::type>
{
public:

    Operator(TLeft& left, const TRight& right, bool auto_delete = false) : m_left(left), m_right(const_cast<TRight&>(right))
    {
        m_auto_delete = auto_delete;
    }

    ~Operator() 
    {   
        if(m_auto_delete)
            delete &m_left, delete &m_right;
    }

    // Recursively set table pointers for all Columns object in the expression tree. Used for late binding of table
    void set_table(const Table* table)
    {
        m_left.set_table(table);
        m_right.set_table(table);
    }

    // Recursively fetch tables of columns in expression tree. Used when user first builds a stand-alone expression and 
    // binds it to a Query at a later time
    virtual const Table* get_table()
    {
        const Table* l = m_left.get_table();
        const Table* r = m_right.get_table();
        return l ? l : r;
    }

    void evaluate(size_t index, ValueBase& destination) {
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
                m_left.evaluate(index, left);
    /*
            if(!SameType<TRight, Subexpr>::value)
                m_right->TRight::evaluate(i, &right);
            else*/
                m_right.evaluate(index, right);

            result.template fun<oper>(&left, &right);
//        }
        destination.import(result);
    }

private:
    typedef typename oper::type T;
    bool m_auto_delete;
    TLeft& m_left;
    TRight& m_right;
};


template <class TCond, class T, class TLeft, class TRight> class Compare : public Expression
{
public:
   
    // Compare extends Expression which extends Query. This constructor for Compare initializes the Query part by 
    // adding an ExpressionNode (see query_engine.hpp) and initializes Query::table so that it's ready to call
    // Query methods on, like find_first(), etc.
    Compare(TLeft& left, const TRight& right, bool auto_delete = false) : m_left(left), m_right(const_cast<TRight&>(right))
    {
        m_auto_delete = auto_delete;
        Query::expression(this, auto_delete);
        Table* t = const_cast<Table*>(get_table()); // todo, const

        if(t)
            Query::m_table = t->get_table_ref();
    }

    ~Compare()
    {
        if(m_auto_delete) {
            delete &m_left;
            delete &m_right;   
        }
    }

    // Recursively set table pointers for all Columns object in the expression tree. Used for late binding of table
    void set_table(const Table* table) 
    {
        m_left.set_table(table);
        m_right.set_table(table);
    }

    // Recursively fetch tables of columns in expression tree. Used when user first builds a stand-alone expression and 
    // binds it to a Query at a later time
    virtual const Table* get_table()
    {
        const Table* l = m_left.get_table();
        const Table* r = m_right.get_table();
        return l ? l : r;
    }

    size_t find_first(size_t start, size_t end) const {
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
    bool m_auto_delete;
    TLeft& m_left;
    TRight& m_right;
};

//}
#endif // TIGHTDB_QUERY_EXPRESSION_HPP

