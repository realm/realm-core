#ifndef TIGHTDB_QUERY_EXPRESSION_HPP
#define TIGHTDB_QUERY_EXPRESSION_HPP

//#include "column_fwd.hpp"
//#include "column_basic.hpp"
#include "column.hpp"

//#include <tightdb/column_basic.hpp>

//class ArrayDouble;

namespace tightdb {


template<class T>struct Plus { 
    T operator()(T v1, T v2) const {return v1 + v2;} 
};

template<class T>struct Minus { 
    T operator()(T v1, T v2) const {return v1 - v2;} 
};

template <class T> struct Power { 
    T operator()(T v) const {v * v;} 
};


struct ValueBase 
{
    static const size_t elements = 8;
    virtual void put_ints(ValueBase* c) = 0;
    virtual void put_floats(ValueBase* c) = 0;
    virtual void get(ValueBase* destination) = 0;
};

struct Subexpr
{
    TIGHTDB_FORCEINLINE virtual void evaluate(size_t i, ValueBase* destination) = 0;
    
    virtual void set_table(const Table* table) { }

    const Table* m_table;
};

// Performance note: If members are declared as an 8-entry array, VS2010 emits slower code compared to declaring them
// as separately named members - both if you address them individually/unrolled and if you use loops. gcc is equally slow
// in either case
template<class T> struct Value : public ValueBase, public Subexpr
{
    Value() { 
    }

    Value(T v) {
        std::fill(m_v, m_v + ValueBase::elements, v); 
    }

    TIGHTDB_FORCEINLINE void evaluate(size_t i, ValueBase* destination) {
        destination->get(this);
    }

    template <class TOperator> TIGHTDB_FORCEINLINE void fun(Value& q1, Value& q2) {
        TOperator o;
        for(size_t t = 0; t < ValueBase::elements; t++)
            m_v[t] = o(q1.m_v[t], q2.m_v[t]);
    }

    template<class U> TIGHTDB_FORCEINLINE void put(ValueBase* c)
    {
        Value<U>* c2 = static_cast<Value<U>*>(c);
        for(size_t t = 0; t < ValueBase::elements; t++)
            c2->m_v[t] = m_v[t];
    }

    TIGHTDB_FORCEINLINE void put_ints(ValueBase* c)
    {
        put<int64_t>(c);
    }

    TIGHTDB_FORCEINLINE void put_floats(ValueBase* c)
    {
        put<float>(c);
    }

    TIGHTDB_FORCEINLINE void get(ValueBase* destination)
    {
        if(SameType<T, int64_t>::value)
            destination->put_ints(this);
        else
            destination->put_floats(this);
    }

    
    T m_v[elements];
    
};


template <class T> struct Columns : public Subexpr
{
    virtual void set_table(const Table* table) {
        m_table = table; 
        ci = (Column*)&m_table->GetColumnBase(m_column);
//        cf = (ColumnFloat*)ci;
        //        sg.init(c);
    }

    Columns(size_t column) {
        m_column = column;

    }

    TIGHTDB_FORCEINLINE void evaluate(size_t i, ValueBase* destination) {
        destination->get(&c);
//        cf->get(i);
//        sg.cache_next(i);
//        sg.m_array_ptr->put_chunk(i - sg.m_leaf_start, result.first());
    }
//    SequentialGetter<int64_t> sg;
    Value<T> c;
    size_t m_column;

//    ColumnFloat* cf;
    Column* ci;
};


template <class T, class oper> struct Operator : public Subexpr
{
    Operator(Subexpr* left, Subexpr* right) {
        m_left = left;
        m_right = right;
    };

    void set_table(const Table* table)
    {
        m_left->set_table(table);
        m_right->set_table(table);
    }

    TIGHTDB_FORCEINLINE void evaluate(size_t i, ValueBase* destination) {
        Value<T> q1;
        Value<T> q2;

        m_left->evaluate(i, &q1);
        m_right->evaluate(i, &q2); 

        Value<T> result;
        result.template fun<oper>(q1, q2);
        destination->get(&result);
    }

    Subexpr* m_left;
    Subexpr* m_right;
};


struct Expression
{
    virtual size_t compare(size_t start, size_t end) = 0;
    virtual void set_table(const Table* table) = 0;
};


template <class TCond, class T> struct Compare : public Expression
{
    Compare(Subexpr* e1, Subexpr* e2) 
    {
        m_e1 = e1;
        m_e2 = e2;
    }

    void set_table(const Table* table) 
    {
        m_e1->set_table(table);
        m_e2->set_table(table);
    }

    size_t compare(size_t start, size_t end) {
        TCond c;
        Value<T> q1;
        Value<T> q2;

        for(; start < end; start += ValueBase::elements) {
            m_e1->evaluate(start, &q1);
            m_e2->evaluate(start, &q2);
            
            if(start + ValueBase::elements < end) {
                for(size_t t = 0; t < ValueBase::elements; t++)
                    if(c(q1.m_v[t], q2.m_v[t]))
                        return start + t;
            }
            else {
                for(size_t t = 0; start + t < end; t++) 
                    if(c(q1.m_v[t], q2.m_v[t]))
                        return start + t;
            }
        }
        return end; // no match
    }
    
    
    Subexpr* m_e1;
    Subexpr* m_e2;
};

}

#endif // TIGHTDB_QUERY_EXPRESSION_HPP