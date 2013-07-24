#ifndef TIGHTDB_QUERY_EXPRESSION_HPP
#define TIGHTDB_QUERY_EXPRESSION_HPP

#include "column_fwd.hpp"

class ArrayFloat;
class ArrayDouble;

namespace tightdb {

template<class T> struct ColumnTypeTraits;

template<> struct ColumnTypeTraits<int64_t> {
    typedef Column column_type;
    typedef Array array_type;
    typedef int64_t sum_type;
    static const DataType id = type_Int;
};
template<> struct ColumnTypeTraits<bool> {
    typedef Column column_type;
    typedef Array array_type;
    typedef int64_t sum_type;
    static const DataType id = type_Bool;
};
template<> struct ColumnTypeTraits<float> {
    typedef ColumnFloat column_type;
    typedef ArrayFloat array_type;
    typedef double sum_type;
    static const DataType id = type_Float;
};
template<> struct ColumnTypeTraits<double> {
    typedef ColumnDouble column_type;
    typedef ArrayDouble array_type;
    typedef double sum_type;
    static const DataType id = type_Double;
};
template<> struct ColumnTypeTraits<Date> {
    typedef Column column_type;
    typedef Array array_type;
    typedef int64_t sum_type;
    static const DataType id = type_Int;
};
// Only purpose is to return 'double' if and only if source column (T) is float and you're doing a sum (A)
template<class T, Action A> struct ColumnTypeTraitsSum {
    typedef T sum_type;
};

template<> struct ColumnTypeTraitsSum<float, act_Sum> {
    typedef double sum_type;
};


struct SequentialGetterBase { virtual ~SequentialGetterBase() {} };

template<class T>class SequentialGetter : public SequentialGetterBase {
public:
    typedef typename ColumnTypeTraits<T>::column_type ColType;
    typedef typename ColumnTypeTraits<T>::array_type ArrayType;

    SequentialGetter() : m_array((Array::no_prealloc_tag()))
    {
    }

    SequentialGetter(const Table& table, size_t column_ndx) : m_array((Array::no_prealloc_tag()))
    {
        if (column_ndx != not_found)
            m_column = static_cast<const ColType*>(&table.GetColumnBase(column_ndx));
        m_leaf_end = 0;
    }

    SequentialGetter(ColType* column) : m_array((Array::no_prealloc_tag()))
    {
        init(column);
    }

    void init (ColType* column) {
        m_column = column;
        m_leaf_end = 0;
    }

    TIGHTDB_FORCEINLINE bool cache_next(size_t index)
    {
        // Return wether or not leaf array has changed (could be useful to know for caller)
        if (index >= m_leaf_end || index < m_leaf_start) {
            // GetBlock() does following: If m_column contains only a leaf, then just return pointer to that leaf and
            // leave m_array untouched. Else call init_from_header() on m_array (more time consuming) and return pointer to m_array.
            m_array_ptr = static_cast<const ArrayType*>(m_column->GetBlock(index, m_array, m_leaf_start, true));
            const size_t leaf_size = m_array_ptr->size();
            m_leaf_end = m_leaf_start + leaf_size;
            return true;
        }
        return false;
    }

    TIGHTDB_FORCEINLINE T get_next(size_t index)
    {
        cache_next(index);
        T av = m_array_ptr->get(index - m_leaf_start);
        return av;
    }

    size_t local_end(size_t global_end)
    {
        if (global_end > m_leaf_end)
            return m_leaf_end - m_leaf_start;
        else
            return global_end - m_leaf_start;
    }

    size_t m_leaf_start;
    size_t m_leaf_end;
    const ColType* m_column;

    // See reason for having both a pointer and instance above
    const ArrayType* m_array_ptr;
private:
    // Never access through m_array because it's uninitialized if column is just a leaf
    ArrayType m_array;
};


// Performance note: If members are declared as an 8-entry array, VS2010 emits slower code compared to declaring them
// as separately named members - both if you address them individually/unrolled and if you use loops. gcc is equally slow
// in either case
struct Chunk
{
    int64_t ma;
    int64_t mb;
    int64_t mc;
    int64_t md;
    int64_t me;
    int64_t mf;
    int64_t mg;
    int64_t mh;

    static const size_t elements = 8;

    int64_t* first() {
        TIGHTDB_STATIC_ASSERT(sizeof(Chunk) == elements * sizeof(ma), "Elements must be concecutive and without padding - keep Chunk a POD");
        return &ma;
    }

    template <class TOperator> TIGHTDB_FORCEINLINE void fun(Chunk& q1, Chunk& q2) {
        TOperator o;
        ma = o(q1.ma, q2.ma);
        mb = o(q1.mb, q2.mb);
        mc = o(q1.mc, q2.mc);
        md = o(q1.md, q2.md);
        me = o(q1.me, q2.me);
        mf = o(q1.mf, q2.mf);
        mg = o(q1.mg, q2.mg);
        mh = o(q1.mh, q2.mh);
    }

    TIGHTDB_FORCEINLINE void set_all(int64_t v) {
        ma = v;
        mb = v;
        mc = v;
        md = v;
        me = v;
        mf = v;
        mg = v;
        mh = v;
    }
};


struct Expression
{
    TIGHTDB_FORCEINLINE virtual void evaluate(size_t i, Chunk& result) = 0;
};


struct ConstantBase
{
};


template <int64_t v> struct StaticConstant : public Expression, public ConstantBase
{
    TIGHTDB_FORCEINLINE void evaluate(size_t i, Chunk& result) {
        result.set_all(v);
    }

};


struct DynamicConstant : public Expression, public ConstantBase
{
    DynamicConstant(int64_t v) {
        for(size_t t = 0; t < Chunk::elements; t++)
            mq.first()[t] = v;
    }

    TIGHTDB_FORCEINLINE void evaluate(size_t i, Chunk& result) {
        result = mq;
    }
    int64_t c;
    Chunk mq;
};


struct ColumnExpression : public Expression
{
    ColumnExpression(Column* c) {
        sg.init(c);
    }

    TIGHTDB_FORCEINLINE void evaluate(size_t i, Chunk& result) {
        sg.cache_next(i);
        sg.m_array_ptr->get_chunk(i - sg.m_leaf_start, result.first());
    }
    SequentialGetter<int64_t> sg;
};


template <class oper, class TLeft = Expression, class TRight = Expression> struct COperator : public Expression
{
    COperator(TLeft* left, TRight* right) {
        m_left = left;
        m_right = right;
    };

    TIGHTDB_FORCEINLINE void evaluate(size_t i, Chunk& result) {
        // The first three special handlers make VS2010 create faster code even though the last general case may be
        // semantically equivalent; the general case emit push/pop for vast amounts of state, for unknown reasons. gcc
        // is equally slow in either case
        if(SameType<TLeft, DynamicConstant>::value && SameType<TRight, ColumnExpression>::value) {
            static_cast<ColumnExpression*>(static_cast<Expression*>(m_right))->ColumnExpression::evaluate(i, result); 
            result.fun<oper>(static_cast<DynamicConstant*>(static_cast<Expression*>(m_left))->DynamicConstant::mq, result);
        }
        else if(SameType<TLeft, ColumnExpression>::value && SameType<TRight, DynamicConstant>::value) {
            static_cast<ColumnExpression*>(static_cast<Expression*>(m_left))->ColumnExpression::evaluate(i, result); 
            result.fun<oper>(static_cast<DynamicConstant*>(static_cast<Expression*>(m_right))->DynamicConstant::mq, result);
        }
        else if(SameType<TLeft, DynamicConstant>::value && SameType<TRight, DynamicConstant>::value) {
            result.fun<oper>(static_cast<DynamicConstant*>(static_cast<Expression*>(m_right))->DynamicConstant::mq, 
                             static_cast<DynamicConstant*>(static_cast<Expression*>(m_left))->DynamicConstant::mq);
        }
        else {
            Chunk q1;
            Chunk q2;
            static_cast<TLeft*>(m_left)->TLeft::evaluate(i, q1);
            static_cast<TRight*>(m_right)->TRight::evaluate(i, q2); 
            result.fun<oper>(q1, q2);
        }
    }

    TLeft* m_left;
    TRight* m_right;
};


struct ComparerBase
{
    virtual size_t Compare(size_t start, size_t end) = 0;
};


template <class TCond> struct Comparer : public ComparerBase
{
    Comparer(Expression* e1, Expression* e2) 
    {
        m_e1 = e1;
        m_e2 = e2;
    }

    size_t Compare(size_t start, size_t end) {
        TCond c;
        Chunk q1;
        Chunk q2;
        size_t i;

        for(i = start; i + Chunk::elements < end; i += Chunk::elements) {
            m_e1->evaluate(i, q1);
            m_e2->evaluate(i, q2);

            if(c(q1.ma, q2.ma))
                return i + 0;
            else if(c(q1.mb, q2.mb))
                return i + 1;
            else if(c(q1.mc, q2.mc))
                return i + 2;
            else if(c(q1.md, q2.md))
                return i + 3;
            else if(c(q1.me, q2.me))
                return i + 4;
            else if(c(q1.mf, q2.mf))
                return i + 5;
            else if(c(q1.mg, q2.mg))
                return i + 6;
            else if(c(q1.mh, q2.mh))
                return i + 7;
        }

        if(i < end) {
            m_e1->evaluate(i, q1);
            m_e2->evaluate(i, q2);
            
            for(size_t t = 0; t < end - i; t++)
                if(c(q1.first()[t], q2.first()[t]))
                    return i + t;

        }

        return end;
    }
    
    Expression* m_e1;
    Expression* m_e2;
};

}

#endif // TIGHTDB_QUERY_EXPRESSION_HPP