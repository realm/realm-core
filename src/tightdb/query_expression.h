#ifndef TIGHTDB_QUERY_EXPRESSION_HPP
#define TIGHTDB_QUERY_EXPRESSION_HPP

// namespace tightdb {

template<class T>struct Plus { 
    T operator()(T v1, T v2) const {return v1 + v2;} 
};

template<class T>struct Minus { 
    T operator()(T v1, T v2) const {return v1 - v2;} 
};

template <class T> struct Power { 
    T operator()(T v) const {v * v;} 
};

class ValueBase 
{
public:
    static const size_t elements = 8;
    virtual void output_ints(ValueBase* destination) = 0;
    virtual void output_floats(ValueBase* destination) = 0;
    virtual void fetch(ValueBase* destination) = 0;
};

class Subexpr
{
public:
    TIGHTDB_FORCEINLINE virtual void evaluate(size_t i, ValueBase* destination) = 0; 
    virtual void set_table(const Table* table) { }
    const Table* m_table;
};


template<class T> class Value : public ValueBase, public Subexpr
{
public:
    Value() { 
    }

    Value(T v) {
        std::fill(m_v, m_v + ValueBase::elements, v); 
    }

    TIGHTDB_FORCEINLINE void evaluate(size_t i, ValueBase* destination) {
        destination->fetch(this);
    }

    template <class TOperator> TIGHTDB_FORCEINLINE void fun(Value* left, Value* right) {
        TOperator o;
        for(size_t t = 0; t < ValueBase::elements; t++)
            m_v[t] = o(left->m_v[t], right->m_v[t]);
    }

    template<class U> TIGHTDB_FORCEINLINE void output(ValueBase* destination)
    {
        Value<U>* c2 = static_cast<Value<U>*>(destination);
        for(size_t t = 0; t < ValueBase::elements; t++)
            c2->m_v[t] = static_cast<U>(m_v[t]);
    }

    TIGHTDB_FORCEINLINE void output_ints(ValueBase* destination)
    {
        output<int64_t>(destination);
    }

    TIGHTDB_FORCEINLINE void output_floats(ValueBase* destination)
    {
        output<float>(destination);
    }

    TIGHTDB_FORCEINLINE void fetch(ValueBase* source)
    {
        if(SameType<T, int64_t>::value)
            source->output_ints(this);
        else
            source->output_floats(this);
    }
    
    // Performance note: Declaring values as separately named members generates faster (10% or so) code in VS2010, 
    // compared to array, even if the array accesses elements individually instead of in for-loops.
    T m_v[elements];
};


template <class T> class Columns : public Subexpr
{
public:
    Columns(size_t column) {
        m_column = column;
    }

    virtual void set_table(const Table* table) {
        m_table = table;
        typedef typename ColumnTypeTraits<T>::column_type ColType;
        ColType* c;
        c = (ColType*)&table->GetColumnBase(m_column);
        sg.init(c);
    }

    TIGHTDB_FORCEINLINE void evaluate(size_t i, ValueBase* destination) {
        Value<T> v;            
        sg.cache_next(i);
        if(SameType<T, int64_t>::value) {
            sg.m_array_ptr->get_chunk(i - sg.m_leaf_start, static_cast<Value<int64_t>*>(static_cast<ValueBase*>(&v))->m_v);
        }
        else {
            for(size_t t = 0; t < ValueBase::elements && i + t < sg.m_leaf_end; t++)
                v.m_v[t] = sg.get_next(i + t);
        }
        destination->fetch(&v);
    }

private:
    SequentialGetter<T> sg;
    size_t m_column;
};


template <class T, class oper, class TLeft = Subexpr, class TRight = Subexpr> class Operator : public Subexpr
{
public:
    Operator(TLeft* left, TRight* right) {
        m_left = left;
        m_right = right;
    };

    void set_table(const Table* table)
    {
        m_left->set_table(table);
        m_right->set_table(table);
    }

    TIGHTDB_FORCEINLINE void evaluate(size_t i, ValueBase* destination) {
        Value<T> result;
        Value<T> left;
        Value<T> right;

        // Optimize for Constant <operator> Column and Column <operator> Constant
        if(SameType<TLeft, Value<T>>::value && SameType<TRight, Columns<T>>::value) {
            m_right->evaluate(i, &right);
            result.template fun<oper>(static_cast<Value<T>*>(static_cast<Subexpr*>(m_left)), &right);
        }
        else if(SameType<TRight, Value<T>>::value && SameType<TLeft, Columns<T>>::value) {
            m_left->evaluate(i, &left);
            result.template fun<oper>(static_cast<Value<T>*>(static_cast<Subexpr*>(m_right)), &left);
        }
        else {
            // Avoid vtable lookups. Qualifying is apparently required even with 'final' keyword in C++11
            if(!SameType<TLeft, Subexpr>::value)
                m_left->TLeft::evaluate(i, &left);
            else
                m_left->evaluate(i, &left);
    
            if(!SameType<TRight, Subexpr>::value)
                m_right->TRight::evaluate(i, &right);
            else
                m_right->evaluate(i, &right);

            result.template fun<oper>(&left, &right);
        }

        destination->fetch(&result);
    }

private:
    TLeft* m_left;
    TRight* m_right;
};


class Expression
{
public:
    virtual size_t compare(size_t start, size_t end) = 0;
    virtual void set_table(const Table* table) = 0;
};


template <class TCond, class T, class TLeft = Subexpr, class TRight = Subexpr> class Compare : public Expression
{
public:
    Compare(TLeft* left, TRight* right) 
    {
        m_left = left;
        m_right = right;
    }

    void set_table(const Table* table) 
    {
        m_left->set_table(table);
        m_right->set_table(table);
    }

    size_t compare(size_t start, size_t end) {
        TCond c;
        Value<T> vleft;
        Value<T> vright;

        Value<T>* pleft = &vleft;
        Value<T>* pright = &vright;

        // Whole chunks
        for(; start + ValueBase::elements < end; start += ValueBase::elements) {
            if(!SameType<TLeft, Value<T>>::value)
                m_left->evaluate(start, pleft);
            else
                pleft = static_cast<Value<T>*>(static_cast<Subexpr*>(m_left));
    
            if(!SameType<TRight, Value<T>>::value)
                m_right->evaluate(start, pright);
            else
                pright = static_cast<Value<T>*>(static_cast<Subexpr*>(m_right));

            // 665 ms unrolled vs 698 (vs2010)
            if(c(pleft->m_v[0], pright->m_v[0]))
                return start + 0;          
            if(c(pleft->m_v[1], pright->m_v[1]))
                return start + 1;          
            if(c(pleft->m_v[2], pright->m_v[2]))
                return start + 2;          
            if(c(pleft->m_v[3], pright->m_v[3]))
                return start + 3;          
            if(c(pleft->m_v[4], pright->m_v[4]))
                return start + 4;          
            if(c(pleft->m_v[5], pright->m_v[5]))
                return start + 5;          
            if(c(pleft->m_v[6], pright->m_v[6]))
                return start + 6;          
            if(c(pleft->m_v[7], pright->m_v[7]))
                return start + 7;          
        }

        // Partial remainder
        if(start < end) {
            m_left->evaluate(start, &vleft);
            m_right->evaluate(start, &vright);

            for(size_t t = 0; start + t < end; t++) 
                if(c(vleft.m_v[t], vright.m_v[t]))
                    return start + t;
        }

        return end; // no match
    }
    
private: 
    TLeft* m_left;
    TRight* m_right;
};

//}
#endif // TIGHTDB_QUERY_EXPRESSION_HPP






/*



        TCond c;
        Value<T> vleft;
        Value<T> vright;




        for(; start < end; start += ValueBase::elements) {
            m_left->evaluate(start, &vleft);
            m_right->evaluate(start, &vright);          

            if(start + ValueBase::elements < end) {
                for(size_t t = 0; t < ValueBase::elements; t++)
                    if(c(vleft.m_v[t], vright.m_v[t]))
                        return start + t;
            }
            else {
                for(size_t t = 0; start + t < end; t++) 
                    if(c(vleft.m_v[t], vright.m_v[t]))
                        return start + t;
            }
        }



        return end; // no match

        */