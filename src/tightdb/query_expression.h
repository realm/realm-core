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

// Finds a common type for T1 and T2 according to C++ conversion/promotion in arithmetic (float + int => float, etc)
template<class T1, class T2,
    bool T1_is_int = std::numeric_limits<T1>::is_integer,
    bool T2_is_int = std::numeric_limits<T2>::is_integer,
    bool T1_is_wider_than_T2 = (sizeof(T1) > sizeof(T2)) > struct Common;

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


class Expression
{
public:
    virtual size_t compare(size_t start, size_t end) = 0;
    virtual void set_table(const Table* table) = 0;
};

class ValueBase 
{
public:
    static const size_t elements = 8;
    virtual void export_ints(ValueBase* destination) = 0;
    virtual void export_floats(ValueBase* destination) = 0;
    virtual void import(ValueBase* destination) = 0;
};

class Subexpr
{
public:
    TIGHTDB_FORCEINLINE virtual void evaluate(size_t, ValueBase*) { TIGHTDB_ASSERT(false); }
    virtual void set_table(const Table* table) { (void)table; }

};

template <class T> class Columns;
template <class T> class Value;
template <class T> class Subexpr2;
template <class oper, class TLeft = Subexpr, class TRight = Subexpr> class Operator;
template <class TCond, class T, class TLeft = Subexpr, class TRight = Subexpr> class Compare;


template<class T> class Value : public ValueBase, public Subexpr2<T>
{
public:
    Value() { 
    }

    Value(T v) {
        std::fill(m_v, m_v + ValueBase::elements, v); 
    }

    TIGHTDB_FORCEINLINE void evaluate(size_t, ValueBase* destination) {
        destination->import(this);
    }

    template <class TOperator> TIGHTDB_FORCEINLINE void fun(Value* left, Value* right) {
        TOperator o;
        for(size_t t = 0; t < ValueBase::elements; t++)
            m_v[t] = o(left->m_v[t], right->m_v[t]);
    }

    template<class U> TIGHTDB_FORCEINLINE void export2(ValueBase* destination)
    {
        Value<U>* c2 = static_cast<Value<U>*>(destination);
        for(size_t t = 0; t < ValueBase::elements; t++)
            c2->m_v[t] = static_cast<U>(m_v[t]);
    }

    TIGHTDB_FORCEINLINE void export_ints(ValueBase* destination)
    {
        export2<int64_t>(destination);
    }

    TIGHTDB_FORCEINLINE void export_floats(ValueBase* destination)
    {
        export2<float>(destination);
    }

    TIGHTDB_FORCEINLINE void import(ValueBase* source)
    {
        if(SameType<T, int64_t>::value)
            source->export_ints(this);
        else
            source->export_floats(this);
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



template <class T, class U> struct Overloads
{
    typedef typename Common<T, U>::type CommonType;

    // Arithmetic, right side constant
    Operator<Plus<CommonType> > operator + (U other) { 
       return Operator<Plus<CommonType> > ((Subexpr2<T>*)this, new Value<U>(other)); 
    }
    Operator<Minus<CommonType> > operator - (U other) { 
       return Operator<Minus<CommonType> > ((Subexpr2<T>*)this, new Value<U>(other)); 
    }
    Operator<Mul<CommonType> > operator * (U other) { 
       return Operator<Mul<CommonType> > ((Subexpr2<T>*)this, new Value<U>(other)); 
    }
    Operator<Div<CommonType> > operator / (U other) { 
        return Operator<Div<CommonType> > ((Subexpr2<T>*)this, new Value<U>(other)); 
    }    

    // Arithmetic, right side subexpression
    Operator<Plus<CommonType> > operator + (const Subexpr2<U>& other) { 
        return Operator<Plus<CommonType> > ((Subexpr2<T>*)this, (Subexpr2<U>*)&other); 
    }
    Operator<Plus<CommonType> > operator - (const Subexpr2<U>& other) { 
        return Operator<Minus<CommonType> > ((Subexpr2<T>*)this, (Subexpr2<U>*)&other); 
    }
    Operator<Plus<CommonType> > operator * (const Subexpr2<U>& other) { 
        return Operator<Mul<CommonType> > ((Subexpr2<T>*)this, (Subexpr2<U>*)&other); 
    }
    Operator<Plus<CommonType> > operator / (const Subexpr2<U>& other) { 
        return Operator<Div<CommonType> > ((Subexpr2<T>*)this, (Subexpr2<U>*)&other); 
    }

    // Compare, right side constant
    Expression* operator > (U other) {
        return new Compare<Greater, CommonType>((Subexpr2<T>*)this, new Value<U>(other));
    }
    Expression* operator < (U other) {
        return new Compare<Greater, CommonType>((Subexpr2<T>*)this, new Value<U>(other));
    }
    Expression* operator >= (U other) {
        return new Compare<GreaterEqual, CommonType>((Subexpr2<T>*)this, new Value<U>(other));
    }
    Expression* operator <= (U other) {
        return new Compare<LessEqual, CommonType>((Subexpr2<T>*)this, new Value<U>(other));
    }
    Expression* operator == (U other) {
        return new Compare<Equal, CommonType>((Subexpr2<T>*)this, new Value<U>(other));
    }
    Expression* operator != (U other) {
        return new Compare<NotEqual, CommonType>((Subexpr2<T>*)this, new Value<U>(other));
    }

    // Compare, right side subexpression
    Expression* operator == (const Subexpr2<U>& other) { 
        return new Compare<Greater, CommonType>((Subexpr2<T>*)this, (Subexpr2<U>*)&other); 
    }
    Expression* operator != (const Subexpr2<U>& other) { 
        return new Compare<NotEqual, CommonType>((Subexpr2<T>*)this, (Subexpr2<U>*)&other); 
    }
    Expression* operator > (const Subexpr2<U>& other) { 
        return new Compare<Greater, CommonType>((Subexpr2<T>*)this, (Subexpr2<U>*)&other); 
    }
    Expression* operator < (const Subexpr2<U>& other) { 
        return new Compare<Less, CommonType>((Subexpr2<T>*)this, (Subexpr2<U>*)&other); 
    }
    Expression* operator >= (const Subexpr2<U>& other) { 
        return new Compare<GreaterEqual, CommonType>((Subexpr2<T>*)this, (Subexpr2<U>*)&other); 
    }
    Expression* operator <= (const Subexpr2<U>& other) { 
        return new Compare<LessEqual, CommonType>((Subexpr2<T>*)this, (Subexpr2<U>*)&other); 
    }
};


template <class T> class Subexpr2 : public Subexpr, public Overloads<T, int>, public Overloads<T, float>, 
                                    public Overloads<T, double>, public Overloads<T, int64_t> 
{
public:
    #define TDB_U2(t, o) using Overloads<T, t>::operator o;
    #define TDB_U(o) TDB_U2(int, o) TDB_U2(float, o) TDB_U2(double, o) TDB_U2(int64_t, o)
    TDB_U(+) TDB_U(-) TDB_U(*) TDB_U(/) TDB_U(>) TDB_U(<) TDB_U(==) TDB_U(!=) TDB_U(>=) TDB_U(<=)

    const Table* m_table; // gcc can't find this unless defined here, fixme/todo

};

// Compare
template <class T> Expression* operator > (double lhs, const Subexpr2<T>& rhs) { 
    return new Compare<Greater, typename Common<T, double>::type>(new Value<double>(lhs), (Subexpr2<T>*)&rhs); 
}

template <class T> Expression* operator > (float lhs, const Subexpr2<T>& rhs) {
    return new Compare<Greater, typename Common<T, float>::type>(new Value<float>(lhs), (Subexpr2<T>*)&rhs);
}
template <class T> Expression* operator > (int lhs, const Subexpr2<T>& rhs) {
    return new Compare<Greater, typename Common<T, int>::type>(new Value<int>(lhs), (Subexpr2<T>*)&rhs);
}
template <class T> Expression* operator > (int64_t lhs, const Subexpr2<T>& rhs) { return new Compare<Greater, typename Common<T, int64_t>::type>(new Value<int64_t>(lhs), (Subexpr2<T>*)&rhs); }

template <class T> Expression* operator < (double lhs, const Subexpr2<T>& rhs) {
    return new Compare<Less, typename Common<T, double>::type>(new Value<double>(lhs), (Subexpr2<T>*)&rhs); 
}
template <class T> Expression* operator < (float lhs, const Subexpr2<T>& rhs) { 
    return new Compare<Less, typename Common<T, float>::type>(new Value<float>(lhs), (Subexpr2<T>*)&rhs); 
}
template <class T> Expression* operator < (int lhs, const Subexpr2<T>& rhs) {
    return new Compare<Less, typename Common<T, int>::type>(new Value<int>(lhs), (Subexpr2<T>*)&rhs);
}
template <class T> Expression* operator < (int64_t lhs, const Subexpr2<T>& rhs) {
    return new Compare<Less, typename Common<T, int64_t>::type>(new Value<int64_t>(lhs), (Subexpr2<T>*)&rhs); 
}

template <class T> Expression* operator == (double lhs, const Subexpr2<T>& rhs) { 
    return new Compare<Equal, typename Common<T, double>::type>(new Value<double>(lhs), (Subexpr2<T>*)&rhs);
}
template <class T> Expression* operator == (float lhs, const Subexpr2<T>& rhs) {
    return new Compare<Equal, typename Common<T, float>::type>(new Value<float>(lhs), (Subexpr2<T>*)&rhs); 
}
template <class T> Expression* operator == (int lhs, const Subexpr2<T>& rhs) {
    return new Compare<Equal, typename Common<T, int>::type>(new Value<int>(lhs), (Subexpr2<T>*)&rhs); 
}
template <class T> Expression* operator == (int64_t lhs, const Subexpr2<T>& rhs) { 
    return new Compare<Equal, typename Common<T, int64_t>::type>(new Value<int64_t>(lhs), (Subexpr2<T>*)&rhs); 
}

template <class T> Expression* operator >= (double lhs, const Subexpr2<T>& rhs) {
    return new Compare<GreaterEqual, typename Common<T, double>::type>(new Value<double>(lhs), (Subexpr2<T>*)&rhs);
}
template <class T> Expression* operator >= (float lhs, const Subexpr2<T>& rhs) {
    return new Compare<GreaterEqual, typename Common<T, float>::type>(new Value<float>(lhs), (Subexpr2<T>*)&rhs);
}
template <class T> Expression* operator >= (int lhs, const Subexpr2<T>& rhs) {
    return new Compare<GreaterEqual, typename Common<T, int>::type>(new Value<int>(lhs), (Subexpr2<T>*)&rhs);
}
template <class T> Expression* operator >= (int64_t lhs, const Subexpr2<T>& rhs) {
    return new Compare<GreaterEqual, typename Common<T, int64_t>::type>(new Value<int64_t>(lhs), (Subexpr2<T>*)&rhs); 
}

template <class T> Expression* operator <= (double lhs, const Subexpr2<T>& rhs) {
    return new Compare<LessEqual, typename Common<T, double>::type>(new Value<double>(lhs), (Subexpr2<T>*)&rhs); 
}
template <class T> Expression* operator <= (float lhs, const Subexpr2<T>& rhs) {
    return new Compare<LessEqual, typename Common<T, float>::type>(new Value<float>(lhs), (Subexpr2<T>*)&rhs); 
}
template <class T> Expression* operator <= (int lhs, const Subexpr2<T>& rhs) {
    return new Compare<LessEqual, typename Common<T, int>::type>(new Value<int>(lhs), (Subexpr2<T>*)&rhs); 
}
template <class T> Expression* operator <= (int64_t lhs, const Subexpr2<T>& rhs) { 
    return new Compare<LessEqual, typename Common<T, int64_t>::type>(new Value<int64_t>(lhs), (Subexpr2<T>*)&rhs);
}

// Arithmetic
template <class T> Operator<Plus<typename Common<T, double>::type> > operator + (double lhs, const Subexpr2<T>& rhs) { 
    return Operator<Plus<typename Common<T, double>::type> >(new Value<double>(lhs), (Subexpr2<T>*)&rhs); 
}
template <class T> Operator<Plus<typename Common<T, float>::type> > operator + (float lhs, const Subexpr2<T>& rhs) {
    return Operator<Plus<typename Common<T, float>::type> >(new Value<float>(lhs), (Subexpr2<T>*)&rhs); 
}
template <class T> Operator<Plus<typename Common<T, int>::type> > operator + (int lhs, const Subexpr2<T>& rhs) {
    return Operator<Plus<typename Common<T, int>::type> >(new Value<int>(lhs), (Subexpr2<T>*)&rhs);
}
template <class T> Operator<Plus<typename Common<T, int64_t>::type> > operator + (int64_t lhs, const Subexpr2<T>& rhs) { 
    return Operator<Plus<typename Common<T, int64_t>::type> >(new Value<int64_t>(lhs), (Subexpr2<T>*)&rhs);
}

template <class T> Operator<Minus<typename Common<T, double>::type> > operator - (double lhs, const Subexpr2<T>& rhs) {
    return Operator<Minus<typename Common<T, double>::type> >(new Value<double>(lhs), (Subexpr2<T>*)&rhs); 
}
template <class T> Operator<Minus<typename Common<T, float>::type> > operator - (float lhs, const Subexpr2<T>& rhs) { 
    return Operator<Minus<typename Common<T, float>::type> >(new Value<float>(lhs), (Subexpr2<T>*)&rhs);
}
template <class T> Operator<Minus<typename Common<T, int>::type> > operator - (int lhs, const Subexpr2<T>& rhs) { 
    return Operator<Minus<typename Common<T, int>::type> >(new Value<int>(lhs), (Subexpr2<T>*)&rhs);
}
template <class T> Operator<Minus<typename Common<T, int64_t>::type> > operator - (int64_t lhs, const Subexpr2<T>& rhs) { 
    return Operator<Minus<typename Common<T, int64_t>::type> >(new Value<int64_t>(lhs), (Subexpr2<T>*)&rhs); }

template <class T> Operator<Mul<typename Common<T, double>::type> > operator * (double lhs, const Subexpr2<T>& rhs) {
    return Operator<Mul<typename Common<T, double>::type> >(new Value<double>(lhs), (Subexpr2<T>*)&rhs); 
}
template <class T> Operator<Mul<typename Common<T, float>::type> > operator * (float lhs, const Subexpr2<T>& rhs) { 
    return Operator<Mul<typename Common<T, float>::type> >(new Value<float>(lhs), (Subexpr2<T>*)&rhs); 
}
template <class T> Operator<Mul<typename Common<T, int>::type> > operator * (int lhs, const Subexpr2<T>& rhs) { 
    return Operator<Mul<typename Common<T, int>::type> >(new Value<int>(lhs), (Subexpr2<T>*)&rhs);
}
template <class T> Operator<Mul<typename Common<T, int64_t>::type> > operator * (int64_t lhs, const Subexpr2<T>& rhs) { 
    return Operator<Mul<typename Common<T, int64_t>::type> >(new Value<int64_t>(lhs), (Subexpr2<T>*)&rhs);
}

template <class T> Operator<Div<typename Common<T, double>::type> > operator / (double lhs, const Subexpr2<T>& rhs) { 
    return Operator<Div<typename Common<T, double>::type> >(new Value<double>(lhs), (Subexpr2<T>*)&rhs);
}
template <class T> Operator<Div<typename Common<T, float>::type> > operator / (float lhs, const Subexpr2<T>& rhs) { 
    return Operator<Div<typename Common<T, float>::type> >(new Value<float>(lhs), (Subexpr2<T>*)&rhs);
}
template <class T> Operator<Div<typename Common<T, int>::type> > operator / (int lhs, const Subexpr2<T>& rhs) { return 
    Operator<Div<typename Common<T, int>::type> >(new Value<int>(lhs), (Subexpr2<T>*)&rhs);
}
template <class T> Operator<Div<typename Common<T, int64_t>::type> > operator / (int64_t lhs, const Subexpr2<T>& rhs) { 
    return Operator<Div<typename Common<T, int64_t>::type> >(new Value<int64_t>(lhs), (Subexpr2<T>*)&rhs);
}


template <class T> class Columns : public Subexpr2<T>
{
public:
    explicit Columns(size_t column) {
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
        destination->import(&v);
    }

    const Table* m_table; // gcc can't find this unless defined here, fixme/todo

private:
    SequentialGetter<T> sg;
    size_t m_column;
};


template <class oper, class TLeft, class TRight> class Operator : public Subexpr2<typename oper::type>
{
public:
    typedef typename oper::type T;

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
            else
                m_left->evaluate(i, &left);
    
            if(!SameType<TRight, Subexpr>::value)
                m_right->TRight::evaluate(i, &right);
            else
                m_right->evaluate(i, &right);

            result.template fun<oper>(&left, &right);
        }

        destination->import(&result);
    }




private:
    TLeft* m_left;
    TRight* m_right;
};


template <class TCond, class T, class TLeft, class TRight> class Compare : public Expression
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
        size_t match;
        Value<T> vright;
        Value<T> vleft;

        for(; start < end; start += ValueBase::elements) {
            // Save time by avoid calling evaluate() for constants of the same type as compare object
            if(SameType<TLeft, Value<T> >::value) {
                m_right->evaluate(start, &vright);
                match = Value<T>::template compare<TCond>(static_cast<Value<T>*>(static_cast<Subexpr*>(m_left)), &vright); 
            }
            else if(SameType<TRight, Value<T> >::value) {
                m_left->evaluate(start, &vleft);
                match = Value<T>::template compare<TCond>(&vleft, static_cast<Value<T>*>(static_cast<Subexpr*>(m_right))); 
            }
            else {
                // General case. Again avoid vtable lookup when possible
                if(!SameType<TLeft, Subexpr>::value)               
                    static_cast<TLeft*>(m_left)->TLeft::evaluate(start, &vleft);
                else
                    m_left->evaluate(start, &vleft);

                if(!SameType<TRight, Subexpr>::value)               
                    m_right->TRight::evaluate(start, &vright);
                else
                    m_right->evaluate(start, &vright);

                match = Value<T>::template compare<TCond>(&vleft, &vright);
            }

            // Note the second condition that tests if match position in chunk exceeds column length
            if(match != ValueBase::elements && start + match < end)
                return start + match;
        }

        return end; // no match
    }
    
private: 
    TLeft* m_left;
    TRight* m_right;
};

//}
#endif // TIGHTDB_QUERY_EXPRESSION_HPP

