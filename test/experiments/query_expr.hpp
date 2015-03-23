#include <climits>
#include <cwchar>
#include <algorithm>
#include <ostream>

#include <tightdb/util/assert.hpp>
#include <tightdb/util/meta.hpp>
#include <tightdb/util/tuple.hpp>


// Check that Inlining is still perfect when referring to context variables whose value are not known to be constant (e.g. a function argument)
//   Optimization is not perfect - consider evaluating logical operators before COUNT/EXISTS operation.

// table.count(t.foo || t.bar) is also not perfectly optimized - again consider evaluating logical operators before COUNT/EXISTS operation.



/* Maybe:
Rename Canonicalize to MoveNot, then do ConstFold
Then reconsider AND changes for improved optimization
*/




// ToDo: Optimization issues
// ToDo: Column/B-tree iterator
// GOOD: Refer to any regular variable in context
// GOOD: Use any arithemtic, comparison, or conditional operator
// GOOD: No restrictions on mixing of types (except that it is not allowed to use enums in arithmetic operations that involve column references)
// CAVEAT: Ternary operator cannot be used if any operand involves a column reference.
// CAVEAT: Regular function calls cannot be used if any argument involves a column reference.
// CAVEAT: Type casting/conversion cannot be used if the argument involves a column reference.
// CAVEAT: Cannot currently do subtable queries in mixed columns.

// Consider: For subtables in mixed columns, one must check for each
// row that matches the schema of the query row. That would require a
// fast check on schema compatibility, which might be simply a
// comparison of integer identifiers.

// Optional dynamic column names in table spec


namespace tightdb {



template<class Spec> class BasicTable;


namespace query
{
    struct EmptyType {};

    template<class T> struct IsSubtable { static const bool value = false; };
    template<class T> struct IsSubtable<SpecBase::Subtable<T> > {
        static const bool value = true;
    };



    template<class Tab, int col_idx, class Type> struct ColRef
    {
        typedef Type column_type;
    };

    template<class Op, class A> struct UnOp {
        A arg;
        explicit UnOp(const A& a): arg(a) {}
    };

    template<class Op, class A, class B> struct BinOp {
        A left;
        B right;
        BinOp(const A& a, const B& b): left(a), right(b) {}
    };

    template<class Op, class Col, class Query> struct Subquery {
        Col col;
        Query query;
        Subquery(const Col& c, const Query& q): col(c), query(q) {}
    };



    /// A query expression wrapper whose purpose is to give all
    /// compound expressions a common form.
    template<class T> struct Expr {
        T value;
        explicit Expr(const T& v): value(v) {}
    };

    /// We need to specialize Expr for ColRef because it has to have a
    /// constructor that takes EmptyType as argument.
    template<class Tab, int col_idx, class Type> struct Expr<ColRef<Tab, col_idx, Type> > {
        ColRef<Tab, col_idx, Type> value;
        explicit Expr(EmptyType) {}
    };



    /// Encoding of logical negation.
    struct Not {
        static const char* sym() { return "!"; }
        template<class> struct Result { typedef bool type; };
        template<class A> static bool eval(const A& a) { return !a; }
    };

    /// Encoding of bitwise complementation.
    struct Compl {
        static const char* sym() { return "~"; }
        template<class A> struct Result { typedef typename Promote<A>::type type; };
        template<class A> static typename Result<A>::type eval(const A& a) { return ~a; }
    };

    /// Encoding of unary prefix 'plus' operator, which applies
    /// integral promotion to its argument, but otherwise does
    /// nothing.
    struct Pos {
        static const char* sym() { return "+"; }
        template<class A> struct Result { typedef typename Promote<A>::type type; };
        template<class A> static typename Result<A>::type eval(const A& a) { return +a; }
    };

    /// Encoding of arithmetic negation.
    struct Neg {
        static const char* sym() { return "-"; }
        template<class A> struct Result { typedef typename Promote<A>::type type; };
        template<class A> static typename Result<A>::type eval(const A& a) { return -a; }
    };

    /// Encoding of pointer dereferencing operation.
    struct Deref {
        static const char* sym() { return "*"; }
        template<class A> struct Result { typedef typename RemovePointer<A>::type type; };
        template<class A> static typename Result<A>::type eval(const A& a) { return *a; }
    };

    /// Encoding of multiplication.
    struct Mul {
        static const char* sym() { return "*"; }
        template<class A, class B>
        struct Result { typedef typename ArithBinOpType<A,B>::type type; };
        template<class A, class B>
        static typename Result<A,B>::type eval(const A& a, const B& b) { return a * b; }
    };

    /// Encoding of division.
    struct Div {
        static const char* sym() { return "/"; }
        template<class A, class B>
        struct Result { typedef typename ArithBinOpType<A,B>::type type; };
        template<class A, class B>
        static typename Result<A,B>::type eval(const A& a, const B& b) { return a / b; }
    };

    /// Encoding of modulus operation (remainder of integer division).
    struct Mod {
        static const char* sym() { return "%"; }
        template<class A, class B>
        struct Result { typedef typename ArithBinOpType<A,B>::type type; };
        template<class A, class B>
        static typename Result<A,B>::type eval(const A& a, const B& b) { return a % b; }
    };

    /// Encoding of addition.
    struct Add {
        static const char* sym() { return "+"; }
        template<class A, class B>
        struct Result { typedef typename ArithBinOpType<A,B>::type type; };
        template<class A, class B>
        static typename Result<A,B>::type eval(const A& a, const B& b) { return a + b; }
    };

    /// Encoding of subtraction.
    struct Sub {
        static const char* sym() { return "-"; }
        template<class A, class B>
        struct Result { typedef typename ArithBinOpType<A,B>::type type; };
        template<class A, class B>
        static typename Result<A,B>::type eval(const A& a, const B& b) { return a - b; }
    };

    /// Encoding of 'shift left' operation.
    struct Shl {
        static const char* sym() { return "<<"; }
        template<class A, class> struct Result { typedef typename Promote<A>::type type; };
        template<class A, class B>
        static typename Result<A,B>::type eval(const A& a, const B& b) { return a << b; }
    };

    /// Encoding of 'shift right' operation.
    struct Shr {
        static const char* sym() { return ">>"; }
        template<class A, class> struct Result { typedef typename Promote<A>::type type; };
        template<class A, class B>
        static typename Result<A,B>::type eval(const A& a, const B& b) { return a >> b; }
    };

    /// Encoding of equality comparison operation.
    struct Eq {
        static const char* sym() { return "=="; }
        template<class, class> struct Result { typedef bool type; };
        template<class A, class B> static bool eval(const A& a, const B& b) { return a == b; }
    };

    /// Encoding of 'not equal' comparison operation.
    struct Ne {
        static const char* sym() { return "!="; }
        template<class, class> struct Result { typedef bool type; };
        template<class A, class B> static bool eval(const A& a, const B& b) { return a != b; }
    };

    /// Encoding of 'less than' comparison operation.
    struct Lt {
        static const char* sym() { return "<"; }
        template<class, class> struct Result { typedef bool type; };
        template<class A, class B> static bool eval(const A& a, const B& b) { return a < b; }
    };

    /// Encoding of 'greater than' comparison operation.
    struct Gt {
        static const char* sym() { return ">"; }
        template<class, class> struct Result { typedef bool type; };
        template<class A, class B> static bool eval(const A& a, const B& b) { return a > b; }
    };

    /// Encoding of 'less than or equal' comparison operation.
    struct Le {
        static const char* sym() { return "<="; }
        template<class, class> struct Result { typedef bool type; };
        template<class A, class B> static bool eval(const A& a, const B& b) { return a <= b; }
    };

    /// Encoding of 'greater than or equal' comparison operation.
    struct Ge {
        static const char* sym() { return ">="; }
        template<class, class> struct Result { typedef bool type; };
        template<class A, class B> static bool eval(const A& a, const B& b) { return a >= b; }
    };

    /// Encoding of bitwise 'and' operation.
    struct And {
        static const char* sym() { return "&"; }
        template<class A, class B>
        struct Result { typedef typename ArithBinOpType<A,B>::type type; };
        template<class A, class B>
        static typename Result<A,B>::type eval(const A& a, const B& b) { return a & b; }
    };

    /// Encoding of bitwise 'exclusiv or' operation.
    struct Xor {
        static const char* sym() { return "^"; }
        template<class A, class B>
        struct Result { typedef typename ArithBinOpType<A,B>::type type; };
        template<class A, class B>
        static typename Result<A,B>::type eval(const A& a, const B& b) { return a ^ b; }
    };

    /// Encoding of bitwise 'or' operation.
    struct Or {
        static const char* sym() { return "|"; }
        template<class A, class B>
        struct Result { typedef typename ArithBinOpType<A,B>::type type; };
        template<class A, class B>
        static typename Result<A,B>::type eval(const A& a, const B& b) { return a | b; }
    };

    /// Encoding of logical conjunction.
    struct Conj {
        static const char* sym() { return "&&"; }
        template<class, class> struct Result { typedef bool type; };
        template<class A, class B> static bool eval(const A& a, const B& b) { return a && b; }
    };

    /// Encoding of logical disjunction.
    struct Disj {
        static const char* sym() { return "||"; }
        template<class, class> struct Result { typedef bool type; };
        template<class A, class B> static bool eval(const A& a, const B& b) { return a || b; }
    };


    struct Exists {
        static const char* name() { return "exists"; }
        typedef bool ResultType;
        template<class Tab, class Query>
        static bool eval(const Tab* t, const Query& q) { return t && t->exists(q); }
    };

    struct Count {
        static const char* name() { return "count"; }
        typedef std::size_t ResultType;
        template<class Tab, class Query>
        static std::size_t eval(const Tab* t, const Query& q) { return t ? t->count(q) : 0; }
    };



    template<class Q> inline Expr<Q> expr(const Q& q)
    {
        return Expr<Q>(q);
    }

    template<class Op, class Q> inline UnOp<Op, Q> unop(const Q& q)
    {
        return UnOp<Op, Q>(q);
    }

    template<class Op, class A, class B>
    inline BinOp<Op, A, B> binop(const A& a, const B& b)
    {
        return BinOp<Op, A, B>(a,b);
    }

    template<class Op, class Col, class Query>
    inline Subquery<Op, Col, Query> subquery(const Col& c, const Query& q)
    {
        REALM_STATIC_ASSERT(IsSubtable<typename Col::column_type>::value,
                              "A subtable column is required at this point");
        return Subquery<Op, Col, Query>(c,q);
    }



    template<class Q> inline Expr<UnOp<Not, Q> > operator!(const Expr<Q>& q)
    {
        return expr(unop<Not>(q.value));
    }

    template<class Q> inline Expr<UnOp<Compl, Q> > operator~(const Expr<Q>& q)
    {
        return expr(unop<Compl>(q.value));
    }

    template<class Q> inline Expr<UnOp<Pos, Q> > operator+(const Expr<Q>& q)
    {
        return expr(unop<Pos>(q.value));
    }

    template<class Q> inline Expr<UnOp<Neg, Q> > operator-(const Expr<Q>& q)
    {
        return expr(unop<Neg>(q.value));
    }

    template<class Q> inline Expr<UnOp<Deref, Q> > operator*(const Expr<Q>& q)
    {
        return expr(unop<Deref>(q.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Mul, A, B> > operator*(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Mul>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Mul, A, B> > operator*(const Expr<A>& a, const B& b)
    {
        return expr(binop<Mul>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Mul, A, B> > operator*(const A& a, const Expr<B>& b)
    {
        return expr(binop<Mul>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Div, A, B> > operator/(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Div>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Div, A, B> > operator/(const Expr<A>& a, const B& b)
    {
        return expr(binop<Div>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Div, A, B> > operator/(const A& a, const Expr<B>& b)
    {
        return expr(binop<Div>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Mod, A, B> > operator%(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Mod>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Mod, A, B> > operator%(const Expr<A>& a, const B& b)
    {
        return expr(binop<Mod>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Mod, A, B> > operator%(const A& a, const Expr<B>& b)
    {
        return expr(binop<Mod>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Add, A, B> > operator+(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Add>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Add, A, B> > operator+(const Expr<A>& a, const B& b)
    {
        return expr(binop<Add>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Add, A, B> > operator+(const A& a, const Expr<B>& b)
    {
        return expr(binop<Add>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Sub, A, B> > operator-(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Sub>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Sub, A, B> > operator-(const Expr<A>& a, const B& b)
    {
        return expr(binop<Sub>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Sub, A, B> > operator-(const A& a, const Expr<B>& b)
    {
        return expr(binop<Sub>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Shl, A, B> > operator<<(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Shl>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Shl, A, B> > operator<<(const Expr<A>& a, const B& b)
    {
        return expr(binop<Shl>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Shl, A, B> > operator<<(const A& a, const Expr<B>& b)
    {
        return expr(binop<Shl>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Shr, A, B> > operator>>(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Shr>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Shr, A, B> > operator>>(const Expr<A>& a, const B& b)
    {
        return expr(binop<Shr>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Shr, A, B> > operator>>(const A& a, const Expr<B>& b)
    {
        return expr(binop<Shr>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Eq, A, B> > operator==(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Eq>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Eq, A, B> > operator==(const Expr<A>& a, const B& b)
    {
        return expr(binop<Eq>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Eq, A, B> > operator==(const A& a, const Expr<B>& b)
    {
        return expr(binop<Eq>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Ne, A, B> > operator!=(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Ne>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Ne, A, B> > operator!=(const Expr<A>& a, const B& b)
    {
        return expr(binop<Ne>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Ne, A, B> > operator!=(const A& a, const Expr<B>& b)
    {
        return expr(binop<Ne>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Lt, A, B> > operator<(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Lt>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Lt, A, B> > operator<(const Expr<A>& a, const B& b)
    {
        return expr(binop<Lt>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Lt, A, B> > operator<(const A& a, const Expr<B>& b)
    {
        return expr(binop<Lt>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Gt, A, B> > operator>(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Gt>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Gt, A, B> > operator>(const Expr<A>& a, const B& b)
    {
        return expr(binop<Gt>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Gt, A, B> > operator>(const A& a, const Expr<B>& b)
    {
        return expr(binop<Gt>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Le, A, B> > operator<=(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Le>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Le, A, B> > operator<=(const Expr<A>& a, const B& b)
    {
        return expr(binop<Le>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Le, A, B> > operator<=(const A& a, const Expr<B>& b)
    {
        return expr(binop<Le>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Ge, A, B> > operator>=(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Ge>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Ge, A, B> > operator>=(const Expr<A>& a, const B& b)
    {
        return expr(binop<Ge>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Ge, A, B> > operator>=(const A& a, const Expr<B>& b)
    {
        return expr(binop<Ge>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<And, A, B> > operator&(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<And>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<And, A, B> > operator&(const Expr<A>& a, const B& b)
    {
        return expr(binop<And>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<And, A, B> > operator&(const A& a, const Expr<B>& b)
    {
        return expr(binop<And>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Xor, A, B> > operator^(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Xor>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Xor, A, B> > operator^(const Expr<A>& a, const B& b)
    {
        return expr(binop<Xor>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Xor, A, B> > operator^(const A& a, const Expr<B>& b)
    {
        return expr(binop<Xor>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Or, A, B> > operator|(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Or>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Or, A, B> > operator|(const Expr<A>& a, const B& b)
    {
        return expr(binop<Or>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Or, A, B> > operator|(const A& a, const Expr<B>& b)
    {
        return expr(binop<Or>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Conj, A, B> > operator&&(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Conj>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Conj, A, B> > operator&&(const Expr<A>& a, const B& b)
    {
        return expr(binop<Conj>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Conj, A, B> > operator&&(const A& a, const Expr<B>& b)
    {
        return expr(binop<Conj>(a, b.value));
    }


    template<class A, class B>
    inline Expr<BinOp<Disj, A, B> > operator||(const Expr<A>& a, const Expr<B>& b)
    {
        return expr(binop<Disj>(a.value, b.value));
    }

    template<class A, class B>
    inline Expr<BinOp<Disj, A, B> > operator||(const Expr<A>& a, const B& b)
    {
        return expr(binop<Disj>(a.value, b));
    }

    template<class A, class B>
    inline Expr<BinOp<Disj, A, B> > operator||(const A& a, const Expr<B>& b)
    {
        return expr(binop<Disj>(a, b.value));
    }


    template<class Tab, int col_idx, class Type, class Query>
    inline Expr<Subquery<Exists, ColRef<Tab, col_idx, Type>, Query> >
    exists(const Expr<ColRef<Tab, col_idx, Type> >& col, const Query& query)
    {
        return expr(subquery<Exists>(col.value, query));
    }

    template<class Tab, int col_idx, class Type, class Query>
    inline Expr<Subquery<Count, ColRef<Tab, col_idx, Type>, Query> >
    count(const Expr<ColRef<Tab, col_idx, Type> >& col, const Query& query)
    {
        return expr(subquery<Count>(col.value, query));
    }



    /**
     * Determine whether the specified query expression contains a
     * column reference.
     */
    template<class> struct HasCol {
        static const bool value = false;
    };
    template<class Tab, int col_idx, class Type> struct HasCol<ColRef<Tab, col_idx, Type> > {
        static const bool value = true;
    };
    template<class Op, class A> struct HasCol<UnOp<Op, A> > {
        static const bool value = HasCol<A>::value;
    };
    template<class Op, class A, class B> struct HasCol<BinOp<Op, A, B> > {
        static const bool value = HasCol<A>::value || HasCol<B>::value;
    };
    template<class Op, class Col, class Query> struct HasCol<Subquery<Op, Col, Query> > {
        static const bool value = true;
    };



    template<bool, class, class> struct GetColBinOp;

    /**
     * Get the type and the index of the first column reference in the
     * specified query expression.
     */
    template<class> struct GetCol {};
    template<class Tab, int i, class T> struct GetCol<ColRef<Tab, i, T> > {
        typedef T type;
        static const int col_idx = i;
    };
    template<class Op, class A> struct GetCol<UnOp<Op, A> > {
        typedef typename GetCol<A>::type type;
        static const int col_idx = GetCol<A>::col_idx;
    };
    template<class Op, class A, class B> struct GetCol<BinOp<Op, A, B> > {
        typedef typename GetColBinOp<HasCol<A>::value, A, B>::type type;
        static const int col_idx = GetColBinOp<HasCol<A>::value, A, B>::col_idx;
    };
    template<class Op, class Col, class Query> struct GetCol<Subquery<Op, Col, Query> > {
        typedef typename GetCol<Col>::type type;
        static const int col_idx = GetCol<Col>::col_idx;
    };



    template<bool a_has_col, class A, class B> struct GetColBinOp {
        typedef typename GetCol<A>::type type;
        static const int col_idx = GetCol<A>::col_idx;
    };
    template<class A, class B> struct GetColBinOp<false, A, B> {
        typedef typename GetCol<B>::type type;
        static const int col_idx = GetCol<B>::col_idx;
    };



    template<class Op, class A> struct UnOpResult;
    template<class Op, class A, class B> struct BinOpResult;


    /**
     * Determine the type of the result of executing the specified
     * query expression.
     */
    template<class T> struct ExprResult { typedef T type; };
    template<class Tab, int col_idx, class Type> struct ExprResult<ColRef<Tab, col_idx, Type> > {
        typedef Type type;
    };
    template<class Op, class A> struct ExprResult<UnOp<Op, A> > {
        typedef typename UnOpResult<Op, A>::type type;
    };
    template<class Op, class A, class B> struct ExprResult<BinOp<Op, A, B> > {
        typedef typename BinOpResult<Op, A, B>::type type;
    };
    template<class Op, class Col, class Query> struct ExprResult<Subquery<Op, Col, Query> > {
        typedef typename Op::ResultType type;
    };



    template<class Op, class A> struct UnOpResult {
        typedef typename Op::template Result<typename ExprResult<A>::type>::type type;
    };

    template<class Op, class A, class B> struct BinOpResult {
        typedef typename Op::template Result<typename ExprResult<A>::type,
                                             typename ExprResult<B>::type>::type type;
    };



    template<class Query> struct Canonicalize;
    template<class Query> inline typename Canonicalize<Query>::Result canon(const Query& q)
    {
        return Canonicalize<Query>::exec(q);
    }

    // Canonicalization of a query expression involves elimination of
    // cases where NOT is applied to AND or OR operations. This is
    // because we can evaluate the query expression more efficiently
    // when the AND and OR operations are close to the root.
    template<class Query> struct Canonicalize {
        typedef Query Result;
        static Result exec(const Query& q) { return q; }
    };
    // Get rid of the Expr<Q> wrapper
    template<class Q> struct Canonicalize<Expr<Q> > {
        typedef typename Canonicalize<Q>::Result Result;
        static Result exec(const Expr<Q>& q) { return canon(q.value); }
    };
    // Reduce (!!q) to (q)
    template<class Q> struct Canonicalize<UnOp<Not, UnOp<Not, Q> > > {
        typedef typename Canonicalize<Q>::Result Result;
        static Result exec(const UnOp<Not, UnOp<Not, Q> >& q) { return canon(q.arg.arg); }
    };
    // Rewrite (!(a || b)) to (!a && !b) (De Morgan's law)
    template<class A, class B> struct Canonicalize<UnOp<Not, BinOp<Disj, A, B> > > {
    private:
        typedef typename Canonicalize<UnOp<Not, A> >::Result A2;
        typedef typename Canonicalize<UnOp<Not, B> >::Result B2;
    public:
        typedef BinOp<Conj, A2, B2> Result;
        static Result exec(const UnOp<Not, BinOp<Disj, A, B> >& q)
        {
            return binop<Conj>(canon(unop<Not>(q.arg.left)), canon(unop<Not>(q.arg.right)));
        }
    };
    // Rewrite (!(a && b)) to (!a || !b) (De Morgan's law)
    template<class A, class B> struct Canonicalize<UnOp<Not, BinOp<Conj, A, B> > > {
    private:
        typedef typename Canonicalize<UnOp<Not, A> >::Result A2;
        typedef typename Canonicalize<UnOp<Not, B> >::Result B2;
    public:
        typedef BinOp<Disj, A2, B2> Result;
        static Result exec(const UnOp<Not, BinOp<Conj, A, B> >& q)
        {
            return binop<Disj>(canon(unop<Not>(q.arg.left)), canon(unop<Not>(q.arg.right)));
        }
    };



    /**
     * Handles the evaluation of a query expression based on a
     * specific principal column.
     */
    template<class Tab, int col_idx, class Type> class ColEval {
    public:
        ColEval(const void* c, const Tab* t): m_column(c), m_table(t) {}

        template<class T> T operator()(const T& expr, std::size_t) const
        {
            return expr;
        }

        Type operator()(const ColRef<Tab, col_idx, Type>&, std::size_t i) const
        {
            REALM_STATIC_ASSERT(!IsSubtable<Type>::value,
                                  "A subtable column not acceptable at this point"); // FIXME: Why is this never triggered?
            return static_cast<const Type*>(m_column)[i];
        }

        template<int col_idx2, class Type2>
        Type2 operator()(const ColRef<Tab, col_idx2, Type2>&, std::size_t i) const
        {
            REALM_STATIC_ASSERT(!IsSubtable<Type2>::value,
                                  "A subtable column not acceptable at this point"); // FIXME: Why is this never triggered?
            return m_table->template get<col_idx2, Type2>(i);
        }

        template<class Op, class A>
        typename UnOpResult<Op, A>::type operator()(const UnOp<Op, A>& o, std::size_t i) const
        {
            return Op::eval((*this)(o.arg, i));
        }

        template<class Op, class A, class B>
        typename BinOpResult<Op, A, B>::type
        operator()(const BinOp<Op, A, B>& o, std::size_t i) const
        {
            return Op::eval((*this)(o.left, i), (*this)(o.right, i));
        }

        template<class Op, class Col, class Query>
        typename Op::ResultType operator()(const Subquery<Op, Col, Query>& s, std::size_t i) const
        {
            typedef typename Col::column_type::table_type Subtab;
            return Op::eval(subtable<Subtab>(s.col, i), s.query);
        }

    private:
        const void* const m_column;
        const Tab* m_table;

        template<class Subtab> const Subtab*
        subtable(const ColRef<Tab, col_idx, Type>&, std::size_t i) const
        {
            return static_cast<const Subtab* const*>(m_column)[i];
        }

        template<class Subtab, int col_idx2, class Type2> const Subtab*
        subtable(const ColRef<Tab, col_idx2, Type2>&, std::size_t i) const
        {
            return m_table->template get<col_idx2, const Subtab*>(i);
        }
    };



    template<class Ch, class Tr, class Tab, int col_idx, class Type>
    inline std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out,
                                                  const ColRef<Tab, col_idx, Type>&)
    {
        out << "t" << '.' << Tab::get_column_name(col_idx);
        return out;
    }
    template<class Ch, class Tr, class Op, class Q>
    inline std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out,
                                                  const UnOp<Op, Q>& q)
    {
        out << Op::sym() << '(' << q.arg << ')';
        return out;
    }
    template<class Ch, class Tr, class Op, class A, class B>
    inline std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out,
                                                  const BinOp<Op, A, B>& q)
    {
        out << '(' << q.left << ')' << Op::sym() << '(' << q.right << ')';
        return out;
    }
    template<class Ch, class Tr, class Op, class Col, class Query>
    inline std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out,
                                                  const Subquery<Op, Col, Query>& q)
    {
        out << Op::name() << '(' << q.col << ';' << q.query << ')';
        return out;
    }
}





template<class Spec> class BasicTable {
private:
    /**
     * This template class simply maps a column index to the
     * appropriate query expression type.
     */
    template<int col_idx> struct QueryCol {
        typedef typename TypeAt<typename Spec::Columns, col_idx>::type val_type;
        typedef query::ColRef<BasicTable, col_idx, val_type> expr_type;
        typedef query::Expr<expr_type> type;
    };

    typedef typename Spec::template ColNames<QueryCol, query::EmptyType> QueryRowBase;

public:
    // FIXME: Make private
    template<int col_idx, class Type> Type get(std::size_t i) const
    {
        return static_cast<Type*>(m_cols[col_idx])[i];

    }

    std::size_t size() const { return m_size; }

    static const char* get_column_name(int col_idx)
    {
        return Spec::col_names()[col_idx];
    }

    BasicTable(): m_size(0)
    {
        m_size = 256;
        m_cols = new void*[TypeCount<typename Spec::Columns>::value];
        ForEachType<typename Spec::Columns, MakeCol>::exec(this);
    }

    struct QueryRow: QueryRowBase { QueryRow(): QueryRowBase(query::EmptyType()) {} };

    template<class Query> bool exists(const Query& q) const
    {
        return _exists(query::canon(q));
    }

    template<class Query> std::size_t count(const Query& q) const
    {
        return _count(query::canon(q));
    }

private:
    std::size_t m_size;
    void** m_cols;

    template<class Type, int col_idx> struct MakeCol {
        static void exec(BasicTable* t) { t->m_cols[col_idx] = new Type[t->m_size]; }
    };
    template<class T, int col_idx> struct MakeCol<SpecBase::Subtable<T>, col_idx> {
        static void exec(BasicTable* t) { t->m_cols[col_idx] = new T*[t->m_size]; }
    };

    template<class T> bool _exists(const T& q) const
    {
        const std::size_t end = size();
        const std::size_t i = _find(q, 0, end);
        return i != end;
    }

    template<class T> std::size_t _count(const T& q) const
    {
//        std::cout << q << std::endl;
        std::size_t n = 0, i = 0;
        const std::size_t end = size();

        for (;;) {
            i = _find(q, i, end);
            if (i == end) break;
            ++n;
            ++i;

        }
        return n;
    }



    template<class A, class B> std::size_t _find(const query::BinOp<query::Disj, A, B>& q, std::size_t begin, std::size_t end) const
    {
        return Find_OR<query::HasCol<A>::value, query::HasCol<B>::value, A, B>::find(this, q.left, q.right, begin, end);
    }

    template<class A, class B> std::size_t _find(const query::BinOp<query::Conj, A, B>& q, std::size_t begin, std::size_t end) const
    {
/*
        return Find_AND<query::HasCol<A>::value, query::HasCol<B>::value, A, B>::find(this, q.left, q.right, begin, end);
--
*/
        for (;;) {
            const std::size_t i = _find_AND(q, begin, end);
            if (i == begin || i == end) return i;
            begin = i;
        }
    }

    template<class A, class B> std::size_t _find_AND(const query::BinOp<query::Conj, A, B>& q, std::size_t begin, std::size_t end) const
    {
        begin = _find_AND(q.left,  begin, end);
        begin = _find_AND(q.right, begin, end);
        return begin;
    }

    template<class T> std::size_t _find_AND(const T& q, std::size_t begin, std::size_t end) const
    {
        return _find(q, begin, end);
    }

    template<class Query> std::size_t _find(const Query& q, std::size_t begin, std::size_t end) const
    {
        return Find<query::HasCol<Query>::value, Query>::find(this, q, begin, end);
    }


    template<bool has_col, class T> struct Find {
        static std::size_t find(const BasicTable* t, const T& q, std::size_t begin, std::size_t end)
        {
            typedef typename query::GetCol<T>::type Type;
            const int col_idx = query::GetCol<T>::col_idx;
            query::ColEval<BasicTable, col_idx, Type> eval(t->m_cols[col_idx], t);
            for (std::size_t i = begin; i != end; ++i)
                if (eval(q,i)) return i;
            return end;
        }
    };
    template<class T> struct Find<false, T> {
        static std::size_t find(const BasicTable*, const T& q, std::size_t begin, std::size_t end)
        {
            return q ? begin : end;
        }
    };



    // FIXME: This implementation is not omptimal in that a range of
    // rows may be scanned multiple times for a particular
    // condition. In particular, if the left hand condition finds a
    // match after 100 rows, and the right hand condition then finds
    // one after 50 rows, then rows 51 through 100 will be scanned
    // again for the left hand condition during the subsequent
    // invocation of find().
    template<bool a_has_col, bool b_has_col, class A, class B> struct Find_OR {
        static std::size_t find(const BasicTable* t, const A& a, const B& b, std::size_t begin, std::size_t end)
        {
            const std::size_t i = t->_find(a, begin, end);
            const std::size_t j = t->_find(b, begin, i);
            return std::min(i,j);
        }
    };
    template<class A, class B> struct Find_OR<false, true, A, B> {
        static std::size_t find(const BasicTable* t, const A& a, const B& b, std::size_t begin, std::size_t end)
        {
            if (a) return begin; // because (true || x) is aways true
            return t->_find(b, begin, end);
        }
    };
    template<class A, class B> struct Find_OR<true, false, A, B> {
        static std::size_t find(const BasicTable* t, const A& a, const B& b, std::size_t begin, std::size_t end)
        {
            if (b) return begin; // because (x || true) is aways true
            return t->_find(a, begin, end);
        }
    };


/*
    template<bool a_has_col, bool b_has_col, class A, class B> struct Find_AND {
        static std::size_t find(const BasicTable* t, const A& a, const B& b, std::size_t begin, std::size_t end)
        {
            for (;;) {
                const std::size_t i = _find_AND(q, begin, end);
                if (i == begin || i == end) return i;
                begin = i;
            }
        }
    };
    template<class A, class B> struct Find_AND<false, true, A, B> {
        static std::size_t find(const BasicTable* t, const A& a, const B& b, std::size_t begin, std::size_t end)
        {
            if (!a) return begin; // because (false && x) is aways false
            return t->_find(b, begin, end);
        }
    };
    template<class A, class B> struct Find_AND<true, false, A, B> {
        static std::size_t find(const BasicTable* t, const A& a, const B& b, std::size_t begin, std::size_t end)
        {
            if (!b) return begin; // because (x && false) is aways false
            return t->_find(a, begin, end);
        }
    };
*/
};

} // namespace tightdb
