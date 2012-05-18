#include <iostream>

/**
 * The 'cons' operator for building lists of types.
 *
 * \tparam H The head of the list, that is, the first type in the
 * list.
 *
 * \tparam T The tail of the list, that is, the list of types
 * following the head. It is 'void' if nothing follows the head,
 * otherwise it matches TypeCons<H2,T2>.
 *
 * Note that 'void' is considered as a zero-length list.
 */
template<class H, class T> struct TypeCons {
  typedef H head;
  typedef T tail;
};


/**
 * Append a type the the end of a type list. The resulting type list
 * is available as TypeAppend<List, T>::type.
 *
 * \tparam List A list of types constructed using TypeCons<>. Note
 * that 'void' is considered as a zero-length list.
 *
 * \tparam T The new type to be appended.
 */
template<class List, class T> struct TypeAppend {
  typedef TypeCons<typename List::head, typename TypeAppend<typename List::tail, T>::type> type;
};
template<class T> struct TypeAppend<void, T> {
  typedef TypeCons<T, void> type;
};


/**
 * Get an element from the specified list of types. The
 * result is available as TypeAt<List, i>::type.
 *
 * \tparam List A list of types constructed using TypeCons<>. Note
 * that 'void' is considered as a zero-length list.
 *
 * \tparam i The index of the list element to get.
 */
template<class List, int i> struct TypeAt {
  typedef typename TypeAt<typename List::tail, i-1>::type type;
};
template<class List> struct TypeAt<List, 0> { typedef typename List::head type; };


/**
 * Count the number of elements in the specified list of types. The
 * result is available as TypeCount<List>::value.
 *
 * \tparam List The list of types constructed using TypeCons<>. Note
 * that 'void' is considered as a zero-length list.
 */
template<class List> struct TypeCount {
  static const int value = 1 + TypeCount<typename List::tail>::value;
};
template<> struct TypeCount<void> { static const int value = 0; };


/**
 * Execute an action for each element in the specified list of types.
 *
 * \tparam List The list of types constructed using TypeCons<>. Note
 * that 'void' is considered as a zero-length list.
 */
template<class List, int i=0> struct ForEachType {
  template<class Op> static void exec(Op& o)
  {
    o.template exec<typename List::head, i>();
    ForEachType<typename List::tail, i+1>::exec(o);
  }
  template<class Op> static void exec(const Op& o)
  {
    o.template exec<typename List::head, i>();
    ForEachType<typename List::tail, i+1>::exec(o);
  }
};
template<int i> struct ForEachType<void, i> {
  template<class Op> static void exec(Op&) {}
  template<class Op> static void exec(const Op&) {}
};







template<class L> struct Tuple {
  typedef typename L::head head_type;
  typedef Tuple<typename L::tail> tail_type;
  head_type m_head;
  tail_type m_tail;
  Tuple(const head_type& h, const tail_type& t): m_head(h), m_tail(t) {}
};
template<> struct Tuple<void> {};


inline Tuple<void> tuple() { return Tuple<void>(); }

template<class T> inline Tuple<TypeCons<T, void> > tuple(const T& v)
{
  return Tuple<TypeCons<T, void> >(v, tuple());
}

template<class H, class T> inline Tuple<TypeCons<H,T> > cons(const H& h, const Tuple<T>& t)
{
  return Tuple<TypeCons<H,T> >(h,t);
}


template<class L, class V>
inline Tuple<typename TypeAppend<L,V>::type> append(const Tuple<L>& t, const V& v)
{
  return cons(t.m_head, append(t.m_tail, v));
}
template<class V>
inline Tuple<TypeCons<V, void> > append(const Tuple<void>&, const V& v)
{
  return tuple(v);
}
template<class L, class V>
inline Tuple<typename TypeAppend<L,V>::type> operator,(const Tuple<L>& t, const V& v)
{
  return append(t,v);
}

namespace _impl {
  template<class L, int i> struct TupleAt {
    static typename TypeAt<L,i>::type exec(const Tuple<L>& t)
    {
      return TupleAt<typename L::tail, i-1>(t.m_tail);
    }
  };
  template<class L> struct TupleAt<L,0> {
    static L::head exec(const Tuple<L>& t) { return t.m_head; }
  };

  template<class Ch, class Tr, class T>
  inline void write(std::basic_ostream<Ch, Tr>& out, const Tuple<TypeCons<T, void> >& t)
  {
    out << t.m_head;
  }
  template<class Ch, class Tr>
  inline void write(std::basic_ostream<Ch, Tr>& out, const Tuple<void>&) {}
  template<class Ch, class Tr, class L>
  inline void write(std::basic_ostream<Ch, Tr>& out, const Tuple<L>& t)
  {
    out << t.m_head << ',';
    write(out, t.m_tail);
  }
}

template<int i, class L> typename TypeAt<L,i>::type at(const Tuple<L>& tuple)
{
  return _impl::TupleAt<L,i>::exec(tuple);
}

template<class Ch, class Tr, class L>
std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out, const Tuple<L>& t)
{
  out << '(';
  _impl::write(out, t);
  out << ')';
  return out;
}


struct A {
  typedef TypeCons<int, TypeCons<bool, TypeCons<char, void> > > ColTypes;
  template<class L> void add(const Tuple<L>& tuple)
  {
    std::cout << tuple << std::endl;
    TIGHTDB_STATIC_ASSERT(TypeCount<L>::value == TypeCount<ColTypes>::value);
    ForEachType<ColTypes>::exec(Inserter<Tuple<L> >(this, size(), tuple));
  }

private:
  template<Tuple> struct Inserter {
    template<class T, int col_idx> void exec()
    {
      ColumnAccessor<col_idx, T>::insert(m_table, m_row_idx, at<col_idx>(m_tuple));
    }
  };
};


int main()
{
  A a;
  a.add((cons(2, cons(3, tuple(4)))));
  a.add((tuple(2), 3, 4));
  a.add((tuple(), (const char*)"Vig", 3, 'y'));
  return 0;
}
