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
#ifndef TIGHTDB_UTIL_BIND_HPP
#define TIGHTDB_UTIL_BIND_HPP

namespace tightdb {


namespace _impl {


template<class A> class FunArgBinder0 {
public:
    FunArgBinder0(void (*fun)(A), const A& a): m_fun(fun), m_a(a) {}
    void operator()() const { (*m_fun)(m_a); }
private:
    void (*const m_fun)(A);
    const A m_a;
};

template<class A, class B> class FunArgBinder1 {
public:
    FunArgBinder1(void (*fun)(A,B), const A& a): m_fun(fun), m_a(a) {}
    void operator()(B b) const { (*m_fun)(m_a, b); }
private:
    void (*const m_fun)(A,B);
    const A m_a;
};

template<class A, class B, class C> class FunArgBinder2 {
public:
    FunArgBinder2(void (*fun)(A,B,C), const A& a): m_fun(fun), m_a(a) {}
    void operator()(B b, C c) const { (*m_fun)(m_a, b, c); }
private:
    void (*const m_fun)(A,B,C);
    const A m_a;
};



template<class A, class B> class FunArgArgBinder0 {
public:
    FunArgArgBinder0(void (*fun)(A,B), const A& a, const B& b): m_fun(fun), m_a(a), m_b(b) {}
    void operator()() const { (*m_fun)(m_a, m_b); }
private:
    void (*const m_fun)(A,B);
    const A m_a;
    const B m_b;
};

template<class A, class B, class C> class FunArgArgBinder1 {
public:
    FunArgArgBinder1(void (*fun)(A,B,C), const A& a, const B& b): m_fun(fun), m_a(a), m_b(b) {}
    void operator()(C c) const { (*m_fun)(m_a, m_b, c); }
private:
    void (*const m_fun)(A,B,C);
    const A m_a;
    const B m_b;
};

template<class A, class B, class C, class D> class FunArgArgBinder2 {
public:
    FunArgArgBinder2(void (*fun)(A,B,C,D), const A& a, const B& b): m_fun(fun), m_a(a), m_b(b) {}
    void operator()(C c, D d) const { (*m_fun)(m_a, m_b, c, d); }
private:
    void (*const m_fun)(A,B,C,D);
    const A m_a;
    const B m_b;
};



template<class O> class MemFunObjBinder0 {
public:
    MemFunObjBinder0(void (O::*mem_fun)(), O* obj): m_mem_fun(mem_fun), m_obj(obj) {}
    void operator()() const { (m_obj->*m_mem_fun)(); }
private:
    void (O::*const m_mem_fun)();
    O* const m_obj;
};

template<class O, class A> class MemFunObjBinder1 {
public:
    MemFunObjBinder1(void (O::*mem_fun)(A), O* obj): m_mem_fun(mem_fun), m_obj(obj) {}
    void operator()(A a) const { (m_obj->*m_mem_fun)(a); }
private:
    void (O::*const m_mem_fun)(A);
    O* const m_obj;
};

template<class O, class A, class B> class MemFunObjBinder2 {
public:
    MemFunObjBinder2(void (O::*mem_fun)(A,B), O* obj): m_mem_fun(mem_fun), m_obj(obj) {}
    void operator()(A a, B b) const { (m_obj->*m_mem_fun)(a,b); }
private:
    void (O::*const m_mem_fun)(A,B);
    O* const m_obj;
};



template<class O, class A> class MemFunObjArgBinder0 {
public:
    MemFunObjArgBinder0(void (O::*mem_fun)(A), O* obj, const A& a):
        m_mem_fun(mem_fun), m_obj(obj), m_a(a) {}
    void operator()() const { (m_obj->*m_mem_fun)(m_a); }
private:
    void (O::*const m_mem_fun)(A);
    O* const m_obj;
    const A m_a;
};

template<class O, class A, class B> class MemFunObjArgBinder1 {
public:
    MemFunObjArgBinder1(void (O::*mem_fun)(A,B), O* obj, const A& a):
        m_mem_fun(mem_fun), m_obj(obj), m_a(a) {}
    void operator()(B b) const { (m_obj->*m_mem_fun)(m_a, b); }
private:
    void (O::*const m_mem_fun)(A,B);
    O* const m_obj;
    const A m_a;
};

template<class O, class A, class B, class C> class MemFunObjArgBinder2 {
public:
    MemFunObjArgBinder2(void (O::*mem_fun)(A,B,C), O* obj, const A& a):
        m_mem_fun(mem_fun), m_obj(obj), m_a(a) {}
    void operator()(B b, C c) const { (m_obj->*m_mem_fun)(m_a, b, c); }
private:
    void (O::*const m_mem_fun)(A,B,C);
    O* const m_obj;
    const A m_a;
};


} // namespace _impl



namespace util {


/// Produce a nullary function by binding the argument of a unary
/// function.
template<class A>
inline _impl::FunArgBinder0<A> bind(void (*fun)(A), const A& a)
{
    return _impl::FunArgBinder0<A>(fun, a);
}

/// Produce a unary function by binding the first argument of a binary
/// function.
template<class A, class B>
inline _impl::FunArgBinder1<A,B> bind(void (*fun)(A,B), const A& a)
{
    return _impl::FunArgBinder1<A,B>(fun, a);
}

/// Produce a binary function by binding the first argument of a
/// ternary function.
template<class A, class B, class C>
inline _impl::FunArgBinder2<A,B,C> bind(void (*fun)(A,B,C), const A& a)
{
    return _impl::FunArgBinder2<A,B,C>(fun, a);
}



/// Produce a nullary function by binding both arguments of a binary
/// function.
template<class A, class B>
inline _impl::FunArgArgBinder0<A,B> bind(void (*fun)(A,B), const A& a, const B& b)
{
    return _impl::FunArgArgBinder0<A,B>(fun, a, b);
}

/// Produce a unary function by binding the first two arguments of a
/// ternary function.
template<class A, class B, class C>
inline _impl::FunArgArgBinder1<A,B,C> bind(void (*fun)(A,B,C), const A& a, const B& b)
{
    return _impl::FunArgArgBinder1<A,B,C>(fun, a, b);
}

/// Produce a binary function by binding the first two arguments of a
/// quaternary function (4-ary).
template<class A, class B, class C, class D>
inline _impl::FunArgArgBinder2<A,B,C,D> bind(void (*fun)(A,B,C,D), const A& a, const B& b)
{
    return _impl::FunArgArgBinder2<A,B,C,D>(fun, a, b);
}



/// Produce a nullary function by binding the object of a nullary
/// class member function.
template<class O>
inline _impl::MemFunObjBinder0<O> bind(void (O::*mem_fun)(), O* obj)
{
    return _impl::MemFunObjBinder0<O>(mem_fun, obj);
}

/// Produce a unary function by binding the object of a unary class
/// member function.
template<class O, class A>
inline _impl::MemFunObjBinder1<O,A> bind(void (O::*mem_fun)(A), O* obj)
{
    return _impl::MemFunObjBinder1<O,A>(mem_fun, obj);
}

/// Produce a binary function by binding the object of a binary class
/// member function.
template<class O, class A, class B>
inline _impl::MemFunObjBinder2<O,A,B> bind(void (O::*mem_fun)(A,B), O* obj)
{
    return _impl::MemFunObjBinder2<O,A,B>(mem_fun, obj);
}



/// Produce a nullary function by binding the object and the argument
/// of a unary class member function.
template<class O, class A>
inline _impl::MemFunObjArgBinder0<O,A> bind(void (O::*mem_fun)(A), O* obj, const A& a)
{
    return _impl::MemFunObjArgBinder0<O,A>(mem_fun, obj, a);
}

/// Produce a unary function by binding the object and first argument
/// of a binary class member function.
template<class O, class A, class B>
inline _impl::MemFunObjArgBinder1<O,A,B> bind(void (O::*mem_fun)(A,B), O* obj, const A& a)
{
    return _impl::MemFunObjArgBinder1<O,A,B>(mem_fun, obj, a);
}

/// Produce a binary function by binding the object and first argument
/// of a ternary class member function.
template<class O, class A, class B, class C>
inline _impl::MemFunObjArgBinder2<O,A,B,C> bind(void (O::*mem_fun)(A,B,C), O* obj, const A& a)
{
    return _impl::MemFunObjArgBinder2<O,A,B,C>(mem_fun, obj, a);
}


} // namespace util
} // namespace tightdb

#endif // TIGHTDB_UTIL_BIND_HPP
