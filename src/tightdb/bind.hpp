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


template<class O> class MemFunObjBinder {
public:
    MemFunObjBinder(void (O::*mem_fun)(), O* obj): m_mem_fun(mem_fun), m_obj(obj) {}
    void operator()() const { (m_obj->*m_mem_fun)(); }
private:
    void (O::* const m_mem_fun)();
    O* const m_obj;
};

template<class O, class A> class MemFunObjBinder1 {
public:
    MemFunObjBinder1(void (O::*mem_fun)(A), O* obj): m_mem_fun(mem_fun), m_obj(obj) {}
    void operator()(A a) const { (m_obj->*m_mem_fun)(a); }
private:
    void (O::* const m_mem_fun)(A);
    O* const m_obj;
};

template<class O, class A, class B> class MemFunObjBinder2 {
public:
    MemFunObjBinder2(void (O::*mem_fun)(A,B), O* obj): m_mem_fun(mem_fun), m_obj(obj) {}
    void operator()(A a, B b) const { (m_obj->*m_mem_fun)(a,b); }
private:
    void (O::* const m_mem_fun)(A,B);
    O* const m_obj;
};


} // namespace _impl

namespace util {


template<class O>
inline _impl::MemFunObjBinder<O> bind(void (O::*mem_fun)(), O* obj)
{
    return _impl::MemFunObjBinder<O>(mem_fun, obj);
}

template<class O, class A>
inline _impl::MemFunObjBinder1<O,A> bind(void (O::*mem_fun)(A), O* obj)
{
    return _impl::MemFunObjBinder1<O,A>(mem_fun, obj);
}

template<class O, class A, class B>
inline _impl::MemFunObjBinder2<O,A,B> bind(void (O::*mem_fun)(A,B), O* obj)
{
    return _impl::MemFunObjBinder2<O,A,B>(mem_fun, obj);
}


} // namespace util
} // namespace tightdb

#endif // TIGHTDB_UTIL_BIND_HPP
