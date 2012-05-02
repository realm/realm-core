#ifndef TIGHTDB_STATIC_ASSERT_H
#define TIGHTDB_STATIC_ASSERT_H


#define TIGHTDB_STATIC_ASSERT(assertion) typedef \
  tightdb::static_assert_dummy<sizeof(tightdb::STATIC_ASSERTION_FAILURE<static_cast<bool>(assertion)>)> \
  TIGHTDB_ADD_LINENO_TO_NAME(tightdb_static_assert_)

#define TIGHTDB_ADD_LINENO_TO_NAME_3(x,y) x##y
#define TIGHTDB_ADD_LINENO_TO_NAME_2(x,y) TIGHTDB_ADD_LINENO_TO_NAME_3(x,y)
#define TIGHTDB_ADD_LINENO_TO_NAME(x) TIGHTDB_ADD_LINENO_TO_NAME_2(x, __LINE__)


namespace tightdb {


template<bool> struct STATIC_ASSERTION_FAILURE;
template<> struct STATIC_ASSERTION_FAILURE<true> {};

template<int> struct static_assert_dummy {};


} // namespace tightdb

#endif // TIGHTDB_STATIC_ASSERT_H
