#ifndef TIGHTDB_META_H
#define TIGHTDB_META_H

namespace tightdb {


template<class From, class To> struct CopyConstness { typedef To type; };
template<class From, class To> struct CopyConstness<const From, To> { typedef const To type; };


} // namespace tightdb

#endif // TIGHTDB_META_H
