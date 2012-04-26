#ifndef TIGHTDB_MIXED_H
#define TIGHTDB_MIXED_H

#ifndef _MSC_VER
#include <stdint.h> // int64_t - not part of C++03
#else
#include "win32/stdint.h"
#endif

#include <cassert>
#include <cstddef> // size_t

#include "ColumnType.hpp"
#include "date.hpp"

namespace tightdb {


class Mixed {
public:
    explicit Mixed(ColumnType v)
    {
        assert(v == COLUMN_TYPE_TABLE);
        static_cast<void>(v);
        m_type = COLUMN_TYPE_TABLE;
    }

    Mixed(bool v)        {m_type = COLUMN_TYPE_BOOL;   m_bool = v;}
    Mixed(Date v)        {m_type = COLUMN_TYPE_DATE;   m_date = v.GetDate();}
    Mixed(int64_t v)     {m_type = COLUMN_TYPE_INT;    m_int  = v;}
    Mixed(const char* v) {m_type = COLUMN_TYPE_STRING; m_str  = v;}
    Mixed(BinaryData v)  {m_type = COLUMN_TYPE_BINARY; m_str = v.pointer; m_len = v.len;}
    Mixed(const char* v, std::size_t len) {m_type = COLUMN_TYPE_BINARY; m_str = v; m_len = len;}

    ColumnType GetType() const {return m_type;}

    int64_t     GetInt()    const {assert(m_type == COLUMN_TYPE_INT);    return m_int;}
    bool        GetBool()   const {assert(m_type == COLUMN_TYPE_BOOL);   return m_bool;}
    std::time_t GetDate()   const {assert(m_type == COLUMN_TYPE_DATE);   return m_date;}
    const char* GetString() const {assert(m_type == COLUMN_TYPE_STRING); return m_str;}
    BinaryData  GetBinary() const {assert(m_type == COLUMN_TYPE_BINARY); BinaryData b = {m_str, m_len}; return b;}

private:
    ColumnType m_type;
    union {
        int64_t m_int;
        bool    m_bool;
        std::time_t  m_date;
        const char* m_str;
    };
    std::size_t m_len;
};


} // namespace tightdb

#endif // TIGHTDB_MIXED_H
