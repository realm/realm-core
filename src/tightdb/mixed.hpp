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
#ifndef TIGHTDB_MIXED_HPP
#define TIGHTDB_MIXED_HPP

#ifndef _MSC_VER
#include <stdint.h> // int64_t - not part of C++03, not even required by C++11 to be present (see C++11 section 18.4.1)
#else
#include <win32/stdint.h>
#endif

#include <cstddef> // size_t
#include <cstring>

#include <tightdb/assert.hpp>
#include <tightdb/meta.hpp>
#include <tightdb/column_type.hpp>
#include <tightdb/date.hpp>
#include <tightdb/binary_data.hpp>

namespace tightdb {


class Mixed {
public:
    Mixed()               {m_type = COLUMN_TYPE_INT;    m_int  = 0;}
    Mixed(bool v)         {m_type = COLUMN_TYPE_BOOL;   m_bool = v;}
    Mixed(int64_t v)      {m_type = COLUMN_TYPE_INT;    m_int  = v;}
    Mixed(float v)       {m_type = COLUMN_TYPE_FLOAT;  m_float = v;}
    Mixed(double v)      {m_type = COLUMN_TYPE_DOUBLE; m_double = v;}
    Mixed(const char* v)  {m_type = COLUMN_TYPE_STRING; m_str  = v;}
    Mixed(BinaryData v)   {m_type = COLUMN_TYPE_BINARY; m_str = v.pointer; m_len = v.len;}
    Mixed(Date v)         {m_type = COLUMN_TYPE_DATE;   m_date = v.get_date();}

    struct subtable_tag {};
    Mixed(subtable_tag): m_type(COLUMN_TYPE_TABLE) {}

    ColumnType get_type() const {return m_type;}

    bool         get_bool()   const;
    int64_t      get_int()    const;
    float       get_float()  const;
    double      get_double() const;
    const char*  get_string() const;
    BinaryData   get_binary() const;
    std::time_t  get_date()   const;

    void set_bool(bool);
    void set_int(int64_t);
    void set_string(const char*);
    void set_binary(BinaryData);
    void set_binary(const char* data, std::size_t size);
    void set_date(std::time_t);

    template<class Ch, class Tr>
    friend std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>&, const Mixed&);

private:
    ColumnType m_type;
    union {
        int64_t      m_int;
        bool         m_bool;
        float        m_float;
        double       m_double;
        const char*  m_str;
        std::time_t  m_date;
    };
    std::size_t m_len;
};

// Note: We cannot compare two mixed values, since when the type of
// both is COLUMN_TYPE_TABLE, we would have to compare the two tables,
// but the mixed values do not provide access to those tables.

// Note: The mixed values are specified as Wrap<Mixed>. If they were
// not, these operators would apply to simple comparisons, such as int
// vs int64_t, and cause ambiguity. This is because the constructors
// of Mixed are not explicit.

// Compare mixed with boolean
bool operator==(Wrap<Mixed>, bool);
bool operator!=(Wrap<Mixed>, bool);
bool operator==(bool, Wrap<Mixed>);
bool operator!=(bool, Wrap<Mixed>);

// Compare mixed with integer
template<class T> bool operator==(Wrap<Mixed>, const T&);
template<class T> bool operator!=(Wrap<Mixed>, const T&);
template<class T> bool operator==(const T&, Wrap<Mixed>);
template<class T> bool operator!=(const T&, Wrap<Mixed>);

// Compare mixed with float
bool operator==(Wrap<Mixed>, float);
bool operator!=(Wrap<Mixed>, float);
bool operator==(float, Wrap<Mixed>);
bool operator!=(float, Wrap<Mixed>);

// Compare mixed with double
bool operator==(Wrap<Mixed>, double);
bool operator!=(Wrap<Mixed>, double);
bool operator==(double, Wrap<Mixed>);
bool operator!=(double, Wrap<Mixed>);

// Compare mixed with zero-terminated string
bool operator==(Wrap<Mixed>, const char*);
bool operator!=(Wrap<Mixed>, const char*);
bool operator==(const char*, Wrap<Mixed>);
bool operator!=(const char*, Wrap<Mixed>);
bool operator==(Wrap<Mixed>, char*);
bool operator!=(Wrap<Mixed>, char*);
bool operator==(char*, Wrap<Mixed>);
bool operator!=(char*, Wrap<Mixed>);

// Compare mixed with binary data
bool operator==(Wrap<Mixed>, BinaryData);
bool operator!=(Wrap<Mixed>, BinaryData);
bool operator==(BinaryData, Wrap<Mixed>);
bool operator!=(BinaryData, Wrap<Mixed>);

// Compare mixed with date
bool operator==(Wrap<Mixed>, Date);
bool operator!=(Wrap<Mixed>, Date);
bool operator==(Date, Wrap<Mixed>);
bool operator!=(Date, Wrap<Mixed>);




// Implementation:

inline bool Mixed::get_bool() const
{
    TIGHTDB_ASSERT(m_type == COLUMN_TYPE_BOOL);
    return m_bool;
}

inline int64_t Mixed::get_int() const
{
    TIGHTDB_ASSERT(m_type == COLUMN_TYPE_INT);
    return m_int;
}

inline float Mixed::get_float() const
{
    TIGHTDB_ASSERT(m_type == COLUMN_TYPE_FLOAT);
    return m_float;
}

inline double Mixed::get_double() const
{
    TIGHTDB_ASSERT(m_type == COLUMN_TYPE_DOUBLE);
    return m_double;
}

inline const char* Mixed::get_string() const
{
    TIGHTDB_ASSERT(m_type == COLUMN_TYPE_STRING);
    return m_str;
}

inline BinaryData Mixed::get_binary() const
{
    TIGHTDB_ASSERT(m_type == COLUMN_TYPE_BINARY);
    return BinaryData(m_str, m_len);
}

inline std::time_t Mixed::get_date() const
{
    TIGHTDB_ASSERT(m_type == COLUMN_TYPE_DATE);
    return m_date;
}

inline void Mixed::set_bool(bool v)
{
    m_type = COLUMN_TYPE_BOOL;
    m_bool = v;
}

inline void Mixed::set_int(int64_t v)
{
    m_type = COLUMN_TYPE_INT;
    m_int = v;
}

inline void Mixed::set_string(const char* v)
{
    m_type = COLUMN_TYPE_STRING;
    m_str = v;
}

inline void Mixed::set_binary(BinaryData v)
{
    set_binary(v.pointer, v.len);
}

inline void Mixed::set_binary(const char* data, std::size_t size)
{
    m_type = COLUMN_TYPE_BINARY;
    m_str = data;
    m_len = size;
}

inline void Mixed::set_date(std::time_t v)
{
    m_type = COLUMN_TYPE_DATE;
    m_date = v;
}


template<class Ch, class Tr>
inline std::basic_ostream<Ch, Tr>& operator<<(std::basic_ostream<Ch, Tr>& out, const Mixed& m)
{
    out << "Mixed(";
    switch (m.m_type) {
    case COLUMN_TYPE_BOOL: out << m.m_bool; break;
    case COLUMN_TYPE_INT: out << m.m_int; break;
    case COLUMN_TYPE_STRING: out << m.m_str; break;   
    case COLUMN_TYPE_FLOAT:  out << m.m_float; break;
    case COLUMN_TYPE_DOUBLE: out << m.m_double; break;
    case COLUMN_TYPE_BINARY: out << BinaryData(m.m_str, m.m_len); break;
    case COLUMN_TYPE_DATE: out << Date(m.m_date); break;
    case COLUMN_TYPE_TABLE: out << "subtable"; break;
    default: TIGHTDB_ASSERT(false); break;
    }
    out << ")";
    return out;
    
    

// Compare mixed with boolean

inline bool operator==(Wrap<Mixed> a, bool b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_BOOL && Mixed(a).get_bool() == b;
}

inline bool operator!=(Wrap<Mixed> a, bool b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_BOOL && Mixed(a).get_bool() != b;
}

inline bool operator==(bool a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_BOOL && a == Mixed(b).get_bool();
}

inline bool operator!=(bool a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_BOOL && a != Mixed(b).get_bool();
}


// Compare mixed with integer

template<class T> inline bool operator==(Wrap<Mixed> a, const T& b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_INT && Mixed(a).get_int() == b;
}

template<class T> inline bool operator!=(Wrap<Mixed> a, const T& b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_INT && Mixed(a).get_int() != b;
}

template<class T> inline bool operator==(const T& a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_INT && a == Mixed(b).get_int();
}

template<class T> inline bool operator!=(const T& a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_INT && a != Mixed(b).get_int();
}

// Compare mixed with float

inline bool operator==(Wrap<Mixed> a, float b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_FLOAT && Mixed(a).get_float() == b;
}

inline bool operator!=(Wrap<Mixed> a, float b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_FLOAT && Mixed(a).get_float() != b;
}

inline bool operator==(float a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_FLOAT && a == Mixed(b).get_float();
}

inline bool operator!=(float a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_FLOAT && a != Mixed(b).get_float();
}


// Compare mixed with double

inline bool operator==(Wrap<Mixed> a, double b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_DOUBLE && Mixed(a).get_double() == b;
}

inline bool operator!=(Wrap<Mixed> a, double b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_DOUBLE && Mixed(a).get_double() != b;
}

inline bool operator==(double a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_DOUBLE && a == Mixed(b).get_double();
}

inline bool operator!=(double a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_DOUBLE && a != Mixed(b).get_double();
}


// Compare mixed with zero-terminated string

inline bool operator==(Wrap<Mixed> a, const char* b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_STRING && std::strcmp(Mixed(a).get_string(), b) == 0;
}

inline bool operator!=(Wrap<Mixed> a, const char* b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_STRING && std::strcmp(Mixed(a).get_string(), b) != 0;
}

inline bool operator==(const char* a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_STRING && std::strcmp(a, Mixed(b).get_string()) == 0;
}

inline bool operator!=(const char* a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_STRING && std::strcmp(a, Mixed(b).get_string()) != 0;
}

inline bool operator==(Wrap<Mixed> a, char* b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_STRING && std::strcmp(Mixed(a).get_string(), b) == 0;
}

inline bool operator!=(Wrap<Mixed> a, char* b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_STRING && std::strcmp(Mixed(a).get_string(), b) != 0;
}

inline bool operator==(char* a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_STRING && std::strcmp(a, Mixed(b).get_string()) == 0;
}

inline bool operator!=(char* a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_STRING && std::strcmp(a, Mixed(b).get_string()) != 0;
}


// Compare mixed with binary data

inline bool operator==(Wrap<Mixed> a, BinaryData b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_BINARY && Mixed(a).get_binary().compare_payload(b);
}

inline bool operator!=(Wrap<Mixed> a, BinaryData b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_BINARY && !Mixed(a).get_binary().compare_payload(b);
}

inline bool operator==(BinaryData a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_BINARY && a.compare_payload(Mixed(b).get_binary());
}

inline bool operator!=(BinaryData a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_BINARY && !a.compare_payload(Mixed(b).get_binary());
}


// Compare mixed with date

inline bool operator==(Wrap<Mixed> a, Date b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_DATE && Date(Mixed(a).get_date()) == b;
}

inline bool operator!=(Wrap<Mixed> a, Date b)
{
    return Mixed(a).get_type() == COLUMN_TYPE_DATE && Date(Mixed(a).get_date()) != b;
}

inline bool operator==(Date a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_DATE && a == Date(Mixed(b).get_date());
}

inline bool operator!=(Date a, Wrap<Mixed> b)
{
    return Mixed(b).get_type() == COLUMN_TYPE_DATE && a != Date(Mixed(b).get_date());
}


} // namespace tightdb

#endif // TIGHTDB_MIXED_HPP
