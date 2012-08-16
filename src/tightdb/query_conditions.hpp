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
#ifndef TIGHTDB_QUERY_CONDITIONS_HPP
#define TIGHTDB_QUERY_CONDITIONS_HPP

#include <string>
#ifdef _MSC_VER
    #include <win32/stdint.h>
#endif

#include <tightdb/utf8.hpp>

namespace tightdb {

enum {COND_EQUAL, COND_NOTEQUAL, COND_GREATER, COND_LESS, COND_NONE};


struct CONTAINS {
    CONTAINS() {};
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        (void)v1_lower;
        (void)v1_upper;
        return strstr(v2, v1) != 0;
    }
};

// is v2 a prefix of v1?
struct BEGINSWITH {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        (void)v1_lower;
        (void)v1_upper;
        return strstr(v2, v1) == v2; // FIXME: Not the most efficient way to do this
    }
};

// does v1 end with s2?
struct ENDSWITH {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        (void)v1_lower;
        (void)v1_upper;
        const size_t l1 = strlen(v1);
        const size_t l2 = strlen(v2);
        if (l1 > l2)
            return false;

        return strcmp(v1, v2 + l2 - l1) == 0;
    }
};

struct EQUAL {
    bool operator()(const char *v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        (void)v1_lower;
        (void)v1_upper;
        return strcmp(v1, v2) == 0;
    }
    template<class T> bool operator()(const T& v1, const T& v2) const {return v1 == v2;}
    int condition(void) {return COND_EQUAL;}
    bool can_match(int64_t v, int64_t lbound, int64_t ubound) { return (v >= lbound && v <= ubound); }
    bool will_match(int64_t v, int64_t lbound, int64_t ubound) { return (v == 0 && ubound == 0 && lbound == 0); }
};

struct NOTEQUAL {
    bool operator()(const char *v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        (void)v1_lower;
        (void)v1_upper;
        return strcmp(v1, v2) != 0;
    }
    template<class T> bool operator()(const T& v1, const T& v2) const { return v1 != v2; }
    int condition(void) {return COND_NOTEQUAL;}
    bool can_match(int64_t v, int64_t lbound, int64_t ubound) { return !(v == 0 && ubound == 0 && lbound == 0); }
    bool will_match(int64_t v, int64_t lbound, int64_t ubound) { return (v > ubound || v < lbound); }
};

// does v1 contain v2?
struct CONTAINS_INS {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        (void)v1;
        return case_strstr(v1_upper, v1_lower, v2);
    }
    int condition(void) {return -1;}
};

// is v2 a prefix of v1?
struct BEGINSWITH_INS {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        (void)v1;
        return case_prefix(v1_upper, v1_lower, v2) != (size_t)-1;
    }
    int condition(void) {return -1;}
};

// does v1 end with s2?
struct ENDSWITH_INS {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        const size_t l1 = strlen(v1);
        const size_t l2 = strlen(v2);
        if (l1 > l2)
            return false;

        bool r = case_cmp(v1_upper, v1_lower, v2 + l2 - l1);
        return r;
    }
    int condition(void) {return -1;}
};

struct EQUAL_INS {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        (void)v1;
        return case_cmp(v1_upper, v1_lower, v2);
    }
    int condition(void) {return -1;}
};

struct NOTEQUAL_INS {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        (void)v1_lower;
        (void)v1;
        return !case_cmp(v1_upper, v1_lower, v2);
    }
    int condition(void) {return -1;}
};

struct GREATER {
    template<class T> bool operator()(const T& v1, const T& v2) const {return v1 > v2;}
    int condition(void) {return COND_GREATER;}
    bool can_match(int64_t v, int64_t lbound, int64_t ubound) { (void)lbound; return (ubound > v); }
    bool will_match(int64_t v, int64_t lbound, int64_t ubound) { (void)ubound; return (lbound > v); }
};

struct NONE {
    template<class T> bool operator()(const T& v1, const T& v2) const {return true;}
    int condition(void) {return COND_NONE;}
};

struct LESS {
    template<class T> bool operator()(const T& v1, const T& v2) const {return v1 < v2;}
    int condition(void) {return  COND_LESS;}
    bool can_match(int64_t v, int64_t lbound, int64_t ubound) { (void)ubound; return (lbound < v); }
    bool will_match(int64_t v, int64_t lbound, int64_t ubound) { (void)lbound; return (ubound < v); }
};

struct LESSEQUAL {
    template<class T> bool operator()(const T& v1, const T& v2) const {return v1 <= v2;}
    int condition(void) {return -1;}
};

struct GREATEREQUAL {
    template<class T> bool operator()(const T& v1, const T& v2) const {return v1 >= v2;}
    int condition(void) {return -1;}
};


} // namespace tightdb

#endif // TIGHTDB_QUERY_CONDITIONS_HPP
