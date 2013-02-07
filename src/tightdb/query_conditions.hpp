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

#include <stdint.h>
#include <cstring>
#include <string>

#include <tightdb/utf8.hpp>

namespace tightdb {

enum {cond_Equal, cond_NotEqual, cond_Greater, cond_GreaterEqual, cond_Less, cond_LessEqual, cond_None, cond_Count};


struct Contains {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        static_cast<void>(v1_lower);
        static_cast<void>(v1_upper);
        return std::strstr(v2, v1) != 0;
    }
};

// is v2 a prefix of v1?
struct BeginsWith {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        static_cast<void>(v1_lower);
        static_cast<void>(v1_upper);
        return std::strstr(v2, v1) == v2; // FIXME: Not the most efficient way to do this
    }
};

// does v1 end with s2?
struct EndsWith {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        static_cast<void>(v1_lower);
        static_cast<void>(v1_upper);
        const size_t l1 = std::strlen(v1);
        const size_t l2 = std::strlen(v2);
        if (l1 > l2)
            return false;

        return std::strcmp(v1, v2 + l2 - l1) == 0;
    }
};

struct Equal {
    bool operator()(const bool v1, const bool v2) const
    {
        return v1 == v2;
    }

    // To avoid a "performance warning" in VC++
    bool operator()(const int64_t v1, const bool v2) const
    {
        return (v1 != 0) == v2;
    }

    bool operator()(const char *v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        static_cast<void>(v1_lower);
        static_cast<void>(v1_upper);
        return std::strcmp(v1, v2) == 0;
    }
    bool operator()(const char *v1, size_t len1, const char* v2, size_t len2) const
    {
        if (len1 != len2)
            return false;
        if (len1 == 0)
            return true;
        if (v1[len1 - 1] != v2[len1 - 1])
            return false;
        int i = memcmp(v1, v2, len1);
        return (i == 0);
    }

    template<class T> bool operator()(const T& v1, const T& v2) const {return v1 == v2;}
    int condition() {return cond_Equal;}
    bool can_match(int64_t v, int64_t lbound, int64_t ubound) { return (v >= lbound && v <= ubound); }
    bool will_match(int64_t v, int64_t lbound, int64_t ubound) { return (v == 0 && ubound == 0 && lbound == 0); }
};

struct NotEqual {
    bool operator()(const char *v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        static_cast<void>(v1_lower);
        static_cast<void>(v1_upper);
        return std::strcmp(v1, v2) != 0;
    }
    template<class T> bool operator()(const T& v1, const T& v2) const { return v1 != v2; }
    int condition() {return cond_NotEqual;}
    bool can_match(int64_t v, int64_t lbound, int64_t ubound) { return !(v == 0 && ubound == 0 && lbound == 0); }
    bool will_match(int64_t v, int64_t lbound, int64_t ubound) { return (v > ubound || v < lbound); }
};

// does v1 contain v2?
struct ContainsIns {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        static_cast<void>(v1);
        return case_strstr(v1_upper, v1_lower, v2);
    }
    int condition() {return -1;}
};

// is v2 a prefix of v1?
struct BeginsWithIns {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        static_cast<void>(v1);
        return case_prefix(v1_upper, v1_lower, v2) != (size_t)-1;
    }
    int condition() {return -1;}
};

// does v1 end with s2?
struct EndsWithIns {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        const size_t l1 = std::strlen(v1);
        const size_t l2 = std::strlen(v2);
        if (l1 > l2)
            return false;

        bool r = case_cmp(v1_upper, v1_lower, v2 + l2 - l1);
        return r;
    }
    int condition() {return -1;}
};

struct EqualIns {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        static_cast<void>(v1);
        return case_cmp(v1_upper, v1_lower, v2);
    }
    int condition() {return -1;}
};

struct NotEqualIns {
    bool operator()(const char* v1, const char* v1_upper, const char* v1_lower, const char* v2) const
    {
        static_cast<void>(v1_lower);
        static_cast<void>(v1);
        return !case_cmp(v1_upper, v1_lower, v2);
    }
    int condition() {return -1;}
};

struct Greater {
    template<class T> bool operator()(const T& v1, const T& v2) const {return v1 > v2;}
    int condition() {return cond_Greater;}
    bool can_match(int64_t v, int64_t lbound, int64_t ubound) { static_cast<void>(lbound); return ubound > v; }
    bool will_match(int64_t v, int64_t lbound, int64_t ubound) { static_cast<void>(ubound); return lbound > v; }
};

struct None {
    template<class T> bool operator()(const T& v1, const T& v2) const {static_cast<void>(v1); static_cast<void>(v2); return true;}
    int condition() {return cond_None;}
    bool can_match(int64_t v, int64_t lbound, int64_t ubound) {static_cast<void>(lbound); static_cast<void>(ubound); static_cast<void>(v); return true; }
    bool will_match(int64_t v, int64_t lbound, int64_t ubound) {static_cast<void>(lbound); static_cast<void>(ubound); static_cast<void>(v); return true; }

};

struct Less {
    template<class T> bool operator()(const T& v1, const T& v2) const {return v1 < v2;}
    int condition() {return cond_Less;}
    bool can_match(int64_t v, int64_t lbound, int64_t ubound) { static_cast<void>(ubound); return lbound < v; }
    bool will_match(int64_t v, int64_t lbound, int64_t ubound) { static_cast<void>(lbound); return ubound < v; }
};

struct LessEqual {
    template<class T> bool operator()(const T& v1, const T& v2) const {return v1 <= v2;}
    int condition() {return cond_LessEqual;}
};

struct GreaterEqual {
    template<class T> bool operator()(const T& v1, const T& v2) const {return v1 >= v2;}
    int condition() {return cond_GreaterEqual;}
};


} // namespace tightdb

#endif // TIGHTDB_QUERY_CONDITIONS_HPP



