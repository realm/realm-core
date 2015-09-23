
#ifndef REALM_UNIVERSAL_DATE_HPP
#define REALM_UNIVERSAL_DATE_HPP

#include <stdint.h>

namespace realm {

class UniversalDate {
private:
    double d;  // seconds since 2001 (ObjC + Swift native format)
    int64_t i; // 100 ns units since year 0 (C# native format)

    const int64_t c1 = 63145400000000000LL;  // microseconds from year 0 UTC to 2001 UTC
    const int64_t c2 = 62167100000000000LL;  // microseconds from year 0 UTC to 1970 UTC
    const int64_t c3 =   978265000000000LL;  // microseconds from 1970 UTC to 2001 UTC

public:
    
    // Setters

    // Input: Seconds since 2001 UTC
    void set_swift(double date) 
    { 
        d = date;
        i = (d * 10000000. + .5) + c1 * 10; 
    }

    // Input: Miliseconds since 1970 UTC
    void set_java(int64_t date)
    { 
        d = date / 1000. - c3 * 1000000;     
        i = date * 10000 + c2 * 10; 
    }

    // Input: 
    void set_java_instant(int64_t date_upper, int64_t date_lower)
    {

//        d = date / 1000. - c3 * 1000000;
//        i = date * 10000 + c2 * 10;
    }

    // Input: 100-nanosecond units since year 0 UTC
    void set_csharp(int64_t date) 
    { 
        d = (date - c1 * 10) / 10000000.;   
        i = date; 
    }

    void set_python(int64_t date) 
    { 
        d = (date - c1) / 1000000.;          
        i = date * 10; 
    }

    // Getters
    double get_swift() 
    { 
        return d; 
    }

    int64_t get_java() 
    { 
        return i / 10000 - c2 * 1000; 
    }

    int64_t get_csharp() 
    { 
        return i; 
    }

    int64_t get_python() { 
        return i / 10; 
    }

    // Find out what the best way is to compare across different languages (&& or ||)
    bool operator == (UniversalDate date) { return date.d == d && date.i == i; }
    bool operator >  (UniversalDate date) { return date.d > d  && date.i > i; }
};

}

#endif