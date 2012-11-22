//
// TightDB C++ coding standard - by example
//


// Max length of a line is 120 characters -----------------------------------------------------------------------------

#define TIGHTDB_MY_DEF 1                    // All defines are uppercase and prefixed TIGHTDB

class tightdb {                             // "{" on same line
    void my_func(int a, int b);             // function and variable identifiers with lowercase and "_" if needed

    int        m_var1;                      // class member variables prefixed with "m_".
    static int s_var1;                      // static variables are prefixed with "s_"
}

void tightdb::my_func(int a,
                      int b)      
// IF multiple parameters are made on multiple lines, align identation
// Function "{" on seperate line. Yes - it's inconsistent...but we like it
{  
    // Indentation is 4 spaces - not TAB
    char* ptr;                              // Pointer "*" belongs to the type
    const char* const ptr;                  // Const placement

    // Single statement if, ok without {}
    if (1)                                  // space between keywords and first "("
        do();                               // Single statement on seperate line

    // "{" on same line, "}" on seperate
    if (1) {
    }
    else {                                  // else on seperate line
    }

    // space between expressions and operators
    if (a && b)                             
        do();

    const size_t count = t.size();          // seperate var for function call "size"
    for (size_t i; i < count; ++i) {        // ++i - NOT i++
    }

}


