//
// Realm C++ coding standard - by example
//

/*
 * The layout of the code is enforced by the use of clang-format - currently in version 3.9. The layout is
 * defined by the .clang-format configuration file in the project root. You can ensure that a code change
 * complies with the formatting rules in the following ways:
 *
 * Before files are added to the staging area:
 *
 * $ git clang-format -f
 *
 * This will immediately modify the changes you have made to comply with the standard (if changes are needed).
 *
 * After you have added files to the staging area:
 *
 * $ git add -u
 * $ git clang-format
 *
 * This will modify the work-area files to comply with the standard, and you can easily inspect the changes made
 * by a 'git diff' command. If you are happy with the changes, add them by 'git add -u'.
 *
 * After you have made your commit:
 *
 * $ git commit
 * $ git clang-format HEAD^
 *
 * This will modify the work-area files to comply with the standard, and you can easily inspect the changes made
 * by a 'git diff' command. If you are happy with the changes, add them by 'git add -u' and amend the commit with
 * 'git commit --amend'.
 *
 * 'git clang-format' can be used in other ways - use 'git clang-format -h' to get info.
 */

// Lines should never exceed 118 characters --------------------------------------------------------------------------

// Code should only include regular ASCII encoded characters.
// Comments may include UTF-8 encoded characters.

// Macro names use uppercase and have "REALM_" as prefix. Non-macro
// names never use all uppercase.

#define REALM_MY_MACRO 1


// A function name uses lowercase and its parts are separated by
// underscores.

my_type my_func()
{
    // Put the opening brace of a function body in the next line below
    // the function prototype. This also applies to class member
    // functions.

    // Put all other opening braces on the same line as the syntax
    // element to which the brace is subordinate.

    // Use 4 spaces per indentation level (no tabs please).

    if (...) {
        // ...
    }
    else {
        // ...
    }

    // Always put subordinate statements on a new line (to ease
    // debugging).

    if (...)
        return ...;

    // No space between type and '*' or '&'
    int* foo1 = ...;
    int& foo2 = ...;

    // 'const' goes before the type
    const int foo3 = ...;

    // ... but not when 'const' operates on a pointer type
    const int* foo3 = ...; // 'const' operates on 'int' not 'int*'
    int* const foo4 = ...; // 'const' operates on 'int*'
    int* const* const foo5 = ...;
}


void my_func_2()
{
    // This indentation and brace placement style agrees with K&R
    // style except for the 'extra' indentation of 'cases' in a switch
    // statement.

    switch (...) {
        case type_Foo: {
            // ...
            break;
        }
        case type_FooBar: {
            // ...
            break;
        }
    }

    try {
        // ...
    }
    catch (...) {
        // ...
    }
}


// A name space name uses lowercase and its parts are separated by
// underscores.

namespace my_namespace {

// No indentation inside name spaces.


// A Class name uses CamelCase with uppercase initial.

template <class T>
class MyClass : public Base {
public:
    // Public member variables do not have a 'm_' prefix.
    int baz;

    MyClass(...)
        : Base(...)
        , m_bar(7)
    {
        // ...
    }

private:
    // Static member variables have prefix 's_'.
    static int s_foo;

    // Regular member variables have prefix 'm_'.
    int m_bar;
};


} // namespace my_namespace


// Names of values of an enumeration are composed of two parts
// separated by an underscore. The first part is a common lowercase
// prefix. The second part identifies the value and uses CamelCase
// with uppercase initial.

enum mode {
    mode_Foo,
    mode_FooBar,
};


// Order of class members (roughly):

class MyClass2 {
public:
    // Types

    // Static variables
    // Regular variables

    // Static functions
    // Regular functions

protected:
    // Same as 'public'

private:
    // Same as 'public'

    // Friends
};


// Use literals when possible

char* str = nullptr; // don't use 0, NULL
bool enable_feature = true;
bool is_last = false;


// Use of 'auto' keyword:
//
// 'auto' should *not* be used for trivial cases where the type declaration
// is short, non-templated, and non-derived (type_t, int64_t, std::string,
// etc.


// About FIXMEs:
//
// A FIXME conveys information about a known issue or shortcoming. It
// may also include information on how to fix the problem, and on
// possible conflicts with anticipated future features.
//
// A FIXME is often added in the following situations:
//
// - While working on, or studying a particular part of the code you
//   uncover an issue or a shortcoming. Additionally, you may have
//   gained an understanding of how to fix it.
//
// - While implementing a new feature, you are forced to cut a corner,
//   but you have some good ideas about how to continue later, and/or
//   you may have knowledge about a certain planned feature that would
//   require a more complete solution.
//
// A FIXME is generally not about a bug or an error, and it should
// generally not be considered a task either. Is is simply a memo to
// oneself or to some other developer who is going to work on the code
// at some later point in time.
//
// A FIXME should never be deleted unless by somebody who understands
// the meaning of it and knows that the problem is fixed, or has
// otherwise disappeared.
