// @@Example: ex_cpp_ng_query_untyped @@

#include <realm.hpp>
#include <assert.h>

using namespace realm;

int main()
{
    Table books;

    books.add_column(type_String, "title");   // Column 0
    books.add_column(type_String, "author");  // Column 1
    books.add_column(type_Int, "pages");      // Column 2

    books.add_empty_row(2);

    books.set_string(0, 0, "Operating Systems Design and Implementation");
    books.set_string(1, 0, "Andrew S Tanenbaum");
    books.set_int(2, 0, 1080);

    books.set_string(0, 1, "Introduction to Quantum Mechanics");
    books.set_string(1, 1, "Griffiths");
    books.set_int(2, 1, 480);

    // Untyped table:
    Query q = books.column<Int>(2) >= 200 && books.column<String>(1) == "Griffiths";
    size_t match = q.find();
    assert(match == 1);

    // You don't need to create a query object first:
    match = (books.column<Int>(2) >= 200 && books.column<String>(1) == "Griffiths").find();
    assert(match == 1);

    // You can also create column objects and use them in expressions:
    Columns<Int> pages = books.column<Int>(2);
    Columns<String> author = books.column<String>(1);
    match = (pages >= 200 && author == "Griffiths").find();
    assert(match == 1);
}
// @@EndExample@@
