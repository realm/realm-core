// @@Example: ex_cpp_ng_query_typed @@

#include <realm.hpp>
#include <assert.h>

REALM_TABLE_3(Books,
                title,  String,
                author, String,
                pages, Int)

using namespace realm;

int main()
{
    Books books;

    books.add("Operating Systems Design and Implementation", "Andrew S Tanenbaum", 1080);
    books.add("Introduction to Quantum Mechanics", "Griffiths", 480);

    // Typed table query:
    Query q = books.column().pages >= 200 && books.column().author == "Griffiths";
    size_t match = q.find();
    assert(match == 1);

    // You don't need to create a query object first:
    match = (books.column().pages >= 200 && books.column().author == "Griffiths").find();
    assert(match == 1);

    // You can also create column objects and use them in expressions:
    Columns<Int> pages = books.column().pages;
    Columns<String> author = books.column().author;
    match = (pages >= 200 && author == "Griffiths").find();
    assert(match == 1);
}
// @@EndExample@@
