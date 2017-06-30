# Global Architecture {#primer_architecture} #

For a more detailed view on the files used and how they interact, review
\ref primer_files "Core files".

## Realm is a database and a library ##

This might be "duh" stuff, but we'll cover it anyway for the sake of
exhaustivity. Realm is a database that is provided in the form of a library.
Most *classical* databases (e.g. Oracle, PostgreSQL, or even NoSQL databases
such as Cassandra) operate under the client-server paradigm.

Realm doesn't embrace this *classical* paradigm, but uses the same embedded
library paradigm that SQLite is known for. The major advantages of being
serverless are as follows:

- Zero-configuration,
- No multi-process overhead,
- No run-time dependencies.

Realm's main area of focus is mobile, and considering the "app" model employed
by most platforms, and more specifically, the way each app is individually
packaged, no dependencies, etc, being serverless and not requiring any
background services is a no-brainer. Plus, there are no guarantees that
services would run in time to reply to queries on memory and power-constrained
devices.

This being said, Realm isn't a *relational* database. There's no SQL. It's API
(publicly available through the SDK's that builds on the core) has an Object API
well integrated into the programming languages.

## Realm distributions ##

There are currently four official Realm distributions: one for [iOS/macOS
platforms][realm-cocoa], one for [Android platforms][realm-java], one for
[Xamarin](realn-dotnet) and one for [React-Native](realm-js). These
distributions are called "SDK's". They are fully Open Source (Apache).

The SDK's, regardless of their platform, rely on Realm's Core library, a
high-performance C++ library that provides all the abstractions and know-how to
use Realm databases. The SDK's use this library directly through the native
interface provided on each platform.

\dot
digraph realm {
  librealm [shape=box, label="librealm\n(C++)"];
  realm_java [shape=box, label="realm-java"];
  realm_cocoa [shape=box, label="realm-cocoa\n(Swift & Obj-C)"];
  realm_dotnet [shape=box, label="realm-dotnet"];
  realm_js [shape=box, label="realm-js"];

  librealm -> realm_java;
  librealm -> realm_cocoa;
  librealm -> realm_dotnet;
  librealm -> realm_js;
}
\enddot

There are currently no public releases of C or C++ API's for Realm, but
those could be imagined.

# Core Architecture #

This chapter will outline the global architecture of the Core library. However,
before going into the actual object-oriented architecture, we'll cover a number
of concepts that relate to the way Realm has been designed; this should help
explain the architecture of the library.

## Basic Concepts ##

We'll go over some keywords that will be used in this chapter, but we won't
discuss/explain them. In a database, we have:

- a table,
- a column,
- a row.

A database is composed of one or multiple tables, which has one or more columns
(also named "fields" or "attributes" in certain literature). The table also has
one or multiple rows. Each row has one value per column. The typical
representation of such a database would something along the lines of:

\dot
digraph database {
  rankdir=RL;
  node [shape=record];
  edge [arrowhead=diamond];

  t1 [label="Users|id : int64\nname : varchar\nproject_ref : int64\n..."];
  t2 [label="Projects|id : int64\nname : varchar\n..."];
  t2 -> t1;
}
\enddot

And when we try to imagine such table, we sort-of see it in a CSV/Excel kinda
way, such as this:

\dot
digraph table {
  rankdir=LR;
  node [shape=record];

  table [label="Users|{{0|1|2}|{John|Jack|Alice}|{0|8|2}}|..."]
}
\enddot

In this model, the first-class citizen is the *table*. The table can represent
almost anything. A properly-modeled design can accomodate for huge growth and
very quick access. If we were to store this table on disk, we could simply
append row after row, and use some clever tricks to store the variable-length
strings, and also be very clever about how to optimise the empty space when
rows are deleted.

In Realm, the first class citizen is the *row*. We still have databases and
tables, but they are merely collections of columns, rather than more important
things. Tables can be generated on the fly, by picking a bunch of columns, and
whatnot. Below is a (schematic) visualisation of how stuff is written to disk
in a Realm file:

\dot
digraph realmdisk {
  node [shape=record];
  edge [arrowhead=diamond];

  t1 [label="Users"];

  c1 [label="0|1|2|..."];
  c2 [label="John|Jack|Alice|..."];
  c3 [label="0|8|2|..."];

  c1, c2, c3 -> t1;
}
\enddot

Realm is thus a column-centric database. Each column lives its own life,
indepently from the other columns. We can apply transformations to a column
based on another column. We can use column *X* as the look-up table to look
iterate through column *Y*.

By having each column live in its own space, we can apply specific compression
algorithms to it, and save precious space when the column is saved to disk.

## Realm-specific lingo ##

In this section, we'll cover a few Realm-specific terms that may not be
entirely obvious to newcomers. Feel free to add more stuff to this section if
we skipped over some too quickly. It's better to have too much info here than
too little.

- `Accessor`: Gives access to a specific piece of data. There can be field
  accessors, row accessors, column accessors, etc. One way to think of an
  accessor is to assimilate the concept to that of a cursor, while still being
  quite different. Because accessors point to specific data locations, whenever
  the underlying data is changed, accessors need to be updated.  
  In a way, an accessor acts like a proxy to a view of an object in an attached
  Realm file. This does mean that an accessor can be in a detached state, a
  state in which access to the data can not be provided.  
  The reason accessors need to be updated at times is because:
    - they point directly to data that can move/be deleted;
    - they cache key information in order to improve access efficiency.
- `Group`: Represents a collection of tables. This is what most other database
  vendors would call a "database". A Group can be synonymous with a Realm file,
  and both are used interchangeably during discussions in the team. A Group is
  also the most basic representation of an Accessor, discussed above. In this
  case, a group could be seen as a "database accessor".
- `SharedGroup`: The \c SharedGroup class includes the regular \c Group class,
  but adds the transactional API to it. This is the key stone of the Realm
  library: multiple threads/processes can access a single Realm file at the
  same time for reads and writes.

Please don't be afraid by the technicality of the above; it's simply quite
difficult to explain certain concepts without pulling in some new ones. We will
go into much more depth and detail of these concepts and classes in later
chapters.

## API overview ##

This section will try to describe the Realm API from a very high level. The
point is not to go into massive detail, or document all the features of each
method, but simply allow a newcomer to get a feel of how to work with the API.

There's two main classes to get started with a Realm file: \c Group and \c
SharedGroup.  Both allow you to open a file, be it in read-only mode or not. \c
SharedGroup allows multiple threads or processes to access the same file by
adding a transaction API on top of \c Group. Transactions are costly, so \c
Group is preferred if you are certain that you have exclusive access rights to
the Realm file. If in doubt, prefer \c SharedGroup.

Once you have an open Realm file, you have to retrieve a handle (accessor) to
one of the tables. This can be done by either creating a new table in the group,
or querying for it. Please see the `has_table`, `get_table`, `get_or_add_table`
(and other) functions in \c Group for more information.

Realm makes heavy use of reference counting. As such, when you get an accessor
to a table (`TableRef` or `ConstTableRef`), these are objects that you can (and
most probably should) keep on the stack, or at the very least, as wholy owned
objects. This allows the Realm library to keep track of what is going on, and
prevents dangling references.

Now that we have a table handle, we can start querying for some values. For
example, to get the string stored in the 2nd column on the 8th row, you would
write the following:

    my_table.get_string(1, 7);

As expected, indices are 0-indexed. You can query for a wide selection of types
(int, bool, float, double, binary, etc.); you can have a look at the
documentation of \c Table for a complete list. There are also methods to get
the number of occurences in a column (`count_TYPE()`), get the min/max values
in a column (`maximum_TYPE()`, `minimum_TYPE()`), the average value in a column,
and finding the first occurence of a given value, etc.

Another way to take a peek at a table is by grabbing a reference to a row, and
working with that. This can be useful when you want to edit a specific row, but
don't want to give access to the whole table to the function that will edit the
row (this being said, it's trivial to get the `Table` object from a `Row`, so
this shouldn't be taken as a "security", only convenience):

    const size_t name_col_idx = my_table.get_column_index("name");
    const size_t user_idx = my_table.find_first_string(name_col_idx, "foobar");
    auto my_row = my_table.get(user_idx);

    update_user(my_row);

For a high level overview on what is required to get a realm up and running to
play around with, it would be a good idea to look at the unit tests. For
good examples of unit tests see [test/test_shared.cpp][test-shared] or [test/test_table.cpp][test-table].
When contributing code to the project, remember to add tests to cover
what you have changed. When submitting a pull request, Jenkins will
automatically report the code coverage of your changes, but to generate
this report locally (or to see what files need more test coverage) you can
run 'sh build.sh lcov' which generates a html coverage report for the project.

[realm-cocoa]: https://github.com/realm/realm-cocoa
[realm-java]: https://github.com/realm/realm-java
[realm-dotnet]: https://github.com/realm/realm-dotnet
[realm-js]: https://github.com/realm/realm-js
[test-shared]: https://github.com/realm/realm-core/blob/master/test/test_shared.cpp
[test-table]: https://github.com/realm/realm-core/blob/master/test/test_table.cpp
