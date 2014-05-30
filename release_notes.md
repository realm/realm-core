Template
=============================================================
x.x.x Release notes (yyyy—MM-dd)
================================

C++ (core)
-----------
The C++ API has been updated and your code will break!

### Bugfixes:

* None.

### API breaking changes:

* `???`

### Enhancements:

* `???`

-----------

### Internals:

* `???`

----------------------------------------------

x.x.x Release notes (yyyy—MM-dd)    <<<------------ PLEASE NOTE, THIS IS THE NEXT RELEASE!!!!!!
================================

C++ (core)
-----------

### Bugfixes:

* Fixed bug in TableView::remove() causing crash or undefined behaviour.
* Fixed bugs in Table::insert_column() and Table::remove_column() causing crash or undefined behaviour.
* Fixed corruption bug when a string enumeration column follows a column with attached search index (index flavor mixup).
* Fixed in Array::erase() causing crash in certain row insertion scenarios.
* Fixed bug in enumerated strings column (corruption was possible when inserting default values).
* Fixed bug in Table::update_from_parent() causing a crash if Group::commit() in presence of generated subtable accessor.

### API breaking changes:

* `???`

### Enhancements:

* Added support for links and lists of links as column types, to enable relationships between tables.
* Adding Table::get_index_in_parent() and Group::get_table(std::size_t table_ndx). They were needed for implicit transactions.
* Table::get_parent_table() can now also return the index of the column in the parent.
* Support for row accessors.
* Table, row, and descriptor accessors are now generally retained and properly adjusted when the parent table is modified.

-----------

### Internals:

* `???`

----------------------------------------------

0.5.0 Release notes (2014-04-02)
================================

C++ (core)
-----------
The C++ API has been updated and your code will break!

### Bugfixes:

* None.

### API breaking changes:

* None.

### Enhancements:

* Read transactions are now non-blocking and their overhead has been reduced by an order of magnitude.

-----------

### Internals:

* New test suite with support for parallelized testing replaces UnitTest++. See section 'Testing' in `README.md`.


----------------------------------------------


TightDB Changelog:
==================

Format:

2012-mm-dd
----------
! Fixed bug [github issuenr]: ....       (user visible bug fixed       - passed on to release notes)
+ Added feature ....                     (user visible new feature     - passed on to release notes)
- Removed/deprecated feature/method .... (user visible removed feature - passed on to release notes)
. Any other notes ....                   (internal changes)


2014-05-14 (Lasse Reinhold)
+ Lets you sort a TableView according to a Float, Double or String column (only integral column types possible before)


2014-05-08 (Finn Schiermer Andersen)
+ Added negation to the query engine.


2014-04-01 (Kristian Spangsege)
+ New framework with support for parallelized unit testing replaces UnitTest++. See section 'Testing' in `README.md`.


2014-03-25 (Kristian Spangsege)
! Fixed bug when clearing table with a float/double column.


2014-03-13 (Finn Schiermer Andersen)
! Fixed missing initialization of empty columns in some scenarios.


2014-02-19 (Finn Schiermer Andersen)
! Fixed space leak in group_writer. Could in some scenarios cause massive increase in database file size.


2014-02-17 (Kristian Spangsege)
+ Adding Table::write() as a means to effieciently serialize a table, or part of a table.
! Fixing table copy bug. The error occured when the table contained strings longer than 64 bytes.
! Fixing table comparison bug. The error occured when the table has a `float` or a `double` column.


2014-02-14 (Kristian Spangsege)

* New test suite with support for parallelized testing replaces UnitTest++. See section 'Testing' in `README.md`.
+ The StringData class now distinguishes `null` from the empty string.
+ Adding StringData::is_null().


2014-02-11 (Kristian Spangsege)
+ Group::write(std::ostream&) added. This allows general online streaming of TightDB databases in memory for the first time.
+ Adding Table::get_name() which returns the name of the table when the table is a direct member of a group.


2014-02-05 (Kenneth Geisshirt)
+ Two new targets in build.sh: get_version and set_version.


2014-02-04 (Kristian Spangsege)
+ Introducing Table::get_parent_table() with allows a subtable to know its parent table.
+ Table::rename_subcolumn() and Table::remove_subcolumn() now take an extra argument which is the index of the column to rename. Before the index was specified as part of the path.
+ Introducing Table::insert_column() and Table::insert_subcolumn() for inserting new columns at arbitrary positions.
+ New class `Descriptor` introduced into the public API. It plays the role that `Spec` played before. Class `Spec` is no longer to be considered part of the public API.
+ Table::add_column() now takes a third optinal argument `DescriptorRef* subdesc`.
+ Introducing Table::get_descriptor() and Table::get_subdescriptor()
- Table::get_spec() and Table::update_from_spec() removed from public API since are part of the now non-public `Spec` API.
. Table::has_shared_spec() renamed to Table::has_shared_type().


2014-01-27 (Brian Munkholm)
! Fixed bug in Query with subtables. Whith empty subtables query returned incorrect results.
  In debug mode it could assert when querying a subtable with more columns than the base table.

>>>>>>> 1e936d3c43874f7e49231aa69c34bdbd67e5f0e9

2014-01-23 (Kenneth Geisshirt)
! Fixed bug: Subtable queries is validated by Query::validate(). An invalid subtable query can lead to a segfault.


2014-01-07 (Kenneth Geisshirt)
+ Table::range() added. The method returns a set of rows as a TableView.


2014-01-06 (Kristian Spangsege)
! 'No parent info in shared specs' conflicts with implementation of `Group::commit()`.
! `ColumnTable::m_spec_ref` not updated when Spec object is reallocated.
! `ColumnSubtableParent::m_index` not updated when preceeding columns are removed.
+ Addition of `template<class L> std::size_t Table::add_subcolumn(const util::Tuple<L>&, DataType, StringData)`. This makes it much easier to add columns to subtable specs.
. `Spec::get_subtable_spec()` now returns `SubspecRef` or `ConstSubspecRef`. This fixes a design bug relating to propagation of constness to subspecs, and it improves the efficiency of access to subspecs by avoiding expensive copying of `Spec` objects.
. Elimination of `Spec::m_table` and `ColumnTable::m_spec_ref`.
. `Spec::add_column()` and `Spec::add_subcolumn()` now take a `Table*` as argument.


2013-12-17 (Kristian Spangsege)
+ Implicit termination of active transaction when SharedGroup is destroyed.
. Class `File` and related exceptions such as `File::AccessError` moved to namespace `tightdb::util`.
. Table::add_column() optimized. For integer columns, the speedup is by more than a factor of 1000 for large tables.


2013-11-07 (Alexander Stigsen)
. BREAKING CHANGE: Schema now handles attributes separately for better performance when there are many colummns.


2013-11-07 (Lasse Reinhold)
+ Added power() operator for next-generation-queries


2013-11-07 (Lasse Reinhold)
! Fixed bug: ng-queries could segfault to bug in Array::get_chunk(). Normal queries and everything else not affected.


2013-10-10 (Kenneth Geisshirt)
. Adding INTERACTIVE mode for the dist-config target in build.sh


2013-10-09 (Kenneth Geisshirt)
. Adding dist-deb target in build.sh for building debian/ubuntu/mint packages. Moreover, the ubuntu/mint version is part of package name so maintaining repositories is easier.


2013-09-26 (Brian Munkholm)
+/- Renamed Table::distinct() to Table::get_distinct_view()
+/- Renamed class Date to DateTime. Renamed DataType::type_Date to type_DateTime
+/- Renamed suffix of all methods operating on DateTime from '_date' to '_datetime'.


2013-09-26 (Kristian Spangsege)
+ File format support for streaming of output from Group::write() (not yet suported by API.)
+ Support for internal null characters in strings. This applies to table and column names as well.


2013-09-19 (Kristian Spangsege)
. CRUD performance has been greatly improved for large tables, as long as they are kept on the "compact" form. A table is kept on the compact form when every row insertion and removal, since the creation of that table, has occured, and continues to occur at the end (i.e., insert after last row, and remove last row).


2013-10-02 (Lasse Reinhold)
- Renamed find_next(lastmatch) into find(begin_at_table_row) for queries and typed tables.


2013-09-12 (Brian Munkholm)
+ Added TableView::row_to_string() and testcases for to_string() and row_to_string()
+ Added row_to_string(), to_string() and to_json() in typed TableView.


2013-08-31 (Kristian Spangsege)
+ Database files are now exanded by doubling the size until it reaches 128 MiB. After that, it is expanded in chunks of 128 MiB. Until now, it was always expanded in chunks of 1 MiB.


2013-08-28 (Kristian Spangsege)
+ Table::is_valid() renamed to Table::is_attached(). The notion of accessor attachment describes much better what is going on than the notion of validity.


2013-08-23 (Kristian Spangsege)
+ Stop throwing from destructors (all), and from SharedGroup::rollback() and SharedGroup::end_read().
+ General stability and error checking improvements.
! Fixed many bugs relating to Group::commit().
! Fixed some bugs relating to SharedGroup::commit().
! Fixed bug in TableViewBase::sort().


2013-08-18 (Kenneth Geisshirt)
! Group::to_string() formatting was incorrect. See https://app.asana.com/0/1441391972580/5659532773181.


2013-08-03 (Kristian Spangsege)
+ Table::find_sorted_int() replaced by Table::lower_bound_int() and Table::upper_bound_int() as these are standardized and provide more flexibility.
+ Addition of Table::lower_bound_bool() and Table::upper_bound_bool().
+ Addition of Table::lower_bound_float() and Table::upper_bound_float().
+ Addition of Table::lower_bound_double() and Table::upper_bound_double().
+ Addition of Table::lower_bound_string() and Table::upper_bound_string(). They rely on simple byte-wise lexicographical comparison. No Unicode or locale dependent collation is taken into account. Comparison occurs exactly as defined by std::lexicographical_compare in the C++ STL.


2013-07-19 (Dennis Fantoni)
+ Added Table::set_subtable(size_t column_ndx, size_t row_ndx, const Table*)


2013-06-25 (Kristian Spangsege)
. The default group open mode has been changed from Group::mode_Normal
  (read/write) to Group::mode_ReadOnly. This makes it possible to open
  a read-only file without specifying a special open mode. Also, since
  changed groups are generally written to new files, there is rarely a
  need for the group to be opened in read/write mode.
. Group::mode_Normal has been renamed to Group::mode_ReadWrite since it is no longer a normal mode.
. Group::mode_NoCreate has been renamed to Group::mode_ReadWriteNoCreate for clarity.


2013-06-05 (Kristian Spangsege)
. Group::write(path) now throws File::Exists if 'path' already exists in the file system.


2013-05-16 (Kristian Spangsege)
+ New SharedGroup::reserve() method added.


2013-05-13 (Kenneth Geisshirt)
. Added "uninstall" target in build.sh for simple uninstallation.


2013-05-07 (Kristian Spangsege)
. Exception File::OpenError renamed to File::AccessError. This affects
  various methods in Group and SharedGroup.


2013-04-23 (Kristian Spangsege)
+ Support for explicit string lengths added. Most method arguments and
  return values of type 'const char*' have been changed to be of type
  'StringData'. This new type is defined in
  <tightdb/string_data.hpp>. 'StringData' can be implicitly
  constructed from 'const char*', so no change is required when
  passing arguments. Source code change is required when dealing with
  returned strings of type 'const char*'. The following is a complete
  list:
    Affected form                      Possible replacement
    ---------------------------------------------------------------------------
    group.get_table_name(...)          group.get_table_name(...).data()
    table.get_column_name()            table.get_column_name().data()
    table.get_string(...)              table.get_string(...).data()
    table.get_mixed(...).get_string()  table.get_mixed(...).get_string().data()
    table[7].string_col                table[7].string_col.c_str()
+ Added support for table[7].string_col.data() and table[7].string_col.size().
+ Full and seamless interoperability with std::string. This comes
  about from the fact that StringData can be implicitly constructed
  from, and convert to std::string.
+ Full support for BinaryData in queries.
+ Added BinaryData::data(), BinaryData::size(), BinaryData::operator[]()
+ Added BinaryData::operator==(), BinaryData::operator!=(), BinaryData::operator<()
+ Added BinaryData::begins_with(), BinaryData::ends_with(), BinaryData::contains()
+ Allow BinaryData to be constructed from fixed size array:
  template<std::size_t N> explicit BinaryData(const char (&)[N])
- BinaryData::pointer removed, use BinaryData::data() instead.
- BinaryData::len removed, use BinaryData::size() instead.
- BinaryData::compare_payload() removed, use BinaryData::operator==() instead.
+ The methods
    Table::set_binary(std::size_t column_ndx, std::size_t row_ndx, const char* data, std::size_t size)
    Table::insert_binary(std::size_t column_ndx, std::size_t row_ndx, const char* data, std::size_t size)
    Table::find_first_binary(std::size_t column_ndx, const char* data, std::size_t size)
    Table::find_all_binary(std::size_t column_ndx, const char* data, std::size_t size)
    TableView::set_binary(std::size_t column_ndx, std::size_t row_ndx, const char* data, std::size_t size)
  have been changed to
    Table::set_binary(std::size_t column_ndx, std::size_t row_ndx, BinaryData)
    Table::insert_binary(std::size_t column_ndx, std::size_t row_ndx, BinaryData)
    Table::find_first_binary(std::size_t column_ndx, BinaryData)
    Table::find_all_binary(std::size_t column_ndx, BinaryData)
    TableView::set_binary(std::size_t column_ndx, std::size_t row_ndx, BinaryData)
  The following changes have been made in the statically
  typed API:
    Affected form                  Possible replacement
    ---------------------------------------------------------
    table[7].binary_col.pointer()  table[7].binary_col.data()
    table[7].binary_col.len()      table[7].binary_col.size()
  These changes were made for consistency with StringData.
+ Added Date::operator<()
+ Return type changed from 'std::time_t' to 'Date' on the following
  methods:
    Mixed::get_date()
    Table::get_date()
    TableView::get_date()
  Argument type changed from 'std::time_t' to 'Date' on many methods including these:
    Mixed::set_date()
    Table::set_date()
    Table::insert_date()
    TableView::set_date()
  Changes corresponding to these have been made in the statically
  typed API. These are some of the affected forms:
    time_t(table[7].date_col)
    table[7].date_col = val
    table[7].mixed_col.get_date()
  These changes were made for consistency, and to improve the
  isolation of the implementation of 'Date' (it is likely that the
  implementation of 'Date' will change). 'Date' can be implicitly
  constructed from std::time_t, but it cannot be implicitly converted
  to std::time_t (nor is it considered desireable to allow such an
  implicit conversion). This means that applications will be affected
  as follows:
    Affected form                    Possible replacement
    ---------------------------------------------------------------------------
    table.get_date(...)              table.get_date(...).get_date()
    table.get_mixed(...).get_date()  table.get_mixed(...).get_date().get_date()
    time_t(table[7].date_col)        Date(table[7].date_col).get_date()
    table[7].date_col.get()          table[7].date_col.get().get_date()
+ Group::write() and Group::write_to_mem() are now 'const'.
+ Group::BufferSpec eliminated. Using BinaryData instead.


2013-03-11 (Kristian Spangsege)
+ On Linux and OS X, installed shared libraries now carry a platform
  dependent API version which is computed from a platform neutral
  version specifier (same as GNU Libtool). This allows for multiple
  versions of the shared library to be concurrently installed.


2013-02-24 (Kristian Spangsege)
+ Adding copy constructors for Table and BasicTable.
+ Adding Table::copy(), BasicTable::copy() and LangBindHelper::copy_table().
+ Adding BasicTable::create() for symmetry with Table::create().


2013-02-21 (Brian Munkholm
-+ Renamed Group::get_table_count() to Group::size()


2013-02-19 (Kristian Spangsege)
+ Type of Group::BufferSpec::m_data changed from <char*> to <const char*>.


2013-02-06 (Kristian Spangsege)
+ New enum DataType replaces ColumnType throughout the public API.


2013-01-27 (Kristian Spangsege)
+ New Group::Group(unattached_tag) added. Same for SharedGroup.
+ New Group::open(...) methods added. Same for SharedGroup.
+ New Group::is_attached() added. Same for SharedGroup.
+ Classes ReadTransaction and WriteTransaction added for handling safe scoped transaction.
+ Many methods have now been qualified with TIGHTDB_NOEXCEPT.


2013-01-14 (Kristian Spangsege)
- Group::set_shared() removed.
- SharedGroup::is_valid() removed. Errors are now reported as exceptions from the constructor.


2013-01-11 (Kristian Spangsege)
+ Simplified open-mode for Group constructor.
- Group::is_valid() removed. Errors are now reported as exceptions from the constructor.
+ Now using Group::BufferSpec to pass a memory buffer to the Group constructor.
+ Group::write_to_mem() now returns a Group::BufferSpec.
+ Addition of 'bool no_create' arguemnt to SharedGroup constructor.


2013-01-08 (Kristian Spangsege)
+ Mixed::set_int() added (same for other value types except subtables).
+ Removed two-argument Mixed constructor for binary data since its signature is expected to be used for strings that are not zero-terminated.


2013-01-08 (Brian Munkholm)
----------
+ New: Added a bunch of methods to support two new column types: float and double.


2012-12-16 (Kristian Spangsege)
----------
+ my_table[i].foo.get() added for all column types except subtables. This is to avoid having to repeat the explicit column type in cast expressions when the actual value is needed.
+ my_table[i].foo.set(...) added for all column types except subtables. This is for completeness.
+ When passing a file name to a Group or a SharedGroup constructor, the type is now a std::string. This is made possible by allowing exception handling. It simplifies the implementation in a few places, and in general it simplifies application code.
+ A 'tag' argument has ben added to the Group constructor that takes a memory buffer as argument. Without this change, two Group constructors were way too similar.


2012-12-06 (Brian Munkholm)
----------
+ 16 New Table:get_string_length(col_ndx, row_ndx) added in Dynamic Table. Missing in Typed Table.


2012-12-06 (Kristian Spangsege)
----------
. "C" API moved to its own repository "tightdb_c".


2012-12-03 (Brian Munkholm)
----------
+ 15 Updated Group() constructor to take an optional 3'rd parameter 'take_ownership', which allows the caller to keep owenership of the provided data: Group::Group(const char* buffer, size_t len, bool take_ownership=true).


2012-11-13 (Kristian Spangsege)
----------
+ 14 Renamed Table::count() to Table::count_int()


2012-11-21
----------
+ Added ShareGroup::has_changed() to detect if there has been changes to the db since last transaction.


2012-11-12 (Kristian Spangsege)
----------
! Fixed a memory leak when using Table::create()


2012-10-24 (Kristian Spangsege)
----------
+ 13 Added Table::has_shared_spec().


2012-10-10 (Kristian Spangsege)
----------
! Fix a problem with MyTable::Query copy constructor that caused access to deallocated memory due to a pointer that was not updated.


2012-10-02 (Kristian Spangsege)
----------
+ New program 'tightdb-config'. Use it to query about the CFLAGs and/or LDFLAGs to use when linking agains the TightDB core library.


2012-10-01 (Brian Munkholm)
----------
+ 12 Added double Table::average(size_t column_ndx) const


2012-09-07 (Alexander Stigsen)
----------
+ File format updated with bigger header and reordered column type [BREAKING]
+ Index is now enabled for string columns (handles string_enum columns as well).
+ 11 Added Table::count_string(size_t column_ndx, const char* target) const;
+ 11 Added Table accessor size_t count(const char* target) const to highlevel interface
+ 11 Spec::add_column(...) now takes an optional parameter for attribute (like indexed).
+ 11 Added Table::to_string() and Group::to_string() for prettified string dump.


2012-08-14 (Brian Munkholm)
----------
+ 10 Renamed FindAllMulti() to find_all_multe(). And SetThreads() to set_threads()
+ 10 Renamed cols() to column().
+ 10 Renamed get_subspec() to get_subtable_spec().
+ 10 Renamed parent() to end_subtable().


2012-08-01 (Kristian Spangsege)
----------
+ 9 Date::operator==(const Date&) and Date::operator!=(const Date&) added.
+ 9 BinaryData::operator==(const BinaryData&) and BinaryData::operator!=(const BinaryData&) added.
+ 9 Operators added for comparison between a value of type Mixed and a value of one of the possible types that a Mixed can contain. Operators are added for both orders of the two arguments.
+ 8 Comparison operators added for "foo" == my_table[i].str and "foo" != my_table[i].str. We already had a comparison operator for the reverse order case, my_table[i].str == "foo".
+ 7 my_table[i].mixed.get_subtable_size() added.


2012-07-27 (Kristian Spangsege)
----------
+ 6 tightdb::is_a<MyTable>(const Table&) added.
+ 6 tightdb::unchecked_cast<MyTable>(TableRef) added.
+ 6 tightdb::checked_cast<MyTable>(TableRef) added.
+ 6 my_table[i].mixed.set_subtable() added.
+ 6 my_table[i].mixed.set_subtable<MySubtable>() added.
+ 6 my_table[i].mixed.is_subtable<MyTable>() added (inefficient, do we want it at all?).
+ 6 my_table[i].mixed.get_subtable<MySubtable>() added (unsafe/unchecked, do we want it at all?).


2012-07-24 (Kristian Spangsege)
----------
+  New macro TIGHTDB_DEBUG to control compilation mode.
    The library (including all headers) is no longer affected in any way by the definition status of NDEBUG or _DEBUG.
    When we (TightDB) compile the library in debug mode, we must define this macro.
    We will deliver two versions of the library, one for release mode, and one for debug mode.
    If the customer wishes to use the debugging version of the library, he must do two things:
    1) Define TIGHTDB_DEBUG in any translation unit that includes a TightDB header.
    2) Use the version of the library that is compiled for debug mode (libtightdb_d.a).
+ 5 Removed obsolete constructor Mixed(ColumnType). Use Mixed(subtable_tag) instead, since this has no runtime overhead.


2012-07-19 (Kristian Spangsege)
----------
+ 4 Table::create() added. Use this to create a freestanding top-level table with dynamic lifetime (determined by reference counting).
+   TableRef::reset() added to set a table reference to null.


2012-07-15 (Kristian Spangsege)
----------
+ 3 Spec::compare() renamed to Spec::operator==(), and made generally available, not only while compiling in debug mode.
+ 3 Spec::operator!=() added.
+ 3 Table::compare() renamed to Table::operator==(), and made generally available, not only while compiling in debug mode.
+ 3 Table::operator!=() added.
+ 3 MyTable::compare() renamed to MyTable::operator==(), and made generally available, not only while compiling in debug mode.
+ 3 MyTable::operator!=() added.
+ 3 Group::operator==() and Group::operator!=() added.
. Array::Compare() and Column::Compare() made generally available, not only while compiling in debug mode.


2012-07-09 (Kristian Spangsege)
----------
+ 1 Table::is_valid() added. Most language bindings must check this flag before calling any member function on any table.
+ 1 MyTable::is_valid() added.
+   See documentation for Table::is_valid() for more details on when a table becomes invalid, and when it does not.
+   Destroying a Group will invalidate all table wrappers (instances of Table) as well as all direct and indirect subtable wrappers.
+   Any modifying operation on a table will generally invalidate all direct and indirect subtable wrappers.
+ 2 my_table[i].mixed.is_subtable() added.
+ 2 my_table[i].mixed.get_subtable() added.


2012-07-08 (Kristian Spangsege)
----------
. LangBindHelper::new_table() now returns null on memory allocation error. This may change in the future to instead throw an exception.


2012-06-27
----------
-+ Table::sorted(...) changed name to get_sorted_view(...)
- Removed Table::find_pos_int(...) from public API

+ Added a the following methods to a TableView:
    template<class E> void set_enum(size_t column_ndx, size_t row_ndx, E value);
    ColumnType  get_mixed_type(size_t column_ndx, size_t row_ndx) const;
    size_t      get_subtable_size(size_t column_ndx, size_t row_ndx) const;
    void        clear_subtable(size_t column_ndx, size_t row_ndx);
    size_t      find_first_bool(size_t column_ndx, bool value) const;
    size_t      find_first_date(size_t column_ndx, time_t value) const;
    void        add_int(size_t column_ndx, int64_t value);
    TableView      find_all_bool(size_t column_ndx, bool value);
    ConstTableView find_all_bool(size_t column_ndx, bool value) const; (for class TableView and ConstTableView)
    TableView      find_all_date(size_t column_ndx, time_t value);
    ConstTableView find_all_date(size_t column_ndx, time_t value) const; (for class TableView and ConstTableView)

2012-06-??
----------
- Group() interfaced changed. Now with multiple options. default option changed from readonly...
+ Generated C++ highlevel API for tables with up to 15 columns
