# 6.0.6 Release notes

### Fixed
* When opening Realms on Apple devices where the file resided on a filesystem that does not support preallocation, such as ExFAT, you may get 'Operation not supported' exception. ([cocoa-6508](https://github.com/realm/realm-cocoa/issues/6508)).
* After upgrading of a realm file, you may at some point receive a 'NoSuchTable' exception. ([#3701](https://github.com/realm/realm-core/issues/3701), since 6.0.0)
* If the upgrade process was interrupted/killed for various reasons, the following run could stop with some assertions failing. We don't have evidence that this has actually happened so we will not refer to any specific issue report.
* When querying on a LnkLst where the target property over a link has an index and the LnkLst has a different order from the target table, you may get incorrect results. ([Cocoa #6540](https://github.com/realm/realm-cocoa/issues/6540), since 5.23.6.

-----------

### Internals
* Work around an issue with MSVC in Visual Studio 2019 where Release optimizations crash the compiler because of a regression in 64bit atomic loads on 32bit Windows.

----------------------------------------------

# 6.0.5 Release notes

### Fixed
* Opening a realm file on streaming form in read-only mode now works correctly. Since Core-6, this would trigger an actual
  write to the file, which would fail if the file was not writable.

### This release also includes the fixes contained in v5.27.9:
* Fix a crash on startup on macOS 10.10 and 10.11. ([Cocoa #6403](https://github.com/realm/realm-cocoa/issues/6403), since 2.9.0).

----------------------------------------------
# 6.0.4 Release notes

### Fixed
* It was not possible to make client resync if a table contained binary data. ([#3619](https://github.com/realm/realm-core/issues/3619), v6.0.0-alpha.0)
 
----------------------------------------------

# 6.0.3 Release notes

### Fixed
* You may under certain conditions get a "Key not found" exception when creating an object. ([#3610](https://github.com/realm/realm-core/issues/3610), 6.0.0-alpha-0)
 
----------------------------------------------

# 6.0.2 Release notes

### Enhancements
* Use search index to speed up Query::count, Query::find_all and generic aggregate function.

### Fixed
* None.
 
-----------

### Internals
* Don't force downstream components to look for OpenSSL on Windows.

----------------------------------------------

# 6.0.1 Release notes

### Fixed
* None.

-----------

### Internals
* Build targets are now RealmCore::Storage and RealmCore::QueryParser
* OpenSSL library will be downloaded automatically on Linux and Android

----------------------------------------------

# 6.0.0 Release notes

### Fixed since 6.0.0-beta.3
* Fixed float and double maximum queries when all values were less or equal to zero..
* Using case insensitive search on a primary key string column would crash. ([#3564](https://github.com/realm/realm-core/issues/3564), since v6.0.0-alpha.25)

### Internals since 6.0.0-beta.3
* Upgrade OpenSSL for Android to version 1.1.1b.
* Upgrade the NDK to version 21.
* Removed support for ARMv5 and MIPS from Android. This is a consequence of the new NDK being used. 

Wrap up of the changes done from v6.0.0.alpha0 to v6.0.0-beta.3 compared to v5.23.7:

### Enhancements
* `Table::contains_unique_values(ColKey) -> bool` function added
* Ability to stream into an GlobalKey added.
* Table iterator to support random access. If the object that iterator points to is deleted, the iterator must be advanced or renewed before used for table access.
* Allow DB::close to release tethered versions.
* Further reduction of the use of virtual address space. ([#3392](https://github.com/realm/realm-core/pull/3392))
* min, max, sum and avg functions added as virtual members of ConstLstBase. Makes it possible to
  get the values without knowing the exact type of list.
* pk handling enhanched so that it can be used by OS
* Ability to get a general pointer to list object added to ConstObj
* All ObjectID handling functionality has been moved from Sync to Core.
* The primary key column concept is now handled in Core in a way consistent with Sync and ObjectStore. This means that the corresponding code can be removed in those subsystems.
* Creating tables and objects can now be done entirely using the Core interface. All sync::create_table...() and sync::create_object...() functions now map directly to a core function.
* Ability to get/set Mixed on ConstObj/Obj.
* Providing option to supply initial values when creating an object. This can be used as an optimization when some columns have a search index. Then you don't have to first insert the default value in the index and subsequently the real value.

### Fixed
* Fixed assert in SlabAlloc::allocate_block() which could falsely trigger when requesting an allocation that
  would be slightly smaller than the underlying free block. ([3490](https://github.com/realm/realm-core/issues/3490))
* Queries can be built without mutating the Table object.([#237](https://github.com/realm/realm-core-private/issues/237), since v1.0.0)
 
### Breaking changes
* We now require uniqieness on table names.
* Implicit conversion between Table* and TableRef is removed. It you want to get a raw Table* from a TableRef, you will
  have to call TableRef::unchecked_ptr(). Several API functions now take a ConstTableRef instead of a 'const Table*'.
* DB objects are now heap allocated and accessed through a DBRef. You must create a DB using static DB::create() function.
* SharedGroup class removed. New DB class added allowing you to specify the Realm file
  only once regardless of the number of threads.
* New Transaction class added, inheriting from Group. Transactions are produced by DB.
* TransactionRef added to provide reference to a Transaction. This is a std::shared_ptr.
* Old export_for_handover/import_from_handover has been removed. Handover is now direct
  from one transaction to another, using a new method Transaction::import_copy_of().
* 'begin' argument removed from Query::count();
* Table::optimize replaced with explicit Table::enumerate_string_column. It will enumerate
  column unconditionally.
* TableView::num_attached_rows() no longer available. get(), front, back()... and similar
  functions will throw `InvalidKey` if the referenced object has been deleted.
* Removed the ability to sort a linklist according to some property of the target table. This
  operation is not supported by the sync protocol and allegedly not used in the bindings.
* Moved `get_uncommitted_changes()` from `History` class to `Replication` class. This removes the
  need to call TrivialReplication::get_uncommitted_changes from a decendant of History.
* Column identifier changed from size_t to ColKey. This identifier is subsequently
  used when accessing/mutating the data and when making queries.
* Table identifiers changed from size_t to TableKey.
* Row interface on Table removed. All access to data goes through the new Obj
  interface. Obj objects are created with an ObjKey identifier, which subsequently
  is used to get access to and optionally delete the object.
* Support for sub-tables removed. The feature was only used to implement support for
  array-of-primitives. This is now implemented by a more specific List class.
* Descriptor class removed. No longer needed as we don't have sub-tables.
* Mixed column no longer supported. Will be revived at a later point in time.
* LinkView is replaced by the more general list interface
* Limiting parameters removed from Query aggregate functions (start, end, limit)
* Table::get_range_view() has been removed.
* Table::merge_rows() not supported. Not needed anymore.
* Ref-counted freestanding tables can no longer be created (by Table::create)
* OldDateTime is no longer supported
* An old database cannot be opened without being updated to the new file version.
  This implies that an old database cannot be opened in read-only, as read-only
  prevents updating.
* Only file format versions from 6 and onwards can be opened (realm core v2.0.0)

-----------

### Internals
* ObjectID renamed to GlobalKey
* Table is now rebuilt when a string property is selected as new primary key.
* pk table removed. Primary key column is stored in Table structure.
* Search index is not added to primary key string columns. Will compute key directly from primary key value.
* ConstTableView::get_dependency_versions() is now public. Needed by realm-object-store.
* ConstObj::to_string() added. Mostly to be used during debugging.
* SortDescriptor::is_ascending() added. Potentially to be used by OS.
* Class ObjectId is moved over from Sync.
* Column keys cannot be used across tables. If a key is obtained from one table, it can only be used in relation to that table.
* realm-browser now able to read Core-6 files and print values for even more columns
* All read transactions will have a separate history object. Read transactions
  use the object stored in the Replication object.
* Major simplifications and optimizations to management of memory mappings.
* Speed improvement for Sort().

----------------------------------------------

# 6.0.0-beta.3 Release notes

### Enhancements
* `Table::contains_unique_values(ColKey) -> bool` function added

### Fixed
* Fixed an assertion failure when rebuilding a table with a null primary key, since 6.0.0-beta.2 ([#3528](https://github.com/realm/realm-core/issues/3528)).
 
### Breaking changes
* We now require uniqieness on table names.

----------------------------------------------

# 6.0.0-beta.2 Release notes

Includes changes introduced by v5.23.7

### Outstanding issues.
* Links lost during Table rebuild.([RCORE-229](https://jira.mongodb.org/browse/RCORE-229))

### Fixed
* None.
 
-----------

### Internals
* ObjectID renamed to GlobalKey
* Table is now rebuilt when a string property is selected as new primary key.

----------------------------------------------

# 6.0.0-beta.1 Release notes

This release was never published
 
----------------------------------------------

# 6.0.0-beta.0 Release notes

### Enhancements
* Ability to stream into an ObjectId added.

### Fixed
* Fixed assert in SlabAlloc::allocate_block() which could falsely trigger when requesting an allocation that
  would be slightly smaller than the underlying free block. ([3490](https://github.com/realm/realm-core/issues/3490))

### Breaking changes
* Implicit conversion between Table* and TableRef is removed. It you want to get a raw Table* from a TableRef, you will
  have to call TableRef::unchecked_ptr(). Several API functions now take a ConstTableRef instead of a 'const Table*'.

----------------------------------------------

# 6.0.0-alpha.27 Release notes

### Enhancements
* Table iterator to support random access. If the object that iterator points to is deleted, the iterator must be advanced or renewed before used for table access.
* Allow DB::close to release tethered versions.

### Fixed
* The way the Table iterator was used in ObjectStore could result in an exception if the element recently accessed was deleted. ([#3482](https://github.com/realm/realm-core/issues/3482), since 6.0.0-alpha.0)

----------------------------------------------

# 6.0.0-alpha.26 Release notes

This release was never published
 
----------------------------------------------

# 6.0.0-alpha.25 Release notes

### Fixed
* Upgrading a realm file with a table with no columns would fail ([#3470](https://github.com/realm/realm-core/issues/3470))
 
### Breaking changes
* Table file layout changed. Will not be able to read files produced by ealier 6.0.0 alpha versions.

-----------

### Internals
* pk table removed. Primary key column is stored in Table structure.
* Search index is not added to primary key string columns. Will compute key directly from primary key value.

----------------------------------------------

# 6.0.0-alpha.24 Release notes

### Enhancements
* Improve performance of looking up objects in a Table by index. ([#3423](https://github.com/realm/realm-core/pull/3423))
* Cascade notifications are sent in batches. Enhances performance of KVO notifications.

### Fixed
* macOS binaries were built with the incorrect deployment target (10.14 rather than 10.9). ([Cocoa #6299](https://github.com/realm/realm-cocoa/issues/6299), since 5.23.4).
* Upgrading medium sized string columns to new file format would not be done correctly.
* BPlusTree used in lists and TableViews could in some rare cases give wrong values or crash. ([#3449](https://github.com/realm/realm-core/issues/3449))

----------------------------------------------

# 6.0.0-alpha.23 Release notes

### Internals
* ConstTableView::get_dependency_versions() is now public. Needed by realm-object-store.

----------------------------------------------

# 6.0.0-alpha.22 Release notes

### Fixed
* Opening a file where the size is a multiplum of 64 MB will crash. ([#3418](https://github.com/realm/realm-core/issues/3418), since v6.0.0-alpha.0)
* If a Replication object was deleted before the DB object the program would crash. ([#3416](https://github.com/realm/realm-core/issues/3416), since v6.0.0-alpha.0)
* Migration of a nullable list would fail.
* Using Query::and_query could crash. (Used by List::filter in realm-object-store)
 
-----------

### Internals
* Several performance improvements.

----------------------------------------------

# 6.0.0-alpha.21 Release notes

### Internals
* Reverting the changes made in the previous release. The fix will be done in Sync instead

----------------------------------------------

# 6.0.0-alpha.20 Release notes

### Internals
* Workaround added for the issues around ArrayInsert in the legacy
  sync implementation.

----------------------------------------------

# 6.0.0-alpha.19 Release notes

### Enhancements
* Further reduction of the use of virtual address space. ([#3392](https://github.com/realm/realm-core/pull/3392))

### Fixed
* Creating an equal query on a string column with an index confined by a list view would give wrong results ([#333](https://github.com/realm/realm-core-private/issues/333), since v6.0.0-alpha.0)
* Setting a null on a link would not get replicated. ([#334](https://github.com/realm/realm-core-private/issues/334), since v6.0.0-alpha.0)
 
-----------

### Internals
* A workaround for the problem that values inserted/added on a list-of-primitives were not stored on the server has been added for strings.

----------------------------------------------

# 6.0.0-alpha.18 Release notes

### Enhancements
* We now reserve way less virtual address space. ([#3384](https://github.com/realm/realm-core/pull/3384))

-----------

### Internals
* You can now do sort as part of distinct on Lists.

----------------------------------------------

# 6.0.0-alpha.17 Release notes

### Fixed
* There would be a crash if you tried to import a query with a detached linkview into a new transaction ([#328](https://github.com/realm/realm-core-private/issues/328), since v6.0.0-alpha.0)
* Queries can be built without mutating the Table object.([#237](https://github.com/realm/realm-core-private/issues/237), since v1.0.0)
 
-----------

### Internals
* sort() and distinct() added to the List interface

----------------------------------------------

# 6.0.0-alpha.16 Release notes

### Fixed
* Clearing a table with links to itself could sometimes result in a crash. 
  ([#324](https://github.com/realm/realm-core-private/issues/324))
 
-----------

### Internals
* Fixes unittest Shared_WaitForChange which could deadlock.
  ([#3346] https://github.com/realm/realm-core/issues/3346)

----------------------------------------------

# 6.0.0-alpha.15 Release notes

### Enhancements
* min, max, sum and avg functions added as virtual members of ConstLstBase. Makes it possible to
  get the values without knowing the exact type of list.

### Fixed
* Program would crash if a primary key column is set in a transaction that is subsequently rolled back.
  ([#814](https://github.com/realm/realm-object-store/issues/814), since 6.0.0-alpha.11)
* Creating new objects in a file with InRealm history and migrated from file format 9 would fail.
  ([#3334](https://github.com/realm/realm-core/issues/3334), since 6.0.0-alpha.8)
 
----------------------------------------------

# 6.0.0-alpha.14 Release notes

### Fixed
* Issues related to history object handling.
* Lst<>.clear() will not issue sync operation if list is empty.
* Fixed compilation issues using GCC 8.
 
-----------

### Internals
* ConstObj::to_string() added. Mostly to be used during debugging.
* SortDescriptor::is_ascending() added. Potentially to be used by OS.

----------------------------------------------

# 6.0.0-alpha.12 Release notes

### Enhancements
* Small improvements in pk handling to support ObjectStore

----------------------------------------------

# 6.0.0-alpha.11 Release notes

### Enhancements
* to_json operations reintroduced.
* get_any() and is_null() operations added to ConstLstBase
* pk handling enhanched so that it can be used by OS

### Fixed
* None.
 
----------------------------------------------

# 6.0.0-alpha.10 Release notes

### Fixed
* Fixed replication of setting and inserting null values in lists. Now also fixed
  for String and Binary.
 
----------------------------------------------

# 6.0.0-alpha.9 Release notes

### Enhancements
* Ability to get a general pointer to list object added to ConstObj

### Fixed
* Fixed replication of null timestamps in list
 
----------------------------------------------

# 6.0.0-alpha.8 Release notes

### Enhancements
* All ObjectID handling functionality has been moved from Sync to Core.
* The primary key column concept is now handled in Core in a way consistent with Sync
  and ObjectStore. This means that the corresponding code can be removed in those
  subsystems.
* Creating tables and objects can now be done entirely using the Core interface. All
  sync::create_table...() and sync::create_object...() functions now map directly to a
  core function.

----------------------------------------------

# 6.0.0-alpha.7 Release notes

### Enhancements
* Ability to get/set Mixed on ConstObj/Obj.
* Now able to migrate files with Sync histories.

-----------

### Internals
* Class ObjectId is moved over from Sync.

----------------------------------------------

# 6.0.0-alpha.6 Release notes

-----------

### Internals
* Column keys cannot be used across tables. If a key is obtained from one table, it can only be
  used in relation to that table.

----------------------------------------------

# 6.0.0-alpha.5 Release notes

-----------

### Internals
* realm-browser now able to read Core-6 files and print values for even more columns

----------------------------------------------

# 6.0.0-alpha.4 Release notes

### Fixed
* A lot of small fixes in order to make sync test pass.
 
-----------

### Internals
* The release binaries for Apple platforms are now built with Xcode 9.2 (up from 8.3.3).
* A new function - History::ensure_updated can be called in places where the history object
  needs to be up-to-date. The function will use a flag to ensure that the object is only
  updated once per transaction.
* All read transactions will have a separate history object. Read transactions
  use the object stored in the Replication object.

# 6.0.0-alpha.3 Release notes

----------------------------------------------

# 6.0.0-alpha.2 Release notes

### Open issues

* Encrypted files may not work on Android
  [Issue#248](https://github.com/realm/realm-core-private/issues/248)
* Building queries are not thread safe
  [Issue#237](https://github.com/realm/realm-core-private/issues/237)
  
### Bugfixes

* A few fixes improving the stability.

-----------

### Internals

* Object keys are now allowed to use all 64 bits. Only illegal value is 0xFFFFFFFFFFFFFFFF
  which is the encoding of a null-value.

----------------------------------------------

# 6.0.0-alpha.1 Release notes

### Open issues

* Encrypted files may not work on Android
  [Issue#248](https://github.com/realm/realm-core-private/issues/248)
* Building queries are not thread safe
  [Issue#237](https://github.com/realm/realm-core-private/issues/237)
  
### Bugfixes

* Many small fixes.

### Breaking changes

* DB objects are now heap allocated and accessed through a DBRef. You must create a DB using 
  static DB::create() function.
* All list like classes have been renamed

### Enhancements

* Providing option to supply initial values when creating an object. This can be used as an
  optimization when some columns have a search index. Then you don't have to first insert
  the default value in the index and subsequently the real value.
* Many small enhancements required by ObjectStore and Sync.

----------------------------------------------

# 6.0.0-alpha.0 Release notes

### Breaking changes

* SharedGroup class removed. New DB class added allowing you to specify the Realm file
  only once regardless of the number of threads.
* New Transaction class added, inheriting from Group. Transactions are produced by DB.
* TransactionRef added to provide reference to a Transaction. This is a std::shared_ptr.
* Old export_for_handover/import_from_handover has been removed. Handover is now direct
  from one transaction to another, using a new method Transaction::import_copy_of().
* 'begin' argument removed from Query::count();
* Table::optimize replaced with explicit Table::enumerate_string_column. It will enumerate
  column unconditionally.
* TableView::num_attached_rows() no longer available. get(), front, back()... and similar
  functions will throw `InvalidKey` if the referenced object has been deleted.
* Removed the ability to sort a linklist according to some property of the target table. This
  operation is not supported by the sync protocol and allegedly not used in the bindings.
* Moved `get_uncommitted_changes()` from `History` class to `Replication` class. This removes the
  need to call TrivialReplication::get_uncommitted_changes from a decendant of History.
* Column identifier changed from size_t to ColKey. This identifier is subsequently
  used when accessing/mutating the data and when making queries.
* Table identifiers changed from size_t to TableKey.
* Row interface on Table removed. All access to data goes through the new Obj
  interface. Obj objects are created with an ObjKey identifier, which subsequently
  is used to get access to and optionally delete the object.
* Support for sub-tables removed. The feature was only used to implement support for
  array-of-primitives. This is now implemented by a more specific List class.
* Descriptor class removed. No longer needed as we don't have sub-tables.
* Mixed column no longer supported. Will be revived at a later point in time.
* LinkView is replaced by the more general list interface
* Limiting parameters removed from Query aggregate functions (start, end, limit)
* Table::get_range_view() has been removed.
* Table::merge_rows() not supported. Not needed anymore.
* Ref-counted freestanding tables can no longer be created (by Table::create)
* OldDateTime is no longer supported
* An old database cannot be opened without being updated to the new file version.
  This implies that an old database cannot be opened in read-only, as read-only
  prevents updating.
* Only file format versions from 6 and onwards can be opened (realm core v2.0.0)

### Enhancements

* None. Or see below.

-----------

### Internals

* Major simplifications and optimizations to management of memory mappings.
* Speed improvement for Sort().

---------------------------------------------

# 5.23.9 Release notes

### Fixed
* Fix a crash on startup on macOS 10.10 and 10.11. ([Cocoa #6403](https://github.com/realm/realm-cocoa/issues/6403), since 2.9.0).

----------------------------------------------

# 5.23.8 Release notes

### Fixed
* A NOT query on a LinkList would incorrectly match rows which have a row index one less than a correctly matching row which appeared earlier in the LinkList. ([Cocoa #6289](https://github.com/realm/realm-cocoa/issues/6289), since 0.87.6).
* Columns with float and double values would not be sorted correctly (since 5.23.7)
 
----------------------------------------------

# 5.23.7 Release notes

### Enhancements
* Reduce the encrypted page reclaimer's impact on battery life on Apple platforms. ([PR #3461](https://github.com/realm/realm-core/pull/3461)).

### Fixed
* macOS binaries were built with the incorrect deployment target (10.14 rather than 10.9). ([Cocoa #6299](https://github.com/realm/realm-cocoa/issues/6299), since 5.23.4).
* Subtable accessors could be double-deleted if the last reference was released from a different
  thread at the wrong time. This would typically manifest as "pthread_mutex_destroy() failed", but
  could also result in other kinds of crashes. ([Cocoa #6333](https://github.com/realm/realm-cocoa/issues/6333)).
* Sorting float or double columns containing NaN values had inconsistent results and would sometimes
  crash due to out-of-bounds memory accesses. ([Cocoa #6357](https://github.com/realm/realm-cocoa/issues/6357)).

----------------------------------------------

# 5.23.6 Release notes

### Enhancements
* Performance significantly improved when making a query on the property of a linked table, when the property is indexed.

### Fixed
* A race between extending the file and activity in the encryption layer could lead to crash and corruption.
  This race has been fixed. The bug was introduced with version 5.3.0 and may hit on Android, if encryption is
  in use. It could also affect Linux on file systems where posix prealloc() is unsupported.
  ([PR #3427](https://github.com/realm/realm-core/issues/3427), since 5.3.0)
* Null values were not printed correctly when using json serialisation. ([PR #3399](https://github.com/realm/realm-core/issues/3399)).
* ListOfPrimitives were not printed correctly when using json serialisation. ([#3408](https://github.com/realm/realm-core/issues/3408)).

-----------

### Internals
* Fixed several warnings found by newer clang compilers. ([#3393](https://github.com/realm/realm-core/issues/3393)).

----------------------------------------------

# 5.23.5 Release notes

### Enhancements
* None.

### Fixed
* Chained OR equals queries on an unindexed string column failed to match any results if any of the strings were 64 bytes or longer. ([PR #3386](https://github.com/realm/realm-core/pull/3386), since 5.17.0).
* Fixed serialization of a query which looks for a null timestamp. This only affects query based sync. ([PR #3389](https://github.com/realm/realm-core/pull/3388), since v5.23.2).

### Breaking changes
* None.

-----------

### Internals

* VersionID comparison operators are now const qualified ([PR #3391](https://github.com/realm/realm-core/pull/3391)).
* Exception `util::File::AccessError`, and it's derivatives such as `util::File::NotFound`, will now include a stacktrace in the message returned by the `what()` method. ([PR #3394](https://github.com/realm/realm-core/pull/3394))


----------------------------------------------

# 5.23.4 Release notes

### Internals
* The release binaries for Apple platforms are now built with Xcode 10.0 (up from 9.4).
* Add a Catalyst/UIKit for Mac library to the Cocoa release package.

----------------------------------------------

# 5.23.3 Release notes

### Fixed
* If a signal interrupted a msync() call, Core would throw an exception. This behavior has new been changed to retry the system call instead. (Issue [#3352](https://github.com/realm/realm-core/issues/3352))
* Fixed a bug in sum() or average() of == and != queries on integer columns sometimes returning an incorrect result. ([#3356](https://github.com/realm/realm-core/pull/3356), since the beginning).

-----------

### Internals
* Changed the metrics timers to more precisely report in nanoseconds, instead of seconds. ([#3359](https://github.com/realm/realm-core/issues/3359))
* Better performance when cloud query metrics are turned on, by not acquiring a backtrace on query serialization errors (permissions queries). ([#3361](https://github.com/realm/realm-core/issues/3361)).
* Performance improved for queries comparing a constant value to a property over unary link path (eg: "someLink.Id == 42"). ([#3670](https://github.com/realm/realm-core/issues/3370))

----------------------------------------------

# 5.23.2 Release notes

### Fixed
* Named pipes on Android are now created with 0666 permissions instead of 0600. This fixes a bug on Huawei devices which caused named pipes to change owners during app upgrades causing subsequent `ACCESS DENIED` errors. This should have not practical security implications. (Issue [#3328](https://github.com/realm/realm-core/pull/3328))
 
-----------

### Internals
* The release binaries for Apple platforms are now built with Xcode 9.4 (up from 9.2).
* Performance of queries on Timestamp is improved

----------------------------------------------

# 5.23.1 Release notes

### Fixed
* Fixed the metrics throwing an exception when a query cannot be serialised. Now it reports the exception message as the description.
 ([#3031](https://github.com/realm/realm-sync/issues/3031), since v3.2.0)
* Queries involving an indexed int column which were constrained by a LinkList with an order different from the table's order would
  give incorrect results. ([#3307](https://github.com/realm/realm-core/issues/3307), since v5.19.0)
* Queries involving an indexed int column had a memory leak if run multiple times. ([#6186](https://github.com/realm/realm-cocoa/issues/6186)), since v5.19.0)

----------------------------------------------

# 5.23.0 Release notes

### Enhancements
* Add a Swift Package Manager package ([#3308](https://github.com/realm/realm-core/pull/3308)).

### Fixed
* Constructing an `IncludeDescriptor` made unnecessary table comparisons. This resulted in poor performance for subscriptions
  using the `includeLinkingObjects` functionality. ([#3311](https://github.com/realm/realm-core/issues/3311), since v5.18.0)
 
### Breaking changes
* None.

-----------

### Internals
* None.

----------------------------------------------

# 5.22.0 Release notes

### Enhancements

* Expose the ability to follow links while printing a TableView in JSON format.
  TableView::to_json() now supports the same arguments as Table::to_json().
  ([#3301](https://github.com/realm/realm-core/pull/3301))

### Fixed
* None.
 
### Breaking changes
* None.

-----------

### Internals
* Fixed an inconsistency in the use of the `REALM_METRICS` compile time option. Now core consumers are able
  to use `SharedGroup::get_metrics()` regardless of whether or not metrics are compiled in. A null pointer
  is returned if the feature has been disabled at compile time.

----------------------------------------------

# 5.21.0 Release notes

### Enhancements
* Added support for unicode characters in realm path and filenames for Windows. Contribution by @rajivshah3.
  ([#3293](https://github.com/realm/realm-core/pull/3293))

### Fixed
* None.

### Breaking changes
* None.

-----------

### Internals
* Introduced new feature test macros for address and thread sanitizers in
  `<realm/util/features.h>`.
* Added Realm file path to Allocator assertions ([3283](https://github.com/realm/realm-core/issues/3283)).

----------------------------------------------

# 5.20.0 Release notes

### Enhancements
* Added the ability to convert a std::chrono::time_point to a Timestamp and
  vice versa. This allows us to make calculations using std::chrono::duration.

### Fixed
* Slab usage was reported wrong by SlabAlloc::get_total_slab_size() ([#3284](https://github.com/realm/realm-core/pull/3284)
  This caused ROS to incorectly report "exabytes" of memory used for slab.
* The control of the page reclaimer did not limit the page reclaimers workload correctly. This could lead
  to the reclaimer not running as much as intended. This is not believed to have been visible to end users.
  This bug also caused ROS to occasionally report odd metrics for the reclaimer.
  ([#3285](https://github.com/realm/realm-core/pull/3285))
* When opening an encrypted file via SharedGroup::open(), it could wrongly fail and indicate a file corruption
  although the file was ok.
  ([#3267](https://github.com/realm/realm-core/issues/3267), since core v5.12.2)
 
----------------------------------------------

# 5.19.1 Release notes

### Fixed
* Freelist would keep growing with a decreased commit performance as a result.
  ([2927](https://github.com/realm/realm-sync/issues/2927))
* Fixed an incorrect debug mode assertion which could be triggered when generating the description of an IncludeDescriptor.
  ([PR #3276](https://github.com/realm/realm-core/pull/3276) since v5.18.0). 
----------------------------------------------

# 5.19.0 Release notes

### Enhancements
* Improved query performance for unindexed integer columns when the query has a chain of OR conditions.
  This will improve performance of "IN" queries generated by SDKs.
  ([PR #2888](https://github.com/realm/realm-sync/issues/2888).
* Use search index in queries on integer columns (equality only). This will improve performance of
  queries on integer primary key properties for example. ([PR #3272](https://github.com/realm/realm-core/pull/3272)).
* Number of 8 byte blocks in freelist is minimized. This will result in a shorter freelist.

### Fixed
* Writing a snapshot to file via Group::write() could produce a file with some parts not
  reachable from top array (a memory leak). ([#2911](https://github.com/realm/realm-sync/issues/2911))
* Fixed a bug in queries on a string column with more than two "or" equality conditions when the last condition also had an
  "and" clause. For example: `first == "a" || (first == "b" && second == 1)` would be incorrectly evaluated as
  `(first == "a" || first == "b")`. ([#3271](https://github.com/realm/realm-core/pull/3271), since v5.17.0)
 
### Breaking changes
* None.

-----------

### Internals
* None.

----------------------------------------------

# 5.18.0 Release notes

### Enhancements
* Adds support for a new IncludeDescriptor type which describes arbitrary link paths
  on a TableView. Applying this to a TableView does not modify the results, but gives
  users the ability to use the reporting method to find rows in a different table that
  are connected by backlinks. This is intended for sync subscriptions.
* Enhances LinksToNode so that it can check links to multiple targets. This can be utilized
  in permissions check in sync.

### Fixed
* None.
 
-----------

### Internals
* The release binaries for Apple platforms are now built with Xcode 9.2 (up from 8.3.3).

----------------------------------------------

# 5.17.0 Release notes

### Enhancements
* Improved query performance for unindexed string columns when the query has a chain of OR conditions.
  This will improve performance of "IN" queries generated by SDKs.
  ([PR #3250](https://github.com/realm/realm-core/pull/3250).

### Fixed
* Making a query that compares two integer properties could cause a segmentation fault on the server.
  ([#3253](https://github.com/realm/realm-core/issues/3253))
 
-----------

### Internals
* The protocol for updating Replication/History is changed. The Replication object will be initialized
  in every transaction. A new parameter will tell if it is a write- or readtransaction. A new function -
  History::ensure_updated can be called in places where the history object needs to be up-to-date. The 
  function will use a flag to ensure that the object is only updated once per transaction.

----------------------------------------------

# 5.16.0 Release notes

### Enhancements
* Improved performance of encryption and decryption significantly by utilizing hardware optimized encryption functions.
  ([#293](https://github.com/realm/realm-core-private/issues/293))
* Added the ability to write a Group with the history stripped.
  ([#3245](https://github.com/realm/realm-core/pull/3245))

### Fixed
* Nothing

-----------

### Internals
* Size of decrypted memory and of currently reserved slab is now available outside of the
  metrics system (to which they were added in 5.15.0). This allows us to get the current
  values independently from transactions start or end (the metrics system is only updated
  at transaction boundaries).
  ([3240] https://github.com/realm/realm-core/pull/3240)
* Current target and workload set for the page reclaimer is now also available from `get_decrypted_memory_stats()`
  ([3246] https://github.com/realm/realm-core/pull/3246)
* Default heuristic for reclaiming pages holding decrypted data has been changed, now
  limiting amount to same as current use of the buffer cache. Previously the limit was
  half of buffer cache usage. This heuristic may still not be good enough for some scenarios
  and we recommend monitoring and explicitly setting a better target in cases where we reclaim
  more memory than nescessary.
  (also [3240] https://github.com/realm/realm-core/pull/3240)
* Now publishing a TSAN compatible linux build.
----------------------------------------------

# 5.15.0 Release notes

### Enhancements
* Metrics history is now capped to a configurable buffer size with a default of 10000 entries.
  If this is exceeded without being consumed, only the most recent entries are stored. This
  prevents excessive memory growth if users turn on metrics but don't use it.
* Metrics transaction objects now store the number of decrypted pages currently in memory.
* SharedGroup::get_stats includes an optional parameter to get size of currently locked memory.
* Metrics now exposes the table name of queries which have been run.

### Fixed
* When shutting down the server you could sometimes experience a crash with "realm::util::Mutex::lock_failed"
  in the stacktrace.
  ([#3237](https://github.com/realm/realm-core/pull/3237), since v5.12.5)

### Internal
* Fix a race between the encryption page reclaim governor running and setting a governor.
  This only affects applications which actually set the governor to something custom which no one does yet.
  ([#3239](https://github.com/realm/realm-core/issues/3239), since v5.12.2)

----------------------------------------------

# 5.14.0 Release notes

### Enhancements
* Add assertion to prevent translating a ref value that is not 8 byte aligned. This will allow
  us to detect file corruptions at an earlier stage.
* You can now get size of the commit being built and the size of currently allocated slab area.
* The amount of memory held by SharedGroup is minimized as most of it will be freed after each commit.

### Fixed
* Compacting a realm into an encrypted file could take a really long time. The process is now optimized by adjusting the write
  buffer size relative to the used space in the realm.
  ([#2754](https://github.com/realm/realm-sync/issues/2754))
* Creating an object after creating an object with the int primary key of "null" would hit an assertion failure.
  ([#3227](https://github.com/realm/realm-core/pull/3227)).

### Breaking changes
* None.

-----------

### Internals
* The buffer size used by util::File::Streambuf is now configurable in construction.

----------------------------------------------

# 5.13.0 Release notes

### Enhancements
* The parser now supports readable timestamps with a 'T' separator in addition to the originally supported "@" separator.
  For example: "startDate > 1981-11-01T23:59:59:1". ([#3198](https://github.com/realm/realm-core/issues/3198)).

### Fixed
* If, in debug mode, you try to compute the used space on a newly compacted realm (with empty free list), the program will
  abort. ([#1171](https://github.com/realm/realm-sync/issues/2724), since v5.12.0)
 
### Breaking changes
* None.

-----------

### Internals
* For convenience, `parser::parse` now accepts a `StringData` type instead of just `std::string`.
* Parsing a query which uses the 'between' operator now gives a better error message indicating
  that support is not yet implemented. ([#3198](https://github.com/realm/realm-core/issues/3198)).

----------------------------------------------

# 5.12.7 Release notes

### Enhancements
* Instead of asserting, an `InvalidDatabase` exception is thrown when a realm file is opened 
  with an invalid top ref. Name of problematic file is included in exception message.

### Fixed
* A bug was fixed in `realm::util::DirScanner` that could cause it to sometimes
  skip directory entries due to faulty error handling around `readdir()`.
  (Issue [realm-sync#2699](https://github.com/realm/realm-sync/issues/2699), since 5.12.5).
 
### Breaking changes
* None.

-----------

### Internals
* Improved performance on `find_first` for small string arrays (ArrayString). This will improve the table name lookup
  performance.
* Upgrade pegtl to 2.6.1. Several issues fixed.
* Introduced Durability::Unsafe, which disables sync'ing to disk. Using this option, 
  a platform crash may corrupt the realm file. Use only, if you'r OK with this.

----------------------------------------------

# 5.12.6 Release notes

### Enhancements
* None.

### Fixed
* On AWS Lambda we may throw an "Operation not permitted" exception when calling posix_fallocate().
  A slower workaround has been supplied.
  ([#3193](https://github.com/realm/realm-core/issues/3293))
 
### Breaking changes
* None.

-----------

### Internals
* None.

----------------------------------------------

# 5.12.5 Release notes

### Enhancements
* None.

### Fixed
* When loading the realm binary from within the realm-js SDK, core could hang on Windows as described in
  https://github.com/realm/realm-js/issues/2169.
  ([#3188](https://github.com/realm/realm-core/pull/3188, since 5.12.2)

### Breaking changes
* None.

-----------

### Internals
* Fixed warnings reported by GCC 8.
* Replaced call to the deprecated `readdir_r()` with `readdir()`.
* Compilation without encryption now possible

----------------------------------------------

# 5.12.4 Release notes

### Enhancements
* None.

### Fixed
* A segmentation fault would occur when calling Group:get_used_space() for a realm file
  with no commits. This method would usually only be called from sync/ROS to calculate
  and report state size.
  ([#3182](https://github.com/realm/realm-core/issues/3182), since v5.12.0)

### Breaking changes
* None.

----------------------------------------------

# 5.12.3 Release notes

### Enhancements
* None.

### Fixed
* Added assertions around use of invalid refs and sizes. Helps in narrowing down the causes for
  asserts like `ref != 0` and `(chunk_pos % 8) == 0`

### Breaking changes
* None.

-----------

### Internals
* None.

----------------------------------------------

# 5.12.2 Release notes

### Enhancements
* None

### Fixed
* If encryption was enabled, decrypted pages were not released until the file was closed, causing
  excessive usage of memory.
  A page reclaim daemon thread has been added, which will work to release decrypted pages back to
  the operating system. To control it, a governing function can be installed. The governing function
  sets the target for the page reclaimer. If no governing function is installed, the system will attempt
  to keep the memory usage below any of the following:

        - 1/4 of physical memory available on the platform as reported by "/proc/meminfo"
        - 1/4 of allowed memory available as indicated by "/sys/fs/cgroup/memory/memory_limit_in_bytes"
        - 1/2 of what is used by the buffer cache as indicated by "/sys/fs/cgroup/memory/memory.stat"
        - A target directly specified as "target <number of bytes>" in a configuration file specified
          by the environment variable REALM_PAGE_GOVERNOR_CFG.
  if none of the above is available, or if a target of -1 is given, the feature is disabled.
  ([#3123](https://github.com/realm/realm-core/issues/3123))

-----------

### Internals
* None.

----------------------------------------------

# 5.12.1 Release notes

### Enhancements
* Illegal freeing of in-file-memory is now detected when freeing is
  actually done. This will make it easier to find the root cause of
  some file corruption issues.

### Fixed
* None.

### Breaking changes
* None.

-----------

### Internals
* None.

----------------------------------------------

# 5.12.0 Release notes

### Enhancements
* Added Group::get_used_space() which will return the size of the data taken up by the current
  commit. This is in contrast to the number returned by SharedGroup::get_stats() which will
  return the size of the last commit done in that SharedGroup. If the commits are the same,
  the number will of course be the same.
  Issue [#259](https://github.com/realm/realm-core-private/issues/259)

### Fixed
* None.

### Breaking changes
* The way the Linux binaries are delivered is changed. They are now distributed
  like the rest of the binaries with two packages (devel/runtime) per build type.
  The file names follow this scheme:
  realm-core-<buildType>-<release>-Linux-{devel|runtime}.tar.gz
  For Linux the following build types are published: Debug, Release, RelAssert
  and RelASAN.

-----------

### Internals
* Replication::get_database_path() is made const.
* TrivialReplication::get_database_path() is made public.
* Added better compatibility for custom allocators with standard library
  containers on GCC 4.9.

----------------------------------------------

# 5.11.3 Release notes

### Compatibility
* File format: ver. 9
  Upgrades automatically from previous formats.
  Can open realms down to file format version 7 in ReadOnly mode (without upgrade).

-----------

### Internals
* Improved assertion checking in release mode in order to detect any corruption
  of our freelist earlier and prevent bogus allocations from a corrupted freelist
  from leading to subsequent corruption of other parts of the file.

----------------------------------------------

# 5.11.2 Release notes

### Compatibility
* File format: ver. 9
  Upgrades automatically from previous formats.
  Can open realms down to file format version 7 in ReadOnly mode (without upgrade).

-----------

### Internals
* Releases no longer include RPM and DEB packages.
* Releases now include RelWithDebInfo+ASAN and RelWithDebInfo+Assertions tarballs for linux.
  [#3112](https://github.com/realm/realm-core/pull/3112).

----------------------------------------------

# 5.11.1 Release notes

### Compatibility
* File format: ver. 9
  Upgrades automatically from previous formats.
  Can open realms down to file format version 7 in ReadOnly mode (without upgrade).

-----------

### Internals
* Fixed a bug in the use of placement new on MSVC, where the implementation is
  buggy. This bug only affected version 5.11.0.
  PR [#3109](https://github.com/realm/realm-core/pull/3109)
* Made improvements to the custom allocation interfaces introduced in 5.11.0,
  which should make them more convenient and use slightly less memory.
  PR [#3108](https://github.com/realm/realm-core/pull/3108)

----------------------------------------------

# 5.11.0 Release notes

### Compatibility
* File format: ver. 9
  Upgrades automatically from previous formats.
  Can open realms down to file format version 7 in ReadOnly mode (without upgrade).

-----------

### Internals
* Added support for custom heap allocators
  PR [#3106](https://github.com/realm/realm-core/pull/3106).

----------------------------------------------

# 5.10.3 Release notes

### Fixed
* When a sort or distinct over links was done on an already-sorted TableView,
  the link translation map was done using the unsorted rows, resulting in the
  second sort/distinct being done with the incorrect values.
  PR [#3102](https://github.com/realm/realm-core/pull/3102).

### Compatibility
* File format: ver. 9 (upgrades automatically from previous formats)

-----------

### Internals

* Will assert if someone tries to free a null ref.
  Issue [#254](https://github.com/realm/realm-core-private/issues/254) and the like.

----------------------------------------------

# 5.10.2 Release notes

### Enhancements

* Add an arm64_32 slice to the watchOS build.

----------------------------------------------

# 5.10.1 Release notes

### Internals

* Stack trace also available when throwing std:: exceptions.

----------------------------------------------

# 5.10.0 Release notes

### Enhancements

* Allow compact to take an optional output encryption key.
  PR [#3090](https://github.com/realm/realm-core/pull/3090).

----------------------------------------------

# 5.9.0 Release notes

### Enhancements

* Allow a version number in Group::write which will cause a file with (sync)
  history to be written.

-----------

### Internals

* Most exception types now report the stack trace of the point where they were
  thrown in their `what()` message. This is intended to aid debugging.
  Additionally, assertion failures on Linux now report their stack traces as
  well, similar to Apple platforms. Recording stack traces is only supported on
  Linux (non-Android) and Apple platforms for now.

----------------------------------------------

# 5.8.0 Release notes

### Bugfixes

* Fix a crash on some platforms when using the query parser to look for a string
  or binary object which has a certain combination of non-printable characters.

### Enhancements

* Support limiting queries via `DescriptorOrdering::set_limit` and by supporting
  "LIMIT(x)" in string queries.
  Issue [realm_sync:#2223](https://github.com/realm/realm-sync/issues/2223)

----------------------------------------------

# 5.7.2 Release notes

### Bugfixes

* Fix a use-after-free when an observer is passed to rollback_and_continue_as_read().

### Enhancements

* More informative InvalidDatabase exception messages
  Issue [#3075](https://github.com/realm/realm-core/issues/3075).

----------------------------------------------

# 5.7.1 Release notes

### Bugfixes

* Fix crash in Group::compute_aggregated_byte_size() when applied on an empty
  realm file. (Issue #3072)

----------------------------------------------

# 5.7.0 Release notes

### Enhancements

* Improved Group::compute_aggregated_byte_size() allowing us to differentiate
  between state, history and freelists.
  (Issue #3063)

----------------------------------------------

# 5.6.5 Release notes

### Enhancements

* Improved scalability for the slab allocator. This allows for larger
  transactions. (PR #3067)

----------------------------------------------

# 5.6.4 Release notes

### Enhancements

* Add Table::add_row_with_keys(), which allows
  sync::create_object_with_primary_key() to avoid updating the index twice when
  creating an object with a string primary key.
* Improved the performance of setting a link to its current value.

----------------------------------------------

# 5.6.3 Release notes

### Enhancements

* Improved scalability for in-file freelist handling. This reduces
  commit overhead on large transactions.
* Improved scalability for in-file allocation during commit.
* Minimized use of memory mappings and msync() on large commits
  on devices which can support large address spaces.

----------------------------------------------

# 5.6.2 Release notes

### Bugfixes

* Fix curruption of freelist with more than 2M entries.
  PR [#3059](https://github.com/realm/realm-core/pull/3059).

----------------------------------------------

# 5.6.1 Release notes

### Bugfixes

* More readable error message in the query parser when requesting an a bad argument.
* Don't write history information in `SharedGroup::compact()` for
  non-syncronized Realms.

-----------

### Internals

* Restore -fvisibility-inlines-hidden for the binaries for Apple platforms.
* Remove a few warnings at compile time.
* Improve error detection related to memory allocation/release

----------------------------------------------

# 5.6.0 Release notes

### Bugfixes

* In the parser, fix `@links.@count` when applied over lists to return
  the sum of backlinks for all connected rows in the list.
* Fix null comparisons in queries not serialising properly in some cases.
  Also explicitly disable list IN list comparisons since its not supported.
  PR [#3037](https://github.com/realm/realm-core/pull/3037).

### Enhancements

* `SharedGroup::compact()` now also compacts history information, which means
  that Sync'ed Realm files can now be compacted (under the usual restrictions;
  see `group_shared.hpp` for details).

----------------------------------------------

# 5.5.0 Release notes

### Enhancements

* Parser improvements:
    - Allow an arbitrary prefix on backlink class names of @links queries.
      This will allow users to query unnamed backlinks using the `@links.Class.property` syntax.
    - Case insensitive `nil` is now recognised as a synonym to `NULL`.
    - Add support for `@links.@count` which gives the count of all backlinks to an object.
      See Issue [#3003](https://github.com/realm/realm-core/issues/3003).

-----------

### Internals

* Apple binaries are now built with Xcode 8.3.3.

----------------------------------------------

# 5.4.2 Release notes

### Bugfixes

* Fix sporadic failures of disk preallocation on APFS.
  PR [#3028](https://github.com/realm/realm-core/pull/3028).

----------------------------------------------

# 5.4.1 Release notes

### Enhancements

* Reduced the number of files opened when the async commit daemon is not used.
  PR [#3022](https://github.com/realm/realm-core/pull/3022).

-----------

### Internals

* Exported CMake targets have been renamed to "modern" conventions, e.g.
  `Realm::Core` and `Realm::QueryParser`.

----------------------------------------------

# 5.4.0 Release notes

### Bugfixes

* Fixed usage of disk space preallocation which would occasionally fail on recent MacOS
  running with the APFS filesystem. PR [#3013](https://github.com/realm/realm-core/pull/3013).
  Issue [#3005](https://github.com/realm/realm-core/issues/3005).
* Fixed a bug in queries containing 'or' at different nesting levels.
  PR [#3006](https://github.com/realm/realm-core/pull/3006).

### Breaking changes

* None.

### Enhancements

* Added `Table::get_link_type()` as a helper method for getting the link type from link columns.
  PR [#2987](https://github.com/realm/realm-core/pull/2987).

-----------

### Internals

* Silenced a false positive strict aliasing warning.
  PR [#3002](https://github.com/realm/realm-core/pull/3002).
* Assertions will print more information in relase mode.
  PR [#2982](https://github.com/realm/realm-core/pull/2982).

----------------------------------------------

# 5.3.0 Release notes

### Bugfixes

* Fixed handling of out-of-diskspace. With encryption in use it would ASSERT like
  `group_writer.cpp:393: [realm-core-5.1.2] Assertion failed: ref + size <= ...`.
  Without encryption it would give a SIGBUS error. It's unknown if it could corrupt
  the .realm file.
* Fix an issue where adding zero rows would add the default value to the keys
  of any string enum columns. Not affecting end users.
  PR [#2956](https://github.com/realm/realm-core/pull/2956).

### Enhancements

* Parser improvements:
    - Support subquery count expressions, for example: "SUBQUERY(list, $x, $x.price > 5 && $x.colour == 'blue').@count > 1"
        - Subqueries can be nested, but all properties must start with the closest variable (no parent scope properties)
    - Support queries over unnamed backlinks, for example: "@links.class_Person.items.cost > 10"
        - Backlinks can be used like lists in expressions including: min, max, sum, avg, count/size, and subqueries
    - Keypath substitution is supported to allow querying over named backlinks and property aliases, see `KeyPathMapping`
    - Parsing backlinks can be disabled at runtime by configuring `KeyPathMapping::set_allow_backlinks`
    - Support for ANY/SOME/ALL/NONE on list properties (parser only). For example: `ALL items.price > 10`
    - Support for operator 'IN' on list properties (parser only). For example: `'milk' IN ingredients.name`
    PR [#2989](https://github.com/realm/realm-core/pull/2989).

-----------

### Internals

* Add support for libfuzzer.
  PR [#2922](https://github.com/realm/realm-core/pull/2922).

----------------------------------------------

# 5.2.0 Release notes

### Bugfixes

* Fix a crash when distinct is applied on two or more properties where
  the properties contain a link and non-link column.
  PR [#2979](https://github.com/realm/realm-core/pull/2979).

### Enhancements

* Parser improvements:
    - Support for comparing two columns of the same type. For example:
        - `wins > losses`
        - `account_balance > purchases.@sum.price`
        - `purchases.@count > max_allowed_items`
        - `team_name CONTAINS[c] location.city_name`
    - Support for sort and distinct clauses
        - At least one query filter is required
        - Columns are a comma separated value list
        - Order of sorting can be `ASC, ASCENDING, DESC, DESCENDING` (case insensitive)
        - `SORT(property1 ASC, property2 DESC)`
        - `DISTINCT(property1, property2)`
        - Any number of sort/distinct expressions can be indicated
    - Better support for NULL synonym in binary and string expressions:
        - `name == NULL` finds null strings
        - `data == NULL` finds null binary data
    - Binary properties can now be queried over links
    - Binary properties now support the full range of string operators
      (BEGINSWITH, ENDSWITH, CONTAINS, LIKE)
    PR [#2979](https://github.com/realm/realm-core/pull/2979).

-----------

### Internals

* The devel-dbg Linux packages now correctly include static libraries instead of shared ones.

----------------------------------------------

# 5.1.2 Release notes

### Bugfixes

* Include the parser libs in the published android packages.

----------------------------------------------

# 5.1.1 Release notes

### Bugfixes

* The `realm-parser` static library now correctly includes both simulator and device architectures on Apple platforms.

----------------------------------------------

# 5.1.0 Release notes

### Enhancements

* Change the allocation scheme to (hopefully) perform better in scenarios
  with high fragmentation.
  PR [#2963](https://github.com/realm/realm-core/pull/2963)
* Avoid excessive bumping of counters in the version management machinery that is
  responsible for supporting live queries. We now prune version bumping earlier if
  when we have sequences of changes without queries in between.
  PR [#2962](https://github.com/realm/realm-core/pull/2962)

----------------------------------------------

# 5.0.1 Release notes

### Bugfixes

* Add a CMake import target for the `realm-parser` library.

----------------------------------------------

# 5.0.0 Release notes

### Bugfixes

* Fix possible corruption or crashes when a `move_row` operates on a subtable.
  PR [#2927](https://github.com/realm/realm-core/pull/2926).
* Table::set_int() did not check if the target column was indeed type_Int. It
  will now assert like the other set methods.

### Breaking changes

* Remove support for the (unused) instructions for moving columns and moving tables.
  This is not a file format breaking change as the instructions are still recognised,
  but now a parser error is thrown if either one is seen in the transaction logs.
  PR [#2926](https://github.com/realm/realm-core/pull/2926).

### Enhancements

* Attempted to fix a false encryption security warning from IBM Bluemix. PR [#2911]
* Utilities gain `Any` from object store and base64 encoding from sync.
* Initial support for query serialisation.
* The query parser from the object store was moved to core.
  It also gained the following enhancements:
    - Support @min, @max, @sum, @avg for types: int, double, float
    - Support @count, @size interchangeably for types list, string, binary
    - Support operator "LIKE" on strings
    - Support operators: =>, =<, <>, which are synonymns for >=, <=, and != respectively
    - Boolean types can now check against “0” or “1” in addition to false and true
    - Fixed "not" and "or" not being applied to TRUEPREDICATE or FALSEPREDICATE
    - Add support for comparing binary and string types using a (internal) base64 format: B64”…”
    - Add support for Timestamps
      - Internal format “Tseconds:nanoseconds”
      - Readable format “YYYY-MM-DD@HH:MM:SS:NANOSECONDS”
        - The nanoseconds part can be optionally omitted
        - Conversion works for UTC from dates between ~1970-3000 on windows and ~1901-2038 on other platforms
  PR [#2947](https://github.com/realm/realm-core/pull/2947).

----------------------------------------------

# 4.0.4 Release notes

### Bugfixes

* Publish the release version of Android-armeabi-v7a binary.

----------------------------------------------

# 4.0.3 Release notes

### Bugfixes

* Switch from using a combination of file write and mmap to using only mmap when
  initializing the lockfile. It is unclear if this counts as a bugfix, because
  it is unclear if there are still systems out there with problems handling that
  scenario. The hope is that it will fix some non-reproducible problems related to
  lockfile initialization.
  PR [#2902](https://github.com/realm/realm-core/pull/2902)
* Make calls to posix_fallocate() robust against interruption and report
  the correct error on failure.
  PR [#2905](https://github.com/realm/realm-core/pull/2905).
* Fix an error in `int_multiply_with_overflow_detect()` which would report
  overflow when no overflow should occur. This could cause out of memory
  exceptions when the `TransactLogParser` reads strings or binary data > 2GB.
  PR [#2906](https://github.com/realm/realm-core/pull/2906).

----------------------------------------------

# 4.0.2 Release notes

### Bugfixes

* Fix a race between SharedGroup::compact() and SharedGroup::open(). The race could
  cause asserts indicating file corruption even if no corruption is caused. It is also
  possible that it could cause real file corruption, though that is much less likely.
  PR [#2892](https://github.com/realm/realm-core/pull/2892)

----------------------------------------------

# 4.0.1 Release notes

### Bugfixes

* Fix case insensitive contains query for null strings not returning all results and
  Fix case insensitive equals query for null strings returning nothing when null strings exist.
  PR [#2871](https://github.com/realm/realm-core/pull/2871).
* Added mentioning of bugfix #2853 to this file for Core 4.0.0. (see 4.0.0 below)
  The mentioning of this fix for 4.0 was originally ommitted.

----------------------------------------------

# 4.0.0 Release notes

### Bugfixes

* Fix a bug in subtable management which caused crashes if a subtable was destroyed
  on a different thread.
  PR [#2855](https://github.com/realm/realm-core/pull/2855).
* Fix corruption caused by `swap_rows()` and `move_column()` operations applied
  to a StringEnumColumn. Currently unused by bindings.
  PR [#2780](https://github.com/realm/realm-core/pull/2780).

### Breaking changes

* Add `Table::move_row()`.
  PR [#2873](https://github.com/realm/realm-core/pull/2873).
* Changing instruction values for `Table::move_row()` requires a version bump to 9.
  Version 8 files in read only mode without any history can be opened without upgrading.
  PR [#2877](https://github.com/realm/realm-core/pull/2877).

### Enhancements

* Add method to recursively delete an object tree
  PR [#2752](https://github.com/realm/realm-core/pull/2752)
  Issue [#2718](https://github.com/realm/realm-core/issues/2718)
* Add method to safely delete or otherwise manipulate realm file
  and management files.
  PR [#2864](https://github.com/realm/realm-core/pull/2864)

-----------

### Internals

* A specialised exception realm::OutOfDiskSpace is thrown instead of a generic
  runtime exception when writing fails because the disk is full or the user exceeds
  the allotted disk quota.
  PR [#2861](https://github.com/realm/realm-core/pull/2861).

----------------------------------------------

# 3.2.1 Release notes

### Bugfixes

* Compact now throws an exception if writing fails for some reason
  instead of ignoring errors and possibly causing corruption.
  In particular, this solves file truncation causing "bad header" exceptions
  after a compact operation on a file system that is running out of disk space.
  PR [#2852](https://github.com/realm/realm-core/pull/2852).

-----------

### Internals

* Moved object store's true and false query expressions down to core.
  PR [#2857](https://github.com/realm/realm-core/pull/2857).

----------------------------------------------

# 3.2.0 Release notes

### Enhancements

* Added metrics tracking as an optional SharedGroup feature.
  PR [#2840](https://github.com/realm/realm-core/pull/2840).

-----------

### Internals

* Improve crash durability on windows.
  PR [#2845](https://github.com/realm/realm-core/pull/2845).
* Removed incorrect string column type traits, which could cause errors.
  They were unused. PR [#2846](https://github.com/realm/realm-core/pull/2846).

----------------------------------------------

# 3.1.0 Release notes

### Bugfixes

* A linker error in some configurations was addressed by adding an explicit
  instantiation of `Table::find_first` for `BinaryData`.
  [#2823](https://github.com/realm/realm-core/pull/2823)

### Enhancements

* Implemented `realm::util::File::is_dir`, `realm::util::File::resolve`,
  and `realm::util::DirScanner` on Windows.

----------------------------------------------

# 3.0.0 Release notes

### Bugfixes

* Fixed handle leak on Windows (https://github.com/realm/realm-core/pull/2781)
* Fixed a use-after-free when a TableRef for a table containing a subtable
  outlives the owning group.

### Breaking changes

* Added support for compound sort and distinct queries.
    - Multiple consecutive calls to sort or distinct compound on each other
      in the order applied rather than replacing the previous one.
    - The order that sort and distinct are applied can change the query result.
    - Applying an empty sort or distinct descriptor is now a no-op, this
      could previously be used to clear a sort or distinct operation.
  PR [#2644](https://github.com/realm/realm-core/pull/2644)
* Support for size query on LinkedList removed. This is perhaps not so
  breaking after all since it is probably not used.
  PR [#2532](https://github.com/realm/realm-core/pull/2532).
* Replication interface changed. The search index functions now operate
  on a descriptor and not a table.
  PR [#2561](https://github.com/realm/realm-core/pull/2561).
* New replication instruction: instr_AddRowWithKey
* Add the old table size to the instr_TableClear replication instruction.
* Throw a MaximumFileSizeExceeded exception during commits or allocations
  instead of causing corruption or asserting. This would most likely be
  seen when creating large Realm files on 32 bit OS.
  PR [#2795](https://github.com/realm/realm-core/pull/2795).

### Enhancements

* Enhanced support for query in subtables:
  Query q = table->column<SubTable>(0).list<Int>() == 5;
  Query q = table->column<SubTable>(0).list<Int>().min() >= 2;
  Query q = table->column<SubTable>(1).list<String>().begins_with("Bar");
  PR [#2532](https://github.com/realm/realm-core/pull/2532).
* Subtable column can now be nullable. You can use `is_null()` and `set_null()`
  on a subtable element.
  PR [#2560](https://github.com/realm/realm-core/pull/2560).
* Support for search index on subtable columns. Only one level of subtables
  are currently supported, that is, you cannot create a search index in a
  subtable of a subtable (will throw exception). NOTE: Core versions prior to
  this version will not be able to open .realm files of this Core version if
  this Core version has added such indexes. Adding or removing an index will
  take place for *all* subtables in a subtable column. There is no way to add
  or remove it from single individual subtables.
  PR [#2561](https://github.com/realm/realm-core/pull/2561).
* Support for encryption on Windows (Win32 + UWP).
  PR [#2643](https://github.com/realm/realm-core/pull/2643).
* Add Table::add_row_with_key(). Adds a row and fills an integer column with
  a value in one operation.
  PR [#2596](https://github.com/realm/realm-core/pull/2596)
  Issue [#2585](https://github.com/realm/realm-core/issues/2585)
* Add more overloads with realm::null - PR [#2669](https://github.com/realm/realm-core/pull/2669)
  - `size_t Table::find_first(size_t col_ndx, null)`
  - `OutputStream& operator<<(OutputStream& os, const null&)`

-----------

### Internals

* The RuntimeLibrary of the Windows build is changed from MultiThreadedDLL to
  just MultiThreaded so as to statically link the Visual C++ runtime libraries,
  removing the onus on end-users to have the correct runtime redistributable
  package or satellite assembly pack installed. Libraries that link against Core
  on Windows will have to adjust their compiler flags accordingly.
  PR [#2611](https://github.com/realm/realm-core/pull/2611).
* Win32+UWP: Switched from pthread-win32 to native API.
  PR [#2602](https://github.com/realm/realm-core/pull/2602).
* Implemented inter-process CondVars on Windows (Win32 + UWP). They should be
  fair and robust.
  PR [#2497](https://github.com/realm/realm-core/pull/2497).
* The archives produced by the packaging process for Mac builds are now
  .tar.gz files rather than .tar.xz files, with the exception of the aggregate
  realm-core-cocoa-VERSION.tar.xz archive, which remains as a .tar.xz file.

----------------------------------------------

# 2.9.2 Release notes

### Bugfixes

* Throw a MaximumFileSizeExceeded exception during commits or allocations
  instead of causing corruption or asserting. This would most likely be
  seen when creating large Realm files on 32 bit OS.
  PR [#2795](https://github.com/realm/realm-core/pull/2795).

**Note: This is a hotfix release built on top of 2.9.1. The above fixes are not present in version 3.0.0.**

----------------------------------------------

# 2.9.1 Release notes

### Bugfixes

* A linker error in some configurations was addressed by adding an explicit
  instantiation of `Table::find_first` for `BinaryData`.
  [#2823](https://github.com/realm/realm-core/pull/2823).

-----------

### Internals

* The archives produced by the packaging process for Mac builds are now
  .tar.gz files rather than .tar.xz files, with the exception of the aggregate
  realm-core-cocoa-VERSION.tar.xz archive, which remains as a .tar.xz file.

**Note: This is a hotfix release built on top of 2.9.0. The above fixes are not present in version 3.0.0.**

----------------------------------------------

# 2.9.0 Release notes

### Bugfixes

* Attempting to open a small unencrypted Realm file with an encryption key would
  produce an empty encrypted Realm file. Fixed by detecting the case and
  throwing an exception.
  PR [#2645](https://github.com/realm/realm-core/pull/2645)
* Querying SharedGroup::wait_for_change() immediately after a commit()
  would return instead of waiting for the next change.
  PR [#2563](https://github.com/realm/realm-core/pull/2563).
* Opening a second SharedGroup may trigger a file format upgrade if the history
  schema version is non-zero.
  Fixes issue [#2724](https://github.com/realm/realm-core/issues/2724).
  PR [#2726](https://github.com/realm/realm-core/pull/2726).
* Fix incorrect results from TableView::find_first().
* Fix crash on rollback of Table::optimize(). Currently unused by bindings.
  PR [#2753](https://github.com/realm/realm-core/pull/2753).
* Update frozen TableViews when Table::swap() is called.
  PR [#2757](https://github.com/realm/realm-core/pull/2757).

### Enhancements

* Add method to get total count of backlinks for a row.
  PR [#2672](https://github.com/realm/realm-core/pull/2672).
* Add try_remove_dir() and try_remove_dir_recursive() functions.

-----------

### Internals

* On Apple platforms, use `os_log` instead of `asl_log` when possible.
  PR [#2722](https://github.com/realm/realm-core/pull/2722).

----------------------------------------------

# 2.8.6 Release notes

### Bugfixes
* Fixed a bug where case insensitive queries wouldn't return all results.
  PR [#2675](https://github.com/realm/realm-core/pull/2675).

----------------------------------------------

# 2.8.5 Release notes

### Internals

* `_impl::GroupFriend::get_top_ref()` was added.
  PR [#2683](https://github.com/realm/realm-core/pull/2683).

----------------------------------------------

# 2.8.4 Release notes

### Bugfixes

* Fixes bug in encryption that could cause deadlocks/hangs and possibly
  other bugs too.
  Fixes issue [#2650](https://github.com/realm/realm-core/pull/2650).
  PR [#2668](https://github.com/realm/realm-core/pull/2668).

-----------

### Internals

* Fix an assert that prevented `Group::commit()` from discarding history from a
  Realm file opened in nonshared mode (via `Group::open()`, as opposed to
  `SharedGroup::open()`).
  PR [#2655](https://github.com/realm/realm-core/pull/2655).
* Improve ASAN and TSAN build modes (`sh build.sh asan` and `sh build.sh tsan`)
  such that they do not clobber the files produced during regular builds, and
  also do not clobber each others files. Also `UNITTEST_THREADS` and
  `UNITTEST_PROGRESS` options are no longer hard-coded in ASAN and TSAN build
  modes.
  PR [#2660](https://github.com/realm/realm-core/pull/2660).

----------------------------------------------

# 2.8.3 Release notes

### Internals

* Disabled a sleep in debug mode that was impairing external tests.
  PR [#2651](https://github.com/realm/realm-core/pull/2651).

----------------------------------------------

# 2.8.2 Release notes

### Bugfixes

* Now rejecting a Realm file specifying a history schema version that is newer
  than the one expected by the code.
  PR [#2642](https://github.com/realm/realm-core/pull/2642).
* No longer triggering a history schema upgrade when opening an empty Realm file
  (when `top_ref` is zero).
  PR [#2642](https://github.com/realm/realm-core/pull/2642).

----------------------------------------------

# 2.8.1 Release notes

### Bugfixes

* Add #include <realm/util/safe_int_ops.hpp> in alloc.hpp.
  PR [#2622](https://github.com/realm/realm-core/pull/2622).
* Fix crash in large (>4GB) encrypted Realm files.
  PR [#2572](https://github.com/realm/realm-core/pull/2572).
* Fix missing symbols for some overloads of Table::find_first
  in some configurations.
  PR [#2624](https://github.com/realm/realm-core/pull/2624).

----------------------------------------------

# 2.8.0 Release notes

### Bugfixes

* Fix a race condition in encrypted files which can lead to
  crashes on devices using OpenSSL (Android).
  PR [#2616](https://github.com/realm/realm-core/pull/2616).

### Enhancements

* Enable encryption on watchOS.
  Cocoa issue [#2876](https://github.com/realm/realm-cocoa/issues/2876).
  PR [#2598](https://github.com/realm/realm-core/pull/2598).
* Enforce consistent use of encryption keys across all threads.
  PR [#2558](https://github.com/realm/realm-core/pull/2558).

----------------------------------------------

# 2.7.0 Release notes

### Bugfixes

* Fix for creating process-shared mutex objects in the wrong kernel object namespace on UWP.
  PR [#2579](https://github.com/realm/realm-core/pull/2579).

### Enhancements

* Add `Group::compute_aggregated_byte_size()` and
  `Table::compute_aggregated_byte_size()` for debugging/diagnostics purposes.
  PR [#2591](https://github.com/realm/realm-core/pull/2591).
* `Table` and `TableView` refactoring and improvements.
  PR [#2571](https://github.com/realm/realm-core/pull/2571).
  * Add a templated version of `Table::set()` to go with `Table::get()`.
  * Add `TableView::find_first_timestamp()`.
  * Add `TableView::find_first<T>()`.
  * Make `Table::find_first<T>()` public and add support for most column types.
  * Add wrappers for `Table::set<T>()` to `Row`.
  * Add support for all column types in `Table::get<T>()`.

-----------

### Internals

* Make `Array::stats()` available in release mode builds (not just in debug mode
  builds).
  PR [#2591](https://github.com/realm/realm-core/pull/2591).

----------------------------------------------

# 2.6.2 Release notes

### Bugfixes

* Fix for incorrect, redundant string index tree traversal for case insensitive searches
  for strings with some characters being identical in upper and lower case (e.g. numbers).
  PR [#2578](https://github.com/realm/realm-core/pull/2578),
  Cocoa issue [#4895](https://github.com/realm/realm-cocoa/issues/4895)

----------------------------------------------

# 2.6.1 Release notes

### Bugfixes

* `mkfifo` on external storage fails with `EINVAL` on some devices with Android 7.x,
  which caused crash when opening Realm.
  PR[#2574](https://github.com/realm/realm-core/pull/2574),
  Issue [#4461](https://github.com/realm/realm-java/issues/4461).

----------------------------------------------

# 2.6.0 Release notes

### Bugfixes

* Work around a bug in macOS which could cause a deadlock when trying to obtain a shared lock
  using flock(). PR [#2552](https://github.com/realm/realm-core/pull/2552),
  issue [#2434](https://github.com/realm/realm-core/issues/2434).

### Enhancements

* Add support for `SharedGroup::try_begin_write()` and corresponding `try_lock()`
  functionality in low level Mutex classes.
  PR [#2547](https://github.com/realm/realm-core/pull/2547/files)
  Fixes issue [#2538](https://github.com/realm/realm-core/issues/2538)
* New file system utility functions: `util::remove_dir_recursive()` and
  `util::File::for_each()`. PR [#2556](https://github.com/realm/realm-core/pull/2556).
* Made case insensitive queries use the new index based case insensitive search.
  PR [#2486](https://github.com/realm/realm-core/pull/2486)

----------------------------------------------

# 2.5.1 Release notes

### Enhancements

* Restore support for opening version 6 files in read-only mode.
  PR [#2549](https://github.com/realm/realm-core/pull/2549).

----------------------------------------------

# 2.5.0 Release notes

### Bugfixes

* Fixed a crash when rolling back a transaction which set binary or string data
  inside a Mixed type.
  PR [#2501](https://github.com/realm/realm-core/pull/2501).
* Properly refresh table accessors connected by backlinks to a row that has had
  a `merge_rows` instruction applied and then rolled back. This could have
  caused corruption if this scenario was triggered but since sync does not use
  the `merge_rows` instruction in this way, this is a preventative fix.
  PR [#2503](https://github.com/realm/realm-core/pull/2503).
* Fixed an assertion on a corner case of reallocation on large arrays.
  PR [#2500](https://github.com/realm/realm-core/pull/2500).
  Fixes issue [#2451](https://github.com/realm/realm-core/issues/2451).

### Breaking changes

* Disable copying of various classes to prevent incorrect use at compile time.
  PR [#2468](https://github.com/realm/realm-core/pull/2468).
* History type enumeration value `Replication::hist_Sync` renamed to
  `Replication::hist_SyncClient`.
  PR [#2482](https://github.com/realm/realm-core/pull/2482).
* Bumps file format version from 6 to 7 due to addition of a 10th element into
  `Group::m_top`. The new element is the history schema version, which is
  crucial for managing the schema upgrade process of sync-type histories in a
  way that is independent of core's Realm file format. The bump is necessary due
  to lack of forwards compatibility. The changes are backwards compatible, and
  automatic upgrade is implemented.
  PR [#2481](https://github.com/realm/realm-core/pull/2481).
* New pure virtual methods `get_history_schema_version()`,
  `is_upgradable_history_schema()`, and `upgrade_history_schema()` in
  `Replication` interface.
  PR [#2481](https://github.com/realm/realm-core/pull/2481).

### Enhancements

* Support setting Mixed(Timestamp) through the transaction logs.
  PR [#2507](https://github.com/realm/realm-core/pull/2507).
* Implement comparison of Mixed objects containing Timestamp types.
  PR [#2507](https://github.com/realm/realm-core/pull/2507).
* Allow query for size of strings, binaries, linklists and subtables:
  Query q = table->where().size_equal(2, 5);
  Query q = table1->column<SubTable>(2).size() == 5;
  PR [#2504](https://github.com/realm/realm-core/pull/2504).
* New history type enumeration value `Replication::hist_SyncServer`. This allows
  for the sync server to start using the same kind of in-Realm history scheme as
  is currently used by clients.
  PR [#2482](https://github.com/realm/realm-core/pull/2482).

-----------

### Internals

* `StringIndex` now supports case insensitive searches.
  PR [#2475](https://github.com/realm/realm-core/pull/2475).
* `AppendBuffer` gained support for move construction/assignment, and had its
  growth factor reduced to 1.5.
  PR [#2462](https://github.com/realm/realm-core/pull/2462).
* Methods on the `Replication` interface were made virtual to allow override.
  PR [#2462](https://github.com/realm/realm-core/pull/2462).
* The order of emission for some instructions in the transaction log was changed
  with respect to carrying out the effect of the instruction on the database, to
  allow implementors of the `Replication` interface a semi-consistent view of
  the database.
  PR [#2462](https://github.com/realm/realm-core/pull/2462).
* Lock file format bumped from version 9 to 10 due to introduction of
  `SharedInfo::history_schema_version`.
  PR [#2481](https://github.com/realm/realm-core/pull/2481).
* Removal of obsolete logic and semantics relating to obsolete history type
  `Replication::hist_OutOfRealm`.
  PR [#2481](https://github.com/realm/realm-core/pull/2481).
* Code specific to history type `Replication::hist_InRealm` (class
  `_impl::InRealmHistory` in particular) was moved from
  `realm/impl/continuous_transactions_history.hpp` and
  `realm/impl/continuous_transactions_history.cpp` to `realm/sync/history.cpp`.
  PR [#2481](https://github.com/realm/realm-core/pull/2481).

----------------------------------------------

# 2.4.0 Release notes

### Bugfixes

* Fixes a bug in chuncked binary column returning null value.
  PR [#2416](https://github.com/realm/realm-core/pull/2416).
  Fixes issue [#2418](https://github.com/realm/realm-core/issues/2418).
* Possibly fixed some cases of extreme file size growth, by preventing starvation
  when trying to start a write transaction, while simultaneously pinning an older
  version.
  PR [#2395](https://github.com/realm/realm-core/pull/2395).
* Fixed a bug when deleting a column used in a query.
  PR [#2408](https://github.com/realm/realm-core/pull/2408).
* Fixed a crash that occurred if you tried to override a binary with a size close
  to the limit.
  PR [#2416](https://github.com/realm/realm-core/pull/2416).
* `seekpos()` and `seekoff()` in `realm::util::MemoryInputStreambuf` now behave
  correctly when argument is out of range.
  PR [#2472](https://github.com/realm/realm-core/pull/2472).

### Breaking changes

* The table macros, supporting the typed interface, has been removed.
  PR [#2392](https://github.com/realm/realm-core/pull/2392).
* Layout and version change for the .lock file required in order to prevent
  starvation when waiting to start a write transaction (see above).
  PR [#2395](https://github.com/realm/realm-core/pull/2395).

### Enhancements

* Now supports case insensitive queries for UWP.
  PR [#2389](https://github.com/realm/realm-core/pull/2389).
* Upgraded Visual Studio project to version 2017.
  PR [#2389](https://github.com/realm/realm-core/pull/2389).
* Support handover of TableViews and Queries based on SubTables.
  PR [#2470](https://github.com/realm/realm-core/pull/2470).
* Enable reading and writing of big blobs via Table interface.
  Only to be used by Sync. The old interface still has a check on
  the size of the binary blob.
  PR [#2416](https://github.com/realm/realm-core/pull/2416).

----------------------------------------------

# 2.3.3 Release notes

### Bugfixes

* Fix a hang in LIKE queries that could occur if the pattern required
  backtracking. PR [#2477](https://github.com/realm/realm-core/pull/2477).
* Bug fixed in `GroupWriter::write_group()` where the maximum size of the top
  array was calculated incorrectly. This bug had the potential to cause
  corruption in Realm files. PR [#2480](https://github.com/realm/realm-core/pull/2480).

### Enhancements

* Use only a single file descriptor in our emulation of interprocess condition variables
  on most platforms rather than two. PR [#2460](https://github.com/realm/realm-core/pull/2460). Fixes Cocoa issue [#4676](https://github.com/realm/realm-cocoa/issues/4676).

----------------------------------------------

# 2.3.2 Release notes

### Bugfixes
* Fixed race condition bug that could cause crashes and corrupted data
  under rare circumstances with heavy load from multiple threads accessing
  encrypted data. (sometimes pieces of data from earlier commits could be seen).
  PR [#2465](https://github.com/realm/realm-core/pull/2465). Fixes issue [#2383](https://github.com/realm/realm-core/issues/2383).
* Added SharedGroupOptions::set_sys_tmp_dir() and
  SharedGroupOptions::set_sys_tmp_dir() to solve crash when compacting a Realm
  file on Android external storage which is caused by invalid default sys_tmp_dir.
  PR [#2445](https://github.com/realm/realm-core/pull/2445). Fixes Java issue [#4140](https://github.com/realm/realm-java/issues/4140).

-----------

### Internals

* Remove the BinaryData constructor taking a temporary object to prevent some
  errors in unit tests at compile time. PR [#2446](https://github.com/realm/realm-core/pull/2446).
* Avoid assertions in aggregate functions for the timestamp type. PR [#2466](https://github.com/realm/realm-core/pull/2466).

----------------------------------------------

# 2.3.1 Release notes

### Bugfixes

* Fixed a bug in handover of detached linked lists. (issue #2378).
* Fixed a bug in advance_read(): The memory mappings need to be updated and
  the translation cache in the slab allocator must be invalidated prior to
  traversing the transaction history. This bug could be reported as corruption
  in general, or more likely as corruption of the transaction log. It is much
  more likely to trigger if encryption is enabled. (issue #2383).

### Enhancements

* Avoid copying copy-on-write data structures when the write does not actually
  change the existing value.
* Improve performance of deleting all rows in a TableView.
* Allow the `add_int()` API to be called on a `Row`
* Don't open the notification pipes on platforms which support the async commit
  daemon when async commits are not enabled

-----------

### Internals

* Updated OpenSSL to 1.0.2k.
* Setting environment variable `UNITTEST_XML` to a nonempty value will no longer
  disable the normal console output while running the test suite. Instead, in
  that case, reporting will happen both to the console and to the JUnit XML
  file.

----------------------------------------------

# 2.3.0 Release notes

### Bugfixes

* Fixed various bugs in aggregate methods of Table, TableView and Query for nullable columns
  (max, min, avg, sum). The results of avg and sum could be wrong and the returned index of
  the min and max rows could be wrong. Non-nullable columns might not have been affected.
  One of the bugs are described here https://github.com/realm/realm-core/issues/2357
* Prevent `stdin` from being accidentally closed during `~InterProcessCondVar()`.

### Breaking changes

* Attempts to open a Realm file with a different history type (Mobile Platform vs
  Mobile Database) now throws an IncompatibleHistories exception instead of a
  InvalidDatabase (as requested in issue #2275).

### Enhancements
* Windows 10 UWP support. Use the new "UWP" configurations in Visual Studio to
  compile core as a static .lib library for that platform. Also see sample App
  in the uwp_demo directory that uses the static library (compile the .lib first).
  Note that it is currently just an internal preview with lots of limitations; see
  https://github.com/realm/realm-core/issues/2059
* Added 'void SharedGroup::get_stats(size_t& free_space, size_t& used_space)'
  allowing access to the size of free and used space (Requested in issue #2281).
* Optimized Contains queries to use Boyer-Moore algorithm (around 10x speedup on large datasets)
* Parameter arguments passed to logger methods (e.g., `util::Logger::info()`)
  are now perfectly forwarded (via perfect forwarding) to `std::stream::operator<<()`.

-----------

### Internals

* Unit tests now support JUnit output format.

----------------------------------------------

# 2.2.1 Release notes

### Enhancements

* Parameter arguments passed to logger methods (e.g., `util::Logger::info()`)
  are now perfectly forwarded (via perfect forwarding) to
  `std::stream::operator<<()`.

-----------

### Internals

* Make `_impl::make_error_code(_impl::SimulatedFailure::FailureType)`
  participate in overload resolution in unqualified ADL contexts like
  `make_error_code(_impl::SimulatedFailure::sync_client__read_head)` and `ec ==
  _impl::SimulatedFailure::sync_client__read_head`.
* `P_tmpdir` should not be used on Android. A better default name for temporary
  folders has been introduced.

----------------------------------------------

# 2.2.0 Release notes

### Bugfixes
* Fix possible corruption of realm file in case of more than 1000 entries in a
  link list (#2289, #2292, #2293, #2295, #2301)
* Fixed crash in query if a table had been modified so much that payload array
  leafs had relocated (#2269)
* Fix a race involving destruction order of InterprocessMutex static variables.
* Fix a crash when a Query is reimported into the SharedGroup it was exported
  for handover from.
* Fix a crash when calling mkfifo on Android 4.x external storage. On 4.x devices,
  errno is EPERM instead of EACCES.
* Fix a crash when updating a LinkView accessor from a leaf to an inner node. (#2321)

### Breaking changes

* The return type of `util::File::copy()` has been changed from `bool` to
  `void`. Errors are now reported via `File::AccessError` exceptions. This
  greatly increases the utility and robustness of `util::File::copy()`, as it
  now catches all errors, and reports them in the same style as the other
  functions in `util::File`.

### Enhancements

* Added support for LIKE queries (wildcard with `?` and `*`)
* Offer facilities to prevent multiple sync agents per Realm file access session
  (`Replication::is_sync_agent()` to be overridden by sync-specific
  implementation). The utilized lock-file flag
  (`SharedInfo::sync_agent_present`) was added a long time ago, but the
  completion of detection mechanism got postponed until now.
* Improve performance of write transactions which free a large amount of
  existing data.
* Added `util::File::compare()` for comparing two files for equality.

-----------

### Internals

* Added extra check for double frees in slab allocator.
* Deprecated Array type parameters in Column<T> and BpTree<T> constructors

----------------------------------------------

# 2.1.4 Release notes

### Bugfixes

* Fix storage of very large refs (MSB set) on 32-bit platforms.
* Fixed a race between destruction of a global mutex as part of main thread exit
  and attempt to lock it on a background thread, or conversely attempt to lock a
  mutex after it has been destroyed. (PR #2238, fixes issues #2238, #2137, #2009)

----------------------------------------------

# 2.1.3 Release notes

### Bugfixes

* Deleting rows through a `TableView` generated wrong instructions by way of
  `Table::batch_erase_rows()`, which would only be noticed after reapplying the
  transaction log to a separate Realm file or via synchronization.

-----------

### Internals

* `array_direct.hpp` added to installed headers.

----------------------------------------------

# 2.1.2 Release notes

### Bugfixes

* When adding a nullable column of type Float while other columns existed
  already, the values of the new column would be non-null. This is now fixed.

----------------------------------------------

# 2.1.1 Release notes

### Internals

* Upgraded to OpenSSL 1.0.2j.

----------------------------------------------

# 2.1.0 Release notes

### Bugfixes

* Fix an assertion failure when upgrading indexed nullable int columns to the
  new index format.
* Extra SetUnique instructions are no longer generated in the transaction log
  when a conflict was resolved locally.

### Breaking changes

* The ChangeLinkTargets instruction was a misnomer and has been renamed to
  MergeRows.

-----------

### Internals

* Android builds: upgraded to OpenSSL 1.0.1u.
* The behavior of MergeRows (formerly ChangeLinkTargets) has been simplified to
  be semantically equivalent to a row swap.

----------------------------------------------

# 2.0.0 Release notes

### Bugfixes

* TimestampColumn::is_nullable() could return a wrong value. Also, when adding a new
  Float/Double column to a Table with rowcount > 0, the new entries would be non-null
  even though the column was created as nullable.
* Update accessors after a change_link_target or set_unique operation, so that users
  will have the latest data immediately. Previously this would require manually
  refetching the data or looking for the unique key again.

----------------------------------------------

# 2.0.0-rc9 Release notes

### Internals

* Use Xcode 7.3.1 to build core for Cocoa

----------------------------------------------

# 2.0.0-rc8 Release notes

### Bugfixes

* Fixed a crash related to queries that was introduced in rc7. (#2186)
* Fixed a bug triggered through set unique of primary keys through
  the ROS. (#2180)

-----------

### Internals

* Optimized query code on a string column with a search index to address a
  performance regression observed in the recent format changes to the
  string index (see #2173)

----------------------------------------------

# 2.0.0-rc7 Release notes

### Bugfixes

* Fixed a race in the handover machinery which could cause crashes following handover
  of a Query or a TableView. (#2117)
* Reversed the decision process of resolving primary key conflicts. Instead of
  letting the newest row win, the oldest row will now always win in order to not
  lose subsequent changes.

-----------

### Breaking changes

* Changed the format of the StringIndex structure to not recursivly store
  strings past a certain depth. This fixes crashes when storing strings
  with a long common prefix in an index. This is a file format breaking change.
  The file format has been incremented and old Realm files must upgrade.
  The upgrade will rebuild any StringIndexes to the new format automatically
  so other than the upgrade, this change should be effectivly invisible to
  the bindings. (see #2153)

-----------

### Internals

* Removed ("deleted") the default copy constructor for RowBase. This constructor
  was used by accident by derived classes, which led to a data race. Said race was
  benign, but would be reported by the thread sanitizer.

----------------------------------------------

# 2.0.0-rc6 Release notes

### Enhancements

* Added debian packages for Ubuntu 16.04.

----------------------------------------------

# 2.0.0-rc4 Release notes

### Bugfixes

* Fixed a bug where find() on a Query constructed from a restricting view
  did not correctly return an row index into the underlying table.
  (issue #2127)
* Fixed a bug where linked tables were not updated after a table move operation, when
  run through the replicator.
* Fixed a bug where moving a column to itself caused a crash.

### Breaking changes

* New instruction for `Table::add_int()`, which impacts the transaction log
  format.

### Enhancements

* Added `Table::add_int()` for implementing CRDT counters.

----------------------------------------------

# 2.0.0-rc3 Release notes

### Bugfixes

* Fixed a bug with link columns incorrectly updating on a `move_last_over`
  operation when the link points to the same table.
* Fix subspecs not updating properly after a move operation.
* Fixed various crashes when using subtables. The crash will occur when the first column
  of the subtable if of type `col_type_Timestamp` or if it is nullable and of type Bool, Int
  or OldDateTime. Caused by bad static `get_size_from_ref()` methods of columns. (#2101)
* Fixed a bug with link columns incorrectly updating on a `move_last_over`
  operation when the link points to the same table.

### Breaking changes

* Refactored the `SharedGroup` constructors and open methods to use a new
  `SharedGroupOptions` parameter which stores all options together.
* BREAKING! Until now, a Query would return indexes into a restricting view if such was
  present (a view given in the `.where(&view) method`, or it would return indexes into the
  Table if no restricting view was present. This would make query results useless if you did
  not know whether or not a restricting view was present. This fix make it *always* return
  indexes into the Table in all cases. Also, any `begin` and `end` arguments could point into
  eitherthe View or the Table. These now always point into the Table. Also see
  https://github.com/realm/realm-core/issues/1565

### Enhancements

* Accessors pointing to subsumed rows are updated to the new row rather than detached.

-----------

### Internals

* When creating a `SharedGroup`, optionally allow setting the temporary
  directory to when making named pipes fails. This is to fix a bug
  involving mkfifo on recent android devices (#1959).
* Bug fixed in test harness: In some cases some tests and checks would be
  counted twice due to counters not being reset at all the right times.

----------------------------------------------

# 2.0.0-rc2 Release notes

### Enhancements

* Add back log level prefixes for `StderrLogger` and `StreamLogger`

----------------------------------------------

# 2.0.0-rc1 Release notes

### Breaking changes

* API Breaking change: Added log level argument to util::Logger::do_log().
  Existing implementations can ignore the argument, or use it to add log level
  info to the log output.
* API Breaking change: The WriteLogCollector is no longer available.
  To create a history object for SharedGroup, make_in_realm_history()
  must now be used instead of make_client_history().
* The commit logs have been moved into the Realm file. This means we no longer
  need the .log_a, .log_b and .log files, significantly reducing the number of
  both files and open file handles. This is a breaking change, since versions
  without .log files cannot interoperate with earlier versions which still
  uses separate .log files. (issues #2065, #1354).
* The version for .lock-file data has been bumped to reflect that this is
  an API breaking change.

### Enhancements

* Elimination of the .log files also eliminates all locking related to
  accessing  the .log files, making read-transactions lock-free.
* The critical phase of commits have been reduced significantly in length.
  If a process is killed while in the critical phase, any other process
  working jointly on the same Realm file is barred from updating the Realm
  file until the next session. Reducing the length of the critical phase
  reduces the risk of any user experiencing this limitation.
  (issues #2065, #1354)

-----------

### Internals

* Added support for very large commit history entries. (issues #2038, #2050)
  This also implies an API change (but to the internal API) to the
  History::get_changesets() method, which must be taken into account by
  any derived classes.
* Support for setting and getting thread names (`util::Thread::set_name()` and
  `util::Thread::get_name()`) when the platform supports
  it. `util::Thread::set_name()` is now used by the test harness as a help while
  debugging. Also, the terminate handler (in `util/terminate.cpp`) writes out
  the name of the terminating thread if the name is available.
* Fixed doxygen warnings.

----------------------------------------------

# 2.0.0-rc0 Release notes

### Internals

* Changed instruction log format of Set instructions to be more amenable to the
addition of future variants.
* Changed instruction log format of LinkList instructions to include information
about the size of the list in question prior to carrying out the instruction.

----------------------------------------------

# 1.5.1 Release notes

### Bugfixes

* Fixed java bug #3144 / Core #2014. Management of Descriptor class was
  not thread safe with respect to destruction/creation/management of
  accessor tree. Bug could be triggered by destruction of TableView on
  one thread, while new TableViews where created on a different thread.
* Fixed incorrect results when updating a backlink TableView after inserting
  new columns into the source table.

----------------------------------------------

# 1.5.0 Release notes

### Bugfixes

* Fix a race condition that could result in a crash if a `LinkView` was
  destroyed while another thread was adjusting accessors on a `LinkListColumn`.
* Fix crashes and general brokenness when constructing a Query, inserting a
  column into the queried table, and then running the query.
* Fix crashes and general brokenness when syncing a sorted or distincted
  TableView after inserting new columns into the source Table.

### Breaking changes

* Added support for sorting and distincting table views through a chain of
  links. (#1030)

### Enhancements

* Improve performance of sorting on non-nullable columns.
* Improve overall sort performance.

-----------

### Internals

* Updated the header license to prepare for open sourcing the code.

----------------------------------------------

# 1.4.2 Release notes

### Bugfixes

* Fix a bug with the local mutex for the robust mutex emulation.
* Reduce the number of file descriptors used in robust mutex emulation,
  multi instances of InterprocessMutex share the same descriptor. (#1986)

----------------------------------------------

# 1.4.1 Release notes

### Bugfixes

* Fixing util::MemoryInputStream to support tellg() and seekg().
* Fix truncation of the supplied value when querying for a float or double that
  is less than a column's value.
* Workaround for the Blackberry mkfifo bug.

-----------

### Internals

* Removed `realm::util::network` library.
* Removed event loop library.
* Reduced the number of open files on Android.

----------------------------------------------

# 1.4.0 Release notes

### Breaking changes

* Throw a logic error (of type `table_has_no_columns`) if an attempt is made to
  add rows to a table with no columns. (#1897)
* S: A clear operation is emitted on removal of the last public column of a table.

----------------------------------------------

# 1.3.1 Release notes

### Bugfixes

* Add missing locks when access `Table::m_views` which may lead to some java
  crashes since java will not guarantee destruction and construction always
  happen in the same thread. (#1958)
* Fixed a bug where tableviews created via backlinks were not automatically
  updated when the source table changed. (#1950)

### Breaking changes

* Throw a logic error (of type `table_has_no_columns`) if an attempt is made to
  add rows to a table with no columns. (#1897)
* S: A clear operation is emitted on removal of the last public column of a table.

### Enhancements

* Increased the verbosity of some exception error messages to help debugging.

----------------------------------------------

# 1.3.0 Release notes

### Bugfixes

* Fix a crash when `Group::move_table()` is called before table accessors are
  initialized. (#1939)

### Breaking changes

* Sorting with `STRING_COMPARE_CORE` now sorts with pre 1.1.2 ordering. Sorting
  with 1.1.2 order is available by using `STRING_COMPARE_CORE_SIMILAR`. (#1947)

-----------

### Internals

* Performance improvements for `LinkListColumn::get_ptr()`. (#1933)

----------------------------------------------

# 1.2.0 Release notes

### Bugfixes

* Update table views so that rows are not attached after calling Table::clear() (#1837)
* The SlabAlloctor was not correctly releasing all its stale memory mappings
  when it was detached. If a SharedGroup was reused to access a database
  following both a call of compact() and a commit() (the latter potentially
  by a different SharedGroup), the stale memory mappings would shadow part
  of the database. This would look like some form of corruption. Specifically
  issues #1092 and #1601 are known to be symptoms of this bug, but issues
  #1506 and #1769 are also likely to be caused by it. Note that even though
  this bug looks like corruption, the database isn't corrupted at all.
  Reopening it by a different SharedGroup will work fine; Only the SharedGroup
  that executed the compact() will have a stale view of the file.
* Check and retry if flock() returns EINTR (issue #1916)
* The slabs (regions of memory used for temporary storage during a write transaction),
  did not correctly track changes in file size, if the allocator was detached, the
  file shrunk and the allocator was re-attached. This scenario can be triggered by
  compact, or by copying/creating a new realm file which is then smaller than the
  old one when you re-attach. The bug led to possible allocation of overlapping
  memory chunks, one of which would then later corrupt the other. To a user this
  would look like file corruption. It is theoretically possibly, but not likely,
  that the corrupted datastructure could be succesfully committed leading to a real
  corruption of the database. The fix is to release all slabs when the allocator
  is detached. Fixes #1898, #1915, #1918, very likely #1337 and possibly #1822.

### Breaking changes

* Removed the 'stealing' variant of export for handover. It was not a great
  idea. It was not being used and required locking which we'd like to avoid.
* S: A concept of log levels was added to `util::Logger`. `util::Logger::log()`
  now takes a log level argument, and new shorthand methods were added
  (`debug()`, `info()`, `warn()`, ...). All loggers now have a `level_threshold`
  property through which they efficiently query for the current log level
  threshold.

### Enhancements

* Allow SharedGroups to pin specific versions for handover
* Reduced the object-size overhead of assertions.
* Fixed a spelling mistake in the message of the `LogicError::wrong_group_state`.

-----------

### Internals

* Non concurrent tests are run on the main process thread. (#1862)
* S: `REALM_QUOTE()` macro moved from `<realm/version.hpp>` to
  `<realm/util/features.h>`. This also fixes a dangling reference to
  `REALM_QUOTE_2()` in `<realm/util/features.h>`.
* Minimize the amount of additional virtual address space used during Commit().
  (#1478)
* New feature in the unit test framework: Ability to specify log level
  threshold for custom intra test logging (`UNITTEST_LOG_LEVEL`).
* Switch from `-O3` to `-Os` to compile OpenSSL: https://github.com/android-ndk/ndk/issues/110

----------------------------------------------


# 1.1.2 Release notes

### Bugfixes

* S: In the network API (namespace `util::network`), do not report an error to
  the application if system calls `read()`, `write()`, or `accept()` fail with
  `EAGAIN` on a socket in nonblocking mode after `poll()` has signalled
  readiness. Instead, go back and wait for `poll()` to signal readiness again.

### Breaking changes

* Sorting order of strings is now according to more common scheme for special
  characters (space, dash, etc), and for letters it's now such that visually
  similiar letters (that is, those that differ only by diacritics, etc) are
  grouped together. (#1639)

-----------

### Internals

* S: New unit tests `Network_ReadWriteLargeAmount` and
  `Network_AsyncReadWriteLargeAmount`.

----------------------------------------------


# 1.1.1 Release notes

### Bugfixes

* Fixed a recently introduced crash bug on indexed columns (#1869)
* Implement `TableViewBase`'s copy-assignment operator to prevent link errors when it is used.
* No longer assert on a "!cfg.session_initiator" in SlabAlloc::attach_file(). This makes issue
  #1784 go away, but also removes an option to detect and flag if the ".lock" file is deleted
  while a SharedGroup is attached to the file. Please note: Removal of the ".lock" file while
  the database is attached may lead to corruption of the database.

### Enhancements

* Improve performance of opening Realm files and making commits when using
  external writelogs by eliminating some unneeded `fsync()`s.

----------------------------------------------

# 1.1.0 Release notes

### Bugfixes

* Fix for #1846: If an exception is thrown from SlabAlloc::attach_file(), it
  forgot to unlock a mutex protecting the shared memory mapping. In cases
  where the last reference to the memory mapping goes out of scope, it would
  cause the assert "Destruction of mutex in use". Fix is to use unique_lock
  to ensure the mutex is unlocked before destruction.
* Fix a crash when `Table::set_string_unique()` is called but the underlying
  column is actually a StringEnumColumn.
* Fix an assertion failure when combining a `Query` with no conditions with another `Query`.

### Breaking changes

* S: Type of completion handler arguments changed from `const H&` to `H` for all
  asynchronous operations offered by the networking API (namespace
  `util::network`).
* S: `util::network::deadline_timer::async_wait()` no longer declared `noexcept`
  (it never should have been).

### Enhancements

* Strictly enforce not allowing search indexes to be created on unsupported column types.
* S: Event loop API reworked to more closely align with the `util::network` API,
  and to better provide for multiple alternative implementations (not considered
  breaking because the event loop API was not yet in use).
* S: Bugs fixed in the POSIX based implementation (not listed under bug fixes
  because the event loop API was not yet in use).
* S: A new Apple CoreFoundation implementation of event loop API was added.
* S: Movable completion handler objects are no longer copied by the networking
  API (namespace `util::network`).

-----------

### Internals

* Upgrade build scripts to build as C++14 by default.
* Corrected two usages of undefined REALM_PLATFORM_ANDROID to REALM_ANDROID.
  This correctly enables Android log output on termination and allows using
  robust mutexes on Android platforms. (#1834)


----------------------------------------------

# 1.0.2 Release notes

### Internals

* This is functionally the same as 1.0.1. For Xamarin we now do a specialized
  cocoa build with only iOS support and without bitcode.

----------------------------------------------

# 1.0.1 Release notes

### Bugfixes

* Fix a situation where a failure during SharedGroup::open() could cause stale
  memory mappings to become accessible for later:
  In case one of the following exceptions are thrown from SharedGroup::open():
  - "Bad or incompatible history type",
  - LogicError::mixed_durability,
  - LogicError::mixed_history_type,
  - "File format version deosn't match: "
  - "Encrypted interprocess sharing is currently unsupported"
  Then:
  a) In a single process setting a later attempt to open the file would
     hit the assert "!cfg.session_initiator" reported in issue #1782.
  b) In a multiprocess setting, another process would be allowed to run
     compact(), but the current process would retain its mapping of the
     old file and attempt to reuse those mappings when a new SharedGroup
     is opened, which would likely lead to a crash later. In that case, the
     !cfg.session_initiator would not be triggered.
  May fix issue #1782.

**Note: This is a hotfix release built on top of 1.0.0

----------------------------------------------

# 1.0.0 Release notes

### Bugfixes

* Fixed move_last_over() replacing null values for binary columns in the moved
  row with zero-length values.

### Enhancements

* File operations would previously throw `std::runtime_error` for error cases without a
  specialized exception. They now throw `AccessError` instead and include path information.

-----------

### Internals

* Fixed an error in Query_Sort_And_Requery_Untyped_Monkey2 test which would cause
  this test to fail sometimes.

----------------------------------------------

# 0.100.4 Release notes

### Bugfixes

* Fix queries over multiple levels of backlinks to work when the tables involved have
  their backlink columns at different indices.

### Breaking changes

* Reverting the breaking changes wrongly introduced by 0.100.3, so that
  this release does NOT have breaking changes with respect to 0.100.2


----------------------------------------------

# 0.100.3 Release notes (This is a faulty release and should not be used)

### Bugfixes

* Fix initialization of read-only Groups which are sharing file mappings with
  other read-only Groups for the same path.
* Fix TableView::clear() to work in imperative mode (issue #1803, #827)
* Fixed issue with Timestamps before the UNIX epoch not being read correctly in
  the `TransactLogParser`. Rollbacks and advances with such Timestamps would
  throw a `BadTransactLog` exception. (#1802)

### Breaking changes

* Search indexes no longer support strings with lengths greater than
  `Table::max_indexed_string_length`. If you try to add a string with a longer length
  (through the Table interface), then a `realm::LogicError` will be thrown with type
  `string_too_long_for_index`. Calling `Table::add_search_index()` will now return a
  boolean value indicating whether or not the index could be created on the column. If
  the column contains strings that exceed the maximum allowed length, then
  `Table::add_search_index()` will return false and the index will not be created, but the data
  in the underlying column will remain unaffected. This is so that bindings can attempt to
  create a search index on a column without knowing the lengths of the strings in the column.
  Realm will continue to operate as before on any search index that already stores strings longer
  than the maximum allowed length meaning that this change is not file breaking (no upgrade is
  required). However, as stated above, any new strings that exceed the maximum length will
  not be allowed into a search index, to insert long strings just turn off the search index
  (although this could be left up to the user).

### Enhancements

* Distinct is now supported for columns without a search index. Bindings no longer
  need to ensure that a column has a search index before calling distinct. (#1739)

-----------

### Internals

* Upgrading to OpenSSL 1.0.1t.

----------------------------------------------

# 0.100.2 Release notes

### Bugfixes

* Fix handing over an out of sync TableView that depends on a deleted link list or
  row so that it doesn't remain perpetually out of sync (#1770).
* Fix a use-after-free when using a column which was added to an existing table
  with rows in the same transaction as it was added, which resulted in the
  automatic migration from DateTime to Timestamp crashing with a stack overflow
  in some circumstances.

----------------------------------------------

# 0.100.1 Release notes

### Bugfixes:

* Fix for: The commit logs were not properly unmapped and closed when a SharedGroup
  was closed. If one thread closed and reopened a SharedGroup which was the sole
  session participant at the time it was closed, while a different SharedGroup opened
  and closed the database in between, the first SharedGroup could end up reusing it's
  memory mappings for the commit logs, while the later accesses through a different
  SharedGroup would operate on a different set of files. This could cause inconsistency
  between the commit log and the database. In turn, this could lead to crashes during
  advance_read(), promote_to_write() and possibly commit_and_continue_as_read().
  Worse, It could also silently lead to accessors pointing to wrong objects which might
  later open for changes to the database that would be percieved as corrupting. (#1762)
* Fix for: When commitlogs change in size, all readers (and writers) must update their
  memory mmapings accordingly. The old mechanism was based on comparing the size of
  the log file with the previous size and remapping if they differ. Unfortunately, this
  is not good enough, as the commitlog may first be shrunk, then expanded back to the
  original size and in this case, the existing mechanism will not trigger remapping.
  Without remapping in such situations, POSIX considers accesses to the part of the
  mapping corresponding to deleted/added sections of the file to be undefined. Consequences
  of this bug could be crashes in advance_read(), promote_to_write() or
  commit_and_continue_as_read(). Conceivably it could also cause wrong accessor updates
  leading to accessors pointing to wrong database objects. This, in turn, could lead
  to what would be percieved as database corruption. (#1764)
* S: Assertion was sometimes dereferencing a dangling pointer in
  `util::network::buffered_input_stream::read_oper<H>::recycle_and_execute()`.

### Enhancements:

* S: `util::bind_ptr<>` extended with capability to adopt and release naked
  pointers.
* The `SharedGroup` constructor now takes an optional callback function so bindings can
  be notified when a Realm is upgraded. (#1740)

----------------------------------------------

# 0.100.0 Release notes

### Bugfixes:

* Fix of #1605 (LinkView destruction/creation should be thread-safe) and most
  likely also #1566 (crash below LinkListColumn::discard_child_accessors...) and
  possibly also #1164 (crash in SharedGroup destructor on OS X).
* Copying a `Query` restricted by a `TableView` will now avoid creating a dangling
  reference to the restricting view if the query owns the view. Dangling references
  may still occur if the `Query` does not own the restricting `TableView`.
* Fixed #1747 (valgrind report of unitialized variable).
* Fixed issue with creation of `ArrayIntNull` with certain default values that would
  result in an all-null array. (Pull request #1721)

### API breaking changes:

* The return value for LangBindHelper::get_linklist_ptr() and the argument
  to LangBindHelper::unbind_linklist_ptr has changed from being a 'LinkView*'
  into a 'const LinkViewRef&'.
* Fixed a bug, where handing over a TableView based on a Query restricted
  by another TableView would fail to propagate synchronization status correctly
  (issue #1698)
* Fixed TableViews that represent backlinks to track the same row, even if that row
  moves within its table. (Issue #1710)
* Fixed incorrect semantics when comparing a LinkList column with a Row using a
  query expression. (Issue #1713)
* Fixed TableViews that represent backlinks to not assert beneath `sync_if_needed` when
  the target row has been deleted.
* `TableView::depends_on_deleted_linklist` is now `TableView::depends_on_deleted_object`,
  and will also return true if the target row of a `TableView` that represents backlinks
  is deleted. (Issue #1710)
* New nanosecond precision `Timestamp` data and column type replace our current `DateTime`
  data and column type. (Issue #1476)
* Notice: Due to the new `Timestamp` data and column type a file upgrade will take place.
  Read-only Realm files in apps will have to be updated manually.

### Enhancements:

* TableView can now report whether its rows are guaranteed to be in table order. (Issue #1712)
* `Query::sync_view_if_needed()` allows for bringing a query's restricting view into sync with
  its underlying data source.

-----------

### Internals:

* Opening a Realm file which already has a management directory no longer throws
  and catches an exception.
* The r-value constructor for StringData has been removed because StringIndex does not
  store any data. This prevents incorrect usage which can lead to strange results.

----------------------------------------------

# 0.99.0 Release notes

### Breaking changes:

* Lock file (`foo.realm.lock`) format bumped.
* Moved all supporting files (all files except the .realm file) into a
  separate ".management" subdirectory.

### Bugfixes:

* S: Misbehavior of empty asynchronous write in POSIX networking API.
* S: Access dangling pointer while handling canceled asynchronous accept
  in POSIX networking API.
* Changed group operator== to take table names into account.

### Enhancements:

* Multiple shared groups now share the read-only memory-mapping of
  the database. This significantly lowers pressure on virtual memory
  in multithreaded scenarios. Fixes issue #1477.
* Added emulation of robust mutexes on platforms which do not
  provide the full posix API for it. This prevents a situation
  where a crash in one process holding the lock, would leave
  the database locked. Fixes #1429
* Added support for queries that traverse backlinks. Fixes #776.
* Improve the performance of advance_read() over transations that inserted rows
  when there are live TableViews.
* The query expression API now supports equality comparisons between
  `Columns<Link>` and row accessors. This allows for link equality
  comparisons involving backlinks, and those that traverse multiple
  levels of links.

* S: Adding `util::network::buffered_input_stream::reset()`.

-----------

### Internals:

* Disabled unittest Shared_RobustAgainstDeathDuringWrite on Linux, as
  it could run forever.
* Fixed a few compiler warnings
* Disabled unittest Shared_WaitForChange again, as it can still run forever
* New features in the unit test framework: Ability to log to a file (one for
  each test thread) (`UNITTEST_LOG_TO_FILES`), and an option to abort on first
  failed check (`UNITTEST_ABORT_ON_FAILURE`). Additionally, logging
  (`util::Logger`) is now directly available to each unit test.
* New failure simulation features: Ability to prime for random triggering.

* S: New unit tests: `Network_CancelEmptyWrite`, `Network_ThrowFromHandlers`.

----------------------------------------------

# 0.98.4 Release notes

### Bugfixes:

* Copying a `Query` restricted by a `TableView` will now avoid creating a dangling
  reference to the restricting view if the query owns the view. Dangling references
  may still occur if the `Query` does not own the restricting `TableView`. (#1741)

### Enhancements:

* `Query::sync_view_if_needed()` allows for bringing a query's restricting view into sync with
  its underlying data source. (#1742)

**Note: This is a hotfix release built on top of 0.98.3. The above fixes are
        not present in version 0.99**

----------------------------------------------

# 0.98.3 Release notes

### Bugfixes:

* Fixed TableViews that represent backlinks to not assert beneath `sync_if_needed` when
  the target row has been deleted. (Issue #1723)

**Note: This is a hotfix release built on top of 0.98.2. The above fixes are
        not present in version 0.99**

----------------------------------------------

# 0.98.2 Release notes

### Bugfixes:

* Fixed TableViews that represent backlinks to track the same row, even if that row
  moves within its table. (Issue #1710)
* Fixed incorrect semantics when comparing a LinkList column with a Row using a
  query expression. (Issue #1713)

### API breaking changes:

* `TableView::depends_on_deleted_linklist` is now `TableView::depends_on_deleted_object`,
  and will also return true if the target row of a `TableView` that represents backlinks
  is deleted. (Issue #1710)

### Enhancements:

* TableView can now report whether its rows are guaranteed to be in table order. (Issue #1712)

**Note: This is a hotfix release built on top of 0.98.1. The above fixes are
        not present in version 0.99

----------------------------------------------

# 0.98.1 Release notes

### Bugfixes:

* Fixed a bug, where handing over a TableView based on a Query restricted
  by another TableView would fail to propagate synchronization status correctly
  (issue #1698)

**Note: This is a hotfix release. The above bugfix is not present
        in version 0.99

----------------------------------------------

# 0.98.0 Release notes

### Enhancements:

* Added support for queries that traverse backlinks. Fixes #776. See #1598.
* The query expression API now supports equality comparisons between
  `Columns<Link>` and row accessors. This allows for link equality
  comparisons involving backlinks, and those that traverse multiple
  levels of links. See #1609.

### Bugfixes:

* Fix a crash that occurred after moving a `Query` that owned a `TableView`.
  See #1672.

**NOTE: This is a hotfix release which is built on top of [0.97.4].**

-----------------------------------------------

# 0.97.4 Release notes

### Bugfixes:

* #1498: A crash during opening of a Realm could lead to Realm files
  which could not later be read. The symptom would be a realm file with zeroes
  in the end but on streaming form (which requires a footer at the end of the
  file instead). See issue #1638.
* Linked tables were not updated properly when calling erase with num_rows = 0
  which could be triggered by rolling back a call to insert with num_rows = 0.
  See issue #1652.
* `TableView`s created by `Table::get_backlink_view` are now correctly handled by
  `TableView`'s move assignment operator. Previously they would crash when used.
  See issue #1641.

**NOTE: This is a hotfix release which is built on top of [0.97.3].**

----------------------------------------------

# 0.97.3 Release notes

### Bugfixes:

* Update table accessors after table move rollback, issue #1551. This
  issue could have caused corruption or crashes when tables are moved
  and then the transaction is rolled back.
* Detach subspec and enumkey accessors when they are removed
  via a transaction (ex rollback). This could cause crashes
  when removing the last column in a table of type link,
  linklist, backlink, subtable, or enumkey. See #1585.
* Handing over a detached row accessor no longer crashes.

**NOTE: This is a hotfix release. The above changes are not present in
versions [0.97.2].**

----------------------------------------------

# 0.97.2 Release notes

### Enhancements:

* Add more information to IncompatibleLockFile.

**NOTE: This is a hotfix release. The above changes are not present in
versions [0.97.1].**

----------------------------------------------

# 0.97.1 Release notes

### Bugfixes:

* Fix an alignment problem which could cause crash when opening a Realm file
  on 32-bit IOS devices. (issue 1558)

**NOTE: This is a hotfix release. The above bugfixes are not present in
versions [0.97.0].**

----------------------------------------------

# 0.97.0 Release notes

### Bugfixes:

* Backlink columns were not being refreshed when the connected link column
  updated it's index in the table (insert/remove/move column). This is now
  fixed. See issue #1499.
* Backlink columns were always inserted at the end of a table, however on a
  transaction rollback in certain cases, backlink columns were removed from
  internal (not the end) indices and the roll back should put them back there.
  This could cause a crash on rollback and was reported in ticket #1502.
* Bumps table version when `Table::set_null()` called.
  `TableView::sync_if_needed()` wouldn't be able to see the version changes
  after `Table::set_null()` was called.
  (https://github.com/realm/realm-java/issues/2366)
* Fix an assertion failure in `Query::apply_patch` when handing over
  certain queries.
* Fix incorrect results from certain handed-over queries.

### API breaking changes:

* Language bindings can now test if a TableView depends on a deleted LinkList
  (detached LinkView) using `bool TableViewBase::depends_deleted_linklist()`.
  See https://github.com/realm/realm-core/issues/1509 and also
  TEST(Query_ReferDeletedLinkView) in test_query.cpp for details.
* `LangBindHelper::advance_read()` and friends no longer take a history
  argument. Access to the history is now gained automatically via
  `Replication::get_history()`. Applications and bindings should simply delete
  the history argument at each call site.
* `SharedGroup::get_current_version()`, `LangBindHelper::get_current_version()`,
  and `Replication::get_current_version()` were all removed. They are not used
  by the Cocoa or Android binding, and `SharedGroup::get_current_version()` was
  never supposed to be public.

### Enhancements:

* Adds support for in-Realm history of changes (`<realm/history.hpp>`), but
  keeps the current history implementation as the default for now
  (`<realm/commit_log.hpp>`).
* New methods `ReadTransaction::get_version()` and
  `WriteTransaction::get_version()` for getting the version of the bound
  snapshot during a transaction.

-----------

### Internals:

* Bumps file format version from 3 to 4 due to support for in-Realm history of
  changes (extra entries in `Group::m_top`). The bump is necessary due to lack
  of forwards compatibility. The changes are backwards compatible, and automatic
  upgrade is implemented.
* Adds checks for consistent use of history types.
* Removes the "server sync mode" flag from the Realm file header. This feature
  is now superseded by the more powerful history type consistency checks. This
  is not considered a file format change, as no released core version will ever
  set the "server sync mode" flag.
* The SharedInfo file format version was bumped due to addition of history type
  information (all concurrent session participants must agree on SharedInfo file
  format version).
* Make it possible to open both file format version 3 and 4 files without
  upgrading. If in-Realm history is required and the current file format version
  is less than 4, upgrade to version 4. Otherwise, if the current file format
  version is less than 3, upgrade to version 3.
* The current file format version is available via
  `Allocator::get_file_format_version()`.
* Set Realm file format to zero (not yet decided) when creating a new empty
  Realm where top-ref is zero. This was done to minimize the number of distinct
  places in the code dealing with file format upgrade logic.
* Check that all session participants agree on target Realm file format for that
  session. File format upgrade required when larger than the actual file format.
* Eliminate a temporary memory mapping of the SharedInfo file during the Realm
  opening process.
* Improved documentation of some of the complicated parts of the Realm opening
  process.
* Introducing `RefOrTagged` value type whan can be used to make it safer to work
  with "tagged integers" in arrays having the "has refs" flag.
* New features in the unit test framework: Ability to specify number of internal
  repetitions of the set of selected tests. Also, progress reporting now
  includes information about which test thread runs which unit test. Also, new
  test introduction macro `NO_CONCUR_TEST()` for those tests that cannot run
  concurrently with other tests, or with other executions of themselves. From
  now on, all unit tests must be able to run multiple times, and must either be
  fully thread safe, or must be introduced with `NO_CONCUR_TEST()`.

----------------------------------------------

# 0.96.2 Release notes

### Bugfixes:

* `Group::TransactAdvancer::move_group_level_table()` was forgetting some of its
  duties (move the table accessor). That has been fixed.
* While generating transaction logs, we didn't always deselect nested
  accessors. For example, when performing a table-level operation, we didn't
  deselect a selected link list. In some cases, it didn't matter, but in others
  it did. The general rule is that an operation on a particular level must
  deselect every accessor at deeper (more nested) levels. This is important for
  the merge logic of the sync mechanism, and for transaction log reversal. This
  has been fixed.
* While reversing transaction logs, group level operations did not terminate the
  preceding section of table level operations. Was fixed.
* Table::clear() issues link nullification instructions for each link that did
  point to a removed row. It did however issue those instructions after the
  clear instruction, which is incorrect, as the links do not exist after the
  clear operation. Was fixed.
* `SharedGroup::compact()` does a sync before renaming to avoid corrupted db
  file after compacting.

### Enhancements:

* Add SharedGroup::get_transact_stage().

### Internals:

* Improve documentation of `Group::move_table()` and `LinkView::move()`.
* Early out from `Group::move_table()` if `from_index == to_index`. This
  behaviour agrees with `LinkView::move()` and is assumed by other parts of
  core, and by the merge logic of the sync mechanism.
* Convert some assertions on arguments of public `Group`, `Table`, and
  `LinkView` methods to throwing checks.
* Align argument naming of `Group::move_table()` and `LinkView::move()`.

----------------------------------------------

# 0.96.1 Release notes

### API breaking changes:

* Important for language bindings: Any method on Query and TableView that
  depends on a deleted LinkView will now return sane return values;
  Query::find() returns npos, Query::find_all() returns empty TableView,
  Query::count() returns 0, TableView::sum() returns 0 (TableView created
  from LinkView::get_sorted_view). So they will no longer throw
  DeletedLinkView or crash. See TEST(Query_ReferDeletedLinkView) in
  test_query.cpp for more info.

### Enhancements:

* Memory errors caused by calls to mmap/mremap will now throw a specific
  AddressSpaceExhausted exception which is a subclass of the previously
  thrown std::runtime_error. This is so that iOS and Android language
  bindings can specifically catch this case and handle it differently
  than the rest of the general std::runtime_errors.
* Doubled the speed of TableView::clear() when parent table has an
  indexed column.

----------------------------------------------

# 0.96.0 Release notes

### Bugfixes:

* Handing over a query that includes an expression node will now avoid
  sharing the expression nodes between `Query` instances. This prevents
  data races that could give incorrect results or crashes.

### Enhancements:

* Subqueries are now supported via `Table::column(size_t, Query)`.
  This allows for queries based on the number of rows in the linked table
  that match the given subquery.

----------------------------------------------

# 0.95.9 Release notes

### Bugfixes:

* Fixed terminate() being called rather than InvalidDatabase being thrown when
  a non-enrypted file that begins with four zero bytes was opened as an
  encrypted file.

----------------------------------------------

# 0.95.8 Release notes

### Bugfixes:

* Fixed error when opening encrypted streaming-form files which would be
  resized on open due to the size not aligning with a chunked mapping section
  boundary.

### API breaking changes:

* Any attempt to execute a query that depends on a LinkList that has been
  deleted from its table will now throw `DeletedLinkView` instead of
  segfaulting. No other changes has been made; you must still verify
  LinkViewRef::is_attached() before calling any methods on a LinkViewRef, as
  usual.

### Enhancements:

* Optimized speed of TableView::clear() on an indexed unordered Table. A clear()
  that before took several minutes with 300000 rows now takes a few seconds.

----------------------------------------------

# 0.95.7 Release notes

### Bugfixes:

* Corrected a bug which caused handover of a query with a restricting
  view to lose the restricting view.

----------------------------------------------

# 0.95.6 Release notes

### Bugfixes:

* Fixed incorrect initialization of TableViews from queries on LinkViews
  resulting in `TableView::is_in_sync()` being incorrect until the first time
  it is brought back into sync.
* Fixed `TableView` aggregate methods to give the correct result when called on
  a table view that at one point had detached refs but has since been synced.
* Fixed another bug in `ColumnBase::build()` which would cause it to produce an
  invalid B+-tree (incorrect number of elements per child in the compact
  form). This is a bug that could have been triggered through proper use of our
  bindings in their current form. In particular, it would have been triggered
  when adding a new attribute to a class that already has a sufficiently large
  number of objects in it (> REALM_MAX_BPNODE_SIZE^2 = 1,000,000).
* Fixed a bug in handover of Queries which use links. The bug was incomplete
  cloning of the underlying data structure. This bug goes unnoticed as long
  as the original datastructure is intact and is only seen if the original
  datastructure is deleted or changed before the handed over query is re-executed

### Enhancements:

* Added support for handing over TableRefs from one thread to another.

-----------

### Internals:

* Add `test_util::to_string()` for convenience. std::to_string() is not
  available via all Android NDK toolchains.
* New operation: ChangeLinkTargets. It replaces all links to one row with
  links to a different row.
* Regular assertions (REALM_ASSERT()) are no longer enabled by default in
  release mode. Note that this is a reversion back to the "natural" state of
  affairs, after a period of having them enabled by default in release mode. The
  Cocoa binding was the primary target when the assertions were enabled a while
  back, and steps were taken to explicitely disable those assertions in the
  Android binding to avoid a performance-wise impact there. It is believed that
  the assertions are no longer needed in the Cocoa binding, but in case they
  are, the right approach, going forward, is to enable them specifically for the
  Cocoa binding. Note that with these changes, the Android binding no longer
  needs to explicitely disable regular assertions in release mode.
* Upgraded Android toolchain to R10E and gcc to 4.9 for all architectures.

----------------------------------------------


# 0.95.5 Release notes

### Bugfixes:

* Fixed Row accessor updating after an unordered `TableView::clear()`.
* Fixed bug in `ColumnBase::build()` which would cause it to produce an invalid
  (too shallow) B+-tree. This is a bug that could have been triggered through
  proper use of our bindings in their current form. In particular, it would have
  been triggered when adding a new attribute to a class that already has a
  sufficiently large number of objects in it (> REALM_MAX_BPNODE_SIZE^2 =
  1,000,000).

### Enhancements:

* New default constructor added to `BasicRowExpr<>`. A default constructed
  instance is in the detached state.

----------------------------------------------

# 0.95.4 Release notes

### Bugfixes:

* Fixed incorrect handling of a race between a commit() and a new thread
  or process opening the database. In debug mode, the race would trigger an
  assert "cfg.session_initiator || !cfg.is_shared", in release mode it could
  conceivably result in undefined behaviour.
* Fixed a segmentation fault in SharedGroup::do_open_2
* Fixed a bug en ringbuffer handling that could cause readers to get a wrong
  top pointer - causing later asserts regarding the size of the top array, or
  asserts reporting mismatch between versions.

### API breaking changes:

* Primary key support has been removed. Instead, new instructions have been
  introduced: SetIntUnique, SetStringUnique. To implement primary keys, callers
  should manually check the PK constraint and then emit these instructions in
  place of the regular SetInt and SetString instructions.

### Enhancements:

* Added TableView::distinct() method. It obeys TableView::sync_if_needed().
  A call to distinct() will first fully populate the TableView and then perform
  a distinct algorithm on that (i.e. it will *not* add a secondary distinct filter
  to any earlier filter applied). See more in TEST(TableView_Distinct) in
  test_table_view.cpp.

-----------

### Internals:

* Changed `Group::remove_table`, `Group::TransactAdvancer::insert_group_level_table`
  and `Group::TransactAdvancer::erase_group_level_table` from _move-last-over_ to
  preserve table ordering within the group.

----------------------------------------------

# 0.95.3 Release notes

### Bugfixes:

* Reverted what was presumably a fix for a race between commit and opening the database (0.95.2).

----------------------------------------------

# 0.95.2 Release notes

### Bugfixes:

* Fixed bug where Query::average() would include the number of nulls in the
  result.
* Presumably fixed a race between commit and opening the database.

### Enhancements:

* Recycle memory allocated for asynchronous operations in the networking
  subsystem (`util::network`).

----------------------------------------------

# 0.95.1 Release notes

### Bugfixes:
* Fixed bug that would give false search results for queries on integer columns
  due to bug in bithacks deep inside Array::find()

### Enhancements:

* Added Table::get_version_counter() exposing the versioning counter for the Table
* Add `TableView::get_query()`.


----------------------------------------------

# 0.95.0 Release notes

### Bugfixes:

* When inserting a new non-nullable Binary column to a table that had
  *existing* rows, then the automatically added values would become null
* Fixed updating TableViews when applying a transaction log with a table clear.
* Fewer things are copied in TableView's move constructor.
* Prevent spurious blocking in networking subsystem (put sockets in nonblocking
  mode even when used with poll/select).
* Fixed the shared group being left in an inconsistent state if the transaction
  log observer threw an exception.
* Fixed issue with table accessors not being updated properly, when link columns
  were changed (e.g. in Group::remove_table, when the table had link columns).

### API breaking changes:

* Use `util::Logger` instead of `std::ostream` for logging during changeset
  replay (`Replication::apply_changeset()`).

### Enhancements:

* Eliminated use of signals in encryption. This also fixes failures related
  to signals on some devices.

-----------

### Internals:

* More checking and throwing of logical errors in `Table::set_binary()` and
  `Table::set_link()`.

----------------------------------------------

# 0.94.4 Release notes

### Bugfixes:

* Fixed crash in find_all()

### Enhancements:

* Queries are no longer limited to 16 levels of grouping.
* New substring operations (ranged insert, erase on values in string columns).
* Adds schema change notification handler API to Group.

-----------

### Internals:

* New operations: Swap rows, move rows, move column, move group level table.
* Changes order of nullify instructions that appeared as a result of erase
  to occur in the transaction log before the erase instruction that caused
  them.
* New utility class: DirScanner.
* New test utility function: quote.
* New assertion macro: REALM_ASSERT_EX, replacing REALM_ASSERT_n macros.


----------------------------------------------

# 0.94.3 Release notes

### Bugfixes:

* Fixed mremap() fallback on Blackberry.

----------------------------------------------

# 0.94.2 Release notes

### Bugfixes:

* Fixed a bug that lead to SharedGroup::compact failing to attach to the newly
  written file.

----------------------------------------------

# 0.94.1 Release notes

### Bugfixes:

* Fixed a bug in SharedGroup::Compact() which could leave the database in an
  inconsistent state.

### Enhancements:

* Queries are no longer limited to 16 levels of grouping.

-----------

### Internals:

* Obsolete YAML-based documentation removed.
* Removed `std::` in front integral types (e.g. `size_t`, `int64_t` etc.)

----------------------------------------------

# 0.94.0 Release notes

### Bugfixes:

* Fixed a crash bug that could be triggered if a Realm is rapidly opened and
  closed and reopened many times on multiple threads. The bug caused the
  internal version information structure to overflow, causing an assert or a
  crash (if assert was disabled).
* The error handling for `pthread_cond_wait()/pthread_cond_timedwait()`
  incorrectly attributed the failure to `pthread_mutex_lock()`.
* The error handling for several File functions incorrectly attributed the
  failure to `open()`.
* Added the bitcode marker to iOS Simulator builds so that bitcode for device
  builds can actually be used.
* Build with bitcode both enabled and disabled for iOS for compatibility with
  Xcode 6.

### API breaking changes:
* None.

### Enhancements:
* Supports finding non-null links (Link + LinkList) in queries, using
  syntax like `Query q = table->column<Link>(col).is_not_null();`
* Comparisons involving unary links on each side of the operator are now
  supported by query_expression.hpp.
* Added version chunk information and failure reason for
  `pthread_mutex_lock()`.
* Termination routines now always display the library's version before the
  error message.
* Automatically clean up stale MemOnly files which were not deleted on close
  due to the process crashing.

-----------

### Internals:

* All calls to `REALM_TERMINATE` or `util::terminate()` now display the
  library's version. It is no longer necessary to include `REALM_VER_CHUNK` in
  calls to those functions.
* Various bug fixes in `util::network`, most notably, asynchronous operations
  that complete immediately can now be canceled.
* Improved documentation in `util::network`.
* Improved exception safety in `util::network`.
* `util::network::socket_base::close()` is now `noexcept`.
* New `util::network::socket_base::cancel()`.
* Added `util::network::deadline_timer` class.
* Breaking: Free-standing functions `util::network::write()` and
  `util::network::async_write()` converted to members of
  `util::network::socket`.


----------------------------------------------

# 0.93.0 Release notes

### Bugfixes:
* Fixed severe bug in Array allocator that could give asserts like
  `Assertion failed: value <= 0xFFFFFFL [26000016, 16777215]`, especially
  for BinaryData columns. This bug could be triggered by using binary data
  blobs with a size in the range between 8M and 16M.
* Fixed assert that could happen in rare cases when calling set_null() on an
  indexed nullable column.
* Fixed all aggregate methods on Table (min, max, etc) that hadn't been
  updated/kept in sync for a long while (null support, return_ndx argument,..).
* Bug in upgrading from version 2 -> 3 (upgrade could be invoked twice for the
  same file if opened from two places simultaneously)
* `Spec` and thereby `Descriptor` and `Table` equality has been fixed. Now
  handles attributes (nullability etc), sub tables, optimized string columns
  and target link types correctly.
* A stackoverflow issue in encrypted_file_mapping. Allocing 4k bytes on the
  stack would cause some random crashes on small stack size configurations.
* Now includes a statically-linked copy of OpenSSL crypto functions rather
  than dynamically linking Androids system OpenSSL to avoid bugs introduced
  by system crypto functions on some devices.
* Added copy constructor to `BasicRow<Table>` to fix a bug that could lead to
  unregistered row accessors being created. This bug is also part of a list of
  blocking issues that prevent the test suite from running when compiled with
  `-fno-elide-constructors`.
* A bug in the `Query` copy constructor has been fixed that could cause asserts
  due to missing capacity extension in one of the object's internal members.
* `Expression` subclasses now update `Query`s current descriptor after setting
  the table. This prevents a null dereference when adding further conditions
  to the query.
* Fixes a crash due to an assert when rolling back a transaction in which a link
  or linklist column was removed.
* A bug in `Query` copying has been fixed. The bug could cause references to
  Tables which should stay under the supervision of one SharedGroup to leak
  to another during handover_export() leading to corruption.
* Query expression operators now give correct results when an argument comes
  from a link.
* Fixed a bug in the way the new memory mapping machinery interacted with
  encryption.
* Query expression comparisons now give correct results when comparing a linked
  column with a column in the base table.
* Fixed assertion failure when TableViewBase::is_row_attached() would return
  false in a debug build.

### API breaking changes:

* A number of methods in the following classes have been renamed to match the
  coding guidelines (lowercase, underscore separation):
    * `Array`, `ArrayBlob`, `ArrayInteger`, `ArrayString`, `BasicArray<T>`;
    * `Column<T, N>`, `IntegerColumn`, `StringColumn`, `StringEnumColumn`;
    * `Group`;
    * `Query`;
    * `StringIndex`.
* `TableView::remove()`, `TableView::remove_last()`, and `TableView::clear()`
  now take an extra argument of type `RemoveMode` which specifies whether rows
  must be removed in a way that does, or does not maintain the order of the
  remaining rows in the underlying table. In any case, the order of remaining
  rows in the table view is maintained. This is listed as an API breaking change
  because the situation before this change was confusing, to say the least. In
  particular, `TableView::clear()` would choose between the ordered and the
  unordered mode based on whether the underlying table had at least one link (or
  link list) column. You are strongly advised to revisit all call sites and
  check that they do the right thing. Note that both bindings (Cocoa and
  Android) are likely to want to use unordered mode everywhere.

### Enhancements:
* Added argument to Table::average() and TableView::average() that returns number
  of values that were used for computing the average
* Full null support everywhere and on all column types. See
  `TEST(Query_NullShowcase)` in `test_query.cpp` in core repo.
* Added `Descriptor::get_link_target()`, for completeness.
* Added extra `allow_file_format_upgrade` argument to `SharedGroup::open()`.
* Modifying `Descriptor` methods now throw `LogicError` when appropriate (rather
  than asserting).
* Allow querying based on the number of rows that a linked list column links to,
  using expressions like `table->column<LinkList>(0).count() > 5`.
* New `util::File::AccessError::get_path()` returns the file system path
  associated with the exception. Note that exception classes
  `util::File::PermissionDenied`, `util::File::NotFound`, `util::File::Exists`,
  and `InvalidDatabase` are subclasses of `util::File::AccessError`.
* Allow queries to include expressions that compute aggregates on columns in linked tables,
  such as `table->column<LinkList>(0).column<Int>(1).sum() >= 1000`.
* Added a check for functioning SEGV signals to fail more gracefully when
  they're broken.

-----------

### Internals:

* Added argument to SharedGroup to prevent automatic file format upgrade. If an
  upgrade is required, the constructor will throw `FileFormatUpgradeRequired`.
* Let `LinkColumn` and `LinkListColumn` adhere to the same nullability interface
  as the rest of the column types.
* The code coverage CI job now builds with the `-fno-elide-constructors` flag,
  which should improve the depth of the coverage analysis. All bugs that were
  blocking the use of this flag have been fixed.
* SharedGroup no longer needs to remap the database file when it grows. This is
  a key requirement for reusing the memory mapping across threads.
* `NOEXCEPT*` macros have been replaced by the C++11 `noexcept` specifier.
* The `REALM_CONSTEXPR` macro has been replaced by the C++11 `constexpr` keyword.
* Removed conditional compilation of null string support.

----------------------------------------------

# 0.92.3 Release notes

### Bugfixes:

* Added the bitcode marker to iOS Simulator builds so that bitcode for device
  builds can actually be used.

**NOTE: This is a hotfix release. The above bugfixes are not present in
versions [0.93.0].**

----------------------------------------------

# 0.92.2 Release notes

### Bugfixes:

* Fixed assertion failure when TableViewBase::is_row_attached() would return
  false in a debug build.
* Fixes a crash due to an assert when rolling back a transaction in which a link
  or linklist column was removed.

**NOTE: This is a hotfix release.**

-----------

### Internals:

* Now built for Apple platforms with the non-beta version of Xcode 7.

----------------------------------------------

# 0.92.1 Release notes

### Bugfixes:

* Reverted prelinking of static libraries on Apple platforms as it caused
  `dynamic_cast<>()` and `typeid()` checks to fail in some scenarios, including
  when sorting by integer or floating point columns.

-----------

### Internals:

* Renamed `Column` to `IntegerColumn` and `TColumn` to `Column`.
* Renamed `AdaptiveStringColumn` to `StringColumn`.
* Several column classes were renamed to follow the `XxxColumn` naming scheme
  (e.g., `ColumnLink` to `LinkColumn`).
* Removed conditional compilation of replication features.
* More information from `InvalidDatabase::what()`.
* Disabled support for the async daemon on iOS and watchOS.

----------------------------------------------

# 0.92.0 Release notes

### Bugfixes:

* The upgraded file format version is written out to disk, eliminating potential
  deadlocks.

### API breaking changes:

* Support for the following deprecated operations on Table has been removed:
  insert_int, insert_string, etc., insert_done, and add_int. To insert a value,
  one must now call insert_empty_row, then set the appropriate values for each
  column.
* Changed `LinkView::move` so that the `new_link_ndx` will be the index at which
  the moved link can be found after the move is completed.

### Enhancements:

* Support for ordered row removal in tables with links. This was done for
  completeness, and because an ordered insertion in tables with links, when
  reversed, becomes an ordered removal. Support for ordered insertion in tables
  with links was added recently because the sync mechanism can produce
  them. Also added a few missing pieces of support for ordered insertion in
  tables with links.

-----------

### Internals:

* Added static `Array::create_array()` for creating non-empty arrays, and extend
  `Array::create()` such that it can create non-empty arrays.
* The creation of the free-space arrays (`Group::m_free_positions`,
  `Group::m_free_lengths`, `Group::m_free_versions`) is now always done by
  `GroupWriter::write_group()`. Previously it was done in various places
  (`Group::attach()`, `Group::commit()`, `Group::reset_free_space_versions()`).
* `Group::reset_free_space_versions()` has been eliminated. These days the Realm
  version is persisted across sessions, so there is no longer any cases where
  version tracking on free-space chunks needs to be reset.
* Free-space arrays (`Group::m_free_positions`, `Group::m_free_lengths`,
  `Group::m_free_versions`) was moved to `GroupWriter`, as they are now only
  needed during `GroupWriter::write_Group()`. This significantly reduces the
  "shallow" memory footprint of `Group`.
* Improved exception safety in `Group::attach()`.
* `Group::commit()` now throws instead of aborting on an assertion if the group
  accessor is detached or if it is used in transactional mode (via
  `SharedGroup`).
* Instruction encoding changed for `InsertEmptyRows` and `EraseRows` (also used
  for `move_last_over()`). The third operand is now `prior_num_rows` (the number
  of rows prior to modification) in all cases. Previously there was a serious
  confusion about this.
* Cleaned up the batch removal of rows used by `TableView`.
* Optimize row removal by skipping cascade mechanism when table has no forward
  link columns.
* Virtual `ColumnBase::insert(row_ndx, num_rows, is_append)` was changed to
  `ColumnBase::insert_rows(row_ndx, num_rows_to_insert,
  prior_num_rows)`. Virtual `ColumnBase::erase(row_ndx, is_last)` was changed to
  `ColumnBase::erase_rows(row_ndx, num_rows_to_erase, prior_num_rows)`. Virtual
  `ColumnBase::move_last_over(row_ndx, last_row_ndx)` was changed to
  `ColumnBase::move_last_row_over(row_ndx, prior_num_rows)`. Function names were
  changed to avoid confusing similarity to the various non-virtual operations of
  the same name on subclasses of `ColumnBase`. `prior_num_rows` is passed
  because if carries more useful information than
  `is_append`/`is_last`. `num_rows_to_erase` was added for consistency.
* On some subclasses of `ColumnBase` a new non-virtual `erase(row_ndx, is_last)`
  was added for practical reasons; an intended overload of `erase(row_ndx)` for
  when you know whether the specified row index is the last one.
* Slight performance improvements in `Array::FindGTE()`.
* Renamed `Array::FindGTE()` to `Array::find_gte()`.

----------------------------------------------

# 0.91.2 Release notes

### Enhancements:

* Added support for building for watchOS.

----------------------------------------------

# 0.91.1 Release notes

### Bugfixes:

* Fixed a bug in SharedGroup::grab_specific_readlock() which would fail to
  grab the specified readlock even though the requested version was available
  in the case where a concurrent cleanup operation had a conflicting request
  for the same (oldest) entry in the ringbuffer.
* Fixed a performance regression in TableView::clear().

### API breaking changes:

* Argument `is_backend` removed from from the public version of
  `SharedGroup::open()`. Fortunately, bindings are not currently calling
  `SharedGroup::open()`.
* `File::resize()` no longer calls `fcntl()` with `F_FULLFSYNC`. This feature
  has been moved to `File::sync()`.

### Enhancements:

* New feature added to disable all forms of 'sync to disk'. This is supposed to
  be used only during unit testing. See header `disable_sync_to_disk.hpp`.
* Added `LinkList.swap()` to swap two members of a link list.
* Added a Query constructor that takes ownership of a TableView.

### Internals:

* On Linux we now call 'sync to disk' after Realm file resizes. Previusly, this
  was only done on Apple platforms.

----------------------------------------------

# 0.91.0 Release notes

### Bugfixes:

* Fixed assertion when tests are run with `REALM_OLDQUERY_FALLBACK` disabled by
  updating Value::import to work with DateTime
* Fix incorrect results when querying for < or <= on ints which requires 64 bits
  to represent with a CPU that supports SSE 4.2.

### API breaking changes:

* Named exception UnreachableVersion replaced by "unspecified" LogicError
  exception.

### Enhancements:

* Generic networking API added.
* Support for transfer/handover of TableViews, Queries, ListViews and Rows
  between SharedGroups in different threads.  Cooperative handover (where boths
  threads participate) is supported for arbitrarily nested TableViews and
  Queries.  Restrictions apply for non-cooperative handover (aka stealing): user
  must ensure that the producing thread does not trigger a modifying operation
  on any of the involved TableViews.  For TableViews the handover can be one of
  *moving*, *copying* or *staying*, reflecting how the actual payload is
  treated.
* Support for non-end row insertion in tables with link and link list columns.
* Improved documentation of functions concerning the initiation and termination
  of transactions.
* Improved exception safety in connection with the initiation and termination of
  transactions.
* Add support for systems where mremap() exists but fails with ENOTSUP.

### Internals:

* New facility for simulating failures, such as system call failures.

----------------------------------------------

# 0.90.0 Release notes

### API breaking changes:

* Merged lr_nulls into master (support for null in String column and bugfix in
String index with 0 bytes). If you want to disable all this again, then #define
REALM_NULL_STRINGS to 0 in features.h. Else API is as follows: Call add_column()
with nullable = true. You can then use realm::null() in place of any
StringData (in Query, Table::find(), get(), set(), etc) for that column. You can
also call Table::is_null(), Table::set_null() and StringData::is_null(). This
upgrades the database file from version 2 to 3 initially the first time a file
is opened. NOTE NOTE NOTE: This may take some time. It rebuilds all indexes.

----------------------------------------------

# 0.89.9 Release notes

### Bugfixes:

* The check for functioning SEGV signals threw the exception only once. Now it
always throws when trying to use encryption.

**NOTE: This is a hotfix release. The above bugfixes are not present in
versions [0.90.0, 0.92.1].**

----------------------------------------------

# 0.89.8 Release notes

### Enhancements:

* Added a check for functioning SEGV signals to fail more gracefully when
  they're broken.

**NOTE: This is a hotfix release. The above bugfixes are not present in
versions [0.90.0, 0.92.1].**

----------------------------------------------

# 0.89.7 Release notes

### Bugfixes:

* A stackoverflow issue in encrypted_file_mapping. Allocing 4k bytes on the
  stack would cause some random crashes on small stack size configurations.
* Now includes a statically-linked copy of OpenSSL crypto functions rather
  than dynamically linking Androids system OpenSSL to avoid bugs introduced
  by system crypto functions on some devices.

**NOTE: This is a hotfix release. The above bugfixes are not present in
versions [0.90.0, 0.92.1].**

----------------------------------------------

# 0.89.6 Release notes

### Bugfixes:

* Fixed durability issue in case of power / system failures on Apple
  platforms. We now use a stronger synchronization (`fcntl(fd, F_FULLFSYNC)`) to
  stable storage when the file is extended.

----------------------------------------------

# 0.89.5 Release notes

### Bugfixes:

* Fixed errors when a changes to a table with an indexed int column are rolled
  back or advanced over.

----------------------------------------------

# 0.89.4 Release notes

### Enhancements:

* Detaching (and thus destroying) row acessors and TableViews can now be done
  safely from any thread.
* Improved performance of Query::find_all() with assertions enabled.

----------------------------------------------

# 0.89.3 Release notes

### Bugfixes:

* Fixed LinkViews containing incorrect data after a write transaction
  containing a table clear is rolled back.
* Fixed errors when a changes to a table with an indexed int column are rolled
  back.

### Enhancements:

* Changes the mmap doubling treshold on mobile devices from 128MB to 16MB.
* SharedGroup::compact() will now throw a runtime_error if called in detached state.
* Make the start index of `ListView::find()` overrideable for finding multiple
  occurances of a given row in a LinkList.
* Add `Group::set_cascade_notification_handler()` to simplify tracking changes
  due to link nullification and cascading deletes.

### Internals:

* Can now be built with encryption enabled on Linux.

----------------------------------------------

# 0.89.1 Release notes

### Bugfixes:

* Fixed bug in "index rebuilding" (would delete the wrong column, causing
  crash). See https://github.com/realm/realm-core/pull/798 ; "Remove the correct
  column when removing search indexes #798"

----------------------------------------------

# 0.89.0 Release notes

### Bugfixes:

* Added a check for NUL bytes in indexed strings to avoid corrupting data
  structures.
* Fixed bug in SharedGroup::compact(). The bug left the freelist outdated in
  some situations, which could cause crash later, if work continued on the same
  shared group. The bug did not affect the data written to the compacted
  database, but later commits working on the outdated freelist might have. The
  fix forces proper (re)initialization of the free list.
* Fixed incorrect results in querying on an indexed string column via a
  LinkView.
* Fixed corruption of indexes when using move_last_over() on rows with
  duplicated values for indexed properties.

### API breaking changes:

* Changed the `tightdb` namespace to `realm`.
* We switched to C++11, and removed functionality that was duplicated from the
  C++11 standard library, including `null_ptr` and `util::UniquePtr`.

### Enhancements:

* Improved performance of advance_read() over commits with string or binary data
  insertions.
* Improved performance sorting TableView and LinkView.
* Added Table::remove_search_index().

----------------------------------------------

# 0.88.6 Release notes

### Bugfixes:

* Fixed bug in Integer index that could make it crash or return bad results
  (String index not affected)

----------------------------------------------

# 0.88.5 Release notes

### Bugfixes:

* Fixed crash when `std::exit()` is called with active encrypted mappings.
* Fixed writing over 4KB of data to an encrypted file with `Group::write()`.
* Fixed crash after making commits which produced over 4KB of writelog data with
  encryption enabled.

-----------

### Internals:

* Switched to using mach exceptions rather than signal() for encrypted mappings
  on Apple platforms.

----------------------------------------------

# 0.88.4 Release notes

### Bugfixes:

* Fixed out-of-bounds reads when using aggregate functions on sorted TableViews.
* Fixed issues with ArrayString that *could* be the cause of all the asserts the
  past few days

-----------

### Internals:

* Many `REALM_ASSERT` invocations replaced by new `REALM_ASSERT_3` macro
  that prints variable/argument contents on failure. It's not implemented
  optimally yet.

----------------------------------------------

# 0.88.3 Release notes

### Enhancements:

* Added emulation of inter-process condition variables for use on platforms which
  do not properly implement them.

----------------------------------------------

# 0.88.2 Release notes

### Bugfixes:

* Fixed duplicated results when querying on a link column with matches at row
  1000+.

-----------

### Internals:

* Added support for Android ARM64

----------------------------------------------

# 0.88.1 Release notes

### Bugfixes:

* Fix encryption on platforms with non-4KB pages.

----------------------------------------------

# 0.88.0 Release notes

### Enhancements:

* SharedGroup::compact() now appends ".tmp_compaction_space" to the database
  name in order to get the name of its temporary workspace file instead of
  ".tmp". It also automatically removes the file in case it already exists
  before compaction.
* Add support for comparing string columns to other string columns in queries.
* `WriteTransaction::has_table()` and `WriteTransaction::rollback()` were
  added. Previously, only implicit rollback was possible with
  `WriteTransaction`.

-----------

### Internals:

* All assert failures now print the release version number.

----------------------------------------------

# 0.87.6 Release notes

### Bugfixes:

* Fixed a crashbug which could cause a reading thread to miss accessor updates
  during advance_read(), if the pending updates consisted of two or more empty
  commits followed by one or more non-empty commit. The left out accessor
  updates could lead to inconsistent datastructures which could presumably later
  cause database corruption.

### Enhancements:

* Adding *null* support to `BinaryData` in exactly the same way as it was added
  to `StringData`.

----------------------------------------------

# 0.87.5 Release notes

### Bugfixes:

* `AdaptiveStringColumn::find_all()` with an index no longer returns its results
  twice.
* Fixed `Table::get_distinct_view()` on tables which have not been modified
  since they were loaded.

### Enhancements:

* Added `SharedGroup::wait_for_change_release()` which allows you to release a
  thread waiting inside wait_for_change() on a specific SharedGroup instance.
* SharedGroup now allows you to coordinate which version of the data a read
  transaction can see. The type `VersionID` represents a specific commit to the
  database. A user can obtain the `VersionID` for the active transaction from
  `SharedGroup::get_version_of_current_transaction()`, and use it to obtain a a
  transaction accessing that same version from another ShareGroup. This is done
  by new forms of `SharedGroup::begin_read()`, `SharedGroup::advance_read()`.
  Operators are provided so that VersionID's may be compared.
* Creating distinct views on integer, datetime, bool and enum columns is now
  possible.
* Add `Table::minimum_datetime()` and `Table::maximum_datetime()`.
* Extending `Table::get_sorted_view()` to support multi-column sorting.

-----------

### Internals:

* Now uses system OpenSSL on Android rather than a statically-linked copy for
  encryption.

----------------------------------------------

# 0.87.4 Release notes

### Bugfixes:

* Fixed a crash when calling get_sorted_view() on an empty LinkList.

----------------------------------------------

# 0.87.3 Release notes

### Bugfixes:

* Fixed bug in String and Integer index where find()/find_all() would return a
  wrong match.
* Fixed the values of `Table::max_string_size`, and `Table::max_binary_size`.
* Fixed a bug occuring when claring a table with a search index on a string
  column with many rows (>1000).

----------------------------------------------

# 0.87.2 Release notes

### Internals:

* Extra assertions in `src/realm/util.file.cpp`.

----------------------------------------------

# 0.87.1 Release notes

### Enhancements:

* Added 'compact' method to SharedGroup for better control of compaction of the
  database file.
* The following constants were added: `Group::max_table_name_length`,
  `Table::max_column_name_length`, `Table::max_string_size`, and
  `Table::max_binary_size`.
* Now throwing on overlong table and column names, and on oversized strings and
  binary data values.
* Fall back to the old query nodes for String as well as int/double/float.
* Log assertions failures to the native logging system on android and Apple.

-----------

### Internals:

* There is now three kinds of runtime assertions, `REALM_ASSERT_DEBUG()`,
  which is retained only in debug-mode builds, `REALM_ASSERT_RELEASE()`, which
  is also retained in release-mode builds, and finally, `REALM_ASSERT()`,
  which is normally only retained in debug-mode builds, but may occasionally be
  retained in release-mode builds too, depending on the specific build
  configuration.
* `REALM_ASSERT()` assertions are now enabled in release-mode builds by
  default.

----------------------------------------------

# 0.87.0 Release notes

### API breaking changes:

* `TransactLogRegistry` is no longer available and must therefore no longer be
  passed to `LangBindHelper::advance_read()` and
  `LangBindHelper::promote_to_write()`.
* The exceptions `PresumablyStaleLockFile` and `LockFileButNoData` are no longer
  thrown from SharedGroup and has been removed from the API.

### Enhancements:

* Support for implicit transactions has been extended to work between multiple
  processes.
* Commitlogs can now be persisted and support server-synchronization

----------------------------------------------

# 0.86.1 Release notes

### Enhancements:

* Added `SharedGroup::get_number_of_versions()` which will report the number of
  distinct versions kept in the database.
* Added support for encryption
* Adding `SharedGroup::wait_for_change()` which allows a thread to sleep until
  the database changes.

----------------------------------------------

# 0.86.0 Release notes

### Bugfixes:

* Fixed a bug where rollback of an empty transaction could cause a crash.

### API breaking changes:

* Table::erase() can no longer be used with unordered tables. Previously it was
  allowed if the specified index was the index of the last row in the table. One
  must now always use Table::move_last_over() with unordered tables. Whether a
  table is ordered or unordered is entirely decided by the way it is used by the
  application, and note that only unordered tables are allowed to contain link
  columns.

### Enhancements:

* TableView::sync_if_needed() now returns a version number. Bindings can compare
  version numbers returned in order to determine if the TableView has changed.
* Added not_equal(), equal(), contains(), begins_with(), ends_with() for String
  columns in the Query expression syntax. They work both case sensitive and
  insensitive. So now you can write 'size_t m =
  table1->column<String>(0).contains("A", true).find();'. Works with Links too.
* Major simplification of ".lock" file handling.  We now leave the ".lock" file
  behind.
* Support added for cascading row removal. See `Descriptor::set_link_type()` for
  details. All previsouly created link columns will effectively have link-type
  'weak'.
* Rows can now be removed via a row accessors (`Row::remove()`,
  `Row::move_last_over()`).
* Speedup of double/float conditions in query expression of a factor ~5 (uses
  fallback to old query nodes for double/float too, instead of only for integer
  conditions).

----------------------------------------------

# 0.85.1 Release notes

### Bugfixes:

* Made Query store a deep copy of user given strings when using the expression
  syntax

----------------------------------------------

# 0.85.0 Release notes

### Bugfixes:

* Fixed a crash when copying a query checking for equality on an indexed string
  column.
* Fixed a stack overflow when too many query conditions were combined with Or().

### API breaking changes:

* Now supports index on Integer, Bool and Date columns; API is the same as for
  String index
* `Query::tableview()` removed as it might lead to wrong results - e.g., when
  sorting a sorted tableview.

### Enhancements:

* Make the durability level settable in the `SharedGroup` constructor and
  `open()` overloads taking a `Replication`.

----------------------------------------------

# 0.84.0 Release notes

### API breaking changes:

* `Table::set_index()` and `Table::has_index()` renamed to
  `Table::add_search_index()` and `Table::has_search_index()` respectively, and
  `Table::add_search_index()` now throws instead of failing in an unspecified
  way.
* `Table::find_pkey_string()` replaces `Table::lookup()` and has slightly
  different semantics. In particular, it now throws instead of failing in an
  unspecified way.

### Enhancements:

* A row accessor (`Row`) can now be evaluated in boolean context to see whether
  it is still attached.
* `Table::try_add_primary_key()` and `Table::remove_primary_key()` added.
* `Table::find_pkey_int()` added, but not yet backed by an integer search index.
* Added method `LangBindHelper::rollback_and_continue_as_read()`. This method
  provides the ability to rollback a write transaction while retaining
  accessors: Accessors which are detached as part of the rolled back write
  transaction are *not* automatically re-attached. Accessors that were attached
  before the write transaction and which are not detached during the write
  transaction will remain attached after the rollback.

-----------

### Internals:

* Introducing `LogicError` as an alternative to expected exceptions. See
  https://github.com/Realm/realm/wiki/Exception-safety-guarantees for more
  on this.
* Various query related speed improvements.
* Test suite now passes ASAN (address sanitizer).

----------------------------------------------

# 0.83.1 Release notes

### Bugfixes:

* Fixed bug where a TableView generated from a LinkViewRef did not update when
  the origin or target table changed.

----------------------------------------------

# 0.83.0 Release notes

### API breaking changes:

* Sorting on LinkView and TableView by multiple columns: Both classes now have
  get_sorted_view() (returns sorted view) and sort() (in-place sort). Both
  methods can take either a single column index as argument (as size_t) or a
  std::vector of columns to sort by multiple columns.
* You can now query a LinkView by calling Query::where(link_view.get()).... See
  TEST(LinkList_QueryOnLinkList) in test_link_query_view.cpp for an example.
  *** IMPORTANT NOTE: Do not call sort() on a LinkView because it does not
  yet support replication ***. get_sorted_view() works fine though.

-----------

### Internals:

* Made common base class for TableView and LinkView with common shared
  functionality (so far just sort).

----------------------------------------------

# 0.82.3 Release notes

### Bugfixes:

* Fixed bug in deep copy of Query, causing the experienced crash at end of scope
  of a Query after add_constraint_to_query() had been executed. The fix may not
  be optimal as it limits nesting of group/end_group to 16 levels, and also
  makes Query take 128 extra bytes of space. Asana task has been made.

* Fixed bug that would cause `Group::commit()` and
  `LangBindHelper::commit_and_continue_as_read()` to fail in the presence of
  search indexes.

* Bugfix: Replication::m_selected_link_list was not cleared. This bug could lead
  to general corruption in cases involving link lists.

----------------------------------------------

# 0.82.2 Release notes

### Internals:

* Query must now be deep-copied using the '=' operator instead of using
  TCopyExpressionTag. Also fixed a bug in this deep-copying.

----------------------------------------------

# 0.82.1 Release notes

### Internals:

* `REALM_MAX_LIST_SIZE` was renamed to `REALM_MAX_BPNODE_SIZE`. `BPNODE`
  stands for "B+-tree node".
* `REALM_MAX_BPNODE_SIZE` now defaults to 1000 in both *release* and *debug*
  mode.

----------------------------------------------

# 0.82.0 Release notes

### API breaking changes:

* `Group::has_table<T>()` removed, because it had awkward and incongruous
  semantics, and could not be efficiently implemented.
* The version of `Group::get_table()`, that takes a name argument, can no longer
  add a table to the group, instead it returns null if there is no table with
  the spaecified name. Addition is now handled by either `Group::add_table()` or
  `Group::get_or_add_table()`.
* `Group::get_table()` and Group::get_table_name() now throw
  `realm::InvalidArgument` if the specified table index is out of range.
* Template version of `Group::get_table()` now throws `DescriptorMismatch` if
  the dynamic type disagrees with the statically specified custom table type.
* `LangBindHelper::bind_table_ref()` was renamed to
  `LangBindHelper::bind_table_ptr()`, and `LangBindHelper::unbind_table_ref()`
  to `LangBindHelper::unbind_table_ptr()`.
* LangBindHelper functions such as get_table() have been updated to reflect the
  changed Group API.
* Exception type `ResourceAllocError` eliminated, as there was no good reason
  for keeping it (it had no clear role).

### Enhancements:

* `Group::find_table()` added as a way of mapping a table name to the index of
  table in the group.
* `Group::add_table()` and `Group::get_or_add_table()` were added.
* `Group::remove_table()` and `Group::rename_table()` were added.
* `WriteTransaction::add_table()` and `WriteTransaction::get_or_add_table()`
  ware added.

----------------------------------------------

# 0.81.0 Release notes

### API breaking changes:

* `Table::get_parent_row_index()` and `Table::get_index_in_group()` together
  replace `Table::get_index_in_parent()`. This was done to avoid a confusing mix
  of distinct concepts.

### Enhancements:

* It's now possible to sort a LinkRef according to a column in the target
  table. Also lets you build a TableView with the sorted result instead. The new
  methods on LinkViewRef are `sort()` and `get_sorted_view()`

----------------------------------------------

# 0.80.5 Release notes

### Bugfixes:

* Fixed bug in `where(&tv)...find()` where it would fail to find a match, if
  usig with a TableView, tv.
* Fixed bug in `Table::remove()` which would leak memory when rows were removed
  and the table was a link target.
* Fixed bug that prevented reuse of free-space when using
  `LangBindHelper::promote_to_write()` and
  `LangBindHelper::commit_and_continue_as_read()`.

### Enhancements:

* Lets you search for null-links, such as
  `table2->column<Link>(col_link2).is_null().find()`. Works for `Link` and
  `LinkedList`.

-----------

### Internals:

* `Group::Verify()` now checks that the managed memory is precisely the
  combination of the recorded free space and the used space reachable from the
  root node.

----------------------------------------------

# 0.80.4 Release notes

### Bugfixes:

* Bug fixed that would always leave a link list accessor (`LinkView`) in a
  corrupt state after a call to `Group::commit()` or
  `LangBindHelper::commit_and_continue_as_read()`, if the link list was modified
  during the ended "transaction", and was non-empty either before, after, or
  both before and after that "transaction".

-----------

### Internals:

* Efficiency of CRUD operations has been greatly improved due to an improvement
  of SlabAlloc). The cost of end-insertion (MyTable::add()), for example, has
  been reduced to less than a 10th of its previous cost.

----------------------------------------------

# 0.80.3 Release notes

### Bugfixes:

* Fixed bug in `Table::add_column()` which would produce a corrupt underlying
  node structure if the table already contains more than N**2 rows, where N is
  `REALM_MAX_LIST_SIZE` (currently set to 1000).
* Fixed bugs in `Table::clear()` which would produce a corrupt underlying node
  structure if the table already contains more than N rows, where N is
  `REALM_MAX_LIST_SIZE` (currently set to 1000).

### Enhancements:

* Lets you find links that point at a specific row index. Works on Query and
  Table. Please see `LinkList_QueryFindLinkTarget` in `test_link_query_view.cpp`
  for usage.

-----------

### Internals:

* Table::Verify() has been heavily extended and now also checks link columns and
  link lists (debug mode only).

----------------------------------------------

# 0.80.2 Release notes

### Bugfixes:

* Fixed bug causing corrupted table accessor when advancing transaction after last regular column is removed from table with remaining hidden backlink columns.
* Fixed replication issue causing too many link list selection instructions to be generated.

----------------------------------------------

# 0.80.1 Release notes

### Bugfixes:

* Fixed several bugs in connection to removal of like-type columns.
* Fixed bug when last regular column is removed from table with remaining hidden backlink columns.
* Fixed bug causing corrupted table accessor when column are added or removed before alink column.

----------------------------------------------

# 0.80.0 Release notes

### Bugfixes:

* Fixed bug in `TableView::clear()` causing crash if its table contained link columns.
* Fixed bug which would corrupt subtable accessors when inserting or removing parent table columns.
* Fixed bug in LinkView::refresh_accessor_tree() causing problems when transaction is advanced after a link list is cleared.
* Fixed bug causing problems when transaction is advanced after a table with link-like columns is cleared.
* Fixed bug in connection with cyclic link relationships.

### Enhancements:

* Added methods `LinkView::remove_target_row()` and `LinkView::remove_all_target_rows()`.
* Support for removing link columns


----------------------------------------------

# 0.23.0 Release notes

### Bugfixes:
* Fixed bug in `TableView::remove()` causing crash or undefined behavior.
* Fixed bugs in `Table::insert_column()` and `Table::remove_column()` causing crash or undefined behaviour.
* Fixed corruption bug when a string enumeration column follows a column with attached search index (index flavor mixup).
* Fixed in `Array::erase()` causing crash in certain row insertion scenarios.
* Fixed bug in enumerated strings column (corruption was possible when inserting default values).
* Fixed bug in `Table::update_from_parent()` causing a crash if `Group::commit()` in presence of generated subtable accessor.
* Fixed several link-related bugs due to confusion about the meaning of `LinkView::m_table`.

### API breaking changes:

* Views can now be be kept synchronized with changes to the tables used to generate the view, use `TableView::sync_if_needed()` to do so. Views are no longer detached when the table they have been generated from are changed. Instead they just go out of sync. See further description in `src/realm/table_view.hpp`.
* is_attached(), detach(), get_table(), and get_index() moved from BasicRow to RowFuncs. This makes it possible to write `link_list[7].get_index()`, for instance.
* `LinkView::get_target_row(link_ndx)` was removed as it is now just a shorthand for the equally efficient `LinkView::get(link_ndx).get_index()`.
* Added missing const versions of `LinkView::get()` and `LinkView::operator[]()`.
* Confusing `LinkView::get_parent()` removed.
* Added `LinkView::get_origin_table()` and `LinkView::get_target_table()`.

### Enhancements:
* Now maximum() and minimum() can return the index of the match and not only the value. Implemented for Query, Table and TableView.
* Now supports links in Table::to_json. Please see unit tests in the new test_json.cpp file
* Now supports DateTime Query::maximum_datetime() and DateTime Query::minimum_datetime()
* Supports links in queries, like `(table1->link(3).column<Int>(0) > 550).find()`.
* Added support for links and lists of links as column types, to enable relationships between tables.
* Adding `Table::get_index_in_parent()` and `Group::get_table(std::size_t table_ndx)`. They were needed for implicit transactions.
* `Table::get_parent_table()` can now also return the index of the column in the parent.
* Support for row accessors.
* Table, row, and descriptor accessors are now generally retained and properly adjusted when the parent table is modified.
* Added methods to find rows by target in TableView and LinkView.


-----------

### Internals:

* TableView now creates and stores a deep-copy of its query, in order for the view to refresh itself

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


Realm Changelog:
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
+ Group::write(std::ostream&) added. This allows general online streaming of Realm databases in memory for the first time.
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
. Class `File` and related exceptions such as `File::AccessError` moved to namespace `realm::util`.
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
  <realm/string_data.hpp>. 'StringData' can be implicitly
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
+ Many methods have now been qualified with REALM_NOEXCEPT.


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
. "C" API moved to its own repository "realm_c".


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
+ New program 'realm-config'. Use it to query about the CFLAGs and/or LDFLAGs to use when linking agains the Realm core library.


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
+ 6 realm::is_a<MyTable>(const Table&) added.
+ 6 realm::unchecked_cast<MyTable>(TableRef) added.
+ 6 realm::checked_cast<MyTable>(TableRef) added.
+ 6 my_table[i].mixed.set_subtable() added.
+ 6 my_table[i].mixed.set_subtable<MySubtable>() added.
+ 6 my_table[i].mixed.is_subtable<MyTable>() added (inefficient, do we want it at all?).
+ 6 my_table[i].mixed.get_subtable<MySubtable>() added (unsafe/unchecked, do we want it at all?).


2012-07-24 (Kristian Spangsege)
----------
+  New macro REALM_DEBUG to control compilation mode.
    The library (including all headers) is no longer affected in any way by the definition status of NDEBUG or _DEBUG.
    When we (Realm) compile the library in debug mode, we must define this macro.
    We will deliver two versions of the library, one for release mode, and one for debug mode.
    If the customer wishes to use the debugging version of the library, he must do two things:
    1) Define REALM_DEBUG in any translation unit that includes a Realm header.
    2) Use the version of the library that is compiled for debug mode (librealm_d.a).
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
