
A Mockup of an alternative Core architecture

- Tables, Rows and Fields have stable/unique identifiers

- There are no accessor updates, no accessor chains and
  no locking related to accessors.

- There are no ref updates, no accessor tree

- In fact there are no accessors, although the Object class
  could be considered a new form of accessor.

- Only two classes, Db and Snapshot, use heap allocation.
  Object class instances are meant to be stack allocated

- No stupid interaction with Java's (or others) garbage collection.

- Parallel access to read-only snapshots are trivially supported
  and lock-free

- Basic types supported: unsigned int, signed int, table identifiers,
  row identifiers (links), float and double.

- The metadata in the database is expressed using real C++ types,
  which makes the code much easier to understand and change. Only
  user data (leaf arrays) are compressed as usual. (structs starting
  with "_" describe data in the file).


Things missing:

- No strings

- No lists

- No Null<>

- Not really following coding style guidelines

- No free space management for the file

- Only a single table

- No access to older versions

- No multithreading or interprocess control


Things *NOT* missing, because they really belong *above* the storage layer:

- No tableviews. Uses std::vector for search results

- No special handover machinery.

- No sophisticated query mechanism

- No "live" objects, results or queries.

- No transaction log or log for RMP.
