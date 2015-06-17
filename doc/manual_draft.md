String and binary data sizes
----------------------------

Realm currently has the following limitations:

- The maximum size of a table and column mames is 63 bytes. Note that this
  applies to the UTF-8 encoding of the name, so the maximum number of characters
  is less in general.

- The maximum size of a string stored in a string column is 16 MiB. Note
  that this applies to the UTF-8 encoding of the string, so the maximum number
  of characters is less in general.

- The maximum size of a binary data object stored in a binary data column is 16
  MiB.



Strings and Unicode
-------------------

Realm has full support for Unicode. Internally it stores all strings using the
UTF-8 character encoding.

Although it is not fully implemented yet, it is the intention that when Realm
performs case insensitive string comparisons, it uses the locale indenpendant
case folding mechanism as described in “Caseless Matching,” in Section 5.18,
Case Mappings of the the Unicode Standard.



Date/time
---------

Realm supports only one date/time data type called 'DateTime'. A DateTime
value represents an absolute point it time, and is represented as an integral
non-negative number of seconds since midnight (UTC), 1 January 1970, not
counting leap seconds.

Please note that it is not currently possible to store a date that preceeds the
year 1970. We are working on an improved date/time representation that does not
have this limitation.



Misc.
-----

Due to memory mapping, read/write errors are not reported by the library in the
usual way. On Linux an application will have to install signal handlers to cach
such errors. The only exception to this rule is Group::write(), which will
report write errors by throwing an exception.
