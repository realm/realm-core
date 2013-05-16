Strings and Unicode:
--------------------

TightDB has full support for Unicode. Internally it stores all strings
using the UTF-8 character encoding.

Although it is not fully implemented yet, it is the intention that when
TightDB performs case insensitive string comparisons, it uses the
locale indenpendant case folding mechanism as described in “Caseless
Matching,” in Section 5.18, Case Mappings of the the Unicode Standard.



Date/time:
----------

TightDB supports only one date/time data type called 'Date'. A Date
value represents an absolute point it time, and is represented as an
integral non-negative number of seconds since midnight (UTC), 1
January 1970, not counting leap seconds.

Please note that it is not currently possible to store a date that
preceeds the year 1970. We are working on an improved date/time
representation that does not have this limitation.
