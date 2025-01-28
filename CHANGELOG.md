# 14.13.5 Release notes

### Enhancements
* None.

### Fixed
* Committing a subscription set prematurely released a read lock, which may have caused a BadVersion exception with an error like `Unable to lock version XX as it does not exist or has been cleaned up` while changing subscriptions. ([PR #8068](https://github.com/realm/realm-core/pull/8068), since v14.12.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* None.

----------------------------------------------

# 14.13.4 Release notes

### Enhancements
* None.

### Fixed
* If you open a synchronized realm and it needs to refresh its access token, and that refresh operation doesn't complete before the realm is closed, the lifetime of the sync session will be extended until the refresh is complete. If you open the realm again before that refresh is complete, then you'll have two sync sessions both try to start synchronizing the realm when the refreshes do complete, and that will crash the process with a MultipleSyncAgents exception. ([PR #8064](https://github.com/realm/realm-core/pull/8064))

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* None.

----------------------------------------------

# 14.13.3 Release notes

### Enhancements
* None.

### Fixed
* Changing type of primary key column crashes if more than one object ([#8056](https://github.com/realm/realm-core/issues/8056), since v14.13.2)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* None.

----------------------------------------------

# 14.13.2 Release notes

### Enhancements
* None.

### Fixed
* Migrating primary key to a new type without migration function would cause an assertion to fail. ([#8045](https://github.com/realm/realm-core/issues/8045), since v10.0.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* None.

----------------------------------------------

# 14.13.1 Release notes

### Enhancements
* None.

### Fixed
* The events library would attempt to upload backup files created as part of file format upgrades, causing backup copies of those backups to be made, looping until the maximum file name size was reached ([#8040](https://github.com/realm/realm-core/issues/8040), since v11.17.0).

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* None.

----------------------------------------------

# 14.13.0 Release notes

### Enhancements
* None.

### Fixed
* Having a query with a number of predicates ORed together may result in a crash on some platforms (strict weak ordering check failing on iphone) ([#8028](https://github.com/realm/realm-core/issues/8028), since v14.6.0)
* None.

### Breaking changes
* Removed http 301/308 redirection support from app services operations provided by App. It is assumed that the SDK's http implementation will handle http redirects instead. ([PR #7996](https://github.com/realm/realm-core/pull/7996))

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* None.

----------------------------------------------

# 14.12.1 Release notes

### Enhancements
* None.

### Fixed
* Using an empty KeyPath in C API would result in no filtering being done ([#7805](https://github.com/realm/realm-core/issues/7805), since v13.24.0)
* Filtering notifications with backlink columns as last element could sometimes give wrong results ([#7530](https://github.com/realm/realm-core/issues/7530), since v11.1.0)
* Fix crash during client app shutdown when Logger log level is set higher than Info. ([#7969](https://github.com/realm/realm-core/issues/7969), since v13.23.3)
* If File::rw_lock() fails to open a file the exception message does not contain the filename ([#7999](https://github.com/realm/realm-core/issues/7999), since v6.0.21)
* Fallback to hashed filename will fail if length of basename is between 240 and 250 ([#8007](https://github.com/realm/realm-core/issues/8007), since v10.0.0)
* Swift API misuse within a callback from core would result in an internal unreachable error rather than the exception being propagated properly ([#7836](https://github.com/realm/realm-core/issues/7836)).

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* Update TestAppSession to allow scope-based usage for restarting the local app resources. ([PR #7672](https://github.com/realm/realm-core/pull/7672))

----------------------------------------------

# 14.12.0 Release notes

### Enhancements
* Improve sync bootstrap performance by reducing the number of table selections in the replication logs for embedded objects. ([#7945](https://github.com/realm/realm-core/issues/7945))
* Released a read lock which was pinned for the duration of a mutable subscription even after commit. This frees resources earlier, and may improve performance of sync bootstraps where the starting state is large. ([#7946](https://github.com/realm/realm-core/issues/7946))
* Client reset cycle detection now checks if the previous recovery attempt was made by the same core version, and if not attempts recovery again ([PR #7944](https://github.com/realm/realm-core/pull/7944)).
* Updated bundled OpenSSL version to 3.3.1. (PR [#7947](https://github.com/realm/realm-core/pull/7947))

### Fixed
* Fixed an "invalid column key" exception when using a RQL "BETWEEN" query on an int or timestamp property across links. ([#7935](https://github.com/realm/realm-core/issues/7935), since v14.10.1)
* Fixed conflict resolution bug related to ArrayErase and Clear instructions, which could sometimes cause an "Invalid prior_size" exception to prevent synchronization ([#7893](https://github.com/realm/realm-core/issues/7893), since v14.8.0).
* Fixed bug which would prevent eventual consistency during conflict resolution. Affected clients would experience data divergence and potentially consistency errors as a result. ([PR #7955](https://github.com/realm/realm-core/pull/7955), since v14.8.0)
* Fixed issues loading the native Realm libraries on Linux ARMv7 systems when they linked against our bundled OpenSSL resulting in errors like `unexpected reloc type 0x03`. ([#7947](https://github.com/realm/realm-core/issues/7947), since v14.1.0)
* `Realm::convert()` would sometimes incorrectly throw an exception claiming that there were unuploaded local changes when the source Realm is a synchronized Realm ([#7966](https://github.com/realm/realm-core/issues/7966), since v10.7.0).

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* Reverted the bfd linker override in the Linux-armv7 toolchain file because the upstream OpenSSL issue it was working around was resolved.

----------------------------------------------

# 14.11.2 Release notes

### Enhancements
* Sync log statements now include the app services connection id in their prefix (e.g `Connection[1:<connection id>] Session[1]: log message`) to make correlating sync activity to server logs easier during troubleshooting ((PR #7849)[https://github.com/realm/realm-core/pull/7849]).

### Fixed
* Sync client may report duplicate compensating write errors ([#7708](https://github.com/realm/realm-core/issues/7708), since v14.8.0).
* String serialization of timestamps with a sufficiently large timestamp value could overflow an int causing undefined behavior, causing potentially bad values for the month/day/year values in stringified dates. ([PR #7934](https://github.com/realm/realm-core/pull/7934)).

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* Fix emscripten build and add emscripten debug/release compile tasks to evergreen. ([PR #7916](https://github.com/realm/realm-core/pull/7916))
* Subscription set state change notifications now work in a multiprocess-compatible manner ([PR #7862](https://github.com/realm/realm-core/pull/7862)).

----------------------------------------------

# 14.11.1 Release notes

### Enhancements
* None.

### Fixed
* App subscription callback was getting fired before the user profile was retrieved on login, leading to an empty user profile when using the callback. ([#7889](https://github.com/realm/realm-core/issues/7889), since v14.7.0)

### Breaking changes
* The websocket error codes `websocket_client_too_old`, `websocket_client_too_new`, and `websocket_protocol_mismatch` along with their C API constants were removed. These corresponded to errors the legacy C++ server could have sent, but the baas sync server never did. Any platform networking implementations that surfaced these errors can report a `websocket_fatal_error` instead if an unknown error occurs during the websocket handshake. If a client connects that is too old or too new, it will finish the websocket handshake and then receive an in-band sync `ERROR` message that will be handled by the sync error handler. [PR #7917](https://github.com/realm/realm-core/pull/7917)

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* None.

----------------------------------------------

# 14.11.0 Release notes

### Enhancements
* On Windows devices Device Sync will additionally look up SSL certificates in the Windows Trusted Root Certification Authorities certificate store when establishing a connection. (PR [#7882](https://github.com/realm/realm-core/pull/7882))
* Updated the return type of `LogCategory::get_category_names()` from `std::vector<const char*>` to `std::vector<std::string_view>`. ([PR #7879](https://github.com/realm/realm-core/pull/7879))
* Added `realm_get_persisted_schema_version` for reading the version of the schema currently stored locally. (PR [#7873](https://github.com/realm/realm-core/pull/7873))
* Added `realm_app_config_get_sync_client_config()` function to the C_API to get the sync_client_config value in `realm_app_config_t` if REALM_APP_SERVICES is enabled. If REALM_APP_SERVICES is not available, `realm_sync_client_config_new()` is available to create a new `sync_client_config_t` to use when initializing the sync manager. ([PR #7891](https://github.com/realm/realm-core/pull/7891))
* Role and permissions changes no longer require a client reset to update the local realm. ([PR #7440](https://github.com/realm/realm-core/pull/7440))

### Fixed
* FLX download progress was only updated when bootstraps completed, making it always be 0 before the first completion and then forever 1. ([PR #7869](https://github.com/realm/realm-core/issues/7869), since v14.10.2)
* Sync client can crash if a session is resumed while the session is being suspended. ([#7860](https://github.com/realm/realm-core/issues/7860), since v12.0.0)
* If a sync session is interrupted by a disconnect or restart while downloading a bootstrap, stale data from the previous bootstrap may be included when the session reconnects and downloads the bootstrap. This can lead to objects stored in the database that do not match the actual state of the server and potentially leading to compensating writes. ([#7827](https://github.com/realm/realm-core/issues/7827), since v12.0.0)
* Fixed unnecessary server roundtrips when there is no download to acknowledge ([#2129](https://jira.mongodb.org/browse/RCORE-2129), since v14.8.0).

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* Protocol version has been updated to v14 to support server intiated bootstraps and role change updates without a client reset. ([PR #7440](https://github.com/realm/realm-core/pull/7440))
* Add support for server initiated bootstraps. ([PR #7440](https://github.com/realm/realm-core/pull/7440))

----------------------------------------------

# 14.10.4 Release notes

### Enhancements

### Fixed
* When a public name is defined on a property, calling `realm::Results::sort()` or `realm::Results::distinct()` with the internal name could throw an error like `Cannot sort on key path 'NAME': property 'PersonObject.NAME' does not exist`. ([realm/realm-js#6779](https://github.com/realm/realm-js/issues/6779), since v12.12.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* Fix a thread sanitizer failure in the "unregister connection change listener during callback" test ([PR #7871](https://github.com/realm/realm-core/pull/7871)).

----------------------------------------------

# 14.10.3 Release notes

### Enhancements
* "Next launch" metadata file actions are now performed in a multi-process safe manner. ([PR #7576](https://github.com/realm/realm-core/pull/7576))

### Fixed
* Fixed a change of mode from Strong to All when removing links from an embedded object that links to a tombstone. This affects sync apps that use embedded objects which have a `Lst<Mixed>` that contains a link to another top level object which has been deleted by another sync client (creating a tombstone locally). In this particular case, the switch would cause any remaining link removals to recursively delete the destination object if there were no other links to it. ([#7828](https://github.com/realm/realm-core/issues/7828), since 14.0.0-beta.0)
* Fixed removing backlinks from the wrong objects if the link came from a nested list, nested dictionary, top-level dictionary, or list of mixed, and the source table had more than 256 objects. This could manifest as `array_backlink.cpp:112: Assertion failed: int64_t(value >> 1) == key.value` when removing an object. ([#7594](https://github.com/realm/realm-core/issues/7594), since v11 for dictionaries)
* Fixed the collapse/rejoin of clusters which contained nested collections with links. This could manifest as `array.cpp:319: Array::move() Assertion failed: begin <= end [2, 1]` when removing an object. ([#7839](https://github.com/realm/realm-core/issues/7839), since the introduction of nested collections in v14.0.0-beta.0)
* wait_for_upload_completion() was inconsistent in how it handled commits which did not produce any changesets to upload. Previously it would sometimes complete immediately if all commits waiting to be uploaded were empty, and at other times it would wait for a server roundtrip. It will now always complete immediately. ([PR #7796](https://github.com/realm/realm-core/pull/7796)).
* `realm_sync_session_handle_error_for_testing` parameter `is_fatal` was flipped changing the expected behavior. ([#7750](https://github.com/realm/realm-core/issues/7750))

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* Fixed `Table::remove_object_recursive` which wouldn't recursively follow links through a single `Mixed` property. This feature is exposed publicly on `Table` but no SDK currently uses it, so this is considered internal. ([#7829](https://github.com/realm/realm-core/issues/7829), likely since the introduction of Mixed)
* Upload completion is now tracked in a multiprocess-compatible manner ([PR #7796](https://github.com/realm/realm-core/pull/7796)).
* The local realm will assume the the client file ident of the fresh realm during a client reset. ([PR #7850](https://github.com/realm/realm-core/pull/7850))
* Building using C++20 on Windows.

----------------------------------------------

# 14.10.2 Release notes

### Enhancements
* None.

### Fixed
* `DB::compact()` on an encrypted Realm without explicitly specifying a new encryption key would only work if the old key happened to be a valid nul-terminated string ([#7842](https://github.com/realm/realm-core/issues/7842), since v14.10.0).
* You could get unexpected merge results when assigning to a nested collection ([#7809](https://github.com/realm/realm-core/issues/7809), since v14.0.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* FLX download estimates are now tracked in a multiprocess-compatible manner ([PR #7780](https://github.com/realm/realm-core/pull/7780)).
* Fixed util::FlatMap so it uses the custom Compare for both ordering and equality checking so you can use util::FlatMap with case-insensitive string keys ([PR #7845](https://github.com/realm/realm-core/pull/7845)).

----------------------------------------------

# 14.10.1 Release notes

### Enhancements
* It is no longer an error to set a base url for an App with a trailing slash - for example, `https://services.cloud.mongodb.com/` instead of `https://services.cloud.mongodb.com` - before this change that would result in a 404 error from the server ([PR #7791](https://github.com/realm/realm-core/pull/7791)).
* Performance has been improved for range queries on integers and timestamps. Requires that you use the "BETWEEN" operation in RQL or the Query::between() method when you build the query. (PR [#7785](https://github.com/realm/realm-core/pull/7785))
* Expose `Obj::add_int()` in the bindgen spec. ([PR #7797](https://github.com/realm/realm-core/pull/7797)).

### Fixed
* Valgrind could report a branch on an uninitialized read when opening something that is not an encrypted Realm file as an encrypted Realm file ([PR #7789](https://github.com/realm/realm-core/pull/7789), since v14.10.0).
* Opening an FLX realm asynchronously may not wait to download all data ([#7720](https://github.com/realm/realm-core/issues/7720), since FLX sync was introduced).
* Clearing a List of Mixed in an upgraded file would lead to an assertion failing ([#7771](https://github.com/realm/realm-core/issues/7771), since 14.0.0)
* Fix compilation with Xcode 16 ([PR #7802](https://github.com/realm/realm-core/pull/7802))

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* Switch to building the Swift package and Cocoa binaries as C++20 ([PR #7802](https://github.com/realm/realm-core/pull/7802)).

----------------------------------------------

# 14.10.0 Release notes

### Enhancements
* Include the originating client reset error in AutoClientResetFailure errors. ([#7761](https://github.com/realm/realm-core/pull/7761))
* Reduce the size of the local transaction log produced by creating objects, improving the performance of insertion-heavy transactions ([PR #7734](https://github.com/realm/realm-core/pull/7734)).

### Fixed
* Fix some client resets (such as migrating to flexible sync) potentially failing with AutoClientResetFailed if a new client reset condition (such as rolling back a flexible sync migration) occurred before the first one completed. ([PR #7542](https://github.com/realm/realm-core/pull/7542), since v13.11.0)
* Encrypted files on Windows had a maximum size of 2GB even on x64 due to internal usage of `off_t`, which is a 32-bit type on 64-bit Windows ([PR #7698](https://github.com/realm/realm-core/pull/7698), since the introduction of encryption support on Windows in v3.0.0).
* The encryption code no longer behaves differently depending on the system page size, which should entirely eliminate a recurring source of bugs related to copying encrypted Realm files between platforms with different page sizes. One known outstanding bug was ([RNET-1141](https://github.com/realm/realm-dotnet/issues/3592)), where opening files on a system with a larger page size than the writing system would attempt to read sections of the file which had never been written to ([PR #7698](https://github.com/realm/realm-core/pull/7698)).
* There were several complicated scenarios which could result in stale reads from encrypted files in multiprocess scenarios. These were very difficult to hit and would typically lead to a crash, either due to an assertion failure or DecryptionFailure being thrown ([PR #7698](https://github.com/realm/realm-core/pull/7698), since v13.9.0).
* Encrypted files have some benign data races where we can memcpy a block of memory while another thread is writing to a limited range of it. It is logically impossible to ever read from that range when this happens, but Thread Sanitizer quite reasonably complains about this. We now perform a slower operations when running with TSan which avoids this benign race ([PR #7698](https://github.com/realm/realm-core/pull/7698)).
* Tokenizing strings for full-text search could pass values outside the range [-1, 255] to `isspace()`, which is undefined behavior ([PR #7698](https://github.com/realm/realm-core/pull/7698), since the introduction of FTS in v13.0.0).

### Breaking changes
* Any `stitch_` prefixed fields in the `BsonDocument` returned from `app::User::custom_data()` are being renamed on the server to have a `baas_` prefix instead ([PR #7769](https://github.com/realm/realm-core/pull/7769)).

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* Removed references to `stitch_` fields in access tokens in sync unit tests ([PR #7769](https://github.com/realm/realm-core/pull/7769)).
* Added back iOS simulator testing to evergreen after Jenkins went away ([PR #7758](https://github.com/realm/realm-core/pull/7758)).
* `realm-trawler -c` did not work on Realm using SyncClient history ([PR #7734](https://github.com/realm/realm-core/pull/7734)).
* `File::Map`'s move constructor and assignment operator left `m_fd` unchanged, which appears to have never actually resulted in problems with how it was used ([PR #7698](https://github.com/realm/realm-core/pull/7698)).

----------------------------------------------

# 14.9.0 Release notes

### Enhancements
* Report the originating error that caused a client reset to occur. ([#6154](https://github.com/realm/realm-core/issues/6154))

### Fixed
* Add a missing file from the bid library to the android blueprint. (PR [#7738](https://github.com/realm/realm-core/pull/7738))
* After compacting, a file upgrade would be triggered. This could cause loss of data if schema mode is SoftResetFile ([#7747](https://github.com/realm/realm-core/issues/7747), since 14.0.0)
* Add missing `REALM_APP_SERVICES` flag to the android blueprint. (PR [#7755](https://github.com/realm/realm-core/pull/7755))

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* Work around a bug in VC++ that resulted in runtime errors when running the tests in a debug build (#[7741](https://github.com/realm/realm-core/issues/7741)).
* Refactor `sync::Session` to eliminate the bind() step of session creation ([#7609](https://github.com/realm/realm-core/pull/7609)).
* Add ScopeExitFail which only calls the handler if exiting the scope via an uncaught exception ([#7609](https://github.com/realm/realm-core/pull/7609)).
* Add the originating error and server requests action that caused a client reset to occur to the client reset tracking metadata storage. ([PR #7649](https://github.com/realm/realm-core/pull/7649))

----------------------------------------------

# 14.8.0 Release notes

### Enhancements
* Add vendor support to the Android Blueprint (PR [#7614](https://github.com/realm/realm-core/pull/7614)).

### Fixed
* A non-streaming progress notifier would not immediately call its callback after registration. Instead you would have to wait for a download message to be received to get your first update - if you were already caught up when you registered the notifier you could end up waiting a long time for the server to deliver a download that would call/expire your notifier ([#7627](https://github.com/realm/realm-core/issues/7627), since v14.6.0).
* Comparing a numeric property with an argument list containing a string would throw. ([#7714](https://github.com/realm/realm-core/issues/7714), since v14.7.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* `util::Thread` no longer has any functionality other than `get_name()` and `set_name()`. Use `std::thread` instead ([PR #7696](https://github.com/realm/realm-core/pull/7696)).

----------------------------------------------

# 14.7.0 Release notes

### Enhancements
* Nested collections have full support for automatic client reset ([PR #7683](https://github.com/realm/realm-core/pull/7683)).

### Fixed
* Having links in a nested collections would leave the file inconsistent if the top object is removed. ([#7657](https://github.com/realm/realm-core/issues/7657), since 14.0.0)
* Accessing App::current_user() from within a notification produced by App:switch_user() (which includes notifications for a newly logged in user) would deadlock ([#7670](https://github.com/realm/realm-core/issues/7670), since v14.6.0).
* Inserting the same typed link to the same key in a dictionary more than once would incorrectly create multiple backlinks to the object. This did not appear to cause any crashes later, but would have affecting explicit backlink count queries (eg: `...@links.@count`) and possibly notifications ([#7676](https://github.com/realm/realm-core/issues/7676) since v14.5.2).
* Automatic client reset recovery would crash when recovering AddInteger instructions on a Mixed property if its type was changed to non-integer ([PR #7683](https://github.com/realm/realm-core/pull/7683), since v11.16.0).

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* (bindgen) Exposing a function `app_user_as_sync_user` to cast the opposite way as `sync_user_as_app_user`. ([PR #7684](https://github.com/realm/realm-core/pull/7684) as a follow-up to [PR #7634](https://github.com/realm/realm-core/pull/7634))
* Protocol version bumped to 13.

----------------------------------------------

# 14.6.2 Release notes

### Enhancements
* None.

### Fixed
* Fixed a bug when running a IN query on a String/Int/UUID/ObjectId property that was indexed. ([7642](https://github.com/realm/realm-core/issues/7642) since v14.6.0)
* Fixed a bug when running a IN query on a integer property where double/float parameters were ignored. ([7642](https://github.com/realm/realm-core/issues/7642) since v14.6.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

----------------------------------------------

# 14.6.1 Release notes

### Enhancements
* None.

### Fixed
* Fix assertion failure or wrong results when evaluating a RQL query with multiple IN conditions on the same property. Applies to non-indexed int/string/ObjectId/UUID properties, or if they were indexed and had > 100 conditions. ((RCORE-2098) [PR #7628](https://github.com/realm/realm-core/pull/7628) since v14.6.0).
* Fixed a bug when running a IN query (or a query of the pattern `x == 1 OR x == 2 OR x == 3`) when evaluating on a string property with an empty string in the search condition. Matches with an empty string would have been evaluated as if searching for a null string instead. ([PR #7628](https://github.com/realm/realm-core/pull/7628) since v10.0.0-beta.9)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* Follow on to ([PR #7300](https://github.com/realm/realm-core/pull/7300)) to allow SDKs to construct a fake user for testing SyncManager::get_user -> App::create_fake_user_for_testing ([PR #7632](https://github.com/realm/realm-core/pull/7632))
* Fix build-apple-device.sh, broken in [#7603](https://github.com/realm/realm-core/pull/7603) ([PR #7640](https://github.com/realm/realm-core/pull/7640)).
* Added a CAPI interface for SDKs to bring their own managed users with core's app services turned off. ([PR #7615](https://github.com/realm/realm-core/pull/7615)).
* Bump the minimum deployment targets on Apple platforms to the minimums supported by Xcode 15 and clean up now unused availability checks. ([PR #7648](https://github.com/realm/realm-core/pull/7648)).
* Build with -Werror on CI to ensure that new warnings don't slip in. ([PR #7646](https://github.com/realm/realm-core/pull/7646))

----------------------------------------------

# 14.6.0 Release notes

### Enhancements
* Add `SyncClientConfig::security_access_group` which allows specifying the access group to use for the sync metadata Realm's encryption key. Setting this is required when sharing the metadata Realm between apps on Apple platforms ([PR #7552](https://github.com/realm/realm-core/pull/7552)).
* When connecting to multiple server apps, a unique encryption key is used for each of the metadata Realms rather than sharing one between them ([#7552](https://github.com/realm/realm-core/pull/7552)).
* Introduce the new `SyncUser` interface which can be implemented by SDKs to use sync without the core App Services implementation (or just for greater control over user behavior in tests). ([PR #7300](https://github.com/realm/realm-core/pull/7300)).
* Improve perfomance of "chained OR equality" queries for UUID/ObjectId types and RQL parsed "IN" queries on string/int/uuid/objectid types. ([.Net #3566](https://github.com/realm/realm-dotnet/issues/3566), since the introduction of these types)
* Introducing `Query::in()` which allows SDKs to take advantage of improved performance when building equality conditions against many constants. ([PR #7582](https://github.com/realm/realm-core/pull/7582))

### Fixed
* SyncUser::all_sessions() included sessions in every state *except* for waiting for access token, which was weirdly inconsistent. It now includes all sessions. ([PR #7300](https://github.com/realm/realm-core/pull/7300)).
* App::all_users() included logged out users only if they were logged out while the App instance existed. It now always includes all logged out users. ([PR #7300](https://github.com/realm/realm-core/pull/7300)).
* Deleting the active user left the active user unset rather than selecting another logged-in user as the active user like logging out and removing users did. ([PR #7300](https://github.com/realm/realm-core/pull/7300)).
* Fixed several issues around encrypted file portability (copying a "bundled" encrypted Realm from one device to another):
  * Fixed `Assertion failed: new_size % (1ULL << m_page_shift) == 0` when opening an encrypted Realm less than 64Mb that was generated on a platform with a different page size than the current platform. ([#7322](https://github.com/realm/realm-core/issues/7322), since v13.17.1)
  * Fixed a `DecryptionFailed` exception thrown when opening a small (<4k of data) Realm generated on a device with a page size of 4k if it was bundled and opened on a device with a larger page size (since the beginning).
  * Fixed an issue during a subsequent open of an encrypted Realm for some rare allocation patterns when the top ref was within ~50 bytes of the end of a page. This could manifest as a DecryptionFailed exception or as an assertion: `encrypted_file_mapping.hpp:183: Assertion failed: local_ndx < m_page_state.size()`. ([#7319](https://github.com/realm/realm-core/issues/7319))
* Non-streaming download sync progress notification is fixed for flexible sync Realms where before it was sometimes stopping to emit values right after the registration of the callback ([#7561](https://github.com/realm/realm-core/issues/7561)).
* Schema initialization could hit an assertion failure if the sync client applied a downloaded changeset while the Realm file was in the process of being opened ([#7041](https://github.com/realm/realm-core/issues/7041), since v11.4.0).
* Queries using query paths on Mixed values returns inconsistent results ([#7587](https://github.com/realm/realm-core/issues/7587), since v14.0.0)
* Enabling 'cancel_waits_on_nonfatal_error' does not cancel waits during location update while offline ([#7527](https://github.com/realm/realm-core/issues/7527), since v13.26.0)

### Breaking changes
* The following things have been renamed or moved as part of moving all of the App Services functionality to the app namespace:
  - SyncUser -> app::User. Note that there is a new, different type named SyncUser.
  - SyncUser::identity -> app::User::user_id. The "identity" word was overloaded to mean two unrelated things, and one has been changed to user_id everywhere.
  - SyncUserSubscriptionToken -> app::UserSubscriptionToken
  - SyncUserProfile -> app::UserProfile
  - App::Config -> AppConfig
  - SyncConfig::MetadataMode -> AppConfig::MetadataMode
  - MetadataMode::NoMetadata -> MetadataMode::InMemory
  - SyncUser::session_for_on_disk_path() -> SyncManager::get_existing_session()
  - SyncUser::all_sessions() -> SyncManager::get_all_sessions_for(User&)
  - SyncManager::immediately_run_file_actions() -> App::immediately_run_file_actions()
  - realm_sync_user_subscription_token -> realm_app_user_subscription_token ([PR #7300](https://github.com/realm/realm-core/pull/7300)).
* The `ClientAppDeallocated` error code no longer exists as this error code can no longer occur. ([PR #7300](https://github.com/realm/realm-core/pull/7300)).
* Some fields have moved from SyncClientConfig to AppConfig. AppConfig now has a SyncClientConfig field rather than it being passed separately to App::get_app(). ([PR #7300](https://github.com/realm/realm-core/pull/7300)).
* Sync user management has been removed from SyncManager. This functionality was already additionally available on App. ([PR #7300](https://github.com/realm/realm-core/pull/7300)).
* AuditConfig now has a base_file_path field which must be set by the SDK rather than inheriting it from the SyncManager. ([PR #7300](https://github.com/realm/realm-core/pull/7300)).
* App::switch_user() no longer returns a user. The return value was always exactly the passed-in user and any code which needs it can just use that. ([PR #7300](https://github.com/realm/realm-core/pull/7300)).
* Non-streaming download progress callback no longer stops reporting values immediately after the registration (if the progress update has happened earlier), but waits for the next batch of data to start syncing to report its progress, since the previous behaviour was not useful (PR [#7561](https://github.com/realm/realm-core/issues/7561)).

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* App metadata storage has been entirely rewritten in preparation for supporting sharing metadata realms between processes. ([PR #7300](https://github.com/realm/realm-core/pull/7300)).
* The metadata disabled mode has been replaced with an in-memory metadata mode which performs similarly and doesn't work weirdly differently from the normal mode. The new mode is intended for testing purposes, but should be suitable for production usage if there is a scenario where metadata persistence is not needed. ([PR #7300](https://github.com/realm/realm-core/pull/7300)).
* The ownership relationship between App and User has changed. User now strongly retains App and App has a weak cache of Users. This means that creating a SyncConfig or opening a Realm will keep the parent App alive, rather than things being in a broken state if the App is deallocated. ([PR #7300](https://github.com/realm/realm-core/pull/7300).
* A new CMake define `REALM_APP_SERVICES` can be used to compile out core's default implmentation of the application services. ([#7268](https://github.com/realm/realm-core/issues/7268))
* Fix a race condition in Promise which could result in an assertion failure if it was destroyed immediately after a `get()` on the Future returned. The problematic scenario only occurred in test code and not in library code ([PR #7602](https://github.com/realm/realm-core/pull/7602)).
* Catch2 is no longer required as a submodule if the `REALM_NO_TESTS` flag is set.
* Sha-2 is no longer required as a submodule on Windows if linking with OpenSSL.
* The Catch2 submodule has moved to `test/external/catch`.
* Fix possible file corruption if using Transaction::copy_to if nested collections are present.
* Evergreen config was udpated so most linux CI testing is done on ubuntu 22.04 on aarch64 with clang 18, upgrading from clang 11 on ubuntu 20.04 mostly on x86_64 ([PR #7475](https://github.com/realm/realm-core/pull/7475)).
* Evergreen config was updated to move most MacOS testing to MacOS 14 on arm64 with Xcode 15.2, updating from Macos 11 on x86_64 with Xcode 13.1 ([PR #7618](https://github.com/realm/realm-core/pull/7618)).

----------------------------------------------

# 14.5.2 Release notes

### Fixed
* Fix compilation errors when using command-line `swift build` ([#7587](https://github.com/realm/realm-core/pull/7587), since v14.5.1).
* Fixed crash when integrating removal of already removed dictionary key ([#7488](https://github.com/realm/realm-core/issues/7488), since v10.0.0).

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* `prior_size` field in Clear instruction is being repurposed as `collection_type` (no protocol changes required)

----------------------------------------------

# 14.5.1 Release notes

### Fixed
* Clearing a nested collection may end with a crash ([#7556](https://github.com/realm/realm-core/issues/7556), since v14.0.0)
* Removing nested collections in Mixed for synced realms throws realm::StaleAccessor ([#7573](https://github.com/realm/realm-core/issues/7573), since v14.0.0)
* Add a privacy manifest to the Swift package ([Swift #8535](https://github.com/realm/realm-swift/issues/8535)).

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

----------------------------------------------
# 14.5.0 Release notes

### Enhancements
* Introduce sync 'progress_estimate' parameter (value from 0.0 to 1.0) for existing sync 'ProgressNotifierCallback' api to report sync progress on current batch of upload/download until completion ([#7450](https://github.com/realm/realm-core/issues/7450))
* Support assigning nested collections via templated API (PR [#7478](https://github.com/realm/realm-core/pull/7478))

### Fixed
* Fix an assertion failure "m_lock_info && m_lock_info->m_file.get_path() == m_filename" that appears to be related to opening a Realm while the file is in the process of being closed on another thread ([Swift #8507](https://github.com/realm/realm-swift/issues/8507)).
* Fixed diverging history due to a bug in the replication code when setting default null values (embedded objects included) ([#7536](https://github.com/realm/realm-core/issues/7536)).
* Version 19.39.33523 of MSVC would crash when compiling for arm64 in release mode ([PR #7533](https://github.com/realm/realm-core/pull/7533)).
* Null pointer exception may be triggered when logging out and async commits callbacks not executed ([#7434](https://github.com/realm/realm-core/issues/7434), since v13.26.0)
* Fixed building for iPhone simulators targeting deployment target 11 ([#7554](https://github.com/realm/realm-core/pull/7554)).

### Breaking changes
* Updated default base URL to be `https://services.cloud.mongodb.com` to support the new domains (was `https://realm.mongodb.com`). ([PR #7534](https://github.com/realm/realm-core/pull/7534))

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* Update libuv used in object store tests from v1.35.0 to v1.48.0 ([PR #7508](https://github.com/realm/realm-core/pull/7508)).
* Made `set_default_logger` nullable in the bindgen spec.yml (PR [#7515](https://github.com/realm/realm-core/pull/7515)).
* Recreating the sync metadata Realm when the encryption key changes is now done in a multi-process safe manner ([PR #7526](https://github.com/realm/realm-core/pull/7526)).
* Added `App::default_base_url()` static accessor for SDKs to retrieve the default base URL from Core. ([PR #7534](https://github.com/realm/realm-core/pull/7534))
* Realm2JSON tool will now correctly upgrade file to current fileformat.
* (bindgen) Remove dependency on the `clang-format` package and rely on a binary provided by the system instead.
* Protocol version bumped to 12 ([#7124](https://github.com/realm/realm-core/issues/7124))

----------------------------------------------

# 14.4.1 Release notes

### Fixed
* Fix pass a thread safe reference to init subscription callback. ([#7497](https://github.com/realm/realm-core/issues/7497), since v13.16.0)

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* Update Catch2 from v3.3.2 to v3.5.3 ([PR #7297](https://github.com/realm/realm-core/pull/7509)).

----------------------------------------------

# 14.4.0 Release notes

### Enhancements
* Nested path included in 'OutOfBoundsø error message ([#7438](https://github.com/realm/realm-core/issues/7438))
* Improve file compaction performance on platforms with page sizes greater than 4k (for example arm64 Apple platforms) for files less than 256 pages in size ([PR #7492](https://github.com/realm/realm-core/pull/7492)).

### Fixed
* Modifying nested collections left the accessor used to make the modification in a stale state, resulting in some unneccesary work being done when making multiple modifications via one accessor ([PR #7470](https://github.com/realm/realm-core/pull/7470), since v14.0.0).
* Fix depth level for nested collection in debug mode, set it to the same level as release ([#7484](https://github.com/realm/realm-core/issues/7484), since v14.0.0).
* Fix opening realm with cached user while offline results in fatal error and session does not retry connection. ([#7349](https://github.com/realm/realm-core/issues/7349), since v13.26.0)
* Fix disallow Sets in ArrayMixed. ([#7502](https://github.com/realm/realm-core/pull/7502), since v14.0.0)

### Breaking changes
* Update C-API log callback signature to include the log category, and `realm_set_log_callback` to not take a `realm_log_level_e`. ([PR #7494](https://github.com/realm/realm-core/pull/7494)

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

----------------------------------------------

# 14.3.0 Release notes

### Enhancements
* Add support to synchronize collections embedded in Mixed properties and other collections (except sets) ([PR #7353](https://github.com/realm/realm-core/pull/7353)).
* Improve performance of change notifications on nested collections somewhat ([PR #7402](https://github.com/realm/realm-core/pull/7402)).
* Improve performance of aggregate operations on Dictionaries of objects, particularly when the dictionaries are empty ([PR #7418](https://github.com/realm/realm-core/pull/7418))
* Added Resumption delay configuration to SyncClientTimeouts. ([PR #7441](https://github.com/realm/realm-core/pull/7441))

### Fixed
* Fixed conflict resolution bug which may result in an crash when the AddInteger instruction on Mixed properties is merged against updates to a non-integer type ([PR #7353](https://github.com/realm/realm-core/pull/7353)).
* Fix a spurious crash related to opening a Realm on background thread while the process was in the middle of exiting ([#7420](https://github.com/realm/realm-core/issues/7420jj))
* Fix a data race in change notification delivery when running at debug log level ([PR #7402](https://github.com/realm/realm-core/pull/7402), since v14.0.0).
* Fix a 10-15% performance regression when reading data from the Realm resulting from Obj being made a non-trivial type ([PR #7402](https://github.com/realm/realm-core/pull/7402), since v14.0.0).

### Breaking changes
* Remove `realm_scheduler_set_default_factory()` and `realm_scheduler_has_default_factory()`, and change the `Scheduler` factory function to a bare function pointer rather than a `UniqueFunction` so that it does not have a non-trivial destructor.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* The CMake option `REALM_MONGODB_ENDPOINT` for running the object-store-tests against baas has been deprecated in favor of an environment variable of the same name ([PR #7423](https://github.com/realm/realm-core/pull/7423)).
* The object-store-tests test suite can now launch baas containers on its own by specifying a `BAASAAS_API_KEY` in the environment ([PR #7423](https://github.com/realm/realm-core/pull/7423)).

----------------------------------------------

# 14.2.0 Release notes

### Enhancements
* Added ability to get current log level via C API (PR [#7419](https://github.com/realm/realm-core/pull/7419))
* Improve performance of object notifiers with complex schemas and very simple changes to process by as much as 20% ([PR #7424](https://github.com/realm/realm-core/pull/7424)).
* Improve performance with very large number of notifiers as much as 75% ([PR #7424](https://github.com/realm/realm-core/pull/7424)).

### Fixed
* Fixed an issue when removing items from a LnkLst that could result in invalidated links becoming visable which could cause crashes or exceptions when accessing those list items later on. This affects sync Realms where another client had previously removed a link in a linklist that has over 1000 links in it, and then further local removals from the same list caused the list to have fewer than 1000 items. ([#7414](https://github.com/realm/realm-core/pull/7414), since v10.0.0)
* Query lists vs lists if the property to check is a path with wildcards would not give correct result. This has for a long time also been a problem for queries with linklist properties in the path ([#7393](https://github.com/realm/realm-core/issues/7393), since v14.0.0)

### Breaking changes
* The fix of querying involving multiple lists may cause tests that depended on the broken beharior to fail.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* Fix several crashes when running the object store benchmarks ([#7403](https://github.com/realm/realm-core/pull/7403)).
* The `dependencies.list` file that defines the realm core library version and the versions of its dependencies is now a YAML file called `dependencies.yml` ([PR #7394](https://github.com/realm/realm-core/pull/7394)).
* Remove SetElementEquals and SetElementLessThan, as Mixed now uses the same comparisons as Set did.

----------------------------------------------

# 14.1.0 Release notes

### Enhancements
* Add support for using aggregate operations on Mixed properties in queries  ([PR #7398](https://github.com/realm/realm-core/pull/7398))

### Fixed
* Fix a performance regression when reading values from Bson containers and revert some breaking changes to the Bson API ([PR #7377](https://github.com/realm/realm-core/pull/7377), since v14.0.0)
* List KVO information was being populated for non-list collections ([PR #7378](https://github.com/realm/realm-core/pull/7378), since v14.0.0)
* Setting a Mixed property to an ObjLink equal to its existing value would remove the existing backlinks and then exit before re-adding them, resulting in later assertion failures due to the backlink state being invalid ([PR #7384](https://github.com/realm/realm-core/pull/7384), since v14.0.0).
* Passing a double as argument for a Query on Decimal128 did not work ([#7386](https://github.com/realm/realm-core/issues/7386), since v14.0.0)
* Opening file with file format 23 in read-only mode will crash ([#7388](https://github.com/realm/realm-core/issues/7388), since v14.0.0)
* Querying a dictionary over a link would sometimes result in out-of-bounds memory reads ([PR #7382](https://github.com/realm/realm-core/pull/7382), since v14.0.0).
* Restore the pre-14.0.0 behavior of missing keys in dictionaries in queries ([PR #7391](https://github.com/realm/realm-core/pull/7391))
* Fix a ~10% performance regression for bulk insertion when using a log level which does not include debug/trace ([PR #7400](https://github.com/realm/realm-core/pull/7400), since v14.0.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.
  You cannot open files with an file format older than v24 in read-only mode.

-----------

### Internals
* The Linux-armv7 cross-compiling toolchain file prefers the bfd linker over gold because of issues linking against OpenSSL 3.2.0.

----------------------------------------------

# 14.0.1 Release notes

### Enhancements
* None.

### Fixed
* Fixed building issues when RealmCore is used as submodule ([#7370](https://github.com/realm/realm-core/pull/7379))
* None.

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

----------------------------------------------

# 14.0.0 Release notes

### Enhancements
* Property keypath in RQL can be substituted with value given as argument. Use '$K\<i\>' in query string. (Issue [#7033](https://github.com/realm/realm-core/issues/7033))
* You can now use query substitution for the @type argument ([#7289](https://github.com/realm/realm-core/issues/7289))

### Fixed
* Fixed crash when adding a collection to an indexed Mixed property ([#7246](https://github.com/realm/realm-core/issues/7246), since 14.0.0-beta.0)
* \[C-API] Fixed the return type of `realm_set_list` and `realm_set_dictionary` to be the newly inserted collection, similarly to the behavior of `list/dictionary_insert_collection` (PR [#7247](https://github.com/realm/realm-core/pull/7247), since 14.0.0-beta.0)
* Throw an exception when trying to insert an embedded object into a list of Mixed ([#7254](https://github.com/realm/realm-core/issues/7254), since 14.0.0-beta.0)
* Queries on dictionaries in Mixed with @keys did not return correct result ([#7255](https://github.com/realm/realm-core/issues/7255), since 14.0.0-beta.0)
* Changes to inner collections will not be reported by notifier on owning collection ([#7270](https://github.com/realm/realm-core/issues/7270), since 14.0.0-beta.0)
* @count/@size not supported for mixed properties ([#7280](https://github.com/realm/realm-core/issues/7280), since v10.0.0)
* Query with @type does not support filtering on collections ([#7281](https://github.com/realm/realm-core/issues/7281), since 14.0.0-beta.0)
* Query involving string operations on nested collections would not work ([#7282](https://github.com/realm/realm-core/issues/7282), since 14.0.0-beta.0)
* Using ANY, ALL or NONE in a query on nested collections would throw an exception ([#7283](https://github.com/realm/realm-core/issues/7283), since 14.0.0-beta.0)
* Results notifications does not report changes to inner collections ([#7335](https://github.com/realm/realm-core/issues/7335), since 14.0.0-beta.0)

### Breaking changes
* If you want to query using @type operation, you must use 'objectlink' to match links to objects. 'object' is reserved for dictionary types.

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10 and onwards. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* to_json API changed according to https://docs.google.com/document/d/1YtJN0sC89LMb4UVcPKFIfwC0Hsi9Vj7sIEP2vHQzVcY/edit?usp=sharing. Links to non-embedded objects will never be followed.

----------------------------------------------

# 14.0.0-beta.0 Release notes

### Enhancements
* Storage of Decimal128 properties has been optimised so that the individual values will take up 0 bits (if all nulls), 32 bits, 64 bits or 128 bits depending on what is needed. (PR [#6111](https://github.com/realm/realm-core/pull/6111))
* You can have a collection embedded in any Mixed property (except Set<Mixed>).
* Querying a specific entry in a collection (in particular 'first and 'last') is supported. (PR [#4269](https://github.com/realm/realm-core/issues/4269))
* Index on list of strings property now supported (PR [#7142](https://github.com/realm/realm-core/pull/7142))
* You can set the threshold levels for trace output on individual categories. (PR [#7004](https://github.com/realm/realm-core/pull/7004))

### Fixed
* Align dictionaries to Lists and Sets when they get cleared. ([#6205](https://github.com/realm/realm-core/issues/6205), since v10.4.0)
* Fixed equality queries on a Mixed property with an index possibly returning the wrong result if values of different types happened to have the same StringIndex hash. ([6407](https://github.com/realm/realm-core/issues/6407) since v11.0.0-beta.5).
* If you have more than 8388606 links pointing to one specific object, the program will crash. ([#6577](https://github.com/realm/realm-core/issues/6577), since v6.0.0)
* Query for NULL value in Dictionary<Mixed> would give wrong results ([6748])(https://github.com/realm/realm-core/issues/6748), since v10.0.0)
* A Realm generated on a non-apple ARM 64 device and copied to another platform (and vice-versa) were non-portable due to a sorting order difference. This impacts strings or binaries that have their first difference at a non-ascii character. These items may not be found in a set, or in an indexed column if the strings had a long common prefix (> 200 characters). ([PR #6670](https://github.com/realm/realm-core/pull/6670), since 2.0.0-rc7 for indexes, and since the introduction of sets in v10.2.0)

### Breaking changes
* Support for upgrading from Realm files produced by RealmCore v5.23.9 or earlier is no longer supported.
* Remove `set_string_compare_method`, only one sort method is now supported which was previously called `STRING_COMPARE_CORE`.
* BinaryData and StringData are now strongly typed for comparisons and queries. This change is especially relevant when querying for a string constant on a Mixed property, as now only strings will be returned. If searching for BinaryData is desired, then that type must be specified by the constant. In RQL the new way to specify a binary constant is to use `mixed = bin('xyz')` or `mixed = binary('xyz')`. ([6407](https://github.com/realm/realm-core/issues/6407)).
* In the C API, `realm_collection_changes_get_num_changes` and `realm_dictionary_get_changes` have got an extra parameter to receive information on the deletion of the entire collection.
* Sorting order of strings has changed to use standard unicode codepoint order instead of grouping similar english letters together. A noticeable change will be from "aAbBzZ" to "ABZabz". ([2573](https://github.com/realm/realm-core/issues/2573))

### Compatibility
* Fileformat: Generates files with format v24. Reads and automatically upgrade from fileformat v10. If you want to upgrade from an earlier file format version you will have to use RealmCore v13.x.y or earlier.

-----------

### Internals
* Refactoring of the StringIndex interface.

----------------------------------------------

# 13.27.0 Release notes

### Enhancements
* Add support in the C API for receiving a notification when sync user state changes. ([#7302](https://github.com/realm/realm-core/pull/7302))
* Allow the query builder to construct >, >=, <, <= queries for string constants. This is a case sensitive lexicographical comparison. Improved performance of RQL (parsed) queries on a non-linked string property using: >, >=, <, <=, operators and fixed behaviour that a null string should be evaulated as less than everything, previously nulls were not matched. ([#3939](https://github.com/realm/realm-core/issues/3939), this is a prerequisite for https://github.com/realm/realm-swift/issues/8008).
* Updated bundled OpenSSL version to 3.2.0 (PR [#7303](https://github.com/realm/realm-core/pull/7303))
* Added `SyncSession::get_file_ident()` so you can trigger a client reset via the BAAS admin API ([PR #7203](https://github.com/realm/realm-core/pull/7203)).
* Audit event scopes containing zero events to save no longer open the audit realm unneccesarily ([PR #7332](https://github.com/realm/realm-core/pull/7332)).
* Added a method to check if a file needs upgrade. ([#7140](https://github.com/realm/realm-core/issues/7140))
* Use `clonefile()` when possible in `File::copy()` on Apple platforms for faster copying. ([PR #7341](https://github.com/realm/realm-core/pull/7341)).

### Fixed
* Fixed queries like `indexed_property == NONE {x}` which mistakenly matched on only x instead of not x. This only applies when an indexed property with equality (==, or IN) matches with `NONE` on a list of one item. If the constant list contained more than one value then it was working correctly. ([realm-js #7862](https://github.com/realm/realm-java/issues/7862), since v12.5.0)
* Uploading the changesets recovered during an automatic client reset recovery may lead to 'Bad server version' errors and a new client reset. ([#7279](https://github.com/realm/realm-core/issues/7279), since v13.24.1)
* Fixed invalid data in error reason string when registering a subscription change notification after the subscription has already failed. ([#6839](https://github.com/realm/realm-core/issues/6839), since v11.8.0)
* Fixed crash in fulltext index using prefix search with no matches ([#7309](https://github.com/realm/realm-core/issues/7309), since v13.18.0)
* Fix compilation and some warnings when building with Xcode 15.3 ([PR #7297](https://github.com/realm/realm-core/pull/7297)).
* util::make_dir_recursive() attempted to create a directory named "\0" if the path did not have a trailing slash ([PR #7329](https://github.com/realm/realm-core/pull/7329)).
* Fixed a crash with `Assertion failed: m_initiated` during sync session startup ([#7074](https://github.com/realm/realm-core/issues/7074), since v10.0.0).
* Fixed a TSAN violation where the user thread could race to read `m_finalized` with the sync event loop ([#6844](https://github.com/realm/realm-core/issues/6844), since v13.15.1)
* Fix a minor race condition when backing up Realm files before a client reset which could have lead to overwriting an existing file. ([PR #7341](https://github.com/realm/realm-core/pull/7341)).

### Breaking changes
* SyncManager no longer supports reconfiguring after calling reset_for_testing(). SyncManager::configure() has been folded into the constructor, and reset_for_testing() has been renamed to tear_down_for_testing(). ([PR #7351](https://github.com/realm/realm-core/pull/7351))

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Expressions in `CHECK()` macros are printed to better; strings are quoted and null strings are printed as NULL instead of no output.
* Refactored version resolution for the `build-apple-device.sh` script. ([#7263](https://github.com/realm/realm-core/pull/7263))
* Remove SyncUser::binding_context() and related things, which were not actually used by any SDKs.
* (bindgen) Upgrade `eslint-config-prettier` & `eslint-plugin-prettier` and add a missing peer dependency on `prettier`.
* The minimum CMake version has changed from 3.15 to 3.22.1. ([#6537](https://github.com/realm/realm-core/issues/6537))
* Update Catch2 to v3.5.2 ([PR #7297](https://github.com/realm/realm-core/pull/7297)).
* The unused `partition` and `user_local_uuid()` fields have been removed from `FileActionMetadata`. ([PR #7341](https://github.com/realm/realm-core/pull/7341)).

----------------------------------------------

# 13.26.0 Release notes

### Enhancements
* Added change_base_url() function to update the App base URL at runtime. (PR [#7173](https://github.com/realm/realm-core/pull/7173))
* Sync Client should use the base_url value provided in App::Config instead of always using stored info after first connection to server. ([#7201](https://github.com/realm/realm-core/issues/7201))

### Fixed
* Handle `EOPNOTSUPP` when using `posix_fallocate()` and fallback to manually consume space. This should enable android users to open a Realm on restrictive filesystems. ([realm-js #6349](https://github.com/realm/realm-js/issues/6349), more prevalent since v13.23.3 with the change to `REALM_HAVE_POSIX_FALLOCATE` but it was also an issue in some platforms before this)
* Application may crash with `incoming_changesets.size() != 0` when a download message is mistaken for a bootstrap message. This can happen if the synchronization session is paused and resumed at a specific time. (PR [#7238](https://github.com/realm/realm-core/pull/7238), since v11.8.0)
* Fixed errors complaining about missing symbols such as `__atomic_is_lock_free` on ARMv7 Linux. (PR [#7257](https://github.com/realm/realm-core/pull/7257))

### Breaking changes
* `App::get_uncached_app(...)` and `App::get_shared_app(...)` have been replaced by `App::get_app(App::CacheMode, ...)`. The App constructor is now enforced to be unusable, use `App::get_app()` instead. ([#7237](https://github.com/realm/realm-core/issues/7237))
* The schema version field in the Realm config had no use for the flexible sync Realms previously. It is now being used for the upcoming Sync Schema Migrations feature. If it was set to a value other than zero, the application will start receiving an error from the server. Data synchronization will be stopped until the Realm is opened with schema version zero. (PR [#7239](https://github.com/realm/realm-core/pull/7239))

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Add support for chunked transfer encoding when using `HTTPParser`.
* Bump the sync protocol to v11. The new protocol version comes with the following changes:
  - JSON_ERROR server message contains the previous schema version
  - Flexible sync BIND client message contains the current schema version
* Add BAAS admin API to create new schema versions (drafts can be used to deploy all changes at once)

----------------------------------------------

# 13.25.1 Release notes

### Fixed
* Bad performance of initial Sync download involving many backlinks ([#7217](https://github.com/realm/realm-core/issues/7217), since v10.0.0)

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

----------------------------------------------

# 13.25.0 Release notes

### Enhancements
* Exceptions thrown during bootstrap application will now be surfaced to the user via the sync error handler rather than terminating the program with an unhandled exception. ([PR #7197](https://github.com/realm/realm-core/pull/7197)).

### Fixed
* Exceptions thrown during bootstrap application could crash the sync client with an `!m_sess` assertion if the bootstrap was being applied during sync::Session activation. ([#7196](https://github.com/realm/realm-core/issues/7196), since v12.0.0).
* If a SyncSession was explicitly resumed via `handle_reconnect()` while it was waiting to auto-resume after a non-fatal error and then another non-fatal error was received, the sync client could crash with a `!m_try_again_activation_timer` assertion. ([#6961](https://github.com/realm/realm-core/issues/6961), since always)

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Dangling links in a Mixed property will now appear as NULL ([#6980](https://github.com/realm/realm-core/issues/6980))

----------------------------------------------

# 13.24.1 Release notes

### Enhancements
* Automatic client reset recovery now preserves the original division of changesets, rather than combining all unsynchronized changes into a single changeset ([PR #7161](https://github.com/realm/realm-core/pull/7161)).
* Automatic client reset recovery now does a better job of recovering changes when changesets were downloaded from the server after the unuploaded local changes were committed. If the local Realm happened to be fully up to date with the server prior to the client reset, automatic recovery should now always produce exactly the same state as if no client reset was involved ([PR #7161](https://github.com/realm/realm-core/pull/7161)).
* [C API] Exposed `realm_app_create_cached` and `realm_app_get_cached` in the C API. (Issue [#7181](https://github.com/realm/realm-core/issues/7181))

### Fixed
* Fixed several causes of "decryption failed" exceptions that could happen when opening multiple encrypted Realm files in the same process while using Apple/linux and storing the Realms on an exFAT file system. ([#7156](https://github.com/realm/realm-core/issues/7156), since the beginning)
* Fixed deadlock which occurred when accessing the current user from the `App` from within a callback from the `User` listener ([#7183](https://github.com/realm/realm-core/issues/7183), since v13.21.0)
* Update existing std exceptions thrown by the Sync Client to use Realm exceptions. ([#6255](https://github.com/realm/realm-core/issues/6255), since v10.2.0)
* Having a class name of length 57 would make client reset crash as a limit of 56 was wrongly enforced (57 is the correct limit) ([#7176](https://github.com/realm/realm-core/issues/7176), since v10.0.0)
* Automatic client reset recovery on flexible sync Realms would apply recovered changes in multiple write transactions, releasing the write lock in between. This had several observable negative effects:
  - Other threads reading from the Realm while a client reset was in progress could observe invalid mid-reset state.
  - Other threads could potentially write in the middle of a client reset, resulting in history diverging from the server.
  - The change notifications produced by client resets were not minimal and would report that some things changed which actually didn't.
  - All pending subscriptions were marked as Superseded and then recreating, resulting in anything waiting for subscriptions to complete firing early.
  ([PR #7161](https://github.com/realm/realm-core/pull/7161), since v12.3.0).
* If the very first open of a flexible sync Realm triggered a client reset, the configuration had an initial subscriptions callback, both before and after reset callbacks, and the initial subscription callback began a read transaction without ending it (which is normally going to be the case), opening the frozen Realm for the after reset callback would trigger a BadVersion exception ([PR #7161](https://github.com/realm/realm-core/pull/7161), since v12.3.0).
* Changesets have wrong timestamps if the local clock lags behind 2015-01-01T00:00:00Z. The sync client now throws an exception if that happens. ([PR #7180](https://github.com/realm/realm-core/pull/7180))
* Allow propagation of user code exceptions happening during client reset callbacks, retrievable via `realm_sync_error_t` in `realm_sync_config_set_error_handler` in the C-API. ([#7098](https://github.com/realm/realm-core/issues/7098), since v11.16.0)

### Breaking changes
* Update existing std exceptions thrown by the Sync Client to use Realm exceptions. ([PR #7141](https://github.com/realm/realm-core/pull/7141/files))

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Added a CombinedTests target that runs tests from object-store, sync, and core together. ([PR #6964](https://github.com/realm/realm-core/pull/6964))
* The version of mongod we test against in evergreen was updated to 7.0.3 ([PR #7187](https://github.com/realm/realm-core/pull/7187)).

----------------------------------------------

# 13.24.0 Release notes

### Enhancements
* Refactored how KeyPathArrays are created in the C-API by adding `realm_create_key_path_array` which allows a SDK to pass in the string representation of the keypath and then let Core calculate the correct TableKey/ColKey pairs instead of doing this on the SDK side. (PR [#7087](https://github.com/realm/realm-core/pull/7087))
* KeyPathArrays can be constructed with a wildcard notation. (Issue [#7125](https://github.com/realm/realm-core/issues/7125))

### Fixed
* `Set::assign_intersection()` on `Set<StringData>`, `Set<BinaryData>`, and `Set<Mixed>` containing string or binary would cause a use-after-free if a set was intersected with itself ([PR #7144](https://github.com/realm/realm-core/pull/7144), since v10.0.0).
* Set algebra on `Set<StringData>` and `Set<BinaryData>` gave incorrect results when used on platforms where `char` is signed ([#7135](https://github.com/realm/realm-core/issues/7135), since v13.23.3).
* Errors encountered while reapplying local changes for client reset recovery on partition-based sync Realms would result in the client reset attempt not being recorded, possibly resulting in an endless loop of attempting and failing to automatically recover the client reset. Flexible sync and errors from the server after completing the local recovery were handled correctly ([PR #7149](https://github.com/realm/realm-core/pull/7149), since v10.2.0).
* During a client reset with recovery when recovering a move or set operation on a `LnkLst` or `Lst<Mixed>` that operated on indices that were not also added in the recovery, links to an object which had been deleted by another client while offline would be recreated by the recovering client. But the objects of these links would only have the primary key populated and all other fields would be default values. Now, instead of creating these zombie objects, the lists being recovered skip such deleted links. ([#7112](https://github.com/realm/realm-core/issues/7112) since the beginning of client reset with recovery in v11.16.0)
* During a client reset recovery a Set of links could be missing items, or an exception could be thrown that prevents recovery ex: "Requested index 1 calling get() on set 'source.collection' when max is 0" ([#7112](https://github.com/realm/realm-core/issues/7112), since the beginning of client reset with recovery in v11.16.0)
* Calling `sort()` or `distinct()` on a `LnkSet` that had unresolved links in it would produce duplicate indices.
* Some fields in `realm_error_t` were uninitialized and contained invalid values when being converted from sync errors - now they should properly be nullptr ([PR #7154](https://github.com/realm/realm-core/pull/7154), since v13.18.0)
* Automatic client reset recovery would duplicate insertions in a list when recovering a write which made an unrecoverable change to a list (i.e. modifying or deleting a pre-existing entry), followed by a subscription change, followed by a write which added an entry to the list ([PR #7155](https://github.com/realm/realm-core/pull/7155), since v12.3.0).

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* None.

----------------------------------------------

# 13.23.4 Release notes

### Enhancements
* None.

### Fixed
* A crash at a very specific time during a DiscardLocal client reset on a FLX Realm could leave subscriptions in an invalid state ([#7110](https://github.com/realm/realm-core/pull/7110), since v12.3.0).
* Fixed an error "Invalid schema change (UPLOAD): cannot process AddColumn instruction for non-existent table" when using automatic client reset with recovery in dev mode to recover schema changes made locally while offline. ([#7042](https://github.com/realm/realm-core/pull/7042) since the server introduced the feature that allows client to redefine the server's schema if the server is in dev mode - fall 2023)
* Fix missing symbol linker error for `Set<ObjKey>` when building with Clang and LTO enabled ([#7121](https://github.com/realm/realm-core/pull/7121), since v12.23.3).

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Add baas-network-tests nightly task for testing sync client operation with non-ideal network conditions. ([PR #6852](https://github.com/realm/realm-core/pull/6852))
* Added non-ideal network conditions and network fault tests to the evergreen nightly test runs. ([PR #7063](https://github.com/realm/realm-core/pull/7063))
* Updated baas tests to run with dev mode disabled by default. ([PR #6852](https://github.com/realm/realm-core/pull/6852))

----------------------------------------------

# 13.23.3 Release notes

### Enhancements
* Cancel asynchronous notifications on subscription state change in case of fatal session errors and when the session becomes inactive. ([PR #7073](https://github.com/realm/realm-core/pull/7073))

### Fixed
* A new design around using a scheduler in C API has enabled the proper release of the user data (See "Breaking Changes") ([#7094](https://github.com/realm/realm-core/issues/7094), since v10.4.0)
* Potential stack-use-after-scope issue on changesets integration with msvc-2019 and mpack code ([PR #6911](https://github.com/realm/realm-core/pull/6911))
* Fix compilation with non-beta Xcode 15. Building for visionOS now requires explicitly specifying `-DCMAKE_XCODE_ATTRIBUTE_SDKROOT=xros` (PR [#7055](https://github.com/realm/realm-core/pull/7055)).
* Fixed FLX subscriptions not being sent to the server if the session was interrupted during bootstrapping. ([#7077](https://github.com/realm/realm-core/issues/7077), since v11.8.0)
* Fixed FLX subscriptions not being sent to the server if an upload message was sent immediately after a subscription was committed but before the sync client checks for new subscriptions via `SubscriptionStore::get_next_pending_version()`. ([#7076](https://github.com/realm/realm-core/issues/7076), since v13.23.1)
* Fixed application crash with 'KeyNotFound' exception when subscriptions are marked complete after a client reset. ([#7090](https://github.com/realm/realm-core/issues/7090), since v12.3.0)

### Breaking changes
* In the C API, the callback function realm_scheduler_notify_func_t now has a second parameter pointing to a work_queue. This pointer has to be kept until the notifications can be run on the proper thread. Then it has to be passed on to realm_scheduler_perform_work. The value returned from realm_scheduler_new (and friends) should be released after it has been used in realm_config_set_scheduler.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* REALM_[ATMU]SAN cmake flags no longer override compilation options and can be combined with Debug|RelWithDebInfo|etc. build types. Rel[ATMU]SAN build type shortcuts are now all slightly optimized debug-based builds with sanitizers. REALM_ASAN now works with msvc (2019/2022) builds. ([PR #6911](https://github.com/realm/realm-core/pull/6911))
* Add support for building against the musl library. ([PR #7067](https://github.com/realm/realm-core/pull/7067))
* Remove ArrayWithFind's ability to use a templated callback parameter. The QueryStateBase consumers now use an index and the array leaf to get the actual value if needed. This allows certain queries such as count() to not do as many lookups to the actual values and results in a small performance gain. Also remove `find_action_pattern()` which was unused for a long time. This reduction in templating throughout the query system produces a small (~100k) binary size reduction. ([#7095](https://github.com/realm/realm-core/pull/7095))
* Rework the implemenatation of the set algrebra functions on Set<T> to reduce the compiled size.
* Rework the internal interface for sync Transformers to simplify it and reduce the compiled size ([PR #7098](https://github.com/realm/realm-core/pull/7098)).

----------------------------------------------

# 13.23.2 Release notes

### Enhancements
* None.

### Fixed
* Deleting an object in an asymmetric table would cause a crash. Likely to solve [#1537](https://github.com/realm/realm-kotlin/issues/1537), since v12.1.0.
* Implement BIO_CTRL_GET_KTLS_SEND and BIO_CTRL_GET_KTLS_RECV as OpenSSL versions < 3.0.4 depend on it.

### Breaking changes
* Added separate enum for callback handler result values in the platform networking C API. ([PR #7015](https://github.com/realm/realm-core/pull/7015))
* Platform networking CAPI now uses different callback types depending on where callback is used. ([PR #7015](https://github.com/realm/realm-core/pull/7015))

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Update JSON library from 3.7.3 to 3.11.2.
* Updated async_write_binary in the Default Socket Provider to properly return a status to the callback handler. ([PR #7015](https://github.com/realm/realm-core/pull/7015))

----------------------------------------------

# 13.23.1 Release notes

### Enhancements
* Empty commits no longer trigger an extra invocation of the sync progress handler reporting the exact same information as the previous invocation ([PR #7031](https://github.com/realm/realm-core/pull/7031)).

### Fixed
* `SyncManager::path_for_realm()` would return `/<path>/filename.realm.realm` if `custom_file_name` was set to `filename.realm` and the file didn't exist. It would correctly return `/<path>/filename.realm` if the file already existed. After this fix `/<path>/filename.realm` is returned in all cases. ([#7038](https://github.com/realm/realm-core/issues/7038))
* Fixed a bug preventing SSL handshake from completing successfuly due to failed hostname verification when linking against BoringSSL. (PR [#7034](https://github.com/realm/realm-core/pull/7034))
* Updating subscriptions did not trigger Realm autorefreshes, sometimes resulting in async refresh hanging until another write was performed by something else ([PR #7031](https://github.com/realm/realm-core/pull/7031)).
* Fix interprocess locking for concurrent realm file access resulting in a interprocess deadlock on FAT32/exFAT filesystems ([PR #6959](https://github.com/realm/realm-core/pull/6959)).

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* None.

----------------------------------------------

# 13.23.0 Release notes

### Enhancements
* Allow collections of non-embedded links in asymmetric objects. ([PR #7003](https://github.com/realm/realm-core/pull/7003))
* Flexible sync API improvements:
  - Erase Subscriptions by class type for C API.
  - `MutableSubscriptionSet::erase(iterator)` now runs in constant time.
  - Introduce `MutableSubscriptionSet::erase_by_id()`.
  - Introduce `MutableSubscriptionSet::erase_by_class_name()`.
  ([PR #7008](https://github.com/realm/realm-core/pull/7008))
* Filtering by user-defined functions (PR [#7020](https://github.com/realm/realm-core/pull/7020))

### Fixed
* Fixed an issue where Android.bp builds would fail with SSL certificate validation errors because we didn't include the trusted CA roots bundle. (PR [#7022](https://github.com/realm/realm-core/pull/7022))

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Update tests to use global logger. ([PR #6917](https://github.com/realm/realm-core/pull/6917))
* TableView::get_num_results_excluded_by_limit() has been removed. Hopefully not used by any SDK.
* bindgen is now part of the release

----------------------------------------------

# 13.22.0 Release notes

### Enhancements
* None.

### Fixed
* Fixed issue with double delete when using the CAPI for timers in platform networking ([#6993](https://github.com/realm/realm-core/issues/6993), since v13.3.0).
* Receiving a write_not_allowed error from the server would have led to a crash. ([#6978](https://github.com/realm/realm-core/issues/6978), since v13.2.0)

### Breaking changes
* Platform Networking CAPI has been updated to provide separate functions (instead of 1) for executing callback handlers depending on purpose ([PR #6994](https://github.com/realm/realm-core/pull/6994)).

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* None.

----------------------------------------------

# 13.21.0 Release notes

### Enhancements
* Allow non-embedded links in asymmetric objects. ([PR #6981](https://github.com/realm/realm-core/pull/6981))

### Fixed
* Logging into a single user using multiple auth providers created a separate SyncUser per auth provider. This mostly worked, but had some quirks:
  - Sync sessions would not necessarily be associated with the specific SyncUser used to create them. As a result, querying a user for its sessions could give incorrect results, and logging one user out could close the wrong sessions.
  - Existing local synchronized Realm files created using version of Realm from August - November 2020 would sometimes not be opened correctly and would instead be redownloaded.
  - Removing one of the SyncUsers would delete all local Realm files for all SyncUsers for that user.
  - Deleting the server-side user via one of the SyncUsers left the other SyncUsers in an invalid state.
  - A SyncUser which was originally created via anonymous login and then linked to an identity would still be treated as an anonymous users and removed entirely on logout.
  ([PR #6837](https://github.com/realm/realm-core/pull/6837), since v10.0.0)
* Reading existing logged-in users on app startup from the sync metadata Realm performed three no-op writes per user on the metadata Realm ([PR #6837](https://github.com/realm/realm-core/pull/6837), since v10.0.0).
* If a user was logged out while an access token refresh was in progress, the refresh completing would mark the user as logged in again and the user would be in an inconsistent state ([PR #6837](https://github.com/realm/realm-core/pull/6837), since v10.0.0).
* If querying over a geospatial dataset that had some objects with a type property set to something other than 'Point' (case insensitive) an exception would have been thrown. Instead of disrupting the query, those objects are now just ignored. ([PR 6989](https://github.com/realm/realm-core/issues/6989), since the introduction of geospatial)
* The Swift package failed to link required libraries when building for macCatalyst.

### Breaking changes
* SyncUser::provider_type() and realm_user_get_auth_provider() have been removed. Users don't have provider types; identities do. `SyncUser::is_anonymous()` is a more correct version of checking if the provider type is anonymous ([PR #6837](https://github.com/realm/realm-core/pull/6837)).
* SyncUser no longer has a `local_identity()`. `identity()` has been guaranteed to be unique per App ever since v10 ([PR #6837](https://github.com/realm/realm-core/pull/6837)).
* SyncUser no longer overrides operator==. Pointer equality should be used to compare sync users ([PR #6837](https://github.com/realm/realm-core/pull/6837)).

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.
* The metadata Realm used to store sync users has had its schema version bumped. It is automatically migrated to the new version on first open. Downgrading will require manually deletion of the metadata Realm and require logging back in.

-----------

### Internals
* Update History Command tool to work with realms with fileformat v23 ([PR #6970](https://github.com/realm/realm-core/pull/6970))
* Don't edit the ObjectStore target to enable the libuv scheduler in tests, just register the factory instead. ([PR #6699](https://github.com/realm/realm-core/pull/6699))
* Removed the core metrics which were unused. ([PR 6990](https://github.com/realm/realm-core/pull/6990))

----------------------------------------------

# 13.20.1 Release notes

### Enhancements
* Throw an exception if `File::unlock` has failed, in order to inform the SDK that we are likely hitting some limitation on the OS filesystem, instead of crashing  the application and use the same file locking logic for all the platforms.([PR #6926](https://github.com/realm/realm-core/pull/6926))

### Fixed
* Fixed crash in slab allocator (Assertion failed: ref + size <= next->first) Many issues like ([#6340](https://github.com/realm/realm-core/issues/6340), since 13.0.0)
* realm/sync/network/websocket_error.hpp was missing from the install package ([PR #6954](https://github.com/realm/realm-core/pull/6954), since v13.18.0).
* DB::get_number_of_versions() will now report the number of versions alive in the realm file. Before it reported the number of versions committed since the oldest live version. Metrics get_num_available_versions() is changed accordingly.
* When using OpenSSL (i.e. on non-Apple platforms) the `TlsHandshakeFailed` error code would never be reported and instead TLS errors would be reported as `SyncConnectFailed` ([PR #6938](https://github.com/realm/realm-core/pull/6938)).
* When using SecureTransport (i.e. on Apple platforms) only some TLS errors were reported as `TlsHandshakeFailed` and most were reported as `SyncConnectFailed` ([PR #6938](https://github.com/realm/realm-core/pull/6938)).
* Sync errors originating from OpenSSL used the error message from the wrong end of the error stack, often resulting in very unhelpful error message ([PR #6938](https://github.com/realm/realm-core/pull/6938)).
* Sending empty UPLOAD messages may lead to 'Bad server version' errors and client reset. ([6966](https://github.com/realm/realm-core/issues/6966), since v11.8.0)
* Fix open async in order to invoke the subscription callback correctly when rerun_on_open is set to true. ([#6937](https://github.com/realm/realm-core/pull/6937), since v13.16.0).

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Add a CI job to validate that the headers in the installation package all build ([PR #6954](https://github.com/realm/realm-core/pull/6954)).
* Fix build of most internal tools, also build them by default. ([PR #6475](https://github.com/realm/realm-core/pull/6475))

----------------------------------------------

# 13.20.0 Release notes

### Enhancements
* Add a distinct error code for timeouts (SyncConnectTimeout) rather than using the same one as for less transient failures ([PR #6932](https://github.com/realm/realm-core/pull/6932)).
* Allow arguments to RQL to be a string representation of a geospatial object for GEOWITHIN queries. This enables SDKs using the CAPI to marshal geo objects to strings. ([PR 6934](https://github.com/realm/realm-core/issues/6934))

### Fixed
* None.

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Add a fake app id to the baas server's schema change history store to prevent server drop optimization from running during integration tests. ([PR #6927](https://github.com/realm/realm-core/pull/6927))

----------------------------------------------

# 13.19.0 Release notes

### Enhancements
* Added support for server log messages that are enabled by sync protocol version 10. Appservices request id will be provided in a server log message in a future server release. ([PR #6476](https://github.com/realm/realm-core/pull/6476))
* A new `ErrorCategory::sync_error` has been added. All errors related to the Sync client, protocol or session will have this category. Note that websocket errors will have both the `websocket_error` and `sync_error` category, similar to `app_error` and `http_error` for failed HTTP requests from the App. ([#6916](https://github.com/realm/realm-core/issues/6916))

### Fixed
* Crash when querying the size of a Object property through a link chain ([#6915](https://github.com/realm/realm-core/issues/6915), since v13.17.2)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Add information about the reason a synchronization session is used for to flexible sync client BIND message. ([PR #6902](https://github.com/realm/realm-core/pull/6902))
* Sync protocol version bumped to 10. ([PR #6902](https://github.com/realm/realm-core/pull/6902))
* Handle badchangeset error when printing changeset contents in debug. ([PR #6921](https://github.com/realm/realm-core/pull/6921))

----------------------------------------------

# 13.18.0 Release notes

### Enhancements
* Full text search supports searching for prefix only. Eg. "description TEXT 'alex*'" ([#6860](https://github.com/realm/realm-core/issues/6860))
* Unknown protocol errors received from the baas server will no longer cause the application to crash if a valid error action is also received. Unknown error actions will be treated as an ApplicationBug error action and will cause sync to fail with an error via the sync error handler. ([PR #6885](https://github.com/realm/realm-core/pull/6885))

### Fixed
* Made binding a `sync::Session` exception safe so if a `MultipleSyncAgents` exception is thrown you can safely tear down the sync client. ([PR #6868](https://github.com/realm/realm-core/pull/6868), since v13.4.1)

### Breaking changes
* The `WebSocketObserver` interface in the sync `SocketProvider` API now takes a `WebSocketError` enum/`std::string_view` for the `websocket_closed_handler()` instead of a `Status`. Implementers of platform networking should make sure all their error handling is expressed in terms of the WebSocketError enum. ([PR #6859](https://github.com/realm/realm-core/pull/6859))
* `Status` no longer holds a `std::error_code` for `SystemError`'s ([PR #6869](https://github.com/realm/realm-core/pull/6869))
* C API no longer has a special type for sync error codes. Instead sync errors codes are converted to `realm_error_t` ([PR #6869](https://github.com/realm/realm-core/pull/6869))
* WebSocket specific error codes are no longer in the ErrorCodes enum or C API. ([PR #6869](https://github.com/realm/realm-core/pull/6869))
* `ProtocolError` is no longer a `std::error_code` enum and is no longer directly exposed by the sync error API ([PR #6869](https://github.com/realm/realm-core/pull/6869))
* The ClientError enum/`std::error_code` in the sync client has been removed in favor of a simplified error set using Status/ErrorCodes ([PR #6846](https://github.com/realm/realm-core/pull/6846)).
* SyncError now contains a Status to hold the error information from the sync client instead of a `std::error_code`/`std::string` ([PR #6824](https://github.com/realm/realm-core/pull/6824)).
* Remove App::Config::local_app_[name|version] parameters. They were not used by the server and were not needed internally also.
* C API: return whether or not an error is present from realm_get_async_error() ([#6897](https://github.com/realm/realm-core/issues/6897))

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Removed some unused files/directories and dogless dependency. ([PR #6884](https://github.com/realm/realm-core/pull/6884))

----------------------------------------------

# 13.17.2 Release notes

### Enhancements
* None.

### Fixed
* Fix failed assertion for unknown app server errors ([#6758](https://github.com/realm/realm-core/issues/6758), since v12.9.0).
* Running a query on @keys in a Dictionary would throw an exception ([#6831](https://github.com/realm/realm-core/issues/6831), since v13.15.1)
* Change JSON selialization format back to follow ISO 8601 - and add output of nanoseconds ([#6855](https://github.com/realm/realm-core/issues/6855), since 13.17.0)
* Testing the size of a collection of links against zero would sometimes fail (sometimes = "difficult to explain"). In particular: ([#6850](https://github.com/realm/realm-core/issues/6850), since v13.15.1)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Timestamp objects can now only be created from a system clock timepoint. ([#6112](https://github.com/realm/realm-core/issues/6112))

----------------------------------------------

# 13.17.1 Release notes

### Enhancements
* None.

### Fixed
* Rare corruption of files on streaming format (often following compact, convert or copying to a new file). ([#6807](https://github.com/realm/realm-core/pull/6807), since v12.12.0)
* Trying to search a full-text indexes created as a result of an additive schema change (i.e. applying the differences between the local schema and a synchronized realm's schema) could have resulted in an IllegalOperation error with the error code `Column has no fulltext index`. ([PR #6823](https://github.com/realm/realm-core/pull/6823), since v13.2.0).
* Sync progress for DOWNLOAD messages from server state was updated wrongly. This may have resulted in an extra round-trip to the server. ([#6827](https://github.com/realm/realm-core/issues/6827), since v12.9.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* `wait_for_upload_completion`/`wait_for_download_completion` internal API was changed to use `Status`'s instead of `std::error_code`. The SDK-facing was already `Status` oriented, so this change should only result in better error messages. ([PR #6796](https://github.com/realm/realm-core/pull/6796))
* Separate local and baas object store tests into separate evergreen tasks and allow custom test specification. ([PR #6805](https://github.com/realm/realm-core/pull/6805))
* Consolidate object store sync util files into test/object-store/util/sync/ directory. ([PR #6789](https://github.com/realm/realm-core/pull/6789))

----------------------------------------------

# 13.17.0 Release notes

### Enhancements
* Sync connection and session reconnect timing/backoff logic has been unified into a single implementation and is now configurable. Previously some errors would cause an hour-long wait before attempting to reconnect, now - by default - the sync client will wait for 1 second before retrying and double the timeout after each subsequent attempt up to 5 minutes, after which a retry will be attempted every 5 minutes. If the cause of the error changes, the backoff will be reset. If the sync client voluntarily disconnects, no backoff will be used. (PR [#6526](https://github.com/realm/realm-core/pull/6526)).
* Add support for the SDK initializing the schema inside a before-client-reset callback ([PR #6780](https://github.com/realm/realm-core/pull/6780)).

### Fixed
* Sync errors included the error message twice ([PR #6774](https://github.com/realm/realm-core/pull/6774), since v13.16.0).
* Fix timestamp representation when serializing to json on different platforms. ([#5451](https://github.com/realm/realm-core/issues/5451)).

### Breaking changes
* Deprecate `Object::obj()` in favour of `Object::get_obj()` in order to provide better cache efficiency and keep `Obj` up to date when writes happened after then object instance is obtained.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Run baas on remote ubuntu host for evergreen object-store-tests. ([PR #6757](https://github.com/realm/realm-core/pull/6757))

----------------------------------------------

# 13.16.1 Release notes

### Enhancements
* None.

### Fixed
* Fix an error during async open and client reset if properties have been added to the schema. This fix applies to PBS to FLX migration if async open is used. ([#6707](https://github.com/realm/realm-core/issues/6707), since v12.3.0)
* Fixed a double move (undefined behaviour) if an async open resulted in an error. ([#6768](https://github.com/realm/realm-core/pull/6768), since v13.16.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* None.

----------------------------------------------

# 13.16.0 Release notes

### Enhancements
* Add visionOS binaries to the Cocoa release package ([PR #6746](https://github.com/realm/realm-core/pull/6746)).
* Added support for running initial subscription callback when opening realm, both for synchronous and asynchronous flows. Sync Config contains `subscription_initializer` and `rerun_init_subscription_on_open` in order to setup the subscription callback, and select whether rerun this the first time the file is opened or not. ([#5962](https://github.com/realm/realm-core/issues/5962))

### Fixed
* None.

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Prebuilt binaries for non-Apple platforms are no longer published as nothing was using them ([PR #6746](https://github.com/realm/realm-core/pull/6746)).
* SystemError exceptions now have a more detailed error message. ([#6739](https://github.com/realm/realm-core/issues/6739))

----------------------------------------------

# 13.15.2 Release notes

### Enhancements
* None.

### Fixed
* A GeoBox is now just a shortcut for the equivilent GeoPolygon. This provides consistent query results and error checking. ([#6703](https://github.com/realm/realm-core/issues/6703), since v13.11.0)
* Fixed several corner cases (eg. around the poles) where invalid points matched a geoWithin query.
* Disallow full text search index for primary key columns. ([#6657](https://github.com/realm/realm-core/issues/6657), since v13.2.0)
* Searching for objects in Results would not always find the requested item. This is particularly the case when the C API is used. ([#6695](https://github.com/realm/realm-core/issues/6695), since v10.0.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Fixed Valgrind error ([#6643](https://github.com/realm/realm-core/issues/6643))

----------------------------------------------

# 13.15.1 Release notes

### Fixed
* Querying with object list arguments does not work. ([#6688](https://github.com/realm/realm-core/issues/6688), since v10.3.3)
* Fix SessionWrapper use-after-free crash when tearing down sessions when using session multiplexing ([#6656](https://github.com/realm/realm-core/issues/6656), since v13.9.3)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

----------------------------------------------

# 13.15.0 Release notes

### Enhancements
* It is now allowed to open old frozen versions with a schema that contains additional classes, but not additional properties. ([PR 6693](https://github.com/realm/realm-core/issues/6693))

### Fixed
* Properties in the frozen _before_ Realm instance in the client reset callbacks may have had properties reordered which could lead to exceptions if accessed. ([PR 6693](https://github.com/realm/realm-core/issues/6693), since v13.11.0)

### Breaking changes
* A new provider called `IdentityProviderAPIKey` replaces both `IdentityProviderUserAPIKey` and `IdentityProviderServerAPIKey` since those two did the same thing. If SDKs wish to keep the old behaviour without requiring users to make code changes, they can adapt both their existing server and user API key providers to use the new core type. ([#5914](https://github.com/realm/realm-core/issues/5914))

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Prebuilt libraries for Apple platforms are now built with Xcode 14.

----------------------------------------------

# 13.14.0 Release notes


### Enhancements
* Support sort/distinct based on values from a dictionary e.g. `TRUEPREDICATE SORT(meta['age'])` (PR [#5311](https://github.com/realm/realm-core/pull/5311))

### Fixed
* Querying with list of objects as argument will always return an empty result. ([#6674](https://github.com/realm/realm-core/pull/6674), since v12.5.0)
* Fixed a potential crash when opening the realm after failing to download a fresh FLX realm during an automatic client reset ([#6494](https://github.com/realm/realm-core/issues/6494), since v12.3.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Added CMake toolchains for cross-targeting Linux x86_64, armv7, and aarch64. (PR [#6559](https://github.com/realm/realm-core/pull/6559))

----------------------------------------------

# 13.13.0 Release notes

### Enhancements
* None.

### Fixed
* Allow numeric substitutions into a geospatial query. Example `location GEOWITHIN geoCircle([$0, $1], $2)`. ([#6662](https://github.com/realm/realm-core/issues/6662))
* Access token refresh for websockets was not updating the location metadata ([#6630](https://github.com/realm/realm-core/issues/6630), since v13.9.3)
* Fix several UBSan failures which did not appear to result in functional bugs ([#6649](https://github.com/realm/realm-core/pull/6649)).
* Fix an out-of-bounds read in sectioned results when sectioned are removed by modifying all objects in that section to no longer appear in that section ([#6649](https://github.com/realm/realm-core/pull/6649), since v13.12.0)
* Using both synchronous and asynchronous transactions on the same thread or scheduler could hit the assertion failure "!realm.is_in_transaction()" if one of the callbacks for an asynchronous transaction happened to be scheduled during a synchronous transaction ([#6659](https://github.com/realm/realm-core/issues/6659), since v11.8.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Simplify the implementation of query expression nodes which have a btree leaf cache.
* Fix a lock order inversion hit by object store tests running on linux. The cycle required test-specific code and so is not applicable to non-tests.
* Remove catch() clause to prevent truncating stack trace in AsyncOper::do_recycle_and_execute() ([PR #6667](https://github.com/realm/realm-core/pull/6667))

----------------------------------------------

# 13.12.0 Release notes

### Enhancements
* Improve performance of SectionedResults. With a single section it is now ~10% faster, and the runtime of sectioning no longer scales significantly with section count, giving >100% speedups when there are large numbers of sections ([PR #6606](https://github.com/realm/realm-core/pull/6606)).
* Very slightly improve performance of runtime thread checking on the main thread on Apple platforms. ([PR #6606](https://github.com/realm/realm-core/pull/6606))

### Fixed
* We could crash when removing backlinks in cases where forward links did not have a corresponding backlink due to corruption. We now silently ignore this inconsistency in release builds, allowing the app to continue. ([#6585](https://github.com/realm/realm-core/issues/6585), v6.0.0)
* If you freeze a Results based on a collection of objects, the result would be invalid if you delete the collection ([#6635](https://github.com/realm/realm-core/issues/6635), since V13.11.0)
* Geospatial polygons now have built in normalization and validation in line with the MongoDB server side behaviour and the geoJSON standard. ([#6607](https://github.com/realm/realm-core/pull/6607), since v13.11.0)
* Dictionary::get_any() would expose unresolved links rather than mapping them to null. In addition to allowing invalid objects to be read from Dictionaries, this resulted in queries on Dictionaries sometimes having incorrect results ([#6644](https://github.com/realm/realm-core/pull/6644)).

### Breaking changes
* `platform` and `cpu_arch` fields in the `device_info` structure in App::Config can no longer be provided by the SDK's, they are inferred by the library ([PR #6612](https://github.com/realm/realm-core/pull/6612))
* `bundle_id` is now a required field in the `device_info` structure in App::Config ([PR #6612](https://github.com/realm/realm-core/pull/6612))
* The API for sectioned results change notifications has changed. Changes are now reported in a vector rather than a sparse map.
* Renamed `GeoCenterSphere` to `GeoCircle` and in RQL `geoSphere` to `geoCircle`. The GeoPoints of query shapes are now validated before use and an exception will be thrown if invalid. Geospatial queries are no longer allowed on top-level tables. Fixed query results using ANY/ALL/NONE and matching on lists ([PR #6645](https://github.com/realm/realm-core/issues/6645))

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Upgraded to Catch from v3.0.1 to v3.3.2. ([#6623](https://github.com/realm/realm-core/issues/6623))
* Added some geospatial benchmarks. ([#6622](https://github.com/realm/realm-core/issues/6622))

----------------------------------------------

# 13.11.0 Release notes

### Enhancements
* The query engine now supports `geowithin` queries on points. Points are embedded objects conforming to the geoJSON format, trying to use a geospatial query on data in the incorrect format produces a run time exception. Example RQL query: `location geoWithin geoPolygon({{-178.0, 10.0}, {178.0, 10.0}, {178.0, -10.0}, {-178.0, -10.0}, {-178.0, 10.0}})`. For SDKs who do not wish to add this yet, the feature can be compiled out by adding `-DREALM_ENABLE_GEOSPATIAL=OFF` to the cmake config. ([#6562](https://github.com/realm/realm-core/issues/6562))
* Partition-Based to Flexible Sync Migration for migrating a client app that uses partition based sync to use flexible sync under the hood if the server has been migrated to flexible sync is officially supported with this release. Any clients using an older version of Realm (including the original support released in Core 13.10.0) will receive a "switch to flexible sync" error message when trying to sync with the app. ([#6554](https://github.com/realm/realm-core/issues/6554))

### Fixed
* Fixed a fatal error (reported to the sync error handler) during client reset (or automatic PBS to FLX migration) if the reset has been triggered during an async open and the schema being applied has added new classes. ([#6601](https://github.com/realm/realm-core/issues/6601), since automatic client resets were introduced in v11.5.0)
* Full text search would sometimes find words where the word only matches the beginning of the search token ([#6591](https://github.com/realm/realm-core/issues/6591), since v13.0.0)
* Added missing includes of `<cstdint>` surfaced by gcc13 ([#6616](https://github.com/realm/realm-core/pull/6616))
* Prevent crashing on Results.freeze if underlying object or table were removed by making Results.is_valid correctly report its state. ([#6401](https://github.com/realm/realm-core/issues/6401))

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Add initial support for targeting WebAssembly with Emscripten ([PR #6263](https://github.com/realm/realm-core/pull/6263)).
* Sync session multiplexing is now enabled by default. The method `SyncManager::enable_session_multiplexing()` has been renamed `SyncManager::set_session_multiplexing()`. (PR [#6557](https://github.com/realm/realm-core/pull/6557))
* Bump protocol to v9 to indicate client has fix for client reset error during async open ([#6609](https://github.com/realm/realm-core/issues/6609))
* Fixed `Results::is_valid()` in order to return `false` if the results is bound to a deleted object or table. (PR [#6445](https://github.com/realm/realm-core/pull/6445))

----------------------------------------------

# 13.10.1 Release notes

### Enhancements
* New notifiers can now be registered in write transactions until changes have actually been made in the write transaction. This makes it so that new notifications can be registered inside change notifications triggered by beginning a write transaction (unless a previous callback performed writes). (PR [#6560](https://github.com/realm/realm-core/pull/6560))
* Expose `Results::get_query()` in the C-API. (PR [#6568](https://github.com/realm/realm-core/pull/6568))
* `get_shared_app` will now generate unique app instances for configurations with the same app ID but different base URLs. `get_cached_app` now accepts the base URL as an optional second parameter which should be provided to ensure the correct app instance is returned when there are multiple instances of the same app ID with different base URLs. The deprecated `realm_app_get_cached` and `realm_app_get` C api methods have been removed ([#6480](https://github.com/realm/realm-core/pull/6480))

### Fixed
* Exclusion of words in a full text search does not work ([#6512](https://github.com/realm/realm-core/issues/6512), since v13.0.0 );

### Breaking changes
* Add `service_name` parameter to `realm_app_call_function` (PR [#6394](https://github.com/realm/realm-core/pull/6394)).

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Reduce the memory footprint of an automatic (discard or recover) client reset when there are large incoming changes from the server. ([#6567](https://github.com/realm/realm-core/issues/6567))
* `get_committed_file_format_version()` safe access to mappings vector from multiple threads.
* Add CI tests for Clang 16/Ubuntu 22.04 and update lint task ([PR #6563](https://github.com/realm/realm-core/pull/6563))

----------------------------------------------

# 13.10.0 Release notes

### Enhancements
* PBS to FLX Migration for migrating a client app that uses partition based sync to use flexible sync under the hood if the server has been migrated to flexible sync. ([#6554](https://github.com/realm/realm-core/issues/6554))

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Bump the sync protocol version to v8 ([PR #6549](https://github.com/realm/realm-core/pull/6549))

----------------------------------------------

# 13.9.4 Release notes

### Enhancements
* Improve performance of rolling back write transactions after making changes. If no KVO observers are used this is now constant time rather than taking time proportional to the number of changes to be rolled back. Rollbacks with KVO observers are 10-20% faster. ([PR #6513](https://github.com/realm/realm-core/pull/6513))

### Fixed
* Performing a query like "{1, 2, 3, ...} IN list" where the array is longer than 8 and all elements are smaller than some values in list, the program would crash ([#1183](https://github.com/realm/realm-kotlin/issues/1183), v12.5.0)
* Performing a large number of queries without ever performing a write resulted in steadily increasing memory usage, some of which was never fully freed due to an unbounded cache ([Swift #7978](https://github.com/realm/realm-swift/issues/7978), since v12.0.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Clear out SubscriptionStore and cancel pending notifications upon rollback to PBS after client migration to FLX. ([#6389](https://github.com/realm/realm-core/issues/6389))
* Simplify the non-sync replication log by emitting the same instruction type for all three types of collections rather than different instructions per collection type. This has no functional effect but eliminates some duplicated code. ([PR #6513](https://github.com/realm/realm-core/pull/6513))
* Remove TransactionChangeInfo::track_all, which was only ever used by the global notifier. ([PR #6513](https://github.com/realm/realm-core/pull/6513))
* Delete util::InputStream and rename util::NoCopyInputStream to util::InputStream.

----------------------------------------------

# 13.9.3 Release notes

### Fixed
* If session multiplexing was enabled in the sync client and multiple realms for multiple users were being synchronized, a connection authenticated for the wrong user could have been used, reuslting in a UserMismatch error from the server. ([PR #6320](https://github.com/realm/realm-core/pull/6320), since v10.0.0).
* If session multiplexing was enabled and an automatic client reset failed, it could cause all sessions to fail with a fatal ProtocolError rather than just the session that failed to client reset. This would mean that no other sync session would be able to be opened for up to an hour without restarting the app. ([PR #6320](https://github.com/realm/realm-core/pull/6320), since v11.5.0)
* If a DOWNLOAD message was received after a sync session was de-activated but before the UNBOUND message was received by the client, a use-after-free error may have occurred when the sync session tried to process the download messaage. So far this has only been reproducible if session multiplexing was enabled. ([PR #6320](https://github.com/realm/realm-core/pull/6320), since v12.9.0)
* HTTP and Websocket redirections are not properly updating URL locations if the network transport implementation handles redirect responses internally ([#6485](https://github.com/realm/realm-core/issues/6485), since v12.9.0)
* Don't report non ssl related errors during ssl handshake as fatal in default socket provider. ([#6434](https://github.com/realm/realm-core/issues/6434), since v13.3.0)

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

----------------------------------------------

# 13.9.2 Release notes

### Enhancements
* Improve performance of equality queries on a non-indexed mixed property by about 30%. ([#6506](https://github.com/realm/realm-core/issues/6506))

### Fixed
* Fix a build error due to an installed header depending on a non-installed on (since v13.9.1).

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Allow to run multiprocess encryption tests outside building folder.

----------------------------------------------

# 13.9.1 Release notes

### Enhancements
* None.

### Fixed
* Fixed a crash or exception when doing a fulltext search for multiple keywords when the intersection of results is not equal. ([#6465](https://github.com/realm/realm-core/issues/6465) since v13.2.0).
* Fixed issue where build would not succeed when consuming core as an installed dependancy due to missing install headers ([#6479](https://github.com/realm/realm-core/pull/6479) since v13.4.1).

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Fix compiler warnings with MacOS Clang 14.0.3 ([PR #6467](https://github.com/realm/realm-core/pull/6467))
* Perform a client reset to migrate a sync'd realm from PBS to FLX and vice versa ([#6393](https://github.com/realm/realm-core/issues/6393))
* The following unused util headers have been deleted: call_with_tuple.hpp, get_file_size.hpp, inspect.hpp, substitute.hpp, type_list.hpp, and utf8.hpp.
* Add migrated state information to flexible sync client BIND message for backfilling PBS partition value in objects once client has migrated to FLX. ([PR #6464](https://github.com/realm/realm-core/pull/6464))
* Add unit test for MigrationStore ([PR #6495](https://github.com/realm/realm-core/pull/6495))

----------------------------------------------

# 13.9.0 Release notes

### Enhancements
* Enable multiple processes to operate on an encrypted Realm simultaneously. ([#1845](https://github.com/realm/realm-core/issues/1845))

### Fixed
* Fix a stack overflow crash when using the query parser with long chains of AND/OR conditions. ([#6428](https://github.com/realm/realm-core/pull/6428), since v11.7.0)
* Changing the log level on the fly would not affect the core level log output ([#6440](https://github.com/realm/realm-core/issues/6440), since 13.7.0)
* `SyncManager::immediately_run_file_actions()` no longer ignores the result of trying to remove a realm. This could have resulted in a client reset action being reported as successful when it actually failed on windows if the `Realm` was still open ([#6050](https://github.com/realm/realm-core/issues/6050)).
* Fix a data race in `DB::VersionManager`. If one thread committed a write transaction which increased the number of live versions above the previous highest seen during the current session at the same time as another thread began a read, the reading thread could read from a no-longer-valid memory mapping ([PR #6411](https://github.com/realm/realm-core/pull/6411), since v13.0.0).
* Performing count() on an undefined Query would crash. ([#6443](https://github.com/realm/realm-core/issues/6443), since v13.7.0)

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.
* Lock file format: New format introduced for multiprocess encryption. All processes accessing the file must be upgraded to the new format.

-----------

### Internals
* PrintChangeset sync CLI utility can now ingest trace-level log files and decode hex/base64-compressed changeset data within the logs ([#6375](https://github.com/realm/realm-core/pull/6375))

----------------------------------------------

# 13.8.0 Release notes

### Enhancements
* Performance improvement for the following queries ([6376](https://github.com/realm/realm-core/issues/6376)):
    * Significant (~75%) improvement when counting (`Query::count()`) the number of exact matches (with no other query conditions) on a string/int/UUID/ObjectID property that has an index. This improvement will be especially noticiable if there are a large number of results returned (duplicate values).
    * Significant (~99%) improvement when querying for an exact match on a Timestamp property that has an index.
    * Significant (~99%) improvement when querying for a case insensitive match on a Mixed property that has an index.
    * Moderate (~25%) improvement when querying for an exact match on a Boolean property that has an index.
    * Small (~5%) improvement when querying for a case insensitive match on a Mixed property that does not have an index.
* Expose `Results::is_valid()` in the C API, in order to prevent to use an invalid Results. (PR [#6407](https://github.com/realm/realm-core/pull/6407))
* Restore `SyncManager::set_logger_factory()` to enable setting different loggers for different SyncManagers ([PR #6400](https://github.com/realm/realm-core/pull/6400)).

### Fixed
* Fixed a crash when querying a mixed property with a string operator (contains/like/beginswith/endswith) or with case insensitivity. ([6376](https://github.com/realm/realm-core/issues/6376) since introduction of Mixed)
* Querying for equality of a string on an indexed mixed property was returning case insensitive matches. For example querying for `myIndexedMixed == "Foo"` would incorrectly match on values of "foo" or "FOO" etc. ([6376](https://github.com/realm/realm-core/issues/6376) since introduction of Mixed)
* Adding an index to a Mixed property on a non-empty table would crash with an assertion. ([6376](https://github.com/realm/realm-core/issues/6376) since introduction of Mixed)
* `SyncSession::pause()` could hold a reference to the database open after shutting down the sync session, preventing users from being able to delete the realm. ([#6372](https://github.com/realm/realm-core/issues/6372), since v13.3.0)
* `Logger::set_default_logger()` did not perform any locking, resulting in data races if it was called while the default logger was being read on another thread ([PR #6398](https://github.com/realm/realm-core/pull/6398), since v13.7.0).

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Add admin api and test for performing the PBS->FLX migration and roll back on the server. (PR [#6366](https://github.com/realm/realm-core/pull/6366))
* Integrate protocol support for PBS->FLX client migration ([PR #6355](https://github.com/realm/realm-core/pull/6355))
* `SyncManager::reset_for_testing()` could race with SyncSession's being torn down in other threads causing an assertion for `REALM_ASSERT_RELEASE(no_sessions)` to fail. `SyncManager::reset_for_testing()` now waits/yields for up to 5 seconds for sessions being torn down in other threads to finish tearing down before checking this assertion. ([#6271](https://github.com/realm/realm-core/issues/6271))
* Update event loop to keep full stack trace if no observer is used ([PR #6410](https://github.com/realm/realm-core/pull/6410))
* `SyncSession::OnlyForTesting::handle_error()` now takes a `realm::sync:SessionErrorInfo` instead of a `realm::SyncError` to reflect internal refactoring ([PR #6433](https://github.com/realm/realm-core/pull/6433)).

----------------------------------------------

# 13.7.1 Release notes

### Enhancements
* Expose `RealmConfig::schema_subset_mode` in the C-API. (PR [#6379](https://github.com/realm/realm-core/pull/6379))

### Fixed
* Fixed a bug that may have resulted in arrays being in different orders on different devices. Some cases of “Invalid prior_size” may be fixed too. ([#6191](https://github.com/realm/realm-core/issues/6191), since v11.13.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* None
----------------------------------------------

# 13.7.0 Release notes

### Enhancements
* Add logging at the Storage level. (PR [#6339](https://github.com/realm/realm-core/pull/6339))

### Fixed
* None.

### Breaking changes
* You can no longer associate a Logger Factory with the SyncManager. Instead you can install one default logger via Logger::set_default_logger(). This logger will then be used all over Core. Logging cmake flags updated to use REALM_TEST_LOGGING and REALM_TEST_LOGGING_LEVEL

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* The OpenSSL dependency is included only exactly where needed. Where OpenSSL is not available and the target system doesn't provide them, we now bundle public domain implementations of the SHA1 and SHA2 hashes. Common hashing operations are exposed by `util/sha_crypto.hpp` so that call sites don't need to reason about the exact hash provider. ([PR #6308](https://github.com/realm/realm-core/pull/6308))

----------------------------------------------

# 13.6.0 Release notes

### Enhancements
* Add per app support for BindingCallbackThreadObserver ([#6250](https://github.com/realm/realm-core/issues/6250))

### Fixed
* You may have a crash on Windows if you try to open a file with non-ASCII path. ([#6336](https://github.com/realm/realm-core/issues/6336), since v13.4.0)
* Creating subscriptions with queries having unicode parameters causes a server error. ([#6350](https://github.com/realm/realm-core/issues/6350), since v11.7.0)

### Breaking changes
* BindingCallbackThreadObserver interface was updated to be part of SyncClientConfig and global instance was removed. ([PR #6156](https://github.com/realm/realm-core/pull/6156))

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Add CAPI test for Binding Callback Thread Observer. ([PR #6156](https://github.com/realm/realm-core/pull/6156))
* Implement MigrationStore to support migration from PBS to FLX ([PR #6324](https://github.com/realm/realm-core/pull/6324))
* Removed overloads of `Session::bind()` that allow binding a sync session to server other than the one configured in `Session::Config` ([PR #6358](https://github.com/realm/realm-core/pull/6358)).

----------------------------------------------

# 13.5.0 Release notes

### Enhancements
* None.

### Fixed
* Converting local realm to sync'd realm crashes if an embedded object is null ([#6294](https://github.com/realm/realm-core/issues/6294), since v11.9.0)
* Fixed performance degradation on SubQueries ([#6327](https://github.com/realm/realm-core/issues/6327), since v6.0.0)
* The C API type `realm_sync_error_code_t` did not include a textural representation of the underlying category. ([#5399](https://github.com/realm/realm-core/issues/5399)),
* Fixed several cases where wrong type of exception was thrown ([#6208](https://github.com/realm/realm-core/issues/6208), never released)
* Fixed classification of InvalidQuery exception ([#6223](https://github.com/realm/realm-core/issues/6223), never released)
* Fix crash if secure transport returns an error with a non-zero length. ([#5435](https://github.com/realm/realm-core/issues/5435), since 10.0.0)

### Breaking changes
* All exceptions thrown out of Core are now of type 'Exception'. All use of std::runtime_error and std::logical_error etc. has stopped and the specialized error classes that beforehand were based on these are now based on Exception.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Fixed a test failure originating due to the location for flexible sync permissions in the new version of baas changing.
([#6319](https://github.com/realm/realm-core/pull/6319))

----------------------------------------------

# 13.4.2 Release notes

### Enhancements
* None.

### Fixed
* Sync client may [have failed an assertion](https://github.com/realm/realm-core/blob/006660c8d20c4941d3838f74aec6f3561ebf6784/src/realm/sync/noinst/client_impl_base.cpp#L388) during shutdown if all sessions hadn't been ready to finalize by the time the Client destructor ran. (PR [#6293](https://github.com/realm/realm-core/pull/6293), since v13.4.1)
* Fixed the sync client being stuck in a cycle if an integration error occurs by issuing a client reset ([#6051](https://github.com/realm/realm-core/issues/6051), since v10.2.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Upgrade OpenSSL from 3.0.7 to 3.0.8. ([#6305](https://github.com/realm/realm-core/pull/6305))
* Windows improvements in util/file ([PR #6221](https://github.com/realm/realm-core/pull/6221)):
  * The following APIs are now implemented using std::filesystem: `try_make_dir`/`make_dir`, `make_dir_recursive`, `try_remove_dir`/`remove_dir`, `try_remove_dir_recursive`/`remove_dir_recursive`, `File::exists`, `File::is_dir`, `File::try_remove`/`File::remove`, `File::move`, `File::copy`, `File::last_write_time`, `File::get_free_space`
  * `File::get_unique_id` now works on Windows
  * Replaced manual path string conversion with `std::filesystem::path`
* Update yarn download path in install_baas.sh ([PR #6309](https://github.com/realm/realm-core/pull/6309))
* Include the websocket close status reason when reporting errors to the sync client ([PR #6298](https://github.com/realm/realm-core/pull/6298))

----------------------------------------------

# 13.4.1 Release notes

### Enhancements
* IntegrationException's which require help from support team mention 'Please contact support' in their message ([#6283](https://github.com/realm/realm-core/pull/6283))
* Add support for nested and overlapping scopes to the Events API. If multiple scopes are active all events generated will be reported to every active scope ([#6288](https://github.com/realm/realm-core/pull/6288)).

### Fixed
* App 301/308 redirection support doesn't use new location if metadata mode is set to 'NoMetadata'. ([#6280](https://github.com/realm/realm-core/issues/6280), since v12.9.0)
* Expose ad hoc interface for querying dictionary key changes in the C-API. ([#6228](https://github.com/realm/realm-core/issues/6228), since v10.3.3)
* Client reset with recovery or discard local could fail if there were dangling links in lists that got ressurected while the list was being transferred from the fresh realm ([#6292](https://github.com/realm/realm-core/issues/6292), since v11.5.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* The lifecycle of the sync client is now separated from the event loop/socket provider it uses for async I/O/timers. The sync client will wait for all outstanding callbacks/sockets to be closed during destruction. The SyncSocketProvider passed to the sync client must run until after the sync client is destroyed but does not need to be stopped as part of tearing down the sync client. ([PR #6276](https://github.com/realm/realm-core/pull/6276))
* The default event loop will now keep running until it is explicitly stopped rather than until there are no more timers/IO to process. Previously there was a timer set for very far in the future to force the event loop to keep running. ([PR #6265](https://github.com/realm/realm-core/pull/6265))
* Disable failing check in Metrics_TransactionTimings test ([PR #6206](https://github.com/realm/realm-core/pull/6206))
* Map ClientError's to ProtocolError's and use ProtocolError's when sending ERROR messages to the server ([PR #6086](https://github.com/realm/realm-core/pull/6086))

----------------------------------------------

# 13.4.0 Release notes

### Enhancements
* Improve performance of interprocess mutexes on iOS which don't need to support reader-writer locking. The primary beneficiary of this is beginning and ending read transactions, which is now almost as fast as pre-v13.0.0 ([PR #6258](https://github.com/realm/realm-core/pull/6258)).

### Fixed
* Sharing Realm files between a Catalyst app and Realm Studio did not properly synchronize access to the Realm file ([PR #6258](https://github.com/realm/realm-core/pull/6258), since v6.0.21).
* Fix websocket redirection after server migration if user is logged in ([#6056](https://github.com/realm/realm-core/issues/6056), since v12.9.0)
* Freezing an immutable Realm would hit an assertion failure ([#6260]https://github.com/realm/realm-core/issues/6260), since v13.3.0).

### Breaking changes
* Remove Group::compute_aggregated_byte_size(), which is no longer used by anything.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Added `REALM_ARCHITECTURE_ARM32` and `REALM_ARCHITECTURE_ARM64` macros to `features.h` for easier platform detection. ([#6256](https://github.com/realm/realm-core/pull/6256))
* Create the fuzzer framework project in order to run fuzz testing on evergreen ([PR #5940](https://github.com/realm/realm-core/pull/5940))

----------------------------------------------

# 13.3.0 Release notes

### Enhancements
* `SyncSession::pause()` and `SyncSession::resume()` allow users to suspend a Realm's sync session until it is explicitly resumed in ([#6183](https://github.com/realm/realm-core/pull/6183)). Previously `SyncSession::log_out()` and `SyncSession::close()` could be resumed under a number of circumstances where `SyncSession::revive_if_needed()` were called (like when freezing a realm) - fixes ([#6085](https://github.com/realm/realm-core/issues/6085))
* Improve the performance of `Realm::freeze()` by eliminating some redudant work around schema initialization and validation. These optimizations do not apply to Realm::get_frozen_realm() ([PR #6211](https://github.com/realm/realm-core/pull/6211)).
* Include context about what object caused the merge exception in OT ([#6204](https://github.com/realm/realm-core/issues/6204))
* Add support for `Dictionary::get_keys()`, `Dictionary::contains()`, `Dictionary::find_any()` in the C API. ([#6181](https://github.com/realm/realm-core/issues/6181))
* Adds upport for in-memory Realms. If a Realm is opened without a path it will be in memory only.

### Fixed
* "find first" on Decimal128 field with value NaN does not find objects ([6182](https://github.com/realm/realm-core/issues/6182), since v6.0.0)
* Value in List of Mixed would not be updated if new value is Binary and old value is StringData and the values otherwise matches ([#6201](https://github.com/realm/realm-core/issues/6201), since v6.0.0)
* When client reset with recovery is used and the recovery does not actually result in any new local commits, the sync client may have gotten stuck in a cycle with a `A fatal error occured during client reset: 'A previous 'Recovery' mode reset from <timestamp> did not succeed, giving up on 'Recovery' mode to prevent a cycle'` error message. ([#6195](https://github.com/realm/realm-core/issues/6195), since v11.16.0)
* Fixed diverging history in flexible sync if writes occur during bootstrap to objects that just came into view ([#5804](https://github.com/realm/realm-core/issues/5804), since v11.7.0)
* Fix several data races when opening cached frozen Realms. New frozen Realms were added to the cache and the lock released before they were fully initialized, resulting in races if they were immediately read from the cache on another thread ([PR #6211](https://github.com/realm/realm-core/pull/6211), since v6.0.0).
* Properties and types not present in the requested schema would be missing from the reported schema in several scenarios, such as if the Realm was being opened with a different schema version than the persisted one, and if the new tables or columns were added while the Realm instance did not have an active read transaction ([PR #6211](https://github.com/realm/realm-core/pull/6211), since v13.2.0).
* If a client reset w/recovery or discard local is interrupted while the "fresh" realm is being downloaded, the sync client may crash with a MultpleSyncAgents exception ([#6217](https://github.com/realm/realm-core/issues/6217), since v11.13.0)
* Changesets from the server sent during FLX bootstrapping that are larger than 16MB can cause the sync client to crash with a LogicError (PR [#6218](https://github.com/realm/realm-core/pull/6218), since v12.0.0)
* Online compaction may cause a single commit to take a long time ([#6245](https://github.com/realm/realm-core/pull/6245), since v13.0.0)
* Expose `collection_was_cleared` in the C API ([#6200](https://github.com/realm/realm-core/issues/6200), since v.10.4.0)
* `Set<Mixed>::sort()` used a different sort order from sorting any other collection, including a filtered `Set<Mixed>` ([PR #6238](https://github.com/realm/realm-core/pull/6238), since v13.0.0).
* Fix issue where calling `RealmCoordinator::get_realm(Realm::Config, util::Optional<VersionID>)` would not correctly set `m_schema_version` to `ObjectStore::NotVersioned` if no schema was provided in the config when the realm is first opened ([PR #6236](https://github.com/realm/realm-core/pull/6236), since v10.0.0).

### Breaking changes
* `SyncSession::log_out()` has been renamed to `SyncSession::force_close()` to reflect what it actually does ([#6183](https://github.com/realm/realm-core/pull/6183))
* Passing an empty `key_path_array` to `add_notification_callback now` now ignores nested property changes. Pass `std::nullopt` to achieve the old meaning. ([#6122](https://github.com/realm/realm-core/pull/6122))
* Whether to report the file's complete schema or only the requested schema is now an option on RealmConfig (schema_subset_mode) rather than always being enabled for Additive schema modes. All schema modes which this applies to are now supported ([PR #6211](https://github.com/realm/realm-core/pull/6211)).

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Fix failures in Metrics_TransactionTimings core test ([#6164](https://github.com/realm/realm-core/issues/6164))
* Make log level threshold atomic and shared ([#6009](https://github.com/realm/realm-core/issues/6009))
* Add c_api error category for resolve errors instead of reporting unknown category, part 2. ([PR #6186](https://github.com/realm/realm-core/pull/6186))
* Remove `File::is_removed` ([#6222](https://github.com/realm/realm-core/pull/6222))
* Client reset recovery froze Realms for the callbacks in an invalid way. It is unclear if this resulted in any actual problems.
* Fix default enabled debug output during realm-sync-tests ([#6233](https://github.com/realm/realm-core/issues/6233))
* Migrate service and event loop into DefaultSyncSocket ([PR #6151](https://github.com/realm/realm-core/pull/6151))
* Move BindingCallbackThreadObserver from object-store to sync ([PR #6151](https://github.com/realm/realm-core/pull/6151))
* Update ClientImpl::Connection and DefaultWebSocketImpl to use the new WebSocketObserver callbacks ([PR #6219](https://github.com/realm/realm-core/pull/6219))
* Switched client reset tests to using private `force_client_reset` server API ([PR #6216](https://github.com/realm/realm-core/pull/6216))

----------------------------------------------

# 13.2.0 Release notes

### Enhancements
* Fulltext search index can now be defined on ObjectStore Property (PR [#6100](https://github.com/realm/realm-core/pull/6100))
* Improve performance of acquiring read locks when a read lock for that version is already held. This speeds up many operations related to change notifications, and particularly refreshing a Realm which has change notifiers registered.
* Upgrade OpenSSL from 1.1.1n to 3.0.7. ([#6097](https://github.com/realm/realm-core/pull/6097))
* Converting flexible sync realms to bundled and local realms is now supported ([#6076](https://github.com/realm/realm-core/pull/6076))
* Compensating write errors are now surfaced to the SDK/user after the compensating write has been applied in a download message ([#6095](https://github.com/realm/realm-core/pull/6095)).
* Normalize sync connection parameters for device information ([#6029](https://github.com/realm/realm-core/issues/6029))
* Add support for providing custom websocket implementations in the C API ([#5917](https://github.com/realm/realm-core/issues/5917))

### Fixed
* Fix `BadVersion` exceptions which could occur when performing multiple writes on one thread while observing change notifications on another thread ([#6069](https://github.com/realm/realm-core/issues/6069), since v13.0.0).
* Compare actual users (`SyncUser::operator!=`), not pointers (`shared_ptr<SyncUser>::operator!=`). ([#realm/realm-dart#1055](https://github.com/realm/realm-dart/issues/1055), since v10.2.0)
* Core should not alter the order of the properties for additive schemas. ([#6134](https://github.com/realm/realm-core/issues/6134))
* Core should not use the version passed in `Realm` constructor to load the schema at some particular version, but only for setting `m_frozen_version`.
* Fixed possible segfault in sync client where async callback was using object after being deallocated ([#6053](https://github.com/realm/realm-core/issues/6053), since v11.7.0)
* Fixed crash when using client reset with recovery and flexible sync with a single subscription ([#6070](https://github.com/realm/realm-core/issues/6070), since v12.3.0)
* Fixed crash with wrong transaction state, during realm migration if realm is frozen due to schema mismatch ([#6144](https://github.com/realm/realm-core/issues/6144), since v13.0.0)

### Breaking changes
* Core no longer provides any vcpkg infrastructure (the ports submodule and overlay triplets), because it handles dependant libraries internally now.
* Allow Realm instances to have a complete view of their schema, if mode is additive. ([PR #5784](https://github.com/realm/realm-core/pull/5784)).
* `realm_sync_immediately_run_file_actions` (c-api) now takes a third argument `bool* did_run` that will be set to the result of `SyncManager::immediately_run_file_actions`. ((#6117)[https://github.com/realm/realm-core/pull/6117])
* Device information in sync connection parameters was moved into a new `device_info` structure in App::Config ([PR #6066](https://github.com/realm/realm-core/pull/6066))
* `sdk` is now a required field in the `device_device` structure in App::Config ([PR #6066](https://github.com/realm/realm-core/pull/6066))

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Updates for upcoming Platform Networking feature, including new SyncSocketProvider class. ([PR #6096](https://github.com/realm/realm-core/pull/6096))
* Update namespaces for files moved to realm/sync/network ([PR #6109](https://github.com/realm/realm-core/pull/6109))
* Replace util::network::Trigger with a Sync client custom trigger. ([PR #6121](https://github.com/realm/realm-core/pull/6121))
* Create DefaultSyncSocket class ([PR #6116](https://github.com/realm/realm-core/pull/6116))
* Improve detection of Windows target architecture when downloading prebuild dependencies. ([#6135](https://github.com/realm/realm-core/issues/6135))
* App services request ID now logged at info level when sync client connects ([#6143](https://github.com/realm/realm-core/pull/6143]))
* Add c_api error category for resolve errors instead of reporting unknown category. ([PR #6157](https://github.com/realm/realm-core/pull/6157))
* Add permanent redirect (308) as a supported redirect response from the server. ([#6162](https://github.com/realm/realm-core/issues/6162))
* Integrate DefaultSocketProvider as SyncSocketProvider in sync client. ([PR #6171](https://github.com/realm/realm-core/pull/6171))

----------------------------------------------

# 13.1.2 Release notes
### Enhancements
* Per-object compensating write error info is now available via the C API. (PR [#6103](https://github.com/realm/realm-core/pull/6103))
### Fixed
* Fixed `realm_add_realm_refresh_callback` and notify immediately that there is not transaction snapshot to advance to. ([#6075](https://github.com/realm/realm-core/issues/6075), since v12.6.0)
* Fix no notification for write transaction that contains only change to backlink property. ([#4994](https://github.com/realm/realm-core/issues/4994), since v11.4.1)

### Breaking changes
* FLX Subscription API reworked to better match SDK consumption patterns ([#6065](https://github.com/realm/realm-core/pull/6065)). Not all changes are breaking, but listing them all here together.
  * `Subscription` is now a plain struct with public fields rather than getter functions
  * `has_name()` and `name()` were merged into a single `optional<string> name` field
  * `SubscriptionSet` now uses the same types for `iterator` and `const_iterator` since neither was intended to support direct mutability
  * `SubscriptionSet::get_state_change_notification()` now offers a callback-taking overload
  * `SubscriptionSet::find()` overloads now return `const Subscription*` with `nullptr` to signal not-found, rather than an at-end iterator
  * `MutableSubscriptionSet::erase()` now has overloads taking a `StringData name` or a `Query`. Both return a bool to indicate if anything was found and removed.
  * `MutableSubscriptionSet::commit()` is no longer `&&`-qualified. This means you no longer need to `std::move()` the set when calling it.

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* None.

----------------------------------------------

# 13.1.1 Release notes

### Fixed
* Not possible to open an encrypted file on a device with a page size bigger than the one on which the file was produced. ([#8030](https://github.com/realm/realm-swift/issues/8030), since v12.11.0)
* Fixed `realm_refresh` so it uses an argument value for the refresh result and returns any error conditions as return value. ([#6068](https://github.com/realm/realm-core/pull/6068), since  v10.4.0)
* Fixed `realm_compact` to actually do the compaction even if the caller did not provide a `did_compact` argument. ([#6068](https://github.com/realm/realm-core/pull/6068), since v12.7.0)
### Breaking changes
* ObjectId constructor made explicit, so no more implicit conversions from const char* or array of 12 bytes. It now accepts a StringData. ([#6059](https://github.com/realm/realm-core/pull/6059))

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

----------------------------------------------

# 13.1.0 Release notes

### Enhancements
* The choice of a faster linker will now automatically be propagated to anything that statically links against realm-core. ([#6043](https://github.com/realm/realm-core/pull/6043))
* Replace explicit callback registration with tokens in the C API. ([#6047](https://github.com/realm/realm-core/pull/6047))

### Fixed
* Fixed wrong assertion on query error that could result in a crash. ([#6038](https://github.com/realm/realm-core/issues/6038), since v11.7.0)
* Freelist may be corrupted if compaction was initiated ([#6054](https://github.com/realm/realm-core/pull/6054), since v13.0.0)

### Breaking changes
* Updated `logger_factory` in SyncClientConfig to return a `shared_ptr` instead of a `unique_ptr` ([PR #5980](https://github.com/realm/realm-core/pull/5980))
* `util::RootLogger` has been replaced with `util::Logger`

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Update logger references in primary Sync/ObjectStore classes to use `shared_ptr` ([#5983](https://github.com/realm/realm-core/issues/5983))

----------------------------------------------

# 13.0.0 Release notes

### Enhancements
* The realm file will be shrunk if the larger file size is no longer needed. (PR [#5755](https://github.com/realm/realm-core/pull/5755))
* Full text index supported at Core level. The words stored in the index will be converted to lower case and diacritics will be removed. The index will support Basic Latin and Latin-1 Supplement characters. All others will be ignored. (PR [#5872](https://github.com/realm/realm-core/pull/5872))
* Most of the file growth caused by version pinning is eliminated. (PR [#5440](https://github.com/realm/realm-core/pull/5440))

### Fixed
* Set<Mixed> consider string and binary data equivalent. This could cause the client to be inconsistent with the server if a string and some binary data with equivalent content was inserted from Atlas. ([#4860](https://github.com/realm/realm-core/issues/4860), since v11.0.0)

### Breaking changes
* File format version bumped. If realm file contains any objects with Set<Mixed> or Dictionary properties, the file will go through an upgrade process.
* The layout of the lock-file has changed, the lock file format version is bumped and all participants in a multiprocess scenario needs to be up to date so they expect the same format. This requires an update of Studio. (PR [#5440](https://github.com/realm/realm-core/pull/5440))

### Compatibility
* Fileformat: Generates files with format v23. Reads and automatically upgrade from fileformat v5.
-----------

### Internals
* Encoding of Dictionary in the realm file has changed. This will change the order of the elements, so if any tests depend on the order, those must be revised.

----------------------------------------------

# 12.13.0 Release notes

### Enhancements
* Add a way to register a thread observer listener in the C API. ([PR #6040](https://github.com/realm/realm-core/pull/6040))

### Fixed
* Fetching a user's profile while the user logs out would result in an assertion failure. ([PR #6017](https://github.com/realm/realm-core/issues/5571), since v11.0.3)
* Removed the ".tmp_compaction_space" file being left over after compacting a Realm on Windows. ([#6002](https://github.com/realm/realm-core/issues/6002), since Windows support for compact was added).
* Restore fallback to full barrier when F_BARRIERSYNC is not available on Apple platforms. ([PR #6033](https://github.com/realm/realm-core/pull/6033), since v12.12.0)
* Validation of Queries constructed by the Fluent QueryBuilder was missing. ([#6034](https://github.com/realm/realm-core/issues/6034), since v12.7.0)
* Allow setting values on a Mixed property through the C API ([#5985](https://github.com/realm/realm-core/issues/5985), since v10.5.0)

### Breaking changes
* `Table::query()` overload taking `vector<vector<Mixed>>` now takes `vector<variant<Mixed, vector<Mixed>>>` in order to distinguish scalar arguments from single-element lists. ([#5973](https://github.com/realm/realm-core/pull/5973))
* Better error handling for `realm_async_begin_write` and `realm_async_commit`. ([#PR6039](https://github.com/realm/realm-core/pull/6039))

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

----------------------------------------------

# 12.12.0 Release notes

### Enhancements
* App::call_function now supports taking and receiving ejson strings, bypassing parsing/serialization in core. ([#4182](https://github.com/realm/realm-core/issues/4182))
* Flexible sync will now wait for the server to have sent all pending history after a bootstrap before marking a subscription as Complete. ([#5795](https://github.com/realm/realm-core/pull/5795))
* Add typedef for realm_app_call_function callbacks. ([#5996](https://github.com/realm/realm-core/pull/5996))
* Expose List::move on C api as realm_list_move. ([#6032](https://github.com/realm/realm-core/pull/6032))

### Fixed
* Fix a race condition which could result in "operation cancelled" errors being delivered to async open callbacks rather than the actual sync error which caused things to fail ([PR #5968](https://github.com/realm/realm-core/pull/5968), since the introduction of async open).
* The name of one of the RLM_SYNC_BOOTSTRAPPING enum member in the C api was updated to match the naming convention of the other members in the enum.
* Fix `Results.distinct(keypaths)` and `Results.sort(keypaths)` not correctly handling keypaths names for properties that have a public/private(column) property name ([PR #5952](https://github.com/realm/realm-core/pull/5952).
* Bootstraps will not be applied in a single write transaction - they will be applied 1MB of changesets at a time, or as configured by the SDK ([#5999](https://github.com/realm/realm-core/pull/5999), since v12.0.0).
* Fix database corruption and encryption issues on apple platforms, reported in several bugs listed in the PR. ([PR #5993](https://github.com/realm/realm-core/pull/5993), since v11.8.0)

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Removed `<realm/util/parent_dir.*pp>` and moved `parent_dir` to be part of `File`.
* Fix `BPlusTreeInner::verify()` ([#5974](https://github.com/realm/realm-core/pull/5974)).
* Updated install_baas stitch support libraries to 4.4.17 ([#5989](https://github.com/realm/realm-core/issues/5989))
* Defer converting Realm files from streaming form until after we've validated the version ([PR #5987](https://github.com/realm/realm-core/pull/5987)).

----------------------------------------------

# 12.11.0 Release notes

### Fixed
* Calling `SectionedResults::reset_section_callback()` on a `SectionedResults` which had been evaluated would result in an assertion failure the next time the sections are evaluated ([PR #5965](https://github.com/realm/realm-core/pull/5965), since v12.10.0).
* Opening an unencrypted file with an encryption key would sometimes report a misleading error message that indicated that the problem was something other than a decryption failure ([PR #5915](https://github.com/realm/realm-core/pull/5915), since 0.86.1).
* Fix a rare deadlock which could occur when closing a synchronized Realm immediately after committing a write transaction when the sync worker thread has also just finished processing a changeset from the server ([PR #5948](https://github.com/realm/realm-core/pull/5948)).

### Breaking changes
* Websocket errors caused by the client sending a websocket message that is too large (i.e. greater than 16MB) now get reported as a `ProtocolError::limits_exceeded` error with a `ClientReset` requested by the server ([#5209](https://github.com/realm/realm-core/issues/5209)).

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Added integration test for opening synchronized realms as in-memory realms ([#5955](https://github.com/realm/realm-core/pull/5955)).
* Added realm core version to the app login request ([#5959](https://github.com/realm/realm-core/issues/5959))

----------------------------------------------

# 12.10.0 Release notes

### Enhancements
* Improve performance of client reset with automatic recovery and converting top-level tables into embedded tables (PR [#5897](https://github.com/realm/realm-core/pull/5897)).
* Adding `realm_query_parse_for_set` in the C API ([#5935](https://github.com/realm/realm-core/pull/5935)).

### Fixed
* Fixed an assertion failure when observing change notifications on a sectioned result, if the first modification was to a linked property that did not cause the state of the sections to change. ([#5912](https://github.com/realm/realm-core/issues/5912), since the introduction of sectioned results in v12.3.0)
* CompensatingWriteErrorInfo reported string primary keys as boolean values instead ([PR #5938](https://github.com/realm/realm-core/pull/5938), since the introduction of CompensatingWriteErrorInfo in 12.1.0).
* Fix a use-after-free if the last external reference to an encrypted Realm was closed between when a client reset error was received and when the download of the new Realm began. ([PR #5949](https://github.com/realm/realm-core/pull/5949), since 12.4.0).
* Fixed an assertion failure during client reset with recovery when recovering a list operation on an embedded object that has a link column in the path prefix to the list from the top level object. ([PR #5957](https://github.com/realm/realm-core/issues/5957), since introduction of automatic recovery in v11.16.0).
* IN Query fails if left operator is a TypedLink ([5946](https://github.com/realm/realm-core/issues/5946), since v10.5.2)

### Breaking changes
* Rename RealmConfig::automatic_handle_backlicks_in_migrations to RealmConfig::automatically_handle_backlinks_in_migrations ([PR #5897](https://github.com/realm/realm-core/pull/5897)).
* Introduced new callback type realm_return_apikey_list_func_t and realm_return_apikey_func_t in the C-API ([PR #5945](https://github.com/realm/realm-core/pull/5945)).

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Remove the unused utility function `copy_dir_recursive()`.
* StringData and Timestamp are now constexpr-constructible.
* Remove `set_backlink_class_prefix()` and just always use the `class_` prefix when parsing or serializing queries.
* Updated `install_baas.sh` to use files stored on s3 ([#5932](https://github.com/realm/realm-core/issues/5932))

----------------------------------------------

# 12.9.0 Release notes

### Enhancements
* Prioritize integration of local changes over remote changes - shorten the time users may have to wait when committing local changes. Stop storing downloaded changesets in history. ([PR #5844](https://github.com/realm/realm-core/pull/5844)).
* Greatly improve the performance of sorting or distincting a Dictionary's keys or values. The most expensive operation is now performed O(log N) rather than O(N log N) times, and large Dictionaries can see upwards of 99% reduction in time to sort. ([PR #5166](https://github.com/realm/realm-core/pulls/5166))
* Expose `realm_get_value_by_property_index` to the C API. ([PR #5906](https://github.com/realm/realm-core/pull/5906))
* Add support for fetching directly backlinks in the C API. ([PR #5904](https://github.com/realm/realm-core/pull/5904))

### Fixed
* Fix a data race reported by thread sanitizer when preparing to deliver change notifications. This probably did not cause observable problems in practice ([PR #5892](https://github.com/realm/realm-core/pull/5892) since 12.7.0).
* Changed the behaviour of creating a collection of non-nullable Mixed type to throw a descriptive error message rather than asserting in debug mode. ([#5894](https://github.com/realm/realm-core/issues/5894), since the introduction of the Mixed type).
* Fix a use-after-free when a sync session is closed and the app is destroyed at the same time ([#5752](https://github.com/realm/realm-core/issues/5752), since v11.5.2).
* Fix Http transport doesn't correctly preserve the request body ([#5890](https://github.com/realm/realm-core/issues/5890), since 12.8.0).

### Breaking changes
* Removed breaking callback changes for `GenericNetworkTransport::send_request_to_server()` ([PR #5898](https://github.com/realm/realm-core/pull/5898)).

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Reenable sync benchmark.
* Add util/http.hpp to the release package.
* Implement test commands in sync wire protocol to be used in flx sync testing.

----------------------------------------------

# 12.8.0 Release notes

### Enhancements
* Cut the runtime of aggregate operations on large dictionaries in half ([PR #5864](https://github.com/realm/realm-core/pull/5864)).
* Improve performance of aggregate operations on collections of objects by 2x to 10x ([PR #5864](https://github.com/realm/realm-core/pull/5864)).
* Adding support in the C API for `realm_results_find` and supporting all native types for `realm_results_get()` ([PR 5875](https://github.com/realm/realm-core/pull/5875)).
* Reconstruct app url after a migration ([#5648](https://github.com/realm/realm-core/issues/5648)).

### Fixed
* Results permitted some nonsensical aggregate operations on column types which do not make sense to aggregate, giving garbage results rather than reporting an error ([#5876](https://github.com/realm/realm-core/pull/5876), since v6.0.0).
* Removed blocking wait for upload/download completion due to possibility of deadlock ([#5829](https://github.com/realm/realm-core/issues/5829), since v11.17.0).

### Breaking changes
* The typed aggregation functions (e.g. `minimum_int`) on `Table`, `TableView`, and `Query` have been removed and replaced with simpler untyped versions which return `Mixed`. This does not effect SDKs which only used them via the Object Store types.
* Callback for `GenericNetworkTransport::send_request_to_server()` now receives both the original Request and Response objects (does not affect the C-API).
* Removed blocking `SyncSession::wait_for_upload_completion` and `SyncSession::wait_for_download_completion`.

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Updated install_baas.sh to use go1.18.6 ([#5863](https://github.com/realm/realm-core/issues/5862)).
* Updated Windows distros on Evergreen CI to use vsCurrent.

----------------------------------------------

# 12.7.0 Release notes

### Enhancements
* Convert object_store::Collection types into Results (PR [#5845](https://github.com/realm/realm-core/pull/5845))
* Expose `realm_object_get_parent` in the C API (PR [#5851](https://github.com/realm/realm-core/pull/5851))
* Expose `realm_list_find` in the C API (PR [#5848](https://github.com/realm/realm-core/pull/5848))
* Expose `Group::remove_table` in the C API (PR [#5860](https://github.com/realm/realm-core/pull/5860))

### Fixed
* Prevent migrations to an embedded object type when there are incoming links from Mixed/TypedLink properties until we can support them. ([#5796](https://github.com/realm/realm-core/pull/5796), since 6.1.0)
* `Realm::refresh()` did not actually advance to the latest version in some cases. If there was a version newer than the current version which did not require blocking it would advance to that instead, contrary to the documented behavior ([#7625](https://github.com/realm/realm-swift/issues/7625), since v10.0.0).
* Several issues around notifications were fixed ([#5799](https://github.com/realm/realm-core/pull/5799))
  * Fix a data race on RealmCoordinator::m_sync_session which could occur if multiple threads performed the initial open of a Realm at once. (since v11.8.0).
  * If a SyncSession outlived the parent Realm and then was adopted by a new Realm for the same file, other processes would not get notified for sync writes on that file (since v10.0.0).
  * Fix one cause of QoS inversion warnings when performing writes on the main thread on Apple platforms. Waiting for async notifications to be ready is now done in a QoS-aware ways (since v10.0.0).
* If you set a subscription on a link in flexible sync, the server would not know how to handle it ([#5409](https://github.com/realm/realm-core/issues/5409), since v11.6.1)
* Fixed `realm_query_parse_for_results` ignoring query for `query_result_t` passed as parameter ([#5841](https://github.com/realm/realm-core/pull/5841), since 11.10.0).
* Fixed `realm_query_parse_for_list` ignoring existing query ([#5850](https://github.com/realm/realm-core/pull/5850), since 11.10.0).
* Fixed not allowing asymmetric tables in partition based sync ([#5691](https://github.com/realm/realm-core/issues/5691), since 12.1.0).
* Disable auto refresh for old realm instance passed to migration callbacks. ([#5856](https://github.com/realm/realm-core/pull/5856), since 10.0.0).
* If a case insensitive query searched for a string including an 4-byte UTF8 character, the program would crash ([#5825](https://github.com/realm/realm-core/issues/5825), since v2.3.0)
* Throw exception if `Realm::Convert` tries to convert to flexible sync. ([#5798](https://github.com/realm/realm-core/issues/5798), since v11.16.0).
* Fix crash when upserting a document with the primary key not an ObjectId into a mongo collection. ([#5345](https://github.com/realm/realm-core/issues/5345), since v10.0.0).

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Traversal functions use a typed IteratorControl value rather than true/false. ([#5857](https://github.com/realm/realm-core/issues/5857))

----------------------------------------------

# 12.6.0 Release notes

### Enhancements
* Notify when read transaction version is advanced. ([PR #5704](https://github.com/realm/realm-core/pull/5704)).
* Action returned from the server in the json error messages is surfaced through the SyncError. ([PR #5690](https://github.com/realm/realm-core/pull/5690)).
* `NotificationToken` grew an `unregister()` method as an alternative to destroying it or doing `token = {};` ([PR #5776](https://github.com/realm/realm-core/pull/5776)).
* Automatic handling backlinks for schema migrations where a class changes to being embedded. ([PR #5737](https://github.com/realm/realm-core/pull/5737)).
* Expose `Obj::add_int()` in the CAPI. ([PR #5770](https://github.com/realm/realm-core/pull/5770)).
* Expose `Realm::async_begin_transaction`, `Realm::async_commit_transaction`, `Realm::async_cancel_transaction` in the CAPI.([PR 5783 #](https://github.com/realm/realm-core/pull/5783)).
* Added `Obj::get_parent_object()` that will allow you to get the parent object of an embedded object ([#3898](https://github.com/realm/realm-core/issues/3898))
* Improve performance when a new Realm file connects to the server for the first time, especially when significant amounts of data has been written while offline.
* Shift more of the work done on the sync worker thread out of the write transaction used to apply server changes, reducing how long it blocks other threads from writing.
* Improve the performance of the sync changeset parser, which speeds up applying changesets from the server.
* Sync client sends integration errors to the server. ([PR #5719](https://github.com/realm/realm-core/pull/5719)).

### Fixed
* Fixed undefined behaviour on queries involving a constant and an indexed column on some property types like UUID and Timestamp. ([#5753](https://github.com/realm/realm-core/issues/5753), since 12.5.0)
* Fix all UBSan failures hit by tests. It is unclear if any of these manifested as visible bugs. ([PR #5665](https://github.com/realm/realm-core/pull/5665))
* Fix sorting order for `realm_query_find_first` in the C API.([#5720](https://github.com/realm/realm-core/issues/5720))
* Upload completion callbacks may have called before the download message that completed them was fully integrated. ([#4865](https://github.com/realm/realm-core/issues/4865)).
* Fixed an exception "fcntl() with F_BARRIERFSYNC failed: Inappropriate ioctl for device" when running with MacOS on an exFAT drive. ([#5789](https://github.com/realm/realm-core/issues/5789) since 12.0.0)
* Syncing of a Decimal128 with big significand could result in a crash. ([#5728](https://github.com/realm/realm-core/issues/5728))
* Recovery/discardLocal client reset modes will now wait for FLX sync realms to be fully synchronized before beginning recovery operations ([#5705](https://github.com/realm/realm-core/issues/5705))

### Breaking changes
* None.

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* `util::Any` is now just a typedef for `std::any`. `util::any_cast()` remains for deployment support on older Apple platforms. Outside of niche ADL implications, this should not have any visible effects. ([PR #5665](https://github.com/realm/realm-core/pull/5665))
* Use correct endpoints for checking if sync has been terminated in client reset tests ([#5815](https://github.com/realm/realm-core/pull/5815))
* The sync protocol is now version 7.

----------------------------------------------

# 12.5.1 Release notes

### Enhancements
* None.

### Fixed
* Fixed an issue where having realm-cocoa as SPM sub-target dependency leads to missing symbols error during iOS archiving. ([#7645](https://github.com/realm/realm-swift/issues/7645))
* Exposed the boolean `merge_with_existing` in the C-API's functions `realm_convert_with_config` and `realm_convert_with_path`. ([#5713](https://github.com/realm/realm-core/issues/5713))
* Processing a pending bootstrap before the sync client connects will properly surface errors to the user's error handler ([#5707](https://github.com/realm/realm-core/issues/5707), since v12.0.0)
* Opening a read-only Realm for the first time via `Realm::get_synchronized_realm()` would remove any columns present on the server but not in the local schema (since 12.5.0).
* Parsing a constant list of strings from RealmJS SDK to QueryParser would result in a "use after free situation". ([#5735](https://github.com/realm/realm-core/issues/5735))

### Breaking changes
* Notification callbacks no longer take an `exception_ptr` argument because errors haven't been possible for a few years. (PR [#5741](https://github.com/realm/realm-core/pull/5741))

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* The top function of an assertion stack trace now includes the core version number.
* Unset SDKROOT env variable when building ios targets with tools/build-apple-device.sh

----------------------------------------------

# 12.5.0 Release notes

### Enhancements
* Allow multiple anonymous sessions. ([PR #5693](https://github.com/realm/realm-core/pull/5693)).
* Introducing query parser support for constant list expressions such as `fruit IN {'apple', 'orange'}`. This also includes general query support for list vs list matching such as `NONE fruits IN {'apple', 'orange'}`. ([Issue #4266](https://github.com/realm/realm-core/issues/4266))
* SubscriptionSet::refresh() does less work if no commits have been made since the last call to refresh(). ([PR #5695](https://github.com/realm/realm-core/pull/5695))

### Fixed
* Fix error message when validating outgoing links from asymmetric objects to non-embedded objects. ([PR #5702](https://github.com/realm/realm-core/pull/5702))
* Fix a use-after-free when an AuditContext is destroyed while a different thread is in the completion handler for an audit Realm upload. ([PR #5714](https://github.com/realm/realm-core/pull/5714))
* Opening a read-only Realm for the first time via `Realm::get_synchronized_realm()` did not set the schema version, which could lead to `m_schema_version != ObjectStore::NotVersioned` assertion failures.
* Using the Query Parser, it was not allowed to query on a property named 'desc'. ([#5723](https://github.com/realm/realm-core/issues/5723))

### Breaking changes
* None

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* None.

----------------------------------------------

# 12.4.0 Release notes

### Enhancements
* Add support for building with Xcode 14 using the CMake project ([PR #5577](https://github.com/realm/realm-core/pull/5577)).
* Expose MongoDB client interface in the C API. ([PR #5638](https://github.com/realm/realm-core/pull/5638)).
* Add support in the C API for constructing a new `realm_app_t` object via `realm_app_create`. ([PR #5570](https://github.com/realm/realm-core/issues/5570))
* Reduce use of memory mappings and virtual address space ([PR #5645](https://github.com/realm/realm-core/pull/5645)). Also fixes some errors (see below)

### Fixed
* Fix exception when decoding interned strings in realm-apply-to-state tool. ([PR #5628](https://github.com/realm/realm-core/pull/5628))
* Fix some warnings when building with Xcode 14 ([PR #5577](https://github.com/realm/realm-core/pull/5577)).
* Throw `runtime_error` if subscription set is requested and flexible sync is not enabled. ([PR #5637](https://github.com/realm/realm-core/pull/5637))
* Fix compilation failures on watchOS platforms which do not support thread-local storage. ([#7694](https://github.com/realm/realm-swift/issues/7694), [#7695](https://github.com/realm/realm-swift/issues/7695) since v11.7.0)
* Fix a data race when committing a transaction while multiple threads are waiting for the write lock on platforms using emulated interprocess condition variables (most platforms other than non-Android Linux).
* Fix a data race when writing audit events which could occur if the sync client thread was busy with other work when the event Realm was opened.
* Fix some cases of running out of virtual address space (seen/reported as mmap failures) ([PR #5645](https://github.com/realm/realm-core/pull/5645))
* Audit event scopes containing only write events and no read events would occasionally throw a `BadVersion` exception when a write transaction was committed (since v11.17.0).
* Fixed the client reset callbacks not populating the Realm instance arguments correctly if the Realm coordinator lifetime had ended. ([#5654](https://github.com/realm/realm-core/pull/5654), since the introduction of DiscardLocal reset mode in v11.5.0)
* Decimal128 values with more than 110 significant bits were not synchronized correctly with the server ([#7868](https://github.com/realm/realm-swift/issues/7868), since v10.0.0)

### Breaking changes
* Make `realm::util::Optional<T>` be an alias for `std::optional<T>` and `realm::none` an alias for `std::nullopt`. ([PR #5667](https://github.com/realm/realm-core/pull/5667)) For now the existing name and some of the helpers are sticking around so this shouldn't affect most consumers, however:
  * At some point in the future we will likely want to remove the aliases, so consumers may want to switch to using `std::optional` and `std::nullopt` directly.
  * `std::optional<T&>` isn't supported. In general, `T*`/`nullptr` is a better replacement. The few uses of optional references in core APIs have been replaced with pointers. Any usages in SDKs will also need to be replaced.
  * `std::optional` cannot be directly streamed (`<<`) to an `ostream`. A helper `realm::util::stream_possible_optional` is provided that can be used instead. For convenience, it will work with both optional and non-optional types.
  * [CTAD](https://en.cppreference.com/w/cpp/language/class_template_argument_deduction) doesn't work through aliases until C++20, so any code like `util::Optional(EXPRESSION)` without the `<Type>` template arguments will need to switch to using `std::optional(Expression)` directly.
  * Aliases and helpers not used by core were removed. If needed by SDKs they can be added back, but it would probably be better to just use the `std::` names instead.
  * The method `std::optional<T>::value()` requires a higher deployment target on iOS than we currently target because it throws exceptions when used incorrectly. You should use `*opt` and `opt->method()` instead to access the value anyway.
  * C++ overload resolution rules are always fun. Since `std::optional` is implicitly convertable from more types, it is possible that this will generate new ambiguous overload errors in overload sets that include optionals. If you have any vexing compiler errors due to this change, please ask the core team for help.

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Add support for running the object store tests and some of the benchmarks on iOS ([PR #5577](https://github.com/realm/realm-core/pull/5577)).
* MemRef is now trivially copyable.
* Fix bloaty CI test that is currently failing on Ubuntu. ([PR #5650](https://github.com/realm/realm-core/pull/5650))
* Fix benchmark tests that are failing on Ubuntu and MacOS. ([PR #5656](https://github.com/realm/realm-core/pull/5656))
* We no longer swap left and right in queries produced by the parser and we now have optimization for right hand side being constant ([#5685](https://github.com/realm/realm-core/pull/5685))

----------------------------------------------

# 12.3.0 Release notes

### Enhancements
* Allow flexible sync with discard local client resets. ([#5404](https://github.com/realm/realm-core/pull/5404))
* Allow flexible sync with recovery client resets. ([#5562](https://github.com/realm/realm-core/issues/5562))
* Add `Results::sectioned_results` which allows Results to be accessed in sections specified by a comparison function. (PR [#5403](https://github.com/realm/realm-core/pull/5403))

### Fixed
* Fix a UBSan failure when mapping encrypted pages.
* Improved performance of sync clients during integration of changesets with many small strings (totalling > 1024 bytes per changeset) on iOS 14, and devices which have restrictive or fragmented memory. ([#5614](https://github.com/realm/realm-core/issues/5614))
* Fixed a bug that prevented the detection of tables being changed to or from asymmetric during migrations. ([#5603](https://github.com/realm/realm-core/pull/5603), since v12.1.0)

### Breaking changes
* In Realm JS, the client reset callback can result in the fatal error `Realm accessed on incorrect thread`. Using a thread safe reference instead of Realm instance fixes the issue. (Issue [realm/realm-js#4410](https://github.com/realm/realm-js/issues/4410))

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Delete everything related to the metered allocations feature used for server monitoring.
* Fixes potential issue where an exception could be hidden by subsequent assertion ([#5615](https://github.com/realm/realm-core/pull/5615)).
* Support Nightly builds in Jenkins. ([#5626](https://github.com/realm/realm-core/issues/5626))

----------------------------------------------

# 12.2.0 Release notes

### Enhancements
* Changed the signature of `Realm::async_cancel_transaction` to return a boolean indicating whether the removal of the scheduled callback was successful (true) or not (false). Previously, the method returned void. (PR [#5546](https://github.com/realm/realm-core/pull/5546))

### Fixed
* Fixed an exception "key not found" during client reset recovery if a list had local moves or deletes and the base object was also deleted. ([#5593](https://github.com/realm/realm-core/issues/5593) since the introduction of recovery in v11.16.0)
* Fixed a segfault in sync compiled by MSVC 2022. ([#5557](https://github.com/realm/realm-core/pull/5557), since 12.1.0)
* Fix a data race when opening a flexible sync Realm (since v12.1.0).
* Fixed a missing backlink removal when setting a Mixed from a TypedLink to null or any other non-link value. Users may have seen exception of "key not found" or assertion failures such as `mixed.hpp:165: [realm-core-12.1.0] Assertion failed: m_type` when removing the destination link object. ([#5574](https://github.com/realm/realm-core/pull/5573), since the introduction of Mixed in v11.0.0)
* Asymmetric sync now works with embedded objects. (Issue [#5565](https://github.com/realm/realm-core/issues/5565), since v12.1.0)
* Fixed an issue on Windows that would cause high CPU usage by the sync client when there are no active sync sessions. (Issue [#5591](https://github.com/realm/realm-core/issues/5591), since the introduction of Sync support for Windows)

### Breaking changes
* `realm_sync_before_client_reset_func_t` and `realm_sync_after_client_reset_func_t` in the C API now return a boolean value to indicate whether the callback succeeded or not, which signals to the sync client that a fatal error occurred. (PR [#5564](https://github.com/realm/realm-core/pull/5564))

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Upgraded to Catch from v2.13.8 to v3.0.1. ([#5559](https://github.com/realm/realm-core/pull/5559))
* Exception with ArrayMove instruction on list of links with dangling links ([#5576](https://github.com/realm/realm-core/issues/5576))

----------------------------------------------

# 12.1.0 Release notes

### Enhancements
* The sync client will gracefully handle compensating write error messages from the server and pass detailed info to the SDK's sync error handler about which objects caused the compensating write to occur. ([#5528](https://github.com/realm/realm-core/pull/5528))
* Support for asymmetric sync. Tables can be marked as Asymmetric when opening the realm. Upon creation, asymmetric objects are sync'd unidirectionally. ([#5505](https://github.com/realm/realm-core/pull/5505))
* Creating an object for a class that has no subscriptions opened for it will now throw a `NoSubscriptionForWrite` exception ([#5488](https://github.com/realm/realm-core/pull/5488)).

### Fixed
* Added better comparator for `realm_user_t` and `realm_flx_sync_subscription_t` when using `realm_equals`. (Issue [#5522](https://github.com/realm/realm-core/issues/5522)).
* Changed `realm_sync_session_handle_error_for_testing` in order to support all SDKs. (Issue [#5550](https://github.com/realm/realm-core/issues/5550)).
* FLX sync subscription state changes will now correctly be reported after sync progress is reported ([#5553](https://github.com/realm/realm-core/pull/5553), since v12.0.0)

### Breaking changes
* Removed scheduler argument to the C API `realm_*_add_notification_callback` functions, because it wasn't actually used. (PR [#5541](https://github.com/realm/realm-core/pull/5541)).
* Merged the `realm_sync_upload_completion_func_t` and the `realm_sync_download_completion_func_t` typedefs in the C API because they were identical. The new typedef is `realm_sync_wait_for_completion_func_t`. (PR [#5548](https://github.com/realm/realm-core/pull/5548))

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* The release package for Apple platforms is now built with Xcode 13 and the SPM package requires Xcode 13. ([5538](https://github.com/realm/realm-core/pull/5538))
* The sync protocol is now version 6.

----------------------------------------------

# 12.0.0 Release notes

### Enhancements
* Expose `SyncSession::OnlyForTesting::handle_error` in the C API. ([#5507](https://github.com/realm/realm-core/issues/5507))
* Greatly improve the performance of `Realm::get_number_of_versions()` and `RealmConfig::max_number_of_active_versions` on iOS. ([#5530](https://github.com/realm/realm-core/pull/5530)).

### Fixed
* In RQL 'NONE x BETWEEN ...' and 'ANY x BETWEEN ...' had incorrect behavior, so it is now disallowed ([#5508](https://github.com/realm/realm-core/issues/5508), since v11.3.0)
* `SyncManager::path_for_realm` now allows custom file names for Flexible Sync enabled Realms. (Issue [#5473](https://github.com/realm/realm-core/issues/5473)).
* Fix ignoring ordering for queries passed into sync subscriptions in the C API. (Issue [#5504](https://github.com/realm/realm-core/issues/5504)).
* Fix adding Flx Sync error codes to the C API. (Issue [#5519](https://github.com/realm/realm-core/issues/5519)).
* OT may have failed with an assertion in debug builds for FLX sync bootstrap messages because changesets were being sorted by version number, which does not increase within a bootstrap. ([#5527](https://github.com/realm/realm-core/pull/5527))
* Partially fix a performance regression in write performance on Apple platforms. Committing an empty write transaction is ~10x faster than 11.17.0, but still slower than pre-11.8.0 due to using more crash-safe file synchronization (since v11.8.0). (Swift issue [#7740](https://github.com/realm/realm-swift/issues/7740)).
* FLX sync will now ensure that a bootstrap from the server will only be applied if the entire bootstrap is received - ensuring there are no orphaned objects as a result of changing the read snapshot on the server ([#5331](https://github.com/realm/realm-core/pull/5331))

### Breaking changes
* Bump the SharedInfo version to 12. This requires update of any app accessing the file in a multiprocess scenario, including Realm Studio.

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Evergreen builders for MacOS now build with Xcode 13.1 on MacOS 11.0

----------------------------------------------

# 11.17.0 Release notes

### Enhancements
* Move the implementation of the Audit API to the open-source repo and update it to work with MongoDB Realm. ([#5436](https://github.com/realm/realm-core/pull/5436))
* Expose delete app user for C API. ([#5490](https://github.com/realm/realm-core/issues/5490))
* Expose an API to get the app from user in the C API. ([#5478](https://github.com/realm/realm-core/issues/5478))

### Fixed
* C API `realm_user_get_all_identities` does not support identity id deep copy. ([#5467](https://github.com/realm/realm-core/issues/5467))

### Breaking changes
* `realm::Realm::Config` has been renamed to `realm::RealmConfig`. ([#5436](https://github.com/realm/realm-core/pull/5436))
* C API `realm_get_class_keys`, `realm_get_class_properties`, `realm_get_property_keys`, `realm_app_get_all_users`, `realm_user_get_all_identities` will immediately return and report how big the SDK allocated array should be, if no enough space is found to accomadate core's array data. No `realm_error_t` is going to be set if memory is not copied. ([#5430](https://github.com/realm/realm-core/issues/5430))
* `realm_app_sync_client_get_default_file_path_for_realm` should not have app as input argument C API.([#5486](https://github.com/realm/realm-core/issues/5486))

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

----------------------------------------------

# 11.16.0 Release notes

### Enhancements
* Adding recovery mode to new automatic client reset handling. In this mode, local unsynced changes which would otherwise be lost during a client reset are replayed on the seamlessly reset Realm. ([#5323](https://github.com/realm/realm-core/pull/5323))
* Client reset in recovery mode is controlled by the server's protocol 4 json error format. ([#5382](https://github.com/realm/realm-core/pull/5382))
* Added `Realm::convert` which consolidates `Realm::write_copy` and `Realm::export_to`. Also added to the C API. ([#5432](https://github.com/realm/realm-core/pull/5432))
* Expose client reset functionalities for C API. ([#5425](https://github.com/realm/realm-core/issues/5425))
* Add missing `userdata` and `userdata_free` arguments to `realm_sync_on_subscription_set_state_change_async` ([#5438](https://github.com/realm/realm-core/pull/5438))
* Added callbacks for freeing userdata used in callbacks set on RealmConfiguration via C API. ([#5222](https://github.com/realm/realm-core/issues/5222))
* Expose Subscription properties on C API. ([#5454](https://github.com/realm/realm-core/pull/5454))
* Added in the C API the possibility for SDKs to catch user code callback excpetions, store them in core and retrieve via `realm_get_last_error()` ([#5406](https://github.com/realm/realm-core/issues/5406))
* Erase Subscription by id for C API. ([#5475](https://github.com/realm/realm-core/issues/5475))
* Erase and Find Subscription by Results for C API. ([#5470](https://github.com/realm/realm-core/issues/5470))

### Fixed
* C API client reset callbacks don't leak the `realm_t` parameter. ([#5464](https://github.com/realm/realm-core/pull/5464))
* The sync client may have sent a corrupted upload cursor leading to a fatal error from the server due to an uninitialized variable. ([#5460](https://github.com/realm/realm-core/pull/5460), since v11.14.0)
* The realm_async_open_task_start() in C API was not really useful as the received realm reference could not be transferred to another thread. ([#5465](https://github.com/realm/realm-core/pull/5465), since v11.5.0)
* FLX sync would not correctly resume syncing if a bootstrap was interrupted ([#5466](https://github.com/realm/realm-core/pull/5466), since v11.8.0)

### Breaking changes
* Extra `realm_free_userdata_func_t` parameter added on some realm_config_set_... functions in the C API. The userdata will be freed when the config object is freed ([#5452](https://github.com/realm/realm-core/pull/5452)).

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Sync protocol version bumped to 4. ([#5382](https://github.com/realm/realm-core/pull/5382))
* Internal sync metadata tables were abstracted into a new schema management framework and their schema versions are now tracked in the `sync_internal_schemas` table. ([#5455](https://github.com/realm/realm-core/pull/5455))

----------------------------------------------

# 11.15.0 Release notes

### Enhancements
* `App::link_user()` and `App::delete_user()` now correctly report `ClientErrorCode::user_not_found` and `ClientErrorCode::user_not_logged_in` instead of only using `ClientErrorCode::user_not_found` for both error cases. ([#5402](https://github.com/realm/realm-core/issues/5402))
* Avoid leaking unresolved mixed links for Lst<Mixed>. ([#5418](https://github.com/realm/realm-core/pull/5418))
* Add support for embedded objects in the C API. ([#5408](https://github.com/realm/realm-core/issues/5408))
* Added `realm_object_to_string()` support for C API. ([#5414](https://github.com/realm/realm-core/issues/5414))
* Added `ObjectStore::rename_property()` support for C API. ([#5424]https://github.com/realm/realm-core/issues/5424)
* Removed deprecated sync protocol errors `disabled_session` and `superseded`. ([#5421]https://github.com/realm/realm-core/issues/5421)
* Support `realm_results_t` and `realm_query_t` for `realm_sync_subscription_set_insert_or_assign` in the c_api. ([#5431]https://github.com/realm/realm-core/issues/5431)
* Added `access_token()` and `refresh_token()` to C API. ([#5419]https://github.com/realm/realm-core/issues/5419)

### Fixed
* Adding an object to a Set, deleting the parent object, and then deleting the previously mentioned object causes crash ([#5387](https://github.com/realm/realm-core/issues/5387), since 11.0.0)
* Synchronized Realm files which were first created using SDK version released in the second half of August 2020 would be redownloaded instead of using the existing file, possibly resulting in the loss of any unsynchronized data in those files (since v11.6.1).

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* The Xcode toolchain no longer explicitly sets `CMAKE_OSX_ARCHITECTURES`. This was a problem with the latest Xcode release complaining about explicit mentions of `i386`.
* The query parser build will no longer attempt to run Bison or Flex when building realm-core as a submodule.

----------------------------------------------

# 11.14.0 Release notes

### Enhancements
* Added a new flag to `CollectionChangeSet` to indicate when collections are cleared. ([#5340](https://github.com/realm/realm-core/pull/5340))
* Added auth code and id token support for google c_api ([#5347](https://github.com/realm/realm-core/issues/5347))
* Added AppCredentials::serialize_as_json() support for c_api ([#5348](https://github.com/realm/realm-core/issues/5348))
* Added `App::close_all_sync_sessions` static method and `SyncManager::close_all_sessions` method ([#5411](https://github.com/realm/realm-core/pull/5411))

### Fixed
* Fixed potential future bug in how async write/commit used encryption ([#5369](https://github.com/realm/realm-core/pull/5369))
* Fixed various corruption bugs when encryption is used. Issues caused by not locking a mutex when needed. Since v11.8.0. Fixes meta-issue https://github.com/realm/realm-core/issues/5360: (https://github.com/realm/realm-core/issues/5332, https://github.com/realm/realm-swift/issues/7659, https://github.com/realm/realm-core/issues/5230, https://github.com/realm/realm-core/issues/5190, https://github.com/realm/realm-swift/issues/7640, https://github.com/realm/realm-js/issues/4428, https://github.com/realm/realm-java/issues/7652, https://github.com/realm/realm-js/issues/4358)
* Changeset upload batching did not calculate the accumulated size correctly, resulting in "error reading body failed to read: read limited at 16777217 bytes" errors from the server when writing large amounts of data ([#5373](https://github.com/realm/realm-core/pull/5373), since 11.13.0).

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

----------------------------------------------

# 11.13.0 Release notes

### Enhancements
* Sync changesets waiting to be uploaded to the server are now compressed, reducing the disk space needed when large write transactions are performed while offline or limited in bandwidth. This bumps the sync history schema version, meaning that synchronized Realms written by this version cannot be opened by older versions. Older Realms are seamlessly upgraded and local Realms are uneffected. ([PR 5260](https://github.com/realm/realm-core/pull/5260)).

### Fixed
* Fixed a potential crash if a sync session is stopped in the middle of a `DiscardLocal` client reset. ([#5295](https://github.com/realm/realm-core/issues/5295), since v11.5.0)
* Opening an encrypted Realm while the keychain is locked on macOS would crash ([Swift #7438](https://github.com/realm/realm-swift/issues/7438)).
* Updating subscription while refreshing the access token would crash ([#5343](https://github.com/realm/realm-core/issues/5343), since v11.8.0)
* Fix several race conditions in SyncSession related to calling `update_configuration()` on one thread while using the SyncSession on another thread. It does not appear that it was possible to hit the broken scenarios via the SDKs public APIs.

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Upgraded OpenSSL from v1.1.1g to v1.1.1n.
* Catch2 was updated to 2.13.8. ([#5327](https://github.com/realm/realm-core/pull/5327))
* Mutating a committed MutableSubscriptionSet will throw a LogicError. ([#5162](https://github.com/realm/realm-core/pull/5162))
* Update test harness to use app.schemas endpoint. ([#5333](https://github.com/realm/realm-core/pull/5333))

----------------------------------------------

# 11.12.0 Release notes

### Enhancements
* Support for new SchemaMode::HardResetFile added. ([#4782](https://github.com/realm/realm-core/issues/4782))
* Support for keypaths in change notifications added to C-API ([#5216](https://github.com/realm/realm-core/issues/5216))
* Release of callback functions done through realm_release() ([#5217](https://github.com/realm/realm-core/issues/5217))
* Supporting flexible sync for C-API. ([#5110](https://github.com/realm/realm-core/issues/5110), Since v11.10.0)

### Fixed
* Query parser would not accept "in" as a property name ([#5312](https://github.com/realm/realm-core/issues/5312))
* Application would sometimes crash with exceptions like 'KeyNotFound' or assertion "has_refs()". Other issues indicating file corruption may also be fixed by this. The one mentioned here is the one that lead to solving the problem. ([#5283](https://github.com/realm/realm-core/issues/5283), since v6.0.0)
* Refreshing the user profile after the app has been destroyed leads to assertion failure ([#5238](https://github.com/realm/realm-core/issues/5238))

### Breaking changes
* SchemaMode::ResetFile renamed to SchemaMode::SoftResetFile.
* Token type changed for registration of callback functions for changes on Realm and Schema. The functions are unregistered be releasing the token through 'realm_release()'.

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* The previous release broke the `REALM_ENABLE_SYNC` CMake option on Windows in that OpenSSL was always a required dependency, regardless of whether Sync was enabled or not. This has been fixed.

----------------------------------------------

# 11.11.0 Release notes

### Enhancements
* Added support for configuring caching of Realms in C-API. ([#5275](https://github.com/realm/realm-core/issues/5275))

### Fixed
* The Swift package set the linker flags on the wrong target, resulting in linker errors when SPM decides to build the core library as a dynamic library ([Swift #7266](https://github.com/realm/realm-swift/issues/7266)).
* Wrong error code returned from C-API on MacOS ([#5233](https://github.com/realm/realm-core/issues/5233), since v10.0.0)
* Mixed::compare() used inconsistent rounding for comparing a Decimal128 to a float, giving different results from comparing those values directly ([#5270](https://github.com/realm/realm-core/pull/5270)).
* Calling Realm::async_begin_transaction() from within a write transaction while the async state was idle would hit an assertion failure (since v11.10.0).
* Fix issue with scheduler being deleted on wrong thread. This caused async open to hang in eg. realm-js. ([#5287](https://github.com/realm/realm-core/pull/5287), Since v11.10.0)
* Waiting for upload after opening a bundled realm file could hang. ([#5277](https://github.com/realm/realm-core/issues/5277), since v11.5.0)

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

----------------------------------------------

# 11.10.0 Release notes

### Enhancements
* Add `util::UniqueFunction::target()`, which does the same thing as `std::function::target()`.
* 'filter', 'sort', 'distinct', and 'limit' functions on Results added to the C-API. ([#5099](https://github.com/realm/realm-core/issues/5099))
* Set and Dictionary supported in the C-API. ([#5031](https://github.com/realm/realm-core/issues/5031))
* realm_query_get_description added to the C-API. ([#5106](https://github.com/realm/realm-core/issues/5106))
* `Realm::begin_transaction()` no longer spawns a worker thread or asynchronously acquires the write lock on Apple platforms.
* Added `realm_get_schema_version` to the C-API. ([#5236](https://github.com/realm/realm-core/issues/5236))
* Added support for configuring 'in_memory' option in C-API. ([#5249](https://github.com/realm/realm-core/issues/5249))
* Added support for configuring 'custom FIFO file paths' in C-API. ([#5250](https://github.com/realm/realm-core/issues/5250))

### Fixed
* If a list of objects contains links to objects not included in the synchronized partition, the indices contained in CollectionChangeSet for that list may be wrong ([#5164](https://github.com/realm/realm-core/issues/5164), since v10.0.0)
* Sending a QUERY message may fail with `Assertion failed: !m_unbind_message_sent` ([#5149](https://github.com/realm/realm-core/pull/5149), since v11.8.0)
* Subscription names correctly distinguish an empty string from a nullptr ([#5160](https://github.com/realm/realm-core/pull/5160), since v11.8.0)
* Converting floats/doubles into Decimal128 would yield imprecise results ([#5184](https://github.com/realm/realm-core/pull/5184), since v6.1.0)
* Fix some warnings when building with Xcode 13.3.
* Using accented characters in class and field names may end the session ([#5196](https://github.com/realm/realm-core/pull/5196), since v10.2.0)
* Several issues related to async write fixed ([#5183](https://github.com/realm/realm-core/pull/5183), since v11.8.0)
  - Calling `Realm::invalidate()` from inside `BindingContext::did_change()` could result in write transactions not being persisted to disk
  - Calling `Realm::close()` or `Realm::invalidate()` from the async write callbacks could result in crashes
  - Asynchronous writes did not work with queue-confined Realms.
  - Releasing all references to a Realm while an asynchronous write was in progress would sometimes result in use-after-frees.
  - Throwing exceptions from asynchronous write callbacks would result in crashes or the Realm being in an invalid state.
  - Using asynchronous writes from multiple threads had several race conditions and would often crash (since v11.8.0).
* Fixed a fatal sync error "Automatic recovery failed" during DiscardLocal client reset if the reset notifier callbacks were not set to something. ([#5223](https://github.com/realm/realm-core/issues/5223), since v11.5.0)
* Fixed running file action BackUpThenDeleteRealm which could silently fail to delete the Realm as long as the copy succeeded. If this happens now, the action is changed to DeleteRealm. ([#5180](https://github.com/realm/realm-core/issues/5180), since the beginning)
* Fix an error when compiling a watchOS Simulator target not supporting Thread-local storage ([#7623](https://github.com/realm/realm-swift/issues/7623), since v11.7.0)
* Check, when opening a realm, that in-memory realms are not encrypted ([#5195](https://github.com/realm/realm-core/issues/5195))
* Changed parsed queries using the `between` operator to be inclusive of the limits, a closed interval instead of an open interval. This is to conform to the published documentation and for parity with NSPredicate's definition. ([#5262](https://github.com/realm/realm-core/issues/5262), since the introduction of this operator in v11.3.0)
* Using a SubscriptionSet after closing the realm could result in a use-after-free violation ([#5208](https://github.com/realm/realm-core/issues/5208), since v11.6.1)

### Breaking changes
* Renamed SubscriptionSet::State::Superceded -> Superseded to correct typo.
* Renamed SubscriptionSet::SupercededTag -> SupersededTag to correct typo.

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* SubscriptionStore's should be initialized with an implict empty SubscriptionSet so users can wait for query version zero to finish synchronizing. ((#5166)[https://github.com/realm/realm-core/pull/5166])
* Fixed `Future::on_completion()` and added missing testing for it ((#5181)[https://github.com/realm/realm-core/pull/5181])
* GenericNetworkTransport::send_request_to_server()'s signature has been changed to `void(Request&& request, util::UniqueFunction<void(const Response&)>&& completion)`. Subclasses implementing it will need to be updated.
* `MongoClient` had a mixture of functions which took `void(Optional<T>, Optional<AppError>)` callbacks and functions which took `void(Optional<AppError>, Optional<T>)` callbacks. They now all have the error parameter last. Most of the error-first functions other than `call_function()` appear to have been unused.
* Many functions which previously took `std::function` parameters now take `util::UniqueFunction` parameters. This generally should not require SDK-side changes, but there may be opportunities for binary-size improvements by propagating this change outward in the SDK code.
* realm_results_snapshot actually implemented. ([#5154](https://github.com/realm/realm-core/issues/5154))
* Updated apply_to_state_command tool to support query based sync download messages. ([#5226](https://github.com/realm/realm-core/pull/5226))
* Fixed an issue that made it necessary to always compile with the Windows 8.1 SDK to produce binaries able to run on Windows 8.1. ([#5247](https://github.com/realm/realm-core/pull/5247))

----------------------------------------------

# 11.9.0 Release notes

### Enhancements
* Support exporting data from a local realm to a synchonized realm. ([#5018](https://github.com/realm/realm-core/issues/5018))
* Add ability to delete a sync user from a MongoDB Realm app. (PR [#5153](https://github.com/realm/realm-core/pull/5153))

### Fixed
* UserIdentity metadata table grows indefinitely. ([#5152](https://github.com/realm/realm-core/issues/5152), since v10.0.0)
* Improved error messaging when opening a Realm with `IncompatibleHistories` when translating file exceptions ([#5161](https://github.com/realm/realm-core/pull/5161), since v6.0.0).

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

----------------------------------------------

# 11.8.0 Release notes

### Enhancements
* Support arithmetric operations (+, -, *, /) in query parser. Operands can be properties and/or constants of numeric types (integer, float, double or Decimal128). You can now say something like "(age + 5) * 2 > child.age".
* Support for asynchronous transactions added. (PR [#5035](https://github.com/realm/realm-core/pull/5035))
* FLX sync now properly handles write_not_allowed client reset errors ([#5147](https://github.com/realm/realm-core/pull/5147))
* `realm_delete_files` added to the C API, equivalent to `Realm::delete_files`. ([#5127](https://github.com/realm/realm-core/pull/5127))

### Fixed
* The release package was missing several headers (since v11.7.0).
* The sync client will now drain the receive queue when send fails with ECONNRESET - ensuring that any error message from the server gets received and processed. ([#5078](https://github.com/realm/realm-core/pull/5078))
* Schema validation was missing for embedded objects in sets, resulting in an unhelpful error being thrown if the user attempted to define one.
* Opening a Realm with a schema that has an orphaned embedded object type performed an extra empty write transaction (since v11.0.0).
* Freezing a Realm with a schema that has orphaned embeeded object types threw a "Wrong transactional state" exception (since v11.5.0).
* SyncManager::path_for_realm now returns a default path when FLX sync is enabled ([#5088](https://github.com/realm/realm-core/pull/5088))
* Having links in a property of Mixed type would lead to ill-formed JSON output when serializing the database. ([#5125](https://github.com/realm/realm-core/issues/5125), since v11.0.0)
* FLX sync QUERY messages are now ordered with UPLOAD messages ([#5135](https://github.com/realm/realm-core/pull/5135))
* Fixed race condition when waiting for state change notifications on FLX subscription sets that may have caused a hang ([#5146](https://github.com/realm/realm-core/pull/5146))
* SubscriptionSet::to_ext_json() now handles empty subscription sets correctly ([#5134](https://github.com/realm/realm-core/pull/5134))

### Breaking changes
* FLX SubscriptionSet type split into SubscriptionSet and MutableSubscriptionSet to add type safety ([#5092](https://github.com/realm/realm-core/pull/5092))

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* 'Obj TableView::get(size_t)' removed. Use 'TableView::get_object' instead.
* Fix issue compiling in debug mode for iOS.
* FLX sync now sends the query version in IDENT messages along with the query body ([#5093](https://github.com/realm/realm-core/pull/5093))
* Errors in C API no longer store or expose a std::exception_ptr. The comparison of realm_async_error_t now compares error code vs object identity. ([#5064](https://github.com/realm/realm-core/pull/5064))
* The JSON output is slightly changed in the event of link cycles. Nobody is expected to rely on that.
* Future::get_async() no longer requires its callback to be marked noexcept. ([#5130](https://github.com/realm/realm-core/pull/5130))
* SubscriptionSet no longer holds any database resources. ([#5150](https://github.com/realm/realm-core/pull/5150))
* ClientHistoryImpl::integrate_server_changesets now throws instead of returning a boolean to indicate success ([#5118](https://github.com/realm/realm-core/pull/5118))
* Send empty access token in bind messages. Remove refresh message from sync protocol. ([#5151](https://github.com/realm/realm-core/pull/5151))

----------------------------------------------

# 11.7.0 Release notes

### Enhancements
* Added `realm_query_append_query` to the C-API. ([#5067](https://github.com/realm/realm-core/issues/5067))

### Fixed
* The client reset callbacks now pass out SharedRealm objects instead of TransactionRefs. ([#5048](https://github.com/realm/realm-core/issues/5048), since v11.5.0)
* A client reset in DiscardLocal mode would revert to Manual mode on the next restart of the session. ([#5050](https://github.com/realm/realm-core/issues/5050), since v11.5.0)
* A client reset in DiscardLocal mode would assert if the server added embedded object tables. ([#5069](https://github.com/realm/realm-core/issues/5069), since v11.5.0)
* `@sum` and `@avg` queries on Dictionaries of floats or doubles used too much precision for intermediates, resulting in incorrect rounding (since v10.2.0).
* Change the exception message for calling refresh on an immutable Realm from "Continuous transaction through DB object without history information." to "Can't refresh a read-only Realm." ([#5061](https://github.com/realm/realm-core/issues/5061), old exception message was since 10.7.2 via https://github.com/realm/realm-core/pull/4688).
* The client reset callbacks have changed so that the pre and post Realm state are passed to the 'after' callback and thee 'before' callback only has the local state. ([#5066](https://github.com/realm/realm-core/issues/5066), since 11.5.0).
* In the C-API, query `count()` did not apply descriptors such as limit/distinct. ([#5073](https://github.com/realm/realm-core/issues/5073), since the beginning of the C-API in v10.4.0)
* Queries of the form "link.collection.@sum = 0" where `link` is null matched when `collection` was a List or Set, but not a Dictionary ([#5080](https://github.com/realm/realm-core/pull/5080), since v11.0.0).

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* The 'power' unary operator template to be used in a query expression is removed
* Renamed ClientResyncMode::SeamlessLoss -> DiscardLocal.
* Updated sync client to be able to open connections to FLX sync-enabled apps in baas ([#5009](https://github.com/realm/realm-core/pull/5009))
* SubscriptionSet::erase now returns the correct itererator for the "next" subscription ([#5053](https://github.com/realm/realm-core/pull/5053))
* SubscriptionSet::insert_or_assign now returns an iterator pointing to the correct subscription ([#5049](https://github.com/realm/realm-core/pull/5049))
* `SimulatedFailure` mmap handling was not thread-safe.
* The overloads for arithmetric operations removed.
* Rename SchemaMode::ReadOnlyAlternative to ReadOnly. ([#5070](https://github.com/realm/realm-core/issues/5070))
* Subscriptions for FLX sync now have a unique ID ([#5054](https://github.com/realm/realm-core/pull/5054))
* Assigning an anonymous subscription will no longer overwrite a named subscription ([#5076](https://github.com/realm/realm-core/pull/5076))

----------------------------------------------

# 11.6.1 Release notes

### Fixed
* A sync user's Realm was not deleted when the user was removed if the Realm path was too long such that it triggered the fallback hashed name (this is OS dependant but is 300 characters on linux). ([#4187](https://github.com/realm/realm-core/issues/4187), since the introduction of hashed paths in object-store before monorepo, circa early v10 (https://github.com/realm/realm-object-store/pull/1049))
* Don't keep trying to refresh the access token if the client's clock is more than 30 minutes fast. ([#4941](https://github.com/realm/realm-core/issues/4941))
* Don't sleep the sync thread artificially if an auth request fails. This could be observed as a UI hang on js applications when sync tries to connect after being offline for more than 30 minutes. ([realm-js#3882](https://github.com/realm/realm-js/issues/3882), since sync to MongoDB was introduced in v10.0.0)

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* Added type to manage flexible sync subscriptions. [#5005](https://github.com/realm/realm-core/pull/5005)
* Added support for notifications on flexible sync subscription state changes [#5027](https://github.com/realm/realm-core/pull/5027)

----------------------------------------------

# 11.6.0 Release notes

### Enhancements
* Adding `Object::set_property_value(ContextType&, const Property&, ValueType, CreatePolicy)` for SDKs which have performed the property lookup.

### Fixed
* SyncManager had some inconsistent locking which could result in data races and/or deadlocks, mostly in ways that would never be hit outside of tests doing very strange things ([#4999](https://github.com/realm/realm-core/pull/4999),since v10.0.0).
* Streaming download notifiers reported incorrect values for transferrable bytes ([#5008](https://github.com/realm/realm-core/pull/5008), since 11.5.2).

### Compatibility
* Fileformat: Generates files with format v22. Reads and automatically upgrade from fileformat v5.

-----------

### Internals
* ConstTableView and TableView are merged into just TableView. TableView::front(), TableView::back(), TableView::remove() and TableView::remove_last() function are removed as they were not used outside tests.
* The client file UUID has been removed as it was no longer being used for anything.
* Reduce the peak memory usage of changeset uploading by eliminating an extra copy of each changeset which was held in memory.

----------------------------------------------

# 11.5.2 Release notes

### Fixed
* The Swift package could not be imported in Xcode 12.5 ([#4997](https://github.com/realm/realm-core/pull/4997), since v11.5.0).
* Sync progress notifiers would not trigger when the downloadable bytes size would equal 0. ([#4989](https://github.com/realm/realm-core/pull/4989))

-----------

### Internals
* Unit tests now run in parallel by default.

----------------------------------------------

# 11.5.1 Release notes

### Fixed
* Calling `remove_all_target_rows()` on a newly constructed `LnkSet` dereferenced a null pointer ([#4972](https://github.com/realm/realm-core/pull/4972), since v11.5.0).
* Mutating a Set via one accessor and then calling `remove_all_target_rows()` on a different accessor for the same Set could potentially result in a stale version of the Set being used ([#4972](https://github.com/realm/realm-core/pull/4972), since v11.0.0).
* Restore support for calling snapshot() on non-Object collections ([#4971](https://github.com/realm/realm-core/issues/4971), since 11.5.0)

-----------

### Internals
* Stopped running RaspberryPi testing in CI ([#4967](https://github.com/realm/realm-core/issues/4967)

----------------------------------------------

# 11.5.0 Release notes

### Enhancements
* Add a "seamless loss" mode to client reset where local changes are overwritten by the server's state without having to handle the reset manually. ([#4809](https://github.com/realm/realm-core/pull/4809))
* Added methods to freeze and thaw realms, objects, results and lists. ([#4658](https://github.com/realm/realm-core/pull/4658))
* Added `Realm::sync_session()` getter as a convenient way to get the sync session for a realm instance. ([#4925](https://github.com/realm/realm-core/pull/4925))
* Added `realm_object_get_or_create_with_primary_key` to C-API. ([#4595](https://github.com/realm/realm-core/issues/4595))
* Added notification callbacks for realm changed and schema changed events to the C API. ([#4940](https://github.com/realm/realm-core/pull/4940))
* Added the `GenericNetworkTransport` API to C API ([#4942](https://github.com/realm/realm-core/pull/4942)).
* Added the `App` functionality (except access to Atlas collections) to the C API. ([#4951](https://github.com/realm/realm-core/pull/4951))

### Fixed
* Fixed the creation of a frozen realm, that was created using the full schema instead of the schema of the originating realm. ([#4939](https://github.com/realm/realm-core/issues/4939))
* Fixed forgetting to insert a backlink when inserting a mixed link directly using Table::FieldValues. ([#4899](https://github.com/realm/realm-core/issues/4899) since the introduction of Mixed in v11.0.0)
* Using "sort", "distinct", or "limit" as field name in query expression would cause an "Invalid predicate" error ([#7545](https://github.com/realm/realm-java/issues/7545) since v10.1.2)
* Crash when quering with 'Not()' followed by empty group. ([#4168](https://github.com/realm/realm-core/issues/4168) since v1.0.0)
* Change Apple/Linux temp dir created with `util::make_temp_dir()` to default to the environment's TMPDIR if available. Make `TestFile` clean up the directories it creates when finished. ([#4921](https://github.com/realm/realm-core/issues/4921))
* Fixed a rare assertion failure or deadlock when a sync session is racing to close at the same time that external reference to the Realm is being released. ([#4931](https://github.com/realm/realm-core/issues/4931))
* Fixed an assertion failure when opening a sync Realm with a user who had been removed. Instead an exception will be thrown. ([#4937](https://github.com/realm/realm-core/issues/4937), since v10)
* Fixed a rare segfault which could trigger if a user was being logged out while the access token refresh response comes in. ([#4944](https://github.com/realm/realm-core/issues/4944), since v10)
* Fixed a bug where progress notifiers continue to be called after the download of a synced realm is complete. ([#4919](https://github.com/realm/realm-core/issues/4919))
* Fixed an issue where the release process was only publishing armeabi-v7a Android binaries. ([#4952](https://github.com/realm/realm-core/pull/4952), since v10.6.0)
* Allow for EPERM to be returned from fallocate(). This improves support for running on Linux environments with interesting filesystems, like AWS Lambda. Thanks to [@ztane](https://github.com/ztane) for reporting and suggesting a fix. ([#4957](https://github.com/realm/realm-core/issues/4957))
* Fixed an issue where the Mac Catalyst target was excluded from the `REALM_HAVE_SECURE_TRANSPORT` macro in the Swift Package. This caused `'SSL/TLS protocol not supported'` to be thrown as an exception if Realm Sync is used. ([#7474](https://github.com/realm/realm-cocoa/issues/7474))
* Fixed a user being left in the logged in state when the user's refresh token expires. ([#4882](https://github.com/realm/realm-core/issues/4882), since v10)
* Calling `size()` on a Results newly constructed via `.as_results().distinct()` on a Collection would give the size of the Collection rather than the distinct count. ([Cocoa #7481](https://github.com/realm/realm-cocoa/issues/7481), since v11.0.0).
* Calling `clear()` on a Results newly constructed via `.as_results().distinct()` on a Collection would delete all objects in the Collection rather than just the distinct objects in the Results (since v11.0.0).
* Calling `clear()` on a Results constructed via `.as_results().distinct()` on a Collection after calling `get()` or `size()` would not re-evaluate the distinct until after the next mutation to the table occurred.

### Breaking changes
* `App::Config::transport_factory` was replaced with `App::Config::transport`. It should now be an instance of `GenericNetworkTransport` rather than a factory for making instances. This allows the SDK to control which thread constructs the transport layer. ([#4903](https://github.com/realm/realm-core/pull/4903))
* Several typedefs in `realm/object-store/sync/sync_session.hpp` were renamed ([#4924](https://github.com/realm/realm-core/pull/4924)):
  * `realm::SyncSession::SyncSessionStateCallback` -> `realm::SyncSession::StateChangeCallback`
  * `realm::SyncSession::ConnectionStateCallback` -> `realm::SyncSession::ConnectionStateChangeCallback`
  * `realm::SyncSessionTransactCallback` -> `realm::SyncSession::TransactionCallback`
  * `realm::SyncProgressNotifierCallback` -> `realm::SyncSession::ProgressNotifierCallback`
  * `realm::SyncSession::NotifierType` -> `realm::SyncSession::ProgressDirection`
* `realm::SyncClientConfig::logger_factory` was changed to a `std::function` that returns logger instances. The abstract class `SyncLoggerFactory` was removed. ([#4926](https://github.com/realm/realm-core/pull/4926))
* C-API function `realm_object_create_with_primary_key` will now fail if an object already exists with given primary key. ([#4936](https://github.com/realm/realm-core/pull/4936))

-----------

### Internals
* Cleaned out some old server tools and add the remaining ones to the default build.
* App can now use bundled realm without getting "progress error" from server.
* Refactored the wire protocol message parsing to not use std::istream or goto's for flow control.

----------------------------------------------

# 11.4.1 Release notes

### Fixed
* Fixed issue when opening a synced realm is prevented by assertion "m_state == SyncUser::State::LoggedIn". ([#4875](https://github.com/realm/realm-core/issues/4875))
* Fixed slow teardown of Realm by immediately freeing shared pointers to scheduler on realm closure ([realm/realm-js#3620](https://github.com/realm/realm-js/issues/3620), [realm/realm-js#2993](https://github.com/realm/realm-js/issues/2993))

----------------------------------------------

# 11.4.0 Release notes

### Enhancements
* ThreadSafeReference no longer pins the source transaction version for anything other than a Results backed by a Query. ([#4857](https://github.com/realm/realm-core/pull/4857))
* A ThreadSafeReference to a Results backed by a collection can now be created inside a write transaction as long as the collection was not created in the current write transaction.
* Synchronized Realms are no longer opened twice, cutting the address space and file descriptors used in half. ([#4839](https://github.com/realm/realm-core/pull/4839))
* A method Obj::get_any(StringData) has been added.

### Fixed
* Failing to refresh the access token due to a 401/403 error will now correctly emit a sync error with `ProtocolError::bad_authentication` rather than `ProtocolError::permission_denied`. ([#4881](https://github.com/realm/realm-core/pull/4881), since 11.0.4)
* If an object with a null primary key was deleted by another sync client, the exception `KeyNotFound: No such object` could be triggered. ([#4885](https://github.com/realm/realm-core/issues/4885) since unresolved links were introduced in v10.0.0)
* Fix a nonexistent file warning when building with Swift Package Manager (since 11.3.1).

-----------

### Internals
* SyncConfig no longer has an encryption_key field, and the key from the parent Realm::Config is used instead.

----------------------------------------------

# 11.3.1 Release notes

### Fixed
* Fixed "Invalid data type" assertion failure in the sync client when applying an AddColumn instruction for a Mixed column when that column already exists locally. ([#4873](https://github.com/realm/realm-core/issues/4873), since v11.0.0)
* Fixed a crash when accessing the lock file during deletion of a Realm on Windows if the folder does not exist. ([#4855](https://github.com/realm/realm-core/pull/4855))

----------------------------------------------

# 11.3.0 Release notes

### Enhancements
* InstructionApplier exceptions now contain information about what object/changeset was being applied when the exception was thrown. ([#4836](https://github.com/realm/realm-core/issues/4836))
* Added ServiceErrorCode for wrong username/password.  ([#4581](https://github.com/realm/realm-core/issues/4581))
* Query parser now accepts "BETWEEN" operator. Can be used like "Age BETWEEN {20, 60}" which means "'Age' must be in the open interval ]20;60[". ([#4268](https://github.com/realm/realm-core/issues/4268))

### Fixed
* Fixes prior_size history corruption when replacing an embedded object in a list ([#4845](https://github.com/realm/realm-core/issues/4845))
* Updated the Catch2 URL to include '.git' extension ([#4608](https://github.com/realm/realm-core/issues/4608))

### Breaking changes
* None.

-----------

### Internals
* Added Status/StatusWith types for representing errors/exceptions as values ([#4859](https://github.com/realm/realm-core/issues/4859))
* ApplyToState tool now exits with a non-zero exit code if download message application fails.

----------------------------------------------

# 11.2.0 Release notes

### Enhancements
* Shift more of the work done when first initializing a collection notifier to the background worker thread rather than doing it on the main thread. ([#4826](https://github.com/realm/realm-core/issues/4826))
* Report if Realm::delete_files() actually deleted the Realm file, separately from if any errors occurred. ([#4771](https://github.com/realm/realm-core/issues/4771))
* Add Dictionary::try_erase(), which returns false if the key is not found rather than throwing. ([#4781](https://github.com/realm/realm-core/issues/4781))

### Fixed
* Fixed a segfault in sync compiled by MSVC 2019. ([#4624](https://github.com/realm/realm-core/issues/4624), since v10.4.0)
* Removing a change callback from a Results would sometimes block the calling thread while the query for that Results was running on the background worker thread. ([#4826](https://github.com/realm/realm-core/issues/4826), since v11.1.0).
* Object observers did not handle the object being deleted properly, which could result in assertion failures mentioning "m_table" in ObjectNotifier ([#4824](https://github.com/realm/realm-core/issues/4824), since v11.1.0).
* Fixed a crash when delivering notifications over a nested hierarchy of lists of Mixed that contain links. ([#4803](https://github.com/realm/realm-core/issues/4803), since v11.0.0)
* Fixed key path filtered notifications throwing on null links and asserting on unresolved links. ([#4828](https://github.com/realm/realm-core/pull/4828), since v11.1.0)
* Fixed a crash when an object which is linked to by a Mixed is invalidated (sync only). ([#4828](https://github.com/realm/realm-core/pull/4828), since v11.0.0)
* Fixed a rare crash when setting a mixed link for the first time which would trigger if the link was to the same table and adding the backlink column caused a BPNode split. ([#4828](https://github.com/realm/realm-core/pull/4828), since v11.0.0)
* Accessing an invalidated dictionary will throw a confusing error message ([#4805](https://github.com/realm/realm-core/issues/4805), since v11.0.0).

-----------

### Internals
* Remove ClientResyncMode::Recover option. This doesn't work and shouldn't be used by anyone currently.
* Remove sync support for CLIENT_VERSION request; it isn't used.
* Remove the STATE message request and handling from the client and test sever. The current server was always responding with empty state anyhow. Clients will now consider receiving this message as an error because they never request it.
* Handling for async open, involving state files is mostly removed.
* When a client reset occured, we would create a metadata Realm to handle state download but this isn't needed anymore so it is removed.
* Realm::write_copy_without_client_file_id and Realm::write_copy are merged. For synchronized realms, the file written will have the client file ident removed. Furthermore it is required that all local changes are synchronized with the server before the copy can. The function will throw if there are pending uploads.

----------------------------------------------

# 11.1.1 Release notes

### Enhancements
* Improve performance of creating collection notifiers for Realms with a complex schema. In the SDKs this means that the first run of a synchronous query, first call to observe() on a collection, or any call to find_async() will do significantly less work on the calling thread.
* Improve performance of calculating changesets for notifications, particularly for deeply nested object graphs and objects which have List or Set properties with small numbers of objects in the collection.

### Fixed
* Opening a synchronized Realm created with a version older than v11.1.0 would fail due to a schema change in the metadata Realm which did not bump the schema version (since v11.1.0).

### Breaking changes
* None.

-----------

### Internals
* Removed the unused ChangesetCooker from the sync client. ([#4811](https://github.com/realm/realm-core/pull/4811))

----------------------------------------------

# 11.1.0 Release notes

### Enhancements
* Change notifications can now be filtered via a key path. This keypath is passed via the `add_notification_callback` on `Object`, `Set`, `List` and `Result`.
  If such a key path was provided when adding a notification callback it will only ever be executed when a changed property was covered by this filter.
  Multiple key path filters can be provided when adding a single notification callback.

### Fixed
* User profile now correctly persists between runs.

### Breaking changes
* User profile fields are now methods instead of fields.

-----------

### Internals
* Added UniqueFunction type ([#4815](https://github.com/realm/realm-core/pull/4815))

----------------------------------------------

# 11.0.4 Release notes

### Fixed
* Fixed an assertion failure when listening for changes to a list of primitive Mixed which contains links. ([#4767](https://github.com/realm/realm-core/issues/4767), since the beginning of Mixed v11.0.0)
* Fixed an assertion failure when listening for changes to a dictionary or set which contains an invalidated link. ([#4770](https://github.com/realm/realm-core/pull/4770), since the beginning of v11)
* Fixed an endless recursive loop that could cause a stack overflow when computing changes on a set of objects which contained cycles. ([#4770](https://github.com/realm/realm-core/pull/4770), since the beginning of v11)
* Add collision handling to Dictionary implementation ([#4776](https://github.com/realm/realm-core/issues/4776), since the beginning of Dictionary v11.0.0)
* Fixed a crash after clearing a list or set of Mixed containing links to objects ([#4774](https://github.com/realm/realm-core/issues/4774), since the beginning of v11)
* Fixed a recursive loop which would eventually crash trying to refresh a user app token when it had been revoked by an admin. Now this situation logs the user out and reports an error. ([#4745](https://github.com/realm/realm-core/issues/4745), since v10.0.0).
* Fixed a race between calling realm::delete_files and concurent opening of the realm file.([#4768](https://github.com/realm/realm-core/pull/4768))
* Fixed a retain cycle on Apple devices that would prevent the SyncClient from ever being stopped. This is likely only relevant for Unity applications which would observe that as the editor hanging on macOS after script recompilation. ([realm-dotnet#2482](https://github.com/realm/realm-dotnet/issues/2482))

-----------

### Internals
* Renamed targets on SPM file from `Core`, `QueryParser` to `RealmCore` and `RealmQueryParser` to avoid conflicts with other libraries products. ([#4763](https://github.com/realm/realm-core/issues/4763), since v10.8.1)

----------------------------------------------

# 11.0.3 Release notes

### Fixed
* Destroying the SyncManager in ObjectStore no longer causes segfaults in async callbacks in SyncUser/SyncSession ([4615](https://github.com/realm/realm-core/issues/4615))
* None.
* When replacing an embedded object, we must emit a sync instruction that sets the link to the embedded object to null so that it is properly cleared. ([#4740](https://github.com/realm/realm-core/issues/4740)
* Fix crashes when sync logging is set to trace or higher (since 11.0.2).
* Fix crash when changing nullability of primary key column ([#4759](https://github.com/realm/realm-core/issues/4759), since v11.0.0-beta.6)

-----------

### Internals
* Releases for Apple platforms are now built with Xcode 12.2.

----------------------------------------------

# 11.0.2 Release notes

### Fixed
* Fixed the string based query parser not supporting integer constants above 32 bits on a 32 bit platform. ([realm-js #3773](https://github.com/realm/realm-js/issues/3773), since v10.4.0 with the introduction of the new query parser)
* Fixed issues around key-based dictionary notifications holding on to a transaction ([#4744](https://github.com/realm/realm-core/issues/4744), since v11.0.1)

-----------

### Internals
* Refactor the string formatting logic for logging, reducing the compiled size of the library. May require adoption downstream.

----------------------------------------------

# 11.0.1 Release notes

### Fixed
* Clearing a Dictionary with a key-based change listener attached will result in a crash. ([#4737](https://github.com/realm/realm-core/issues/4737), since v11.0.0)

----------------------------------------------

# 11.0.0 Release notes

### Enhancements
* Added the functionality to delete files for a given SharedRealm, unlocking  ([realm-dotnet#386](https://github.com/realm/realm-dotnet/issues/386)).

### Fixed
* Setting a collection with a nullable value type to null would hit an assertion failure instead of clearing the collection.
* Fixed an incorrect detection of multiple incoming links in a migration when changing a table to embedded and removing a link to it at the same time. ([#4694](https://github.com/realm/realm-core/issues/4694) since 10.0.0-beta.2)
* Fixed build failure with gcc-11
* Added merge rule between SetInsert/SetErase and Clear to prevent diverging states after a Clear instruction on the same path. ([#4720](https://github.com/realm/realm-core/issues/4720))
* Made Linux implementation of ExternalCommitHelper work with new versions of Linux that [changed epoll behavior](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=6a965666b7e7475c2f8c8e724703db58b8a8a445), including Android 12 ([#4666](https://github.com/realm/realm-core/issues/4666))

### Breaking changes
* The key-based notifications for Dictionary is changed so that the deletions are also reported by their keys. ([#4723](https://github.com/realm/realm-core/pull/4723))

-----------

### Internals
* DB::write_copy will not use write transaction

----------------------------------------------

# 11.0.0-beta.6 Release notes


### Fixed
* Performance regression for some scenarios of writing/creating objects
  with a primary key. ([#4522](https://github.com/realm/realm-core/issues/4522))
* Observing a dictionary holding links to objects would crash. ([#4711](https://github.com/realm/realm-core/issues/4711), since v11.0.0-beta.0)

### Breaking changes
* The file format version is bumped from 21 to 22. The file format is changed in the way that we now - again - have search indexes on primary key columns. This is required as we now stop deriving the ObjKeys from the primary key values, but just use an increasing counter value. This has the effect that all new objects will be created in the same cluster and not be spread out as they would have been before. It also means that upgrading from file format version 11 and earlier formats will be much faster.

----------------------------------------------

# 11.0.0-beta.5 Release notes

### Enhancements
* Added support for creating queries through a dictionary of links, in the same way of LnkLst and LnkSet. ([#4548](https://github.com/realm/realm-core/issues/4593))

### Fixed
* Added the names requested by .NET to match mixed type based querying. ([#4353](https://github.com/realm/realm-core/issues/4353))
* Deleting objects pointed to by a Dictionary may result in a crash.  ([#4632](https://github.com/realm/realm-core/issues/4632), since v11.0.0-beta.0)
* Comparing dictionaries from different realms could sometimes return equality ([#4629](https://github.com/realm/realm-core/issues/4629), since v11.0.0-beta.0)
* Calling Results::snapshot on dictionary values collection containing null returns incorrect results ([#4635](https://github.com/realm/realm-core/issues/4635), Not in any release)
* Making a aggregate query on Set of Objects would cause a crash in the parser ([#4633](https://github.com/realm/realm-core/issues/4633), since v11.0.0-beta.0)
* Copying a Query constrained by a Dictionary would crash ([#4640](https://github.com/realm/realm-core/issues/4640), since v11.0.0-beta.0)
* Changed the average of an empty set from 0 to null. ([#4678](https://github.com/realm/realm-core/issues/4678), since v11.0.0-beta.0)
* Changed the sum of an empty dictionary from null to 0. ([#4678](https://github.com/realm/realm-core/issues/4678), since v11.0.0-beta.0)
* Some way - used in realm-cocoa - of constructing a Query could result in a use-after-free violation ([#4670](https://github.com/realm/realm-core/pull/4670), since v11.0.0-beta.0)
* Fix the order of a sorted set of mixed values. ([#4662](https://github.com/realm/realm-core/pull/4662), since v11.0.0-beta.0)
* Use same rules for handling numeric values in sets as MongoDB uses. All numeric values are compared using the value irregardless of the type.  ([#4686](https://github.com/realm/realm-core/pull/4686), since v11.0.0-beta.0)
* Fixed an incorrect detection of multiple incoming links in a migration when changing a table to embedded and removing a link to it at the same time. ([#4694](https://github.com/realm/realm-core/issues/4694) since 10.0.0-beta.2)

-----------

### Internals
* Includes changes from versions 10.6.1, 10.7.0, 10.7.1 and 10.7.2.
* The `ListColumnAggregate` has been renamed to `CollectionColumnAggregate` which now supports dictionaries instead of having a separate type `DictionaryAggregate` which is now removed. This allows `ColumnsCollection` min/max/sum/avg to work on dictionary column queries as well.


----------------------------------------------

# 11.0.0-beta.4 Release notes

### Enhancements
* Support equal/not_equal on Query, where column is Link/LinkList and value is a Mixed containing a link.

### Fixed
* Dictionary key validation now works on strings without nul termination. ([#4589](https://github.com/realm/realm-core/issues/4589))
* Fixed queries for min/max of a Mixed column not returning the expected value when using the Query::minimum_mixed(). ([#4571](https://github.com/realm/realm-core/issues/4571), since v11.0.0-beta.2)
* Fix collection notification reporting for modifications. This could be observed by receiving the wrong indices of modifications on sorted or distinct results, or notification blocks sometimes not being called when only modifications have occured. ([#4573](https://github.com/realm/realm-core/pull/4573) since v6).

----------------------------------------------

# 11.0.0-beta.3 Release notes

### Enhancements
* Update the to_json() function to properly encode UUIDs, mixed types, dictionaries, and sets as MongoDB extended JSON.
* Remove type coercion on bool and ObjectId when doing queries.
* Pass CreatePolicy to `unbox<T>` from the object accessor.
* We now make a backup of the realm file prior to any file format upgrade. The backup is retained for 3 months.
  Backups from before a file format upgrade allows for better analysis of any upgrade failure. We also restore
  a backup, if a) an attempt is made to open a realm file whith a "future" file format and b) a backup file exist
  that fits the current file format. ([#4166](https://github.com/realm/realm-core/pull/4166))
* Make conversion of Decimal128 to/from string work for numbers with more than 19 significant digits. ([#4548](https://github.com/realm/realm-core/issues/4548))

### Fixed
* Query::links_to() took wrong argument for Mixed columns. ([#4585](https://github.com/realm/realm-core/issues/4585))
* Clearing a set of links would result in crash when target objects are deleted.([#4579](https://github.com/realm/realm-core/issues/4579))

-----------

### Internals
* Add additional debug validation to file map management that will hopefully catch cases where we unmap something which is still in use.

----------------------------------------------

# 11.0.0-beta.2 Release notes

### Enhancements
* Adding overloads of the set methods that operate on collections ([#4226](https://github.com/realm/realm-core/issues/4226))
* Support 'add_int' on a mixed property.
* UUID allowed as partition value ([#4500](https://github.com/realm/realm-core/issues/4500))
* The error message when the intial steps of opening a Realm file fails is now more descriptive.
* Allow UTF8 encoded characters in property names in query parser ([#4467](https://github.com/realm/realm-core/issues/4467))
* Allow unresolved links to be inserted in Dictionary.

### Fixed
* Fixed queries of min/max/sum/avg on list of primitive mixed. ([#4472](https://github.com/realm/realm-core/pull/4472), never before working)
* Fixed sort and distinct on dictionary keys, and distinct on dictionary values. ([#4496](https://github.com/realm/realm-core/pull/4496))
* Fixed getting a dictionary value which is null via `object_store::Dictionary::get<T>(key)` for any type other than Mixed. ([#4496](https://github.com/realm/realm-core/pull/4496))
* Fixed notifications tracking modifications across links in a dictionary or set containing Mixed(TypedLink) values. ([#4505](https://github.com/realm/realm-core/pull/4505)).
* Fixed the notifiers causing an exception `KeyNotFound("No such object");` if a dictionary or set of links of a single type also had a link column in the linked table. ([#4465](https://github.com/realm/realm-core/issues/4465)).
* Added missing stubs for Mixed aggregation: min/max/sum/avg at the Query level. ([4526](https://github.com/realm/realm-core/issues/4526))
* Classes names "class_class_..." was not handled correctly in KeyPathMapping ([#4480](https://github.com/realm/realm-core/issues/4480))
* Syncing large Decimal128 values will cause "Assertion failed: cx.w[1] == 0" ([#4519](https://github.com/realm/realm-core/issues/4519), since v10.0.0)
* Fixed the query parser rejecting <,>,<=,>= queries on UUID types. ([#4475](https://github.com/realm/realm-core/issues/4475), since v10.0.0)
* Fixed min/max/sum/avg not working on dictionaries of Mixed types when more than one type existed as values. ([#4546](https://github.com/realm/realm-core/issues/4546), since v11.0.0-beta.0)
* Creating dictionaries with null links through SDK context would crash. ([#4537](https://github.com/realm/realm-core/issues/4537))
* An exception is now thrown, if a null link is attempted inserted in a set of links ([#4540](https://github.com/realm/realm-core/issues/4540), since v10.0.0)
* Equality queries between mixed and object was not supported in query parser ([#4531](https://github.com/realm/realm-core/issues/4531), since v10.0.0)
* Syncing sets of objects was not supported ([#4538](https://github.com/realm/realm-core/issues/4538), since v10.0.0)
* Potential/unconfirmed fix for crashes associated with failure to memory map (low on memory, low on virtual address space). For example ([#4514](https://github.com/realm/realm-core/issues/4514)).
* Invoking Set<Binary>::clear() - directly or indirectly -  could sometimes leave the database in an inconsistent state leading to a crash.
* Fixed name aliasing not working in sort/distinct clauses of the query parser. ([#4550](https://github.com/realm/realm-core/issues/4550), never before working).
* Fix assertion failures such as "!m_notifier_skip_version.version" or "m_notifier_sg->get_version() + 1 == new_version.version" when performing writes inside change notification callbacks. Previously refreshing the Realm by beginning a write transaction would skip delivering notifications, leaving things in an inconsistent state. Notifications are now delivered recursively when needed instead. ([Cocoa #7165](https://github.com/realm/realm-cocoa/issues/7165)).

-----------

### Internals
* Collection aggregates min/max/sum/avg have changed to return an optional Mixed value. This is to distinguish between returning a valid Mixed null value, and none indicating unsupported for this type. ([#4472](https://github.com/realm/realm-core/pull/4472))
* Includes fixes merged from core v10.5.4.
* Android: build with NDK r22. Make `-Wl,-gc-sections` an interface linker flag, which reduces code size because Core is compiled `-fdata-sections` and `-ffunction-sections`. Use `-Oz` even when we enable link-time optimization (previously we used with `-O2`). ([#4407](https://github.com/realm/realm-core/pull/4407))

----------------------------------------------

# 11.0.0-beta.1 Release notes

### Enhancements
* Dictionary can now contain embedded objects

### Fixed
* Mixed property can now be indexed ([#4342](https://github.com/realm/realm-core/issues/4342))
* Dictionary of Objects need to have a nullable value part, but that would lead to inconsistency in schema and cause an exception ([#4344](https://github.com/realm/realm-core/issues/4344))
* Subscribing for notifications on Dictionaries of Objects results in access violation ([#4346](https://github.com/realm/realm-core/issues/4346))
* AccessViolationException on removing a key from dictionary of objects  ([#4358](https://github.com/realm/realm-core/issues/4358))
* Mixed: crash when removing/setting a null-valued position of a Mixed list or set ([#4304](https://github.com/realm/realm-core/issues/4304))
* Results based on dictionary keys will return wrong value from 'get_type'. ([#4365](https://github.com/realm/realm-core/issues/4365))
* If a Dictionary contains Objects, those can not be returned by Results::get<Obj> ([#4374](https://github.com/realm/realm-core/issues/4374))
* Change listeners not triggered on certain Mixed attribute changes ([#4404](https://github.com/realm/realm-core/issues/4404))
* Calling Dictionary::find_any() on a virgin dictionary will crash (([#4438](https://github.com/realm/realm-core/issues/4438))
* `Dictionary::as_results()` on a dictionary of links gave a Results which did not support most object operations.

### Breaking changes
* Sync protocol version increased to 3. This version adds support for the new data types introduced in file format version 21.

-----------

### Internals
* object_store::Dictionary has now get_keys() and get_values() that will return a Results object giving access to the keys and values resp. ([#4233](https://github.com/realm/realm-core/issues/4233))

----------------------------------------------

# 11.0.0-beta.0 Release notes

### Fixed
* When updating a Dictionary value, wrong notifications are sent out. ([4318](https://github.com/realm/realm-core/issues/4318))

### Breaking changes
* File format version bumped to 21. In this version we support new basic datatypes 'UUID' and 'Mixed', and we support Set and Dictionary collections.

----------------------------------------------

# 10.8.1 Release notes

### Fixed
* Fixed the string based query parser not supporting integer constants above 32 bits on a 32 bit platform. ([realm-js #3773](https://github.com/realm/realm-js/issues/3773), since v10.4.0 with the introduction of the new query parser)
* When replacing an embedded object, we must emit a sync instruction that sets the link to the embedded object to null so that it is properly cleared. ([#4740](https://github.com/realm/realm-core/issues/4740)

-----------

### Internals
* Releases for Apple platforms are now built with Xcode 12.2.

----------------------------------------------

# 10.8.0 Release notes

### Enhancements
* Added the functionality to delete files for a given SharedRealm, unlocking  ([realm-dotnet#386](https://github.com/realm/realm-dotnet/issues/386)).

### Fixed
* Fixed an incorrect detection of multiple incoming links in a migration when changing a table to embedded and removing a link to it at the same time. ([#4694](https://github.com/realm/realm-core/issues/4694) since 10.0.0-beta.2)
* Fixed build failure with gcc-11
* Added merge rule between SetInsert/SetErase and Clear to prevent diverging states after a Clear instruction on the same path. ([#4720](https://github.com/realm/realm-core/issues/4720))
* Made Linux implementation of ExternalCommitHelper work with new versions of Linux that [changed epoll behavior](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=6a965666b7e7475c2f8c8e724703db58b8a8a445), including Android 12 ([#4666](https://github.com/realm/realm-core/issues/4666))

-----------

### Internals
* DB::write_copy will not use write transaction

----------------------------------------------

# 10.7.2 Release notes

### Fixed
* Destruction of the TableRecycler at exit  was unordered compared to other threads running. This could lead to crashes, some with the
  TableRecycler at the top of the stack ([#4600](https://github.com/realm/realm-core/issues/4600), since v6)
* Calling `Realm::get_synchronized_realm()` while the session was waiting for an access token would crash ([PR #4677](https://github.com/realm/realm-core/pull/4677), since v10.6.1).
* Fixed errors related to "uncaught exception in notifier thread: N5realm11KeyNotFoundE: No such object". This could happen in a sync'd app when a linked object was deleted by another client. ([realm-js#3611](https://github.com/realm/realm-js/issues/3611), since v6.1.0-alpha.5)
* Changed the behaviour of `Object::get_property_value(Context)` when fetching an unresolved link from returning an empty Object, to returning null which is consistent with how this method behaves on a null link. ([#4687](https://github.com/realm/realm-core/pull/4687), since v6.1.0-alpha.5)
* Opening a metadata realm with the wrong encryption key or different encryption configuration will remove that metadata realm and create a new metadata realm using the new key or configuration. [#4285](https://github.com/realm/realm-core/pull/4285)
* A read-only Realm does not support `ThreadSafeReference` ([Cocoa #5475](https://github.com/realm/realm-cocoa/issues/5475)).

----------------------------------------------

# 10.7.1 Release notes

### Fixed
* Restored original behavior of Realm::write_copy() as it had breaking pre-conditions. New behavior now in Realm::write_copy_without_client_file_id(). ([#4674](https://github.com/realm/realm-core/pull/4674), since v10.7.0)
* Realm::write_copy() of a copy would fail on a non-synced realm ([#4672](https://github.com/realm/realm-core/pull/4672), since v10.7.0)

----------------------------------------------

# 10.7.0 Release notes

### Enhancements
* Realm::write_copy() will now exclude client file identification from the file written. The file can be used as a starting point for synchronizing a new client. The function will throw if client is not fully synced  with the server. The function will need to be able to make a write transaction. ([#4659](https://github.com/realm/realm-core/issues/4659))

### Fixed
* Building for Apple platforms gave availability warnings for clock_gettime(). The code giving the warning is currently used only on Windows, so this could not actually cause crashes at runtime ([#4614](https://github.com/realm/realm-core/pull/4614) Since v10.6.0).
* Fixed the android scheduler not being supplied which could result in `[realm-core-10.6.1] No built-in scheduler implementation for this platform. Register your own with Scheduler::set_default_factory()` ([#4660](https://github.com/realm/realm-core/pull/4660) Since v10.6.1).
* Fixed a crash that could happen adding a upload/download notification for a sync session. ([#4638](https://github.com/realm/realm-core/pull/4638#issuecomment-832227309) since v10.6.1).

-----------

### Internals
* A separate sync version is removed.

----------------------------------------------

# 10.6.1 Release notes

### Fixed
* Proactively check the expiry time on the access token and refresh it before attempting to initiate a sync session. This prevents some error logs from appearing on the client such as: "ERROR: Connection[1]: Websocket: Expected HTTP response 101 Switching Protocols, but received: HTTP/1.1 401 Unauthorized" ([RCORE-473](https://jira.mongodb.org/browse/RCORE-473), since v10.0.0)
* Fix a race condition which could result in a skipping notifications failing to skip if several commits using notification skipping were made in succession (since v6.0.0).

-----------

### Internals
* The `util::Scheduler` interface was refactored to support multiple implementations existing in a single binary. This allows multiple SDKs targeting the same platform but different language runtimes to use the same build of Core. Current SDKs are not affected by this change.
* The function `util::Scheduler::set_default_factory()` now works on all platforms, and can be used to override the platform-default scheduler implementation.
* The function `util::Scheduler::get_frozen()` was deprecated in favor of `util::Scheduler::make_frozen()`, which has the same behavior.
* The DB class now supports opening a realm file on a read-only file system.
  ([#4582](https://github.com/realm/realm-core/pull/4582))

----------------------------------------------

# 10.6.0 Release notes

### Enhancements
* We now make a backup of the realm file prior to any file format upgrade. The backup is retained for 3 months.
  Backups from before a file format upgrade allows for better analysis of any upgrade failure. We also restore
  a backup, if a) an attempt is made to open a realm file whith a "future" file format and b) a backup file exist
  that fits the current file format.
  ([#4166](https://github.com/realm/realm-core/pull/4166))
* The error message when the intial steps of opening a Realm file fails is now more descriptive.
* Make conversion of Decimal128 to/from string work for numbers with more than 19 significant digits. ([#4548](https://github.com/realm/realm-core/issues/4548))

### Fixed
* Potential/unconfirmed fix for crashes associated with failure to memory map (low on memory, low on virtual address space). For example ([#4514](https://github.com/realm/realm-core/issues/4514)).
* Fixed name aliasing not working in sort/distinct clauses of the query parser. ([#4550](https://github.com/realm/realm-core/issues/4550), never before working).
* Fix assertion failures such as "!m_notifier_skip_version.version" or "m_notifier_sg->get_version() + 1 == new_version.version" when performing writes inside change notification callbacks. Previously refreshing the Realm by beginning a write transaction would skip delivering notifications, leaving things in an inconsistent state. Notifications are now delivered recursively when needed instead. ([Cocoa #7165](https://github.com/realm/realm-cocoa/issues/7165)).
* Fix collection notification reporting for modifications. This could be observed by receiving the wrong indices of modifications on sorted or distinct results, or notification blocks sometimes not being called when only modifications have occured. ([#4573](https://github.com/realm/realm-core/pull/4573) since v6).

### Breaking changes
* None.

-----------

### Internals
* Android: build with NDK r22. Make `-Wl,-gc-sections` an interface linker flag, which reduces code size because Core is compiled `-fdata-sections` and `-ffunction-sections`. Use `-Oz` even when we enable link-time optimization (previously we used with `-O2`). ([#4407](https://github.com/realm/realm-core/pull/4407))
* Add additional debug validation to file map management that will hopefully catch cases where we unmap something which is still in use.
* Add new benchmark used for performance characterization.

----------------------------------------------

# 10.5.6 Release notes

### Fixed
* Classes names "class_class_..." was not handled correctly in KeyPathMapping ([#4480](https://github.com/realm/realm-core/issues/4480))
* Syncing large Decimal128 values will cause "Assertion failed: cx.w[1] == 0" ([#4519](https://github.com/realm/realm-core/issues/4519), since v10.0.0)
* Avoid race condition leading to possible hangs on windows. ([realm-dotnet#2245](https://github.com/realm/realm-dotnet/issues/2245))

----------------------------------------------

# 10.5.5 Release notes

### Fixed
* During integration of a large amount of data from the server, you may get "Assertion failed: !fields.has_missing_parent_update()" ([#4497](https://github.com/realm/realm-core/issues/4497), since v6.0.0)

----------------------------------------------

# 10.5.4 Release notes

### Enhancements
* None.

### Fixed
* Fixed queries for constant null across links to an indexed property not returning matches when the link was null. ([#4460]https://github.com/realm/realm-core/pull/4460), since 5.23.6).
* Support upgrading from file format 5. ([#7089](https://github.com/realm/realm-cocoa/issues/7089), since v6.0.0)
* On 32bit devices you may get exception with "No such object" when upgrading to v10.* ([#7314](https://github.com/realm/realm-java/issues/7314), since v10.0.0)
* The notification worker thread would rerun queries after every commit rather than only commits which modified tables which could effect the query results if the table had any outgoing links to tables not used in the query ([#4456](https://github.com/realm/realm-core/pull/4456), since v6.0.0).
* Fix "Invalid ref translation entry [16045690984833335023, 78187493520]" assertion failure which could occur when using sync or multiple processes writing to a single Realm file. ([Cocoa #7086](https://github.com/realm/realm-cocoa/issues/7086), since v6.0.0.

-----------

### Internals
* Changed some query parser exceptions types around to be more consistent. ([#4474](https://github.com/realm/realm-core/pull/4474)).

----------------------------------------------

# 10.5.3 Release notes

### Fixed
* Fixed a conflict resolution bug related to the ArrayMove instruction, which could sometimes cause an "Invalid prior_size" exception to prevent synchronization ([#4436](https://github.com/realm/realm-core/pull/4436), since v10.3.0).
* Fix another bug which could lead to the assertion failures "!skip_version.version" if a write transaction was committed while the first run of a notifier with no registered observers was happening ([#4449](https://github.com/realm/realm-core/pull/4449), since v10.5.0).
* Skipping a change notification in the first write transaction after the observer was added could potentially fail to skip the notification (since v10.3.3).

-----------

### Internals
* Added query parser headers to release tarballs. ([#4448](https://github.com/realm/realm-core/pull/4448))

----------------------------------------------

# 10.5.2 Release notes

### Enhancements
* Performance of sorting on more than one property has been improved. Especially important if many elements match on the first property. Mitigates ([#7092](https://github.com/realm/realm-cocoa/issues/7092))

### Fixed
* Fixed a bug that prevented an ObjectSchema with incoming links from being marked as embedded during migrations. ([#4414](https://github.com/realm/realm-core/pull/4414))
* `Results::get_dictionary_element()` on frozen Results was not thread-safe.
* The Realm notification listener thread could sometimes hit the assertion failure "!skip_version.version" if a write transaction was committed at a very specific time (since v10.5.0).
* Fixed parsing queries comparing a link or list to an arguments of TypedLink. ([#4429](https://github.com/realm/realm-core/issues/4429), this never previously worked)
* Fixed `links_to` queries that searched for an object key in a list or set of objects that contained more than 1000 objects where sometimes an object might not be found. ([#4429](https://github.com/realm/realm-core/pull/4429), since v6.0.0)
* Added workaround for a case where upgrading an old file with illegal string would crash ([#7111](https://github.com/realm/realm-cocoa/issues/7111))

----------------------------------------------

# 10.5.1 Release notes

### Fixed
* Fixed property aliases not working in the parsed queries which use the `@links.Class.property` syntax. ([#4398](https://github.com/realm/realm-core/issues/4398), this never previously worked)
* Fix "Invalid ref translation entry" assertion failure which could occur when querying over a link after creating objects in the destination table.

----------------------------------------------

# 10.5.0 Release notes

### Enhancements
* Sync client now logs error messages received from server rather than just the size of the error message.
* Added class name substitution to KeyPathMapping for the query parser. ([#4326](https://github.com/realm/realm-core/issues/4326)).
* Errors returned from the server when sync WebSockets get closed are now captured and surfaced as a SyncError.
* Improve performance of sequential reads on a Results backed directly by a Table by 50x.

### Fixed
* Results::get() on a Results backed by a Table would give incorrect results if a new object was created at index zero in the source Table. ([Cocoa #7014](https://github.com/realm/realm-cocoa/issues/7014), since v6.0.0).
* New query parser breaks on argument substitution in relation to LinkList. ([#4381](https://github.com/realm/realm-core/issues/4381))
* During synchronization you might experience crash with 'Assertion failed: ref + size <= next->first' ([#4388](https://github.com/realm/realm-core/issues/4388))

### Breaking changes
* The SchemaMode::Additive has been replaced by two different modes: AdditiveDiscovered and AdditiveExplicit. The former should be used when the schema has been automatically discovered, and the latter should be used when the user has explicitly included the types in the schema. Different schema checks are enforced for each scenario. ([#4306](https://github.com/realm/realm-core/pull/4306))
* Revert change in `app::Response` ([4263](https://github.com/realm/realm-core/pull/4263))
* Notifications will now be triggered for empty collections whose source object has been deleted. ([#4228](https://github.com/realm/realm-core/issues/4228)).

-----------

### Internals
* On Android, the CMake build no longer sets -Oz explicitly for Release builds if `CMAKE_INTERPROCEDURAL_OPTIMIZATION` is enabled. Additionally, Android unit tests are built with LTO.
* On Android, fixed the build to link against the dynamic `libz.so`. CMake was choosing the static library, which is both undesirable and has issues on newer NDKs. ([#4376](https://github.com/realm/realm-core/pull/4376))
* ThreadSafeReference for Dictionary added

----------------------------------------------

# 10.4.0 Release notes

### Enhancements
* Query parser supports property names containing white space. White space characters must be excapes with a '\'.
* Query parser supports `@type` for filtering based on the stored type of a Mixed property. ([#4239](https://github.com/realm/realm-core/pull/4239))
* Rejects dictionary inserts / erases with keys that have a “.” or start with a “$”. ([#4247](https://github.com/realm/realm-core/pull/4247))
* Add support for @min and @max queries on Timestamp lists.
* Add support for comparing non-list link columns to other link columns in queries.
* Add `Set::delete_all`, `Set::is_subset_of`, `Set::is_superset_of`, `Set::intersects`, `Set::assign_intersection`, `Set::assign_union` & `Set::assign_difference` methods to `object_store::Set`.
* Dictionaries can be defined as nullable.

### Fixed
* Calling max/min/sum/avg on a List or Set may give wrong results ([#4252](https://github.com/realm/realm-core/issues/4252), since v10.0.0)
* Calling Table::clear() will in many cases not work for the data types introduced in v10.2.0. ([#4198](https://github.com/realm/realm-core/issues/4198), since v10.2.0)
* Fix `links_to()` queries on sets of objects. ([#4264](https://github.com/realm/realm-core/pull/4264)
* Operations like Set::assign_union() can fail on StringData sets, ([#4288](https://github.com/realm/realm-core/issues/4288)
* Windows `InterprocessCondVar` no longer crashes if destroyed on a different thread than created  ([#4174](https://github.com/realm/realm-core/issues/4174), since v10.3.3)
* Fix an issue when using `Results::freeze` across threads with different transaction versions. Previously, copying the `Results`'s tableview could result in a stale state or objects from a future version. Now there is a comparison for the source and desitnation transaction version when constructing `ConstTableView`, which will cause the tableview to reflect the correct state if needed ([#4254](https://github.com/realm/realm-core/pull/4254)).
* `@min` and `@max` queries on a list of float, double or Decimal128 values could match the incorrect value if NaN or null was present in the list (since 5.0.0).
* Fixed an issue where creating an object after file format upgrade may fail with assertion "Assertion failed: lo() <= std::numeric_limits<uint32_t>::max()" ([#4295](https://github.com/realm/realm-core/issues/4295), since v6.0.0)

### Breaking changes
* Support for IncludeDescriptor has been removed.
* The PEGTL based query parser has been replaced with a parser based on Flex/Bison. The interface to the parser has been changed.
* Add `status` property to `app::Response` to reflect the request result. Optional `body` or `error` property will store the corresponding value.

-----------

### Internals
* Crashes will now request to create a github issue instead of an email.

----------------------------------------------

# 10.3.3 Release notes

### Fixed
* `App::call_function` now catches invalid json parse exceptions during a client timeout.
* Fix a race condition which would lead to "uncaught exception in notifier thread: N5realm15InvalidTableRefE: transaction_ended" and a crash when the source Realm was closed or invalidated at a very specific time during the first run of a collection notifier ([#3761](https://github.com/realm/realm-core/issues/3761), since v6.0.0).
* Deleting and recreating objects with embedded objects may fail ([#4240](https://github.com/realm/realm-core/pull/4240), since v10.0.0)
* SSL certificate validation would fail due to a build misconfiguration on Windows ([#4213](https://github.com/realm/realm-core/issues/4213), since v10.2.0).
* Fix implementation of `InterprocessCondVar` on Windows to not hang in the face of process death. ([#4174](https://github.com/realm/realm-core/issues/4174))
* Snapshotting a list of objects may trigger an assertion ([#4114](https://github.com/realm/realm-core/issues/4114), since v10.0.0)

### Breaking changes
* The lock file format has changed, so a single Realm file may not be concurrently opened by processes running older and newer versions of Realm Database. Note that concurrently accessing a file with multiple versions of Realm is never officially supported.

----------------------------------------------

# 10.3.2 Release notes

### Fixed
* You may get assertion "n != realm::npos" when integrating changesets from the server. ([#4180](https://github.com/realm/realm-core/pull/4180), since v10.0.0)

-----------

### Internals
* Fixed a syntax error in the packaged `RealmConfig.cmake` which prevented it from being imported in CMake projects.
* The xcframework build was missing the arm64 slice for apple simulators.
* The non-xcframework Apple build once again includes fat libraries for the parser rather than separate device/simulator ones.

----------------------------------------------

# 10.3.1 Release notes

### Enhancements
* None.

### Fixed
* None.

### Breaking changes
* None.

-----------

### Internals
* Add missing header from packages (`realm/dictionary_cluster_tree.hpp`).

----------------------------------------------

# 10.3.0 Release notes

### Enhancements
* Add support for Google openId

### Fixed
* Fix an assertion failure when querying for null on a non-nullable string primary key property. ([#4060](https://github.com/realm/realm-core/issues/4060), since v10.0.0-alpha.2)
* Fix a use of a dangling reference when refreshing a user's custom data that could lead to a crash (since v10.0.0).
* Sync client: Upgrade to protocol version 2, which fixes a bug that would
  prevent eventual consistency during conflict resolution. Affected clients
  would experience data divergence and potentially consistency errors as a
  result. ([#4004](https://github.com/realm/realm-core/pull/4004))

### Breaking changes
* Sync client: The sync client now requires a server that speaks protocol
  version 2 (Cloud version `20201202` or newer).

-----------

### Internals
* Fix publishing the Cocoa xcframework release package to s3.
* Remove debug libraries from the Cocoa release packages.

----------------------------------------------

# 10.2.0 Release notes

### Enhancements
* Includes the open-sourced Realm Sync client, as well as the merged Object Store component.
* New data types: Mixed, UUID and TypedLink.
* New collection types: Set and Dictionary
* Enable mixed comparison queries between two columns of arbitrary types according to the Mixed::compare rules. ([#4018](https://github.com/realm/realm-core/pull/4018))
* Added `TableView::update_query()`

### Fixed
* Fix race potentially allowing frozen transactions to access incomplete search index accessors. (Since v6)
* Fix queries for null on non-nullable indexed integer columns returning results for zero entries. (Since v6)
* Fix queries for null on a indexed ObjectId column returning results for the zero ObjectId. (Since v10)
* Fix list of primitives for Optional<Float> and Optional<Double> always returning false for `Lst::is_null(ndx)` even on null values, (since v6.0.0).
* Fix several data races in App and SyncSession initialization. These could possibly have caused strange errors the first time a synchronized Realm was opened (sinrce v10.0.0).

### Breaking changes
* None.

-----------

### Internals
* Set::erase_null() would not properly erase a potential null value ([#4001](https://github.com/realm/realm-core/issues/4001), (not in any release))

----------------------------------------------

# 10.1.3 Release notes

### Enhancements
* Add arm64 slices to the macOS builds ([PR #3921](https://github.com/realm/realm-core/pull/3921)).

----------------------------------------------

# 10.1.4 Release notes

### Fixed
* You may get assertion "n != realm::npos" when integrating changesets from the server. ([#4180](https://github.com/realm/realm-core/pull/4180), since v10.0.0)

----------------------------------------------

# 10.1.3 Release notes

### Enhancements
* Add arm64 slices to the macOS builds ([PR #3921](https://github.com/realm/realm-core/pull/3921)).

----------------------------------------------

# 10.1.2 Release notes

### Fixed
* Issue fixed by release v6.2.1:
  * Files upgraded on 32-bit devices could end up being inconsistent resulting in "Key not found" exception to be thown. ([#6992](https://github.com/realm/realm-java/issues/6992), since v6.0.16)

----------------------------------------------

# 10.1.1 Release notes

### Fixed
* `Obj::set_list_values` inappropriately resizes list for LinkList causing LogicError to be thrown. ([#4028](https://github.com/realm/realm-core/issues/4028), since v6.0.0)
* Set the supported deployment targets for the Swift package, which fixes errors when archiving an iOS app which depends on it.
* Reenable filelock emulation on watchOS so that the OS does not kill the app when it is suspended while a Realm is open on watchOS 7 ([Cocoa #6861](https://github.com/realm/realm-cocoa/issues/6861), since v6.1.4).

----------------------------------------------

# 10.1.0 Release notes

### Enhancements
* Features added by release v6.2.0:
  * In cases where we use an outdated TableRef, we throw InvalidTableRef exception instead of NoSuchTable. NoSuchTable could be misleading as the table is most likely still there.

### Fixed
* Issues fixed by release v6.2.0:
  * Fix crash in case insensitive query on indexed string columns when nothing matches ([#6836](https://github.com/realm/realm-cocoa/issues/6836), since v6.0.0)
  * Fix list of primitives for Optional<Float> and Optional<Double> always returning false for `Lst::is_null(ndx)` even on null values, ([#3987](https://github.com/realm/realm-core/pull/3987), since v6.0.0).
  * Fix queries for the size of a list of primitive nullable ints returning size + 1. This applies to the `Query::size_*` methods (SizeListNode) and not query expression syntax (SizeOperator). ([#4016](https://github.com/realm/realm-core/pull/4016), since v6.0.0).

----------------------------------------------

# 10.0.0 Release notes

### Fixed
* Fix queries for null on non-nullable indexed integer columns returning results for zero entries. (Since v6)
* Fix queries for null on a indexed ObjectId column returning results for the zero ObjectId. (Since v10)
* If objects with incoming links are deleted on the server side and then later re-created it may lead to a crash. (Since v10.0.0-alpha.1)
* Upgrading from file format version 11 would crash with an assertion. ([#6847](https://github.com/realm/realm-cocoa/issues/6847). since v10.0.0-beta.0)

-----------

### Internals
* Uses OpenSSL version 1.1.1g. The prebuilt openssl libraries now have .tar.gz extension instead of .tgz.

----------------------------------------------

# 10.0.0-beta.9 Release notes

### Enhancements
* Features added by release v6.1.0 to 6.1.2:
  * Slightly improve performance of most operations which read data from the Realm file.
  * Allocating one extra entry in ref translation tables. May help finding memory mapping problems.
  * Greatly improve performance of NOT IN queries on indexed string or int columns.

### Fixed
* Fixed a ObjectId sometimes changing from null to ObjectId("deaddeaddeaddeaddeaddead") after erasing rows which triggers a BPNode merge, this can happen when there are > 1000 objects. (Since v10).
* Fixed an assertion failure when adding an index to a nullable ObjectId property that contains nulls. (since v10).
* Added missing `Table::find_first(ColKey, util::Optional<ObjectId>)`. ([#3919](https://github.com/realm/realm-core/issues/3919), since v10).
* Issues fixed by releases v6.1.0 to v6.1.2:
  * Rerunning an equals query on an indexed string column which previously had more than one match and now has one match would sometimes throw a "key not found" exception ([Cocoa #6536](https://github.com/realm/realm-cocoa/issues/6536), since 6.1.0-alpha.4),
  * When querying a table where links are part of the condition, the application may crash if objects has recently been added to the target table. ([Java #7118](https://github.com/realm/realm-java/issues/7118), since v6.0.0)
  * Possibly fix issues related to changing the primary key property from nullable to required. ([PR #3917](https://github.com/realm/realm-core/pull/3918)).

----------------------------------------------

# 10.0.0-beta.8 Release notes

### Enhancements
* Features added by release v6.0.26:
  * Added ability to replace last sort descriptor on DescriptorOrdering in addition to append/prepending to it [#3884]

### Fixed
* Issues fixed by release v6.0.26:
  * Fix deadlocks when opening a Realm file in both the iOS simulator and Realm Studio ([Cocoa #6743](https://github.com/realm/realm-cocoa/issues/6743), since 10.0.0-beta.5).
  * Fix Springboard deadlocking when an app is unsuspended while it has an open Realm file which is stored in an app group ([Cocoa #6749](https://github.com/realm/realm-cocoa/issues/6749), since 10.0.0-beta.5).

----------------------------------------------

# 10.0.0-beta.7 Release notes

### Enhancements
* Added ability to replace last sort descriptor on DescriptorOrdering in addition to append/prepending to it [#3884]
* Add Decimal128 subtraction and multiplication arithmetic operators.

### Fixed
* Issues fixed by releases v6.0.24 to v6.0.25:
  * If you have a realm file growing towards 2Gb and have a table with more than 16 columns, then you may get a "Key not found" exception when updating an object. If asserts are enabled at the binding level, you may get an "assert(m_has_refs)" instead. ([#3194](https://github.com/realm/realm-js/issues/3194), since v6.0.0)
  * In cases where you have more than 32 columns in a table, you may get a currrupted file resulting in various crashes ([#7057](https://github.com/realm/realm-java/issues/7057), since v6.0.0)
  * Upgrading files with string primary keys would result in a file where it was not possible to find the objects by primary key ([#6716](https://github.com/realm/realm-cocoa/issues/6716), since 10.0.0-beta.2)

### Breaking changes
* File format bumped to 20. Automatic upgrade of non syncked realms. Syncked realms produced by pre v10 application cannot be upgraded.

----------------------------------------------

# 10.0.0-beta.6 Release notes

### Fixed
* Issues fixed by releases v6.0.22 to v6.0.23

----------------------------------------------

# 10.0.0-beta.5 Release notes

### Fixed
* Issues fixed by releases v6.0.14 to v6.0.21

-----------

### Internals
* When creating objects without primary keys, it is now checked that the generated ObjKey does not collide with an already existing object. This was a problem in some migration scenarios in ObjectStore.

----------------------------------------------

# 10.0.0-beta.4 Release notes

### Enhancements
* Parser support for ObjectId vs Timestamp

### Fixed in v6.0.14..v6.0.17
* If a realm needs upgrade during opening, the program might abort in the "migrate_links" stage. ([#6680](https://github.com/realm/realm-cocoa/issues/6680), since v6.0.0)
* Fix bug in memory mapping management. This bug could result in multiple different asserts as well as segfaults. In many cases stack backtraces would include members of the EncyptedFileMapping near the top - even if encryption was not used at all. In other cases asserts or crashes would be in methods reading an array header or array element. In all cases the application would terminate immediately. ([#3838](https://github.com/realm/realm-core/pull/3838), since v6)
* Fix missing `Lst` symbols when the library is built as a shared library with LTO. ([Cocoa #6625](https://github.com/realm/realm-cocoa/issues/6625), since v6.0.0).

----------------------------------------------

# 10.0.0-beta.3 Release notes

### Fixed
* Isses fixed by releases v6.0.11 to v6.0.13

----------------------------------------------

# 10.0.0-beta.2 Release notes

### Fixed
* Isses fixed by release v6.0.7 to v6.0.10
* We would allow converting a table to embedded table in spite some objects had no links to them. ([#3729](https://github.com/realm/realm-core/issues/3729), since v6.1.0-alpha.5)
* Fixed parsing queries with substitutions in a subquery, for example on a named linking object property. This also enables support for substitution chains. ([realm-js 2977](https://github.com/realm/realm-js/issues/2977), since the parser supported subqueries).
* Receiving an EraseObject instruction from server would not cause any embedded objects to be erased.  ([RSYNC-128](https://jira.mongodb.org/browse/RSYNC-128), since v6.1.0-alpha.5)

----------------------------------------------

# 10.0.0-beta.1 Release notes

### Enhancements
* Validation errors when opening a DB now provide more information about the invalid values

### Fixed
* When opening Realms on Apple devices where the file resided on a filesystem that does not support preallocation, such as ExFAT, you may get 'Operation not supported' exception. ([cocoa-6508](https://github.com/realm/realm-cocoa/issues/6508)).
* After upgrading of a realm file, you may at some point receive a 'NoSuchTable' exception. ([#3701](https://github.com/realm/realm-core/issues/3701), since 6.0.0)
* If the upgrade process was interrupted/killed for various reasons, the following run could stop with some assertions failing. We don't have evidence that this has actually happened so we will not refer to any specific issue report.
* When querying on a LnkLst where the target property over a link has an index and the LnkLst has a different order from the target table, you may get incorrect results. ([Cocoa #6540](https://github.com/realm/realm-cocoa/issues/6540), since 5.23.6.

-----------

### Internals
* Work around an issue with MSVC in Visual Studio 2019 where Release optimizations crash the compiler because of a regression in 64bit atomic loads on 32bit Windows.

----------------------------------------------

# 10.0.0-alpha.9 Release notes

### Fixed
* Embedded objects would in some cases not be deleted when parent object was deleted.

-----------

### Internals
* 'using namespace realm::util' removed from header. This means that this namespace is no longer used automatically.
* Some unused functionality has been removed.

----------------------------------------------

# 10.0.0-alpha.8 Release notes

### Enhancements

* Add native core query support for ALL/NONE on some query expressions
* Add query parser support for lists of primitives
  * ANY/ALL/NONE support
  * all existing comparison operators supported for appropriate types
  * `.@count` (or `.@size`) for number of items in a list can be compared to other numeric expressions (numeric values or columns)
  * a new operation `.length` (case insensitive) is supported for string and binary lists which evaluates the length of each item in the list
* Add query parser support for keypath comparisons to null over links
* Use the new core support for ALL/NONE queries instead of rewriting these queries as subqueries (optimizaiton)

### Fixed
* Fix an assertion failure when querying a list of primitives over a link
* Fix serialisation of list of primitive queries
* Fix queries of lists of nullable ObjectId, Int and Bool
* Fix count of list of primitives with size 0
* Fix aggregate queries of list of primitives which are nullable types
* Throw an exception when someone tries to add a search index to a list of primitives instead of crashing
* Changed query parser ALL/NONE support to belong to one side of a comparison instead of the entire expression, this enables support for reversing expressions (in addition to `ALL prices > 20` we now also support `20 < ALL prices`)

-----------

### Internals
* Replication interface for adding tables and creating objects changed. Adaptation required in Sync.

----------------------------------------------

# 10.0.0-alpha.7 Release notes

### Enhancements
* Query::sum_decimal128 added.

### Fixed
* None.

-----------

### Internals
* Switch back to default off_t size so consumers don't need to define _FILE_OFFSET_BITS=64

----------------------------------------------

# 10.0.0-alpha.6 Release notes

### Enhancements
* Produces builds for RaspberryPi.

### Fixed
* Requirement to have a contiguous memory mapping of the entire realm file is removed. (Now fixed)

-----------

### Internals
* Table::insert_column and Table::insert_column_link methods are removed.
* Can now be built with MSVC 2019

----------------------------------------------

# 10.0.0-alpha.5 Release notes

### Enhancements
* average, min and max operations added to Decimal128 queries.
* 'between' condition added to Decimal128 queries.

### Fixed
* Querying for a null ObjectId value over links could crash.
* Several fixes around tombstone handling

----------------------------------------------

# 10.0.0-alpha.4 Release notes

### Enhancements
* 'old-query' support added for Decimal128 and ObjectId

### Fixed
* Previous enhancement "Requirement to have a contiguous memory mapping of the entire realm file is removed." is reverted. Caused various problems.
* When upgrading a realm file containing a table with integer primary keys, the program could sometimes crash.

### This release also includes the fixes contained in v5.27.9:
* Fix a crash on startup on macOS 10.10 and 10.11. ([Cocoa #6403](https://github.com/realm/realm-cocoa/issues/6403), since 2.9.0).

----------------------------------------------

# 10.0.0-alpha.3 Release notes

### Enhancements
* Requirement to have a contiguous memory mapping of the entire realm file is removed.

### Fixed
* ConstLnkLst filters out unresolved links.

-----------

### Internals
* Migrated to the final `std::filesystem` implementation on Windows from the experimental one.
* Exception class InvalidKey is replaced with KeyNotFound, KeyAlreadyUsed, ColumnNotFound and ColumnAlreadyExists
* Calling `Table::create_object(ObjKey)` on a table with a primary key column is now an error.
* Objects created with `Table::create_object(GlobalKey)` are now subject to tombstone resurrection.
* Table::get_objkey_from_global_key() was introduced to allow getting the ObjKey of an object (dead or alive) identified by its GlobalKey.
* ChunkedBinaryData moved from Sync to Core

----------------------------------------------

# 10.0.0-alpha.2 Release notes

This release also contains the changes introduced by v6.0.4

### Fixed
* Table::find_first<T> on a primary key column would sometimes return the wrong object. Since v10.0.0-alpha.1.

-----------

### Internals
* 'clear_table' and 'list_swap' removed from the replication interface.
* Some 'safe_int_ops' has been removed.

----------------------------------------------

# 10.0.0-alpha.1 Release notes

### Fixed
* Table::find_first() now handles tables with int primary key correctly.
* We will not delete dangling links when otherwise modifying a list.

-----------

### Internals
* Sync should now use Lst<ObjKey> interface when setting possibly dangling links

# 6.1.0-alpha.5 Release notes

### Enhancements
* Initial support for dangling links: Table::get_objkey_from_primary_key() and Table::invalidate_object() added.
* Several minor enhancements for Decimal128 and ObjectId.
* It is now possible to switch embeddedness of a table.

### Fixed
* None.

-----------

### Internals
* Now uses c++17!!!!

----------------------------------------------

# 6.1.0-alpha.4 Release notes

### Fixed
* Fixed a segfault when a list of Decimal128 expanded over 1000 elements.
* Fixed min/max of a list of Decimal128 when the list contained numbers larger than a min/max int64 type.
* Fixed sum/avg of a list of Decimal128 when the list contained nulls.
* Added missing TableView aggregate functions for Decimal128.
* Fixed min/max Decimal128 via Table not returning the result index.
* Fixed sorting Decimal128 behaviour including position of NaN.
* Fixed crash sorting a nullable ObjectID column.

----------------------------------------------

# 6.1.0-alpha.3 Release notes

### Fixes
* Ability to create Decimal128 lists was missing
* No replication of create/delete operations on embedded tables.

----------------------------------------------

# 6.1.0-alpha.2 Release notes

### Fixes
* Fixed issue regarding opening a file format version 10 realm file in RO mode.

----------------------------------------------

# 6.1.0-alpha.1 Release notes

### Enhancements
* Added new data types - Decimal128 and ObjectId types.
* Added support for embedded objects.

### Fixes
* Fixes parsing float and double constants which had been serialised to scientific notation (eg. 1.23E-24). ([#3076](https://github.com/realm/realm-core/issues/3076)).

### Breaking changes
* None.

-----------

### Internals
* File format bumped to 11.

----------------------------------------------

# 6.2.1 Release notes

### Fixed
* Files upgraded on 32-bit devices could end up being inconsistent resulting in "Key not found" exception to be thown. ([#6992](https://github.com/realm/realm-java/issues/6992), since v6.0.16)

----------------------------------------------

# 6.2.0 Release notes

### Enhancements
* In cases where we use an outdated TableRef, we throw InvalidTableRef exception instead of NoSuchTable. NoSuchTable could be misleading as the table is most likely still there.

### Fixed
* Fix crash in case insensitive query on indexed string columns when nothing matches ([#6836](https://github.com/realm/realm-cocoa/issues/6836), since v6.0.0)
* Fix list of primitives for Optional<Float> and Optional<Double> always returning false for `Lst::is_null(ndx)` even on null values, ([#3987](https://github.com/realm/realm-core/pull/3987), since v6.0.0).
* Fix queries for the size of a list of primitive nullable ints returning size + 1. This applies to the `Query::size_*` methods (SizeListNode) and not query expression syntax (SizeOperator). ([#4016](https://github.com/realm/realm-core/pull/4016), since v6.0.0).

### Breaking changes
* None.

-----------

### Internals
* None.

----------------------------------------------

# 6.1.4 Release notes

### Fixed
* If you make a case insignificant query on an indexed string column, it may fail in a way that results in a "No such key" exception. ([#6830](https://github.com/realm/realm-cocoa/issues/6830), since v6.0.0)

----------------------------------------------

# 6.1.3 Release notes

### Fixed
* Making a query in an indexed property may give a "Key not found" exception. ([#2025](https://github.com/realm/realm-dotnet/issues/2025), since v6.0.0)
* Fix queries for null on non-nullable indexed integer columns returning results for zero entries. (Since v6)
----------------------------------------------

# 6.1.2 Release notes

### Enhancements
* Slightly improve performance of most operations which read data from the Realm file.

### Fixed
* Rerunning an equals query on an indexed string column which previously had more than one match and now has one match would sometimes throw a "key not found" exception ([Cocoa #6536](https://github.com/realm/realm-cocoa/issues/6536), since 6.0.2),

----------------------------------------------

# 6.1.1 Release notes

### Enhancements
* Allocating one extra entry in ref translation tables. May help finding memory mapping problems.

### Fixed
* None.
-----------

### Internals
* Fix assertion failure related to Table::clear found by Cocoa SDK in V6.1.0. That release is not expected to be used in SDK releases, so no customers are affected.

----------------------------------------------

# 6.1.0 Release notes

### Enhancements
* Greatly improve performance of NOT IN queries on indexed string or int columns.

### Fixed
* When querying a table where links are part of the condition, the application may crash if objects has recently been added to the target table. ([Java #7118](https://github.com/realm/realm-java/issues/7118), since v6.0.0)
* Possibly fix issues related to changing the primary key property from nullable to required. ([PR #3917](https://github.com/realm/realm-core/pull/3918)).

----------------------------------------------

# 6.0.26 Release notes

### Enhancements
* Added ability to replace last sort descriptor on DescriptorOrdering in addition to append/prepending to it [#3884]

### Fixed
* Fix deadlocks when opening a Realm file in both the iOS simulator and Realm Studio ([Cocoa #6743](https://github.com/realm/realm-cocoa/issues/6743), since 6.0.21).
* Fix Springboard deadlocking when an app is unsuspended while it has an open Realm file which is stored in an app group ([Cocoa #6749](https://github.com/realm/realm-cocoa/issues/6749), since 6.0.21).
* If you use encryption your application cound crash with a message like "Opening Realm files of format version 0 is not supported by this version of Realm". ([#6889](https://github.com/realm/realm-java/issues/6889) among others, since v6.0.0)

----------------------------------------------

# 6.0.25 Release notes

### Fixed
* If you have a realm file growing towards 2Gb and have a table with more than 16 columns, then you may get a "Key not found" exception when updating an object. If asserts are enabled at the binding level, you may get an "assert(m_has_refs)" instead. ([#3194](https://github.com/realm/realm-js/issues/3194), since v6.0.0)
* In cases where you have more than 32 columns in a table, you may get a currrupted file resulting in various crashes ([#7057](https://github.com/realm/realm-java/issues/7057), since v6.0.0)

----------------------------------------------

# 6.0.24 Release notes

### Fixed
* Upgrading files with string primary keys would result in a file where it was not possible to find the objects by primary key ([#6716](https://github.com/realm/realm-cocoa/issues/6716), since 6.0.7)

-----------

### Internals
* File format version bumped to 11.

----------------------------------------------

# 6.0.23 Release notes

### Fixed
* Fix an assertion failure when DB::call_with_lock() was called when the management directory did not exist on iOS (since 6.0.21).
* The non-xcframework Apple release package did not include macOS libraries (since 6.0.22).

----------------------------------------------

# 6.0.22 Release notes

### Enhancements
* Added an enum to `DescriptorOrdering::append_sort` which allows users to choose the merge order of how sorts are applied. The default is the historical behaviour sor this is not a breaking change. ([#3869](https://github.com/realm/realm-core/issues/3869))
* Add arm64 simulator slices to the xcframework release package.

### Fixed
* Fix deadlocks when writing to a Realm file on an exFAT partition from macOS. ([Cocoa #6691](https://github.com/realm/realm-cocoa/issues/6691)).

----------------------------------------------

# 6.0.21 Release notes

### Fixed
* Holding a shared lock while being suspended on iOS would cause the app to be terminated. (https://github.com/realm/realm-cocoa/issues/6671)

----------------------------------------------

# 6.0.20 Release notes

### Fixed
* If an attempt to upgrade a realm has ended with a crash with "migrate_links" in the call stack, the realm ended in a corrupt state where further upgrade was not possible. A remedy for this situation is now provided.

----------------------------------------------

# 6.0.19 Release notes

### Fixed
* Upgrading a table with only backlink columns could crash (No issue created)
* If you upgrade a file where you have "" elements in a list of non-nullable strings, the program would crash ([#3836](https://github.com/realm/realm-core/issues/3836), since v6.0.0)
* None.

----------------------------------------------

# 6.0.18 Release notes

### Fixed
* We no longer throw when an invalid ConstIterator is copied ([#698](https://github.com/realm/realm-cocoa/issues/6597), since v6.0)

### Internals
* Go back to using Visual Studio 2017

----------------------------------------------

# 6.0.17 Release notes

### Fixed
* None

-----------

### Internals
* Workaround for compiler bug in Visual Studio 2019

----------------------------------------------

# 6.0.16 Release notes

### Enhancements
* Upgrade logic changed so that progress is recorded explicitly in a table. This makes the logic simpler and reduces the chance of errors. It will also make it easier to see if we receive a partly upgraded file from a costumer.

### Fixed
* If a realm needs upgrade during opening, the program might abort in the "migrate_links" stage. ([#6680](https://github.com/realm/realm-cocoa/issues/6680), since v6.0.0)

-----------

### Internals
* Using Visual Studio 2019 for Windows builds.

----------------------------------------------

# 6.0.15 Release notes

### Fixed
* Fix bug in memory mapping management. This bug could result in multiple different asserts as well as segfaults. In many cases stack backtraces would include members of the EncyptedFileMapping near the top - even if encryption was not used at all. In other cases asserts or crashes would be in methods reading an array header or array element. In all cases the application would terminate immediately. ([#3838](https://github.com/realm/realm-core/pull/3838), since v6)

----------------------------------------------

# 6.0.14 Release notes

### Fixed
* Fix missing `Lst` symbols when the library is built as a shared library with LTO. ([Cocoa #6625](https://github.com/realm/realm-cocoa/issues/6625), since v6.0.0).

----------------------------------------------

# 6.0.13 Release notes

### Enhancements
* Add support for the 64-bit watchOS simulator.

----------------------------------------------

# 6.0.12 Release notes

### Fixed
* Fix upgrade bug. Could cause assertions like "Assertion failed: ref != 0" during opning of a realm. ([#6644](https://github.com/realm/realm-cocoa/issues/6644), since V6.0.7)
* A use-after-free would occur if a Realm was compacted, opened on multiple threads prior to the first write, then written to while reads were happening on other threads. This could result in a variety of crashes, often inside realm::util::EncryptedFileMapping::read_barrier. (Since v6.0.0, [Cocoa #6652](https://github.com/realm/realm-cocoa/issues/6652), [Cocoa #6628](https://github.com/realm/realm-cocoa/issues/6628), [Cocoa #6655](https://github.com/realm/realm-cocoa/issues/6555)).

----------------------------------------------

# 6.0.11 Release notes

### Fixed
* Table::create_object_with_primary_key(null) would hit an assertion failure when the primary key type is a string and the object already existed.

----------------------------------------------

# 6.0.10 Release notes

### Enhancements
* Upgrade process made more robust. Some progran crashes with assertion failure may be avoided.

### Fixed
* Re-enable compilation using SSE (since v6.0.7)
* Improved error messages when top ref is invalid.

----------------------------------------------

# 6.0.9 Release notes

### Enhancements
* Improve the performance of advancing transaction read versions when not using a transaction log observer or schema change handler.
* Added the ability to produce an XCFramework, usage: `sh ./tools/build-cocoa.sh -x`

----------------------------------------------

# 6.0.8 Release notes

### Fixed
* Empty tables will not have a primary key column after upgrade ([#3795](https://github.com/realm/realm-core/issues/3795), since v6.0.7)
* Calling ConstLst::find_first() immediately after advance_read() would give incorrect results ([Cocoa #6606](https://github.com/realm/realm-cocoa/issues/6606), since 6.0.0).

----------------------------------------------

# 6.0.7 Release notes

### Fixed
* If you upgrade from a realm file with file format version 6 (Realm Core v2.4.0 or earlier) the upgrade will result in a crash ([#3764](https://github.com/realm/realm-core/issues/3764), since v6.0.0-alpha.0)
* Fix building for watchOS with Xcode 12.
* After upgrade, columns with string primary keys would stille have a search index in spite this is generally not the case with file format 10. ([#3787](https://github.com/realm/realm-core/issues/3787), since v6.0.0-alpha.0)
* Realm file format upgrade to version 6 (or later) could be very time consuming if search indexes were present. ([#2767](https://github.com/realm/realm-core/issues/3767), since v6)

-----------

### Internals
* Releases for Apple platforms are now built with Xcode 11.

----------------------------------------------

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
