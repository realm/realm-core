/*************************************************************************
 *
 * Copyright 2024 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_H
#define REALM_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <realm/error_codes.h>

#if defined(_WIN32) || defined(__CYGWIN__)

#if defined(Realm_EXPORTS)
// Exporting Win32 symbols
#define RLM_EXPORT __declspec(dllexport)
#else
// Importing Win32 symbols. Note: Clients linking statically should define
// RLM_NO_DLLIMPORT.
#if !defined(RLM_NO_DLLIMPORT)
#define RLM_EXPORT __declspec(dllimport)
#else
#define RLM_EXPORT
#endif // RLM_NO_DLLIMPORT
#endif // Realm_EXPORTS

#else
// Not Win32
#define RLM_EXPORT __attribute__((visibility("default")))
#endif

#ifdef __cplusplus
#define RLM_API extern "C" RLM_EXPORT
#define RLM_API_NOEXCEPT noexcept
#else
#define RLM_API RLM_EXPORT
#define RLM_API_NOEXCEPT
#endif // __cplusplus

// Some platforms don't support anonymous unions in structs.
// RLM_NO_ANON_UNIONS allows definining a member name for unions in structs where
// RLM_ANON_UNION_MEMBER(name) is used.
#ifdef RLM_NO_ANON_UNIONS
#define RLM_ANON_UNION_MEMBER(name) name
#else
#define RLM_ANON_UNION_MEMBER(name)
#endif

// Some platforms can benefit from redefining the userdata type to another type known to the tooling.
// For example, Dart with its ffigen utility can generate cleaner code if we define realm_userdata_t as Dart_Handle,
// which is a pointer to an opaque struct treated specially by the Dart code generator.
// WARNING: only define this to a pointer type, anything else breaks the ABI.
#ifndef realm_userdata_t
#define realm_userdata_t void*
#endif

typedef struct shared_realm realm_t;
typedef struct realm_schema realm_schema_t;
typedef struct realm_scheduler realm_scheduler_t;
typedef struct realm_work_queue realm_work_queue_t;
typedef struct realm_thread_safe_reference realm_thread_safe_reference_t;
typedef void (*realm_free_userdata_func_t)(realm_userdata_t userdata);
typedef realm_userdata_t (*realm_clone_userdata_func_t)(const realm_userdata_t userdata);
typedef void (*realm_on_object_store_thread_callback_t)(realm_userdata_t userdata);
typedef bool (*realm_on_object_store_error_callback_t)(realm_userdata_t userdata, const char*);
typedef struct realm_key_path_array realm_key_path_array_t;

/* Accessor types */
typedef struct realm_object realm_object_t;

typedef struct realm_list realm_list_t;
typedef struct realm_set realm_set_t;
typedef struct realm_dictionary realm_dictionary_t;

/* Query types */
typedef struct realm_query realm_query_t;
typedef struct realm_results realm_results_t;

/* Config types */
typedef struct realm_config realm_config_t;
typedef bool (*realm_migration_func_t)(realm_userdata_t userdata, realm_t* old_realm, realm_t* new_realm,
                                       const realm_schema_t* schema);
typedef bool (*realm_data_initialization_func_t)(realm_userdata_t userdata, realm_t* realm);
typedef bool (*realm_should_compact_on_launch_func_t)(realm_userdata_t userdata, uint64_t total_bytes,
                                                      uint64_t used_bytes);

typedef enum realm_schema_mode {
    RLM_SCHEMA_MODE_AUTOMATIC,
    RLM_SCHEMA_MODE_IMMUTABLE,
    RLM_SCHEMA_MODE_READ_ONLY,
    RLM_SCHEMA_MODE_SOFT_RESET_FILE,
    RLM_SCHEMA_MODE_HARD_RESET_FILE,
    RLM_SCHEMA_MODE_ADDITIVE_DISCOVERED,
    RLM_SCHEMA_MODE_ADDITIVE_EXPLICIT,
    RLM_SCHEMA_MODE_MANUAL,
} realm_schema_mode_e;

typedef enum realm_schema_subset_mode {
    RLM_SCHEMA_SUBSET_MODE_STRICT,
    RLM_SCHEMA_SUBSET_MODE_ALL_CLASSES,
    RLM_SCHEMA_SUBSET_MODE_ALL_PROPERTIES,
    RLM_SCHEMA_SUBSET_MODE_COMPLETE
} realm_schema_subset_mode_e;

/* Key types */
typedef uint32_t realm_class_key_t;
typedef int64_t realm_property_key_t;
typedef int64_t realm_object_key_t;
typedef uint64_t realm_version_t;

static const realm_class_key_t RLM_INVALID_CLASS_KEY = ((uint32_t)-1) >> 1;
static const realm_property_key_t RLM_INVALID_PROPERTY_KEY = -1;
static const realm_object_key_t RLM_INVALID_OBJECT_KEY = -1;

/* Value types */

typedef enum realm_value_type {
    RLM_TYPE_NULL,
    RLM_TYPE_INT,
    RLM_TYPE_BOOL,
    RLM_TYPE_STRING,
    RLM_TYPE_BINARY,
    RLM_TYPE_TIMESTAMP,
    RLM_TYPE_FLOAT,
    RLM_TYPE_DOUBLE,
    RLM_TYPE_DECIMAL128,
    RLM_TYPE_OBJECT_ID,
    RLM_TYPE_LINK,
    RLM_TYPE_UUID,
    RLM_TYPE_LIST,
    RLM_TYPE_DICTIONARY,
} realm_value_type_e;

typedef enum realm_schema_validation_mode {
    RLM_SCHEMA_VALIDATION_BASIC = 0,
    RLM_SCHEMA_VALIDATION_SYNC_PBS = 1,
    RLM_SCHEMA_VALIDATION_REJECT_EMBEDDED_ORPHANS = 2,
    RLM_SCHEMA_VALIDATION_SYNC_FLX = 4
} realm_schema_validation_mode_e;

/**
 * Represents a view over a UTF-8 string buffer. The buffer is unowned by this struct.
 *
 * This string can have three states:
 * - null
 *   When the data member is NULL.
 * - empty
 *   When the data member is non-NULL, and the size member is 0. The actual contents of the data member in this case
 * don't matter.
 * - non-empty
 *   When the data member is non-NULL, and the size member is greater than 0.
 *
 */
typedef struct realm_string {
    const char* data;
    size_t size;
} realm_string_t;

typedef struct realm_binary {
    const uint8_t* data;
    size_t size;
} realm_binary_t;

typedef struct realm_timestamp {
    int64_t seconds;
    int32_t nanoseconds;
} realm_timestamp_t;

typedef struct realm_decimal128 {
    uint64_t w[2];
} realm_decimal128_t;

typedef struct realm_link {
    realm_class_key_t target_table;
    realm_object_key_t target;
} realm_link_t;

typedef struct realm_object_id {
    uint8_t bytes[12];
} realm_object_id_t;

typedef struct realm_uuid {
    uint8_t bytes[16];
} realm_uuid_t;

typedef struct realm_value {
    union {
        int64_t integer;
        bool boolean;
        realm_string_t string;
        realm_binary_t binary;
        realm_timestamp_t timestamp;
        float fnum;
        double dnum;
        realm_decimal128_t decimal128;
        realm_object_id_t object_id;
        realm_uuid_t uuid;
        realm_link_t link;

        char data[16];
    } RLM_ANON_UNION_MEMBER(values);
    realm_value_type_e type;
} realm_value_t;
typedef struct realm_query_arg {
    size_t nb_args;
    bool is_list;
    realm_value_t* arg;
} realm_query_arg_t;

typedef struct realm_version_id {
    uint64_t version;
    uint64_t index;
} realm_version_id_t;


/* Error types */
typedef struct realm_async_error realm_async_error_t;
typedef unsigned realm_error_categories;

typedef struct realm_error {
    realm_errno_e error;
    realm_error_categories categories;
    const char* message;
    // When error is RLM_ERR_CALLBACK this is an opaque pointer to an SDK-owned error object
    // thrown by user code inside a callback with realm_register_user_code_callback_error(), otherwise null.
    void* user_code_error;
    const char* path;
} realm_error_t;

/* Schema types */

typedef enum realm_column_attr {
    // Values matching `realm::ColumnAttr`.
    RLM_COLUMN_ATTR_NONE = 0,
    RLM_COLUMN_ATTR_INDEXED = 1,
    RLM_COLUMN_ATTR_UNIQUE = 2,
    RLM_COLUMN_ATTR_RESERVED = 4,
    RLM_COLUMN_ATTR_STRONG_LINKS = 8,
    RLM_COLUMN_ATTR_NULLABLE = 16,
    RLM_COLUMN_ATTR_LIST = 32,
    RLM_COLUMN_ATTR_DICTIONARY = 64,
    RLM_COLUMN_ATTR_COLLECTION = 64 + 32,
} realm_column_attr_e;

typedef enum realm_property_type {
    // Values matching `realm::ColumnType`.
    RLM_PROPERTY_TYPE_INT = 0,
    RLM_PROPERTY_TYPE_BOOL = 1,
    RLM_PROPERTY_TYPE_STRING = 2,
    RLM_PROPERTY_TYPE_BINARY = 4,
    RLM_PROPERTY_TYPE_MIXED = 6,
    RLM_PROPERTY_TYPE_TIMESTAMP = 8,
    RLM_PROPERTY_TYPE_FLOAT = 9,
    RLM_PROPERTY_TYPE_DOUBLE = 10,
    RLM_PROPERTY_TYPE_DECIMAL128 = 11,
    RLM_PROPERTY_TYPE_OBJECT = 12,
    RLM_PROPERTY_TYPE_LINKING_OBJECTS = 14,
    RLM_PROPERTY_TYPE_OBJECT_ID = 15,
    RLM_PROPERTY_TYPE_UUID = 17,
} realm_property_type_e;

typedef enum realm_collection_type {
    RLM_COLLECTION_TYPE_NONE = 0,
    RLM_COLLECTION_TYPE_LIST = 1,
    RLM_COLLECTION_TYPE_SET = 2,
    RLM_COLLECTION_TYPE_DICTIONARY = 4,
} realm_collection_type_e;

typedef struct realm_property_info {
    const char* name;
    const char* public_name;
    realm_property_type_e type;
    realm_collection_type_e collection_type;

    const char* link_target;
    const char* link_origin_property_name;
    realm_property_key_t key;
    int flags;
} realm_property_info_t;

typedef struct realm_class_info {
    const char* name;
    const char* primary_key;
    size_t num_properties;
    size_t num_computed_properties;
    realm_class_key_t key;
    int flags;
} realm_class_info_t;

typedef enum realm_class_flags {
    RLM_CLASS_NORMAL = 0,
    RLM_CLASS_EMBEDDED = 1,
    RLM_CLASS_ASYMMETRIC = 2,
    RLM_CLASS_MASK = 3,
} realm_class_flags_e;

typedef enum realm_property_flags {
    RLM_PROPERTY_NORMAL = 0,
    RLM_PROPERTY_NULLABLE = 1,
    RLM_PROPERTY_PRIMARY_KEY = 2,
    RLM_PROPERTY_INDEXED = 4,
    RLM_PROPERTY_FULLTEXT_INDEXED = 8,
} realm_property_flags_e;


/* Notification types */
typedef struct realm_notification_token realm_notification_token_t;
typedef struct realm_callback_token realm_callback_token_t;
typedef struct realm_refresh_callback_token realm_refresh_callback_token_t;
typedef struct realm_object_changes realm_object_changes_t;
typedef struct realm_collection_changes realm_collection_changes_t;
typedef struct realm_dictionary_changes realm_dictionary_changes_t;
typedef void (*realm_on_object_change_func_t)(realm_userdata_t userdata, const realm_object_changes_t*);
typedef void (*realm_on_collection_change_func_t)(realm_userdata_t userdata, const realm_collection_changes_t*);
typedef void (*realm_on_dictionary_change_func_t)(realm_userdata_t userdata, const realm_dictionary_changes_t*);
typedef void (*realm_on_realm_change_func_t)(realm_userdata_t userdata);
typedef void (*realm_on_realm_refresh_func_t)(realm_userdata_t userdata);
typedef void (*realm_async_begin_write_func_t)(realm_userdata_t userdata);
typedef void (*realm_async_commit_func_t)(realm_userdata_t userdata, bool error, const char* desc);

/**
 * Callback for realm schema changed notifications.
 *
 * @param new_schema The new schema. This object is released after the callback returns.
 *                   Preserve it with realm_clone() if you wish to keep it around for longer.
 */
typedef void (*realm_on_schema_change_func_t)(realm_userdata_t userdata, const realm_schema_t* new_schema);

/* Scheduler types */
typedef void (*realm_scheduler_notify_func_t)(realm_userdata_t userdata, realm_work_queue_t* work_queue);
typedef bool (*realm_scheduler_is_on_thread_func_t)(realm_userdata_t userdata);
typedef bool (*realm_scheduler_is_same_as_func_t)(const realm_userdata_t scheduler_userdata_1,
                                                  const realm_userdata_t scheduler_userdata_2);
typedef bool (*realm_scheduler_can_deliver_notifications_func_t)(realm_userdata_t userdata);
typedef realm_scheduler_t* (*realm_scheduler_default_factory_func_t)(realm_userdata_t userdata);

/**
 * Get the VersionID of the current transaction.
 *
 * @param out_found True if version information is available. This requires an available Read or Write transaction.
 * @param out_version The version of the current transaction. If `out_found` returns False, this returns (0,0).
 * @return True if no exception occurred.
 */
RLM_API bool realm_get_version_id(const realm_t*, bool* out_found, realm_version_id_t* out_version);

/**
 * Get a string representing the version number of the Realm library.
 *
 * @return A null-terminated string.
 */
RLM_API const char* realm_get_library_version(void);

/**
 * Get individual components of the version number of the Realm library.
 *
 * @param out_major The major version number (X.0.0).
 * @param out_minor The minor version number (0.X.0).
 * @param out_patch The patch version number (0.0.X).
 * @param out_extra The extra version string (0.0.0-X).
 */
RLM_API void realm_get_library_version_numbers(int* out_major, int* out_minor, int* out_patch,
                                               const char** out_extra);

/**
 * Get the last error that happened on this thread.
 *
 * Errors are thread-local. Getting the error must happen on the same thread as
 * the call that caused the error to occur. The error is specific to the current
 * thread, and not the Realm instance for which the error occurred.
 *
 * Note: The error message in @a err will only be safe to use until the next API
 *       call is made on the current thread.
 *
 * Note: The error is not cleared by subsequent successful calls to this
 *       function, but it will be overwritten by subsequent failing calls to
 *       other library functions.
 *
 * Note: Calling this function does not clear the current last error.
 *
 * This function does not allocate any memory.
 *
 * @param err A pointer to a `realm_error_t` struct that will be populated with
 *            information about the last error, if there is one. May be NULL.
 * @return True if an error occurred.
 */
RLM_API bool realm_get_last_error(realm_error_t* err);

/**
 * Get information about an async error, potentially coming from another thread.
 *
 * This function does not allocate any memory.
 *
 * @param err A pointer to a `realm_error_t` struct that will be populated with
 *            information about the error. May not be NULL.
 * @return A bool indicating whether or not an error is available to be returned
 * @see realm_get_last_error()
 */
RLM_API bool realm_get_async_error(const realm_async_error_t* err, realm_error_t* out_err);

/**
 * Convert the last error to `realm_async_error_t`, which can safely be passed
 * between threads.
 *
 * Note: This function does not clear the last error.
 *
 * @return A non-null pointer if there was an error on this thread.
 * @see realm_get_last_error()
 * @see realm_get_async_error()
 * @see realm_clear_last_error()
 */
RLM_API realm_async_error_t* realm_get_last_error_as_async_error(void);

#if defined(__cplusplus)
/**
 * Invoke a function that may throw an exception, and report that exception as
 * part of the C API error handling mechanism.
 *
 * This is used to test translation of exceptions to error codes.
 *
 * @return True if no exception was thrown.
 */
RLM_EXPORT bool realm_wrap_exceptions(void (*)()) noexcept;
#endif // __cplusplus

/**
 * Clear the last error on the calling thread.
 *
 * Use this if the system has recovered from an error, e.g. by closing the
 * offending Realm and reopening it, freeing up resources, or similar.
 *
 * @return True if an error was cleared.
 */
RLM_API bool realm_clear_last_error(void);

/**
 * Free memory allocated by the module this library was linked into.
 *
 * This is needed for raw memory buffers such as string copies or arrays
 * returned from a library function. Realm C Wrapper objects on the other hand
 * should always be freed with realm_release() only.
 */
RLM_API void realm_free(void* buffer);

/**
 * Free any Realm C Wrapper object.
 *
 * Note: Any pointer returned from a library function is owned by the caller.
 *       The caller is responsible for calling `realm_release()`. The only
 *       exception from this is C++ bridge functions that return `void*`, with
 *       the prefix `_realm`.
 *
 * Note: C++ destructors are typically `noexcept`, so it is likely that an
 *       exception will crash the process.
 *
 * @param ptr A pointer to a Realm C Wrapper object. May be NULL.
 */
RLM_API void realm_release(void* ptr);

/**
 * Clone a Realm C Wrapper object.
 *
 * If the object is not clonable, this function fails with RLM_ERR_NOT_CLONABLE.
 *
 * @return A pointer to an object of the same type as the input, or NULL if
 *         cloning failed.
 */
RLM_API void* realm_clone(const void*);

/**
 * Return true if two API objects refer to the same underlying data. Objects
 * with different types are never equal.
 *
 * Note: This function cannot be used with types that have value semantics, only
 *       opaque types that have object semantics.
 *
 *    - `realm_t` objects are identical if they represent the same instance (not
 *      just if they represent the same file).
 *    - `realm_schema_t` objects are equal if the represented schemas are equal.
 *    - `realm_config_t` objects are equal if the configurations are equal.
 *    - `realm_object_t` objects are identical if they belong to the same realm
 *      and class, and have the same object key.
 *    - `realm_list_t` and other collection objects are identical if they come
 *      from the same object and property.
 *    - `realm_query_t` objects are never equal.
 *    - `realm_scheduler_t` objects are equal if they represent the same
 *      scheduler.
 *    - Query descriptor objects are equal if they represent equivalent
 *      descriptors.
 *    - `realm_async_error_t` objects are equal if they represent the same
 *      exception instance.
 *
 * This function cannot fail.
 */
RLM_API bool realm_equals(const void*, const void*);

/**
 * True if a Realm C Wrapper object is "frozen" (immutable).
 *
 * Objects, collections, and results can be frozen. For all other types, this
 * function always returns false.
 */
RLM_API bool realm_is_frozen(const void*);

/* Logging */
// equivalent to realm::util::Logger::Level in util/logger.hpp and must be kept in sync.
typedef enum realm_log_level {
    RLM_LOG_LEVEL_ALL = 0,
    RLM_LOG_LEVEL_TRACE = 1,
    RLM_LOG_LEVEL_DEBUG = 2,
    RLM_LOG_LEVEL_DETAIL = 3,
    RLM_LOG_LEVEL_INFO = 4,
    RLM_LOG_LEVEL_WARNING = 5,
    RLM_LOG_LEVEL_ERROR = 6,
    RLM_LOG_LEVEL_FATAL = 7,
    RLM_LOG_LEVEL_OFF = 8,
} realm_log_level_e;

typedef void (*realm_log_func_t)(realm_userdata_t userdata, const char* category, realm_log_level_e level,
                                 const char* message);

/**
 * Install the default logger
 */
RLM_API void realm_set_log_callback(realm_log_func_t, realm_userdata_t userdata,
                                    realm_free_userdata_func_t userdata_free) RLM_API_NOEXCEPT;
RLM_API void realm_set_log_level(realm_log_level_e) RLM_API_NOEXCEPT;
/**
 * Set the logging level for given category. Return the previous level.
 */
RLM_API realm_log_level_e realm_set_log_level_category(const char*, realm_log_level_e) RLM_API_NOEXCEPT;
/**
 * Get the logging level for given category.
 */
RLM_API realm_log_level_e realm_get_log_level_category(const char*) RLM_API_NOEXCEPT;
/**
 * Get the actual log category names (currently 15)
  @param num_values number of values in the out_values array
  @param out_values pointer to an array of size num_values
  @return returns the number of categories returned. If num_values is zero, it will
          return the total number of categories.
 */
RLM_API size_t realm_get_category_names(size_t num_values, const char** out_values);

/**
 * Get a thread-safe reference representing the same underlying object as some
 * API object.
 *
 * The thread safe reference can be passed to a different thread and resolved
 * against a different `realm_t` instance, which succeeds if the underlying
 * object still exists.
 *
 * The following types can produce thread safe references:
 *
 * - `realm_object_t`
 * - `realm_results_t`
 * - `realm_list_t`
 * - `realm_t`
 *
 * This does not assume ownership of the object, except for `realm_t`, where the
 * instance is transferred by value, and must be transferred back to the current
 * thread to be used. Note that the `realm_thread_safe_reference_t` object must
 * still be destroyed after having been converted into a `realm_t` object.
 *
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_thread_safe_reference_t* realm_create_thread_safe_reference(const void*);

/**
 * Allocate a new configuration with default options.
 */
RLM_API realm_config_t* realm_config_new(void);

/**
 * Get the path of the realm being opened.
 *
 * This function cannot fail.
 */
RLM_API const char* realm_config_get_path(const realm_config_t*);

/**
 * Set the path of the realm being opened.
 *
 * This function aborts when out of memory, but otherwise cannot fail.
 */
RLM_API void realm_config_set_path(realm_config_t*, const char* path);

/**
 * Get the encryption key for the realm.
 *
 * The output buffer must be at least 64 bytes.
 *
 * @returns The length of the encryption key (0 or 64)
 */
RLM_API size_t realm_config_get_encryption_key(const realm_config_t*, uint8_t* out_key);

/**
 * Set the encryption key for the realm.
 *
 * The key must be either 64 bytes long or have length zero (in which case
 * encryption is disabled).
 *
 * This function may fail if the encryption key has the wrong length.
 */
RLM_API bool realm_config_set_encryption_key(realm_config_t*, const uint8_t* key, size_t key_size);

/**
 * Get the schema for this realm.
 *
 * Note: The caller obtains ownership of the returned value, and must manually
 *       free it by calling `realm_release()`.
 *
 * @return A schema object, or NULL if the schema is not set (empty).
 */
RLM_API realm_schema_t* realm_config_get_schema(const realm_config_t*);

/**
 * Set the schema object for this realm.
 *
 * This does not take ownership of the schema object, and it should be released
 * afterwards.
 *
 * This function aborts when out of memory, but otherwise cannot fail.
 *
 * @param schema The schema object. May be NULL, which means an empty schema.
 */
RLM_API void realm_config_set_schema(realm_config_t*, const realm_schema_t* schema);

/**
 * Get the schema version of the schema.
 *
 * This function cannot fail.
 */
RLM_API uint64_t realm_config_get_schema_version(const realm_config_t*);

/**
 * Set the schema version of the schema.
 *
 * This function cannot fail.
 */
RLM_API void realm_config_set_schema_version(realm_config_t*, uint64_t version);

/**
 * Get the schema mode.
 *
 * This function cannot fail.
 */
RLM_API realm_schema_mode_e realm_config_get_schema_mode(const realm_config_t*);

/**
 * Set the schema mode.
 *
 * This function cannot fail.
 */
RLM_API void realm_config_set_schema_mode(realm_config_t*, realm_schema_mode_e);

/**
 * Get the subset schema mode.
 *
 * This function cannot fail.
 */
RLM_API realm_schema_subset_mode_e realm_config_get_schema_subset_mode(const realm_config_t*);

/**
 * Set schema subset mode
 *
 * This function cannot fail
 */
RLM_API void realm_config_set_schema_subset_mode(realm_config_t*, realm_schema_subset_mode_e);

/**
 * Set the migration callback.
 *
 * The migration function is called during a migration for schema modes
 * `RLM_SCHEMA_MODE_AUTOMATIC` and `RLM_SCHEMA_MODE_MANUAL`. The callback is
 * invoked with a realm instance before the migration and the realm instance
 * that is currently performing the migration.
 *
 * This function cannot fail.
 */
RLM_API void realm_config_set_migration_function(realm_config_t*, realm_migration_func_t, realm_userdata_t userdata,
                                                 realm_free_userdata_func_t userdata_free);

/**
 * Set the data initialization function.
 *
 * The callback is invoked the first time the schema is created, such that the
 * user can perform one-time initialization of the data in the realm.
 *
 * The realm instance passed to the callback is in a write transaction.
 *
 * This function cannot fail.
 */
RLM_API void realm_config_set_data_initialization_function(realm_config_t*, realm_data_initialization_func_t,
                                                           realm_userdata_t userdata,
                                                           realm_free_userdata_func_t userdata_free);

/**
 * Set the should-compact-on-launch callback.
 *
 * The callback is invoked the first time a realm file is opened in this process
 * to decide whether the realm file should be compacted.
 *
 * Note: If another process has the realm file open, it will not be compacted.
 *
 * This function cannot fail.
 */
RLM_API void realm_config_set_should_compact_on_launch_function(realm_config_t*,
                                                                realm_should_compact_on_launch_func_t,
                                                                realm_userdata_t userdata,
                                                                realm_free_userdata_func_t userdata_free);

/**
 * True if file format upgrades on open are disabled.
 *
 * This function cannot fail.
 */
RLM_API bool realm_config_get_disable_format_upgrade(const realm_config_t*);

/**
 * True if you can open the file without a file_format_upgrade
 */
RLM_API bool realm_config_needs_file_format_upgrade(const realm_config_t*);

/**
 * Disable file format upgrade on open (default: false).
 *
 * If a migration is needed to open the realm file with the provided schema, an
 * error is thrown rather than automatically performing the migration.
 *
 * This function cannot fail.
 */
RLM_API void realm_config_set_disable_format_upgrade(realm_config_t*, bool);

/**
 * True if automatic change notifications should be generated.
 *
 * This function cannot fail.
 */
RLM_API bool realm_config_get_automatic_change_notifications(const realm_config_t*);

/**
 * Automatically generated change notifications (default: true).
 *
 * This function cannot fail.
 */
RLM_API void realm_config_set_automatic_change_notifications(realm_config_t*, bool);

/**
 * The scheduler which this realm should be bound to (default: NULL).
 *
 * If NULL, the realm will be bound to the default scheduler for the current thread.
 *
 * This function aborts when out of memory, but otherwise cannot fail.
 */
RLM_API void realm_config_set_scheduler(realm_config_t*, const realm_scheduler_t*);

/**
 * Get maximum number of active versions in the realm file allowed before an
 * exception is thrown.
 *
 * This function cannot fail.
 */
RLM_API uint64_t realm_config_get_max_number_of_active_versions(const realm_config_t*);

/**
 * Set maximum number of active versions in the realm file allowed before an
 * exception is thrown (default: UINT64_MAX).
 *
 * This function cannot fail.
 */
RLM_API void realm_config_set_max_number_of_active_versions(realm_config_t*, uint64_t);

/**
 * Configure realm to be in memory
 */
RLM_API void realm_config_set_in_memory(realm_config_t*, bool) RLM_API_NOEXCEPT;

/**
 * Check if realm is configured in memory
 */
RLM_API bool realm_config_get_in_memory(realm_config_t*) RLM_API_NOEXCEPT;

/**
 * Set FIFO path
 */
RLM_API void realm_config_set_fifo_path(realm_config_t*, const char*);

/**
 Check realm FIFO path
 */
RLM_API const char* realm_config_get_fifo_path(realm_config_t*) RLM_API_NOEXCEPT;

/**
 * If 'cached' is false, always return a new Realm instance.
 */
RLM_API void realm_config_set_cached(realm_config_t*, bool cached) RLM_API_NOEXCEPT;

/**
 * Check if realms are cached
 */
RLM_API bool realm_config_get_cached(realm_config_t*) RLM_API_NOEXCEPT;

/**
 * Allow realm to manage automatically embedded objects when a migration from TopLevel to Embedded takes place.
 */
RLM_API void realm_config_set_automatic_backlink_handling(realm_config_t*, bool) RLM_API_NOEXCEPT;

/**
 * Create a custom scheduler object from callback functions.
 *
 * @param notify Function which will be called whenever the scheduler has work
 *               to do. Each call to this should trigger a call to
 *               `realm_scheduler_perform_work()` from within the scheduler's
 *               event loop. This function must be thread-safe, or NULL, in
 *               which case the scheduler is considered unable to deliver
 *               notifications.
 * @param is_on_thread Function to return true if called from the same thread as
 *                     the scheduler. This function must be thread-safe.
 * @param can_deliver_notifications Function to return true if the scheduler can
 *                                  support `notify()`. This function does not
 *                                  need to be thread-safe.
 */
RLM_API realm_scheduler_t*
realm_scheduler_new(realm_userdata_t userdata, realm_free_userdata_func_t userdata_free,
                    realm_scheduler_notify_func_t notify, realm_scheduler_is_on_thread_func_t is_on_thread,
                    realm_scheduler_is_same_as_func_t is_same_as,
                    realm_scheduler_can_deliver_notifications_func_t can_deliver_notifications);

/**
 * Performs all of the pending work for the given scheduler.
 *
 * This function must be called from within the scheduler's event loop. It must
 * be called each time the notify callback passed to the scheduler
 * is invoked.
 */
RLM_API void realm_scheduler_perform_work(realm_work_queue_t*);
/**
 * Create an instance of the default scheduler for the current platform,
 * normally confined to the calling thread.
 */
RLM_API realm_scheduler_t* realm_scheduler_make_default(void);

/**
 * Get the scheduler used by frozen realms. This scheduler does not support
 * notifications, and does not perform any thread checking.
 *
 * This function is thread-safe, and cannot fail.
 */
RLM_API const realm_scheduler_t* realm_scheduler_get_frozen(void);

/**
 * Open a Realm file.
 *
 * @param config Realm configuration. If the Realm is already opened on another
 *               thread, validate that the given configuration is compatible
 *               with the existing one.
 * @return If successful, the Realm object. Otherwise, NULL.
 */
RLM_API realm_t* realm_open(const realm_config_t* config);

/**
 * The overloaded Realm::convert function offers a way to copy and/or convert a realm.
 *
 * The following options are supported:
 * - local -> local (config or path)
 * - local -> sync (config only)
 * - sync -> local (config only)
 * - sync -> sync  (config or path)
 * - sync -> bundlable sync (client file identifier removed)
 *
 * Note that for bundled realms it is required that all local changes are synchronized with the
 * server before the copy can be written. This is to be sure that the file can be used as a
 * stating point for a newly installed application. The function will throw if there are
 * pending uploads.
 */
/**
 * Copy or convert a Realm using a config.
 *
 * If the file already exists and merge_with_existing is true, data will be copied over object per object.
 * When merging, all classes must have a pk called '_id" otherwise an exception is thrown.
 * If the file exists and merge_with_existing is false, an exception is thrown.
 * If the file does not exist, the realm file will be exported to the new location and if the
 * configuration object contains a sync part, a sync history will be synthesized.
 *
 * @param config The realm configuration that should be used to create a copy.
 *               This can be a local or a synced Realm, encrypted or not.
 * @param merge_with_existing If this is true and the destination file exists, data will be copied over object by
 * object. Otherwise, if this is false and the destination file exists, an exception is thrown.
 */
RLM_API bool realm_convert_with_config(const realm_t* realm, const realm_config_t* config, bool merge_with_existing);
/**
 * Copy a Realm using a path.
 *
 * @param path The path the realm should be copied to. Local realms will remain local, synced
 *             realms will remain synced realms.
 * @param encryption_key The optional encryption key for the new realm.
 * @param merge_with_existing If this is true and the destination file exists, data will be copied over object by
 object.
 *  Otherwise, if this is false and the destination file exists, an exception is thrown.

 */
RLM_API bool realm_convert_with_path(const realm_t* realm, const char* path, realm_binary_t encryption_key,
                                     bool merge_with_existing);

/**
 * Deletes the following files for the given `realm_file_path` if they exist:
 * - the Realm file itself
 * - the .management folder
 * - the .note file
 * - the .log file
 *
 * The .lock file for this Realm cannot and will not be deleted as this is unsafe.
 * If a different process / thread is accessing the Realm at the same time a corrupt state
 * could be the result and checking for a single process state is not possible here.
 *
 * @param realm_file_path The path to the Realm file. All files will be derived from this.
 * @param[out] did_delete_realm If non-null, set to true if the primary Realm file was deleted.
 *                              Discard value if the function returns an error.
 *
 * @return true if no error occurred.
 *
 * @throws RLM_ERR_FILE_PERMISSION_DENIED if the operation was not permitted.
 * @throws RLM_ERR_FILE_ACCESS_ERROR for any other error while trying to delete the file or folder.
 * @throws RLM_ERR_DELETE_OPENED_REALM if the function was called on an open Realm.
 */
RLM_API bool realm_delete_files(const char* realm_file_path, bool* did_delete_realm);

/**
 * Create a `realm_t` object from a thread-safe reference to the same realm.
 *
 * @param tsr Thread-safe reference object created by calling
 *            `realm_get_thread_safe_reference()` with a `realm_t` instance.
 * @param scheduler The scheduler to use for the new `realm_t` instance. May be
 *                  NULL, in which case the default scheduler for the current
 *                  thread is used.
 * @return A non-null pointer if no error occurred.
 */
RLM_API realm_t* realm_from_thread_safe_reference(realm_thread_safe_reference_t* tsr, realm_scheduler_t* scheduler);

/**
 * Create a `realm_t*` from a `std::shared_ptr<Realm>*`.
 *
 * This is intended as a migration path for users of the C++ Object Store API.
 *
 * Call `realm_release()` on the returned `realm_t*` to decrement the refcount
 * on the inner `std::shared_ptr<Realm>`.
 *
 * @param pshared_ptr A pointer to an instance of `std::shared_ptr<Realm>`.
 * @param n Must be equal to `sizeof(std::shared_ptr<Realm>)`.
 * @return A `realm_t*` representing the same Realm object as the passed
 *         `std::shared_ptr<Realm>`.
 */
RLM_API realm_t* _realm_from_native_ptr(const void* pshared_ptr, size_t n);

/**
 * Get a `std::shared_ptr<Realm>` from a `realm_t*`.
 *
 * This is intended as a migration path for users of the C++ Object Store API.
 *
 * @param pshared_ptr A pointer to an instance of `std::shared_ptr<Realm>`.
 * @param n Must be equal to `sizeof(std::shared_ptr<Realm>)`.
 */
RLM_API void _realm_get_native_ptr(const realm_t*, void* pshared_ptr, size_t n);

/**
 * Forcibly close a Realm file.
 *
 * Note that this invalidates all Realm instances for the same path.
 *
 * The Realm will be automatically closed when the last reference is released,
 * including references to objects within the Realm.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_close(realm_t*);

/**
 * True if the Realm file is closed.
 *
 * This function cannot fail.
 */
RLM_API bool realm_is_closed(realm_t*);

/**
 * Begin a read transaction for the Realm file.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_begin_read(realm_t*);

/**
 * Begin a write transaction for the Realm file.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_begin_write(realm_t*);

/**
 * Return true if the realm is in a write transaction.
 *
 * This function cannot fail.
 */
RLM_API bool realm_is_writable(const realm_t*);

/**
 * Commit a write transaction.
 *
 * @return True if the commit succeeded and no exceptions were thrown.
 */
RLM_API bool realm_commit(realm_t*);

/**
 * Roll back a write transaction.
 *
 * @return True if the rollback succeeded and no exceptions were thrown.
 */
RLM_API bool realm_rollback(realm_t*);

/**
 * start a new write transaction asynchronously for the realm passed as argument.
 */
RLM_API bool realm_async_begin_write(realm_t* realm, realm_async_begin_write_func_t, realm_userdata_t userdata,
                                     realm_free_userdata_func_t userdata_free, bool notify_only,
                                     unsigned int* transaction_id);

/**
 * commit a transaction asynchronously for the realm passed as argument.
 */
RLM_API bool realm_async_commit(realm_t* realm, realm_async_commit_func_t, realm_userdata_t userdata,
                                realm_free_userdata_func_t userdata_free, bool allow_grouping,
                                unsigned int* transaction_id);

/**
 * Cancel the transaction referenced by the token passed as argument and set the optional boolean flag in order to
 * inform the caller if the transaction was cancelled.
 */
RLM_API bool realm_async_cancel(realm_t* realm, unsigned int token, bool* cancelled);

/**
 * Add a callback that will be invoked every time the view of this file is updated.
 *
 * This callback is guaranteed to be invoked before any object or collection change
 * notifications for this realm are delivered.
 *
 * @return a registration token used to remove the callback.
 */
RLM_API realm_callback_token_t* realm_add_realm_changed_callback(realm_t*, realm_on_realm_change_func_t,
                                                                 realm_userdata_t userdata,
                                                                 realm_free_userdata_func_t userdata_free);

/**
 * Add a callback that will be invoked the first time that the given realm is refreshed to the version which is the
 * latest version at the time when this is called.
 * @return a refresh token to remove the callback
 */
RLM_API realm_refresh_callback_token_t* realm_add_realm_refresh_callback(realm_t*, realm_on_realm_refresh_func_t,
                                                                         realm_userdata_t userdata,
                                                                         realm_free_userdata_func_t userdata_free);

/**
 * Refresh the view of the realm file.
 *
 * If another process or thread has made changes to the realm file, this causes
 * those changes to become visible in this realm instance.
 *
 * This calls `advance_read()` at the Core layer.
 *
 * @return True if no exceptions are thrown, false otherwise.
 */
RLM_API bool realm_refresh(realm_t*, bool* did_refresh);

/**
 * Produce a frozen view of this realm.
 *
 * @return A non-NULL realm instance representing the frozen state.
 */
RLM_API realm_t* realm_freeze(const realm_t*);

/**
 * Vacuum the free space from the realm file, reducing its file size.
 *
 * @return True if no exceptions are thrown, false otherwise.
 */
RLM_API bool realm_compact(realm_t*, bool* did_compact);

/**
 * Find and delete the table passed as parementer for the realm instance passed to this function.
 * @param table_name for the table the user wants to delete
 * @param table_deleted in order to indicate if the table was actually deleted from realm
 * @return true if no error has occurred, false otherwise
 */
RLM_API bool realm_remove_table(realm_t*, const char* table_name, bool* table_deleted);

/**
 * Create a new schema from classes and their properties.
 *
 * Note: This function does not validate the schema.
 *
 * Note: `realm_class_key_t` and `realm_property_key_t` values inside
 *       `realm_class_info_t` and `realm_property_info_t` are unused when
 *       defining the schema. Call `realm_get_schema()` to obtain the values for
 *       these fields in an open realm.
 *
 * @return True if allocation of the schema structure succeeded.
 */
RLM_API realm_schema_t* realm_schema_new(const realm_class_info_t* classes, size_t num_classes,
                                         const realm_property_info_t** class_properties);

/**
 * Get the schema for this realm.
 *
 * Note: The returned value is allocated by this function, so `realm_release()`
 *       must be called on it.
 */
RLM_API realm_schema_t* realm_get_schema(const realm_t*);

/**
 * Get the schema version for this realm.
 *
 * This function cannot fail.
 */
RLM_API uint64_t realm_get_schema_version(const realm_t* realm);

/**
 * Get the schema version for this realm at the path.
 */
RLM_API uint64_t realm_get_persisted_schema_version(const realm_config_t* config);

/**
 * Update the schema of an open realm.
 *
 * This is equivalent to calling `realm_update_schema_advanced(realm, schema, 0,
 * NULL, NULL, NULL, NULL, false)`.
 */
RLM_API bool realm_update_schema(realm_t* realm, const realm_schema_t* schema);

/**
 * Update the schema of an open realm, with options to customize certain steps
 * of the process.
 *
 * @param realm The realm for which the schema should be updated.
 * @param schema The new schema for the realm. If the schema is the same the
 *               existing schema, this function does nothing.
 * @param version The version of the new schema.
 * @param migration_func Callback to perform the migration. Has no effect if the
 *                       Realm is opened with `RLM_SCHEMA_MODE_ADDITIVE`.
 * @param migration_func_userdata Userdata pointer to pass to `migration_func`.
 * @param data_init_func Callback to perform initialization of the data in the
 *                       Realm if it is opened for the first time (i.e., it has
 *                       no previous schema version).
 * @param data_init_func_userdata Userdata pointer to pass to `data_init_func`.
 * @param is_in_transaction Pass true if the realm is already in a write
 *                          transaction. Otherwise, if the migration requires a
 *                          write transaction, this function will perform the
 *                          migration in its own write transaction.
 */
RLM_API bool realm_update_schema_advanced(realm_t* realm, const realm_schema_t* schema, uint64_t version,
                                          realm_migration_func_t migration_func,
                                          realm_userdata_t migration_func_userdata,
                                          realm_data_initialization_func_t data_init_func,
                                          realm_userdata_t data_init_func_userdata, bool is_in_transaction);

/**
 *  Rename a property for the schame  of the open realm.
 *  @param realm The realm for which the property schema has to be renamed
 *  @param schema The schema to modifies
 *  @param object_type type of the object to modify
 *  @param old_name old name of the property
 *  @param new_name new name of the property
 */
RLM_API bool realm_schema_rename_property(realm_t* realm, realm_schema_t* schema, const char* object_type,
                                          const char* old_name, const char* new_name);

/**
 * Get the `realm::Schema*` pointer for this realm.
 *
 * This is intended as a migration path for users of the C++ Object Store API.
 *
 * The returned value is owned by the `realm_t` instance, and must not be freed.
 */
RLM_API const void* _realm_get_schema_native(const realm_t*);

/**
 * Add a callback that will be invoked every time the schema of this realm is changed.
 *
 * @return a registration token used to remove the callback.
 */
RLM_API realm_callback_token_t* realm_add_schema_changed_callback(realm_t*, realm_on_schema_change_func_t,
                                                                  realm_userdata_t userdata,
                                                                  realm_free_userdata_func_t userdata_free);


/**
 * Validate the schema.
 *
 *  @param validation_mode A bitwise combination of values from the
 *                         enum realm_schema_validation_mode.
 *
 * @return True if the schema passed validation. If validation failed,
 *         `realm_get_last_error()` will produce an error describing the
 *         validation failure.
 */
RLM_API bool realm_schema_validate(const realm_schema_t*, uint64_t validation_mode);

/**
 * Return the number of classes in the Realm's schema.
 *
 * This cannot fail.
 */
RLM_API size_t realm_get_num_classes(const realm_t*);

/**
 * Get the table keys for classes in the schema.
 * In case of errors this function will return false (errors to be fetched via `realm_get_last_error()`).
 * If data is not copied the function will return true and set  `out_n` with the capacity needed.
 * Data is only copied if the input array has enough capacity, otherwise the needed  array capacity will be set.
 *
 * @param out_keys An array that will contain the keys of each class in the
 *                 schema. Array may be NULL, in this case no data will be copied and `out_n` set if not NULL.
 * @param max The maximum number of keys to write to `out_keys`.
 * @param out_n The actual number of classes. May be NULL.
 * @return True if no exception occurred.
 */
RLM_API bool realm_get_class_keys(const realm_t*, realm_class_key_t* out_keys, size_t max, size_t* out_n);

/**
 * Find a by the name of @a name.
 *
 * @param name The name of the class.
 * @param out_found Set to true if the class was found and no error occurred.
 *                  Otherwise, false. May not be NULL.
 * @param out_class_info A pointer to a `realm_class_info_t` that will be
 *                       populated with information about the class. May be
 *                       NULL.
 * @return True if no exception occurred.
 */
RLM_API bool realm_find_class(const realm_t*, const char* name, bool* out_found, realm_class_info_t* out_class_info);

/**
 * Get the class with @a key from the schema.
 *
 * Passing an invalid @a key for this schema is considered an error.
 *
 * @param key The key of the class, as discovered by `realm_get_class_keys()`.
 * @param out_class_info A pointer to a `realm_class_info_t` that will be
 *                       populated with the information of the class. May be
 *                       NULL, though that's kind of pointless.
 * @return True if no exception occurred.
 */
RLM_API bool realm_get_class(const realm_t*, realm_class_key_t key, realm_class_info_t* out_class_info);

/**
 * Get the list of properties for the class with this @a key.
 * In case of errors this function will return false (errors to be fetched via `realm_get_last_error()`).
 * If data is not copied the function will return true and set  `out_n` with the capacity needed.
 * Data is only copied if the input array has enough capacity, otherwise the needed  array capacity will be set.
 *
 * @param out_properties  A pointer to an array of `realm_property_info_t`, which
 *                       will be populated with the information about the
 *                       properties.  Array may be NULL, in this case no data will be copied and `out_n` set if not
 * NULL.
 * @param max The maximum number of entries to write to `out_properties`.
 * @param out_n The actual number of properties written to `out_properties`.
 * @return True if no exception occurred.
 */
RLM_API bool realm_get_class_properties(const realm_t*, realm_class_key_t key, realm_property_info_t* out_properties,
                                        size_t max, size_t* out_n);

/**
 * Get the property keys for the class with this @a key.
 * In case of errors this function will return false (errors to be fetched via `realm_get_last_error()`).
 * If data is not copied the function will return true and set  `out_n` with the capacity needed.
 * Data is only copied if the input array has enough capacity, otherwise the needed  array capacity will be set.
 *
 * @param key The class key.
 * @param out_col_keys An array of property keys. Array may be NULL,
 *                     in this case no data will be copied and `out_n` set if not NULL.
 * @param max The maximum number of keys to write to `out_col_keys`. Ignored if
 *            `out_col_keys == NULL`.
 * @param out_n The actual number of properties written to `out_col_keys` (if
 *              non-NULL), or number of properties in the class.
 * @return True if no exception occurred.
 **/
RLM_API bool realm_get_property_keys(const realm_t*, realm_class_key_t key, realm_property_key_t* out_col_keys,
                                     size_t max, size_t* out_n);

/**
 * Get the value for the property at the specified index in the object's schema.
 * @param prop_index The index of the property in the class properties array the realm was opened with.
 * @return True if no exception occurred.
 */
RLM_API bool realm_get_value_by_property_index(const realm_object_t* object, size_t prop_index,
                                               realm_value_t* out_value);

/**
 * Find a property by its column key.
 *
 * It is an error to pass a property @a key that is not present in this class.
 *
 * @param class_key The key of the class.
 * @param key The column key for the property.
 * @param out_property_info A pointer to a `realm_property_info_t` that will be
 *                          populated with information about the property.
 * @return True if no exception occurred.
 */
RLM_API bool realm_get_property(const realm_t*, realm_class_key_t class_key, realm_property_key_t key,
                                realm_property_info_t* out_property_info);

/**
 * Find a property by the internal (non-public) name of @a name.
 *
 * @param class_key The table key for the class.
 * @param name The name of the property.
 * @param out_found Will be set to true if the property was found. May not be
 *                  NULL.
 * @param out_property_info A pointer to a `realm_property_info_t` that will be
 *                          populated with information about the property. May
 *                          be NULL.
 * @return True if no exception occurred.
 */
RLM_API bool realm_find_property(const realm_t*, realm_class_key_t class_key, const char* name, bool* out_found,
                                 realm_property_info_t* out_property_info);

/**
 * Find a property with the public name of @a name.
 *
 * @param class_key The table key for the class.
 * @param public_name The public name of the property.
 * @param out_found Will be set to true if the property was found. May not be
 *                  NULL.
 * @param out_property_info A pointer to a `realm_property_info_t` that will be
 *                          populated with information about the property. May
 *                          be NULL.
 * @return True if no exception occurred.
 */
RLM_API bool realm_find_property_by_public_name(const realm_t*, realm_class_key_t class_key, const char* public_name,
                                                bool* out_found, realm_property_info_t* out_property_info);

/**
 * Find the primary key property for a class, if it has one.
 *
 * @param class_key The table key for this class.
 * @param out_found Will be set to true if the property was found. May not be
 *                  NULL.
 * @param out_property_info A property to a `realm_property_info_t` that will be
 *                          populated with information about the property, if it
 *                          was found. May be NULL.
 * @return True if no exception occurred.
 */
RLM_API bool realm_find_primary_key_property(const realm_t*, realm_class_key_t class_key, bool* out_found,
                                             realm_property_info_t* out_property_info);

/**
 * Get the number of objects in a table (class).
 *
 * @param out_count A pointer to a `size_t` that will contain the number of
 *                  objects, if successful.
 * @return True if the table key was valid for this realm.
 */
RLM_API bool realm_get_num_objects(const realm_t*, realm_class_key_t, size_t* out_count);

/**
 * Get the number of versions found in the Realm file.
 *
 * @param out_versions_count A pointer to a `size_t` that will contain the number of
 *                           versions, if successful.
 * @return True if no exception occurred.
 */
RLM_API bool realm_get_num_versions(const realm_t*, uint64_t* out_versions_count);

/**
 * Get an object with a particular object key.
 *
 * @param class_key The class key.
 * @param obj_key The key to the object. Passing a non-existent key is
 *                considered an error.
 * @return A non-NULL pointer if no exception occurred.
 */
RLM_API realm_object_t* realm_get_object(const realm_t*, realm_class_key_t class_key, realm_object_key_t obj_key);

/**
 * Get the parent object for the object passed as argument. Only works for embedded objects.
 * @return true, if no errors occurred.
 */
RLM_API bool realm_object_get_parent(const realm_object_t* object, realm_object_t** parent,
                                     realm_class_key_t* class_key);

/**
 * Find an object with a particular primary key value.
 *
 * @param out_found A pointer to a boolean that will be set to true or false if
 *                  no error occurred.
 * @return A non-NULL pointer if the object was found and no exception occurred.
 */
RLM_API realm_object_t* realm_object_find_with_primary_key(const realm_t*, realm_class_key_t, realm_value_t pk,
                                                           bool* out_found);

/**
 * Find all objects in class.
 *
 * Note: This is faster than running a query matching all objects (such as
 *       "TRUEPREDICATE").
 *
 * @return A non-NULL pointer if no exception was thrown.
 */
RLM_API realm_results_t* realm_object_find_all(const realm_t*, realm_class_key_t);

/**
 * Create an object in a class without a primary key.
 *
 * @return A non-NULL pointer if the object was created successfully.
 */
RLM_API realm_object_t* realm_object_create(realm_t*, realm_class_key_t);

/**
 * Create an object in a class with a primary key. Will not succeed if an
 * object with the given primary key value already exists.
 *
 * @return A non-NULL pointer if the object was created successfully.
 */
RLM_API realm_object_t* realm_object_create_with_primary_key(realm_t*, realm_class_key_t, realm_value_t pk);

/**
 * Create an object in a class with a primary key. If an object with the given
 * primary key value already exists, that object will be returned.
 *
 * @return A non-NULL pointer if the object was found/created successfully.
 */
RLM_API realm_object_t* realm_object_get_or_create_with_primary_key(realm_t*, realm_class_key_t, realm_value_t pk,
                                                                    bool* did_create);

/**
 * Delete a realm object.
 *
 * Note: This does not call `realm_release()` on the `realm_object_t` instance.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_object_delete(realm_object_t*);

/**
 * Resolve the Realm object in the provided Realm.
 *
 * This is equivalent to producing a thread-safe reference and resolving it in the target realm.
 *
 * If the object can be resolved in the target realm, '*resolved' points to the new object
 * If the object cannot be resolved in the target realm, '*resolved' will be null.
 * @return True if no exception occurred (except exceptions that may normally occur if resolution fails)
 */
RLM_API bool realm_object_resolve_in(const realm_object_t* live_object, const realm_t* target_realm,
                                     realm_object_t** resolved);

/**
 * Increment atomically property specified as parameter by value, for the object passed as argument.
 * @param object valid ptr to an object store in the database
 * @param property_key id of the property to change
 * @param value increment for the property passed as argument
 * @return True if not exception occurred.
 */
RLM_API bool realm_object_add_int(realm_object_t* object, realm_property_key_t property_key, int64_t value);


RLM_API realm_object_t* _realm_object_from_native_copy(const void* pobj, size_t n);
RLM_API realm_object_t* _realm_object_from_native_move(void* pobj, size_t n);
RLM_API const void* _realm_object_get_native_ptr(realm_object_t*);

/**
 * True if this object still exists in the realm.
 *
 * This function cannot fail.
 */
RLM_API bool realm_object_is_valid(const realm_object_t*);

/**
 * Get the key for this object.
 *
 * This function cannot fail.
 */
RLM_API realm_object_key_t realm_object_get_key(const realm_object_t* object);

/**
 * Get the table for this object.
 *
 * This function cannot fail.
 */
RLM_API realm_class_key_t realm_object_get_table(const realm_object_t* object);

/**
 * Get a `realm_link_t` representing a link to @a object.
 *
 * This function cannot fail.
 */
RLM_API realm_link_t realm_object_as_link(const realm_object_t* object);

/**
 * Helper method for making it easier to to convert SDK input to the underlying
 * `realm_key_path_array_t`.
 *
 * @return A pointer to the converted key path array. NULL in case of an error.
 */
RLM_API realm_key_path_array_t* realm_create_key_path_array(const realm_t* realm,
                                                            const realm_class_key_t object_class_key,
                                                            size_t num_key_paths, const char** user_key_paths);

/**
 * Subscribe to notifications for this object.
 *
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_notification_token_t* realm_object_add_notification_callback(realm_object_t*, realm_userdata_t userdata,
                                                                           realm_free_userdata_func_t userdata_free,
                                                                           realm_key_path_array_t* key_path_array,
                                                                           realm_on_object_change_func_t on_change);

/**
 * Get an object from a thread-safe reference, potentially originating in a
 * different `realm_t` instance
 */
RLM_API realm_object_t* realm_object_from_thread_safe_reference(const realm_t*, realm_thread_safe_reference_t*);

/**
 * Get the value for a property.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_get_value(const realm_object_t*, realm_property_key_t, realm_value_t* out_value);

/**
 * Get the values for several properties.
 *
 * This is provided as an alternative to calling `realm_get_value()` multiple
 * times in a row, which is particularly useful for language runtimes where
 * crossing the native bridge is comparatively expensive. In addition, it
 * eliminates some parameter validation that would otherwise be repeated for
 * each call.
 *
 * Example use cases:
 *
 *  - Extracting all properties of an object for serialization.
 *  - Converting an object to some in-memory representation.
 *
 * @param num_values The number of elements in @a properties and @a out_values.
 * @param properties The keys for the properties to fetch. May not be NULL.
 * @param out_values Where to write the property values. If an error occurs,
 *                   this array may only be partially initialized. May not be
 *                   NULL.
 * @return True if no exception occurs.
 */
RLM_API bool realm_get_values(const realm_object_t*, size_t num_values, const realm_property_key_t* properties,
                              realm_value_t* out_values);

/**
 * Set the value for a property.
 *
 * @param new_value The new value for the property.
 * @param is_default True if this property is being set as part of setting the
 *                   default values for a new object. This has no effect in
 *                   non-sync'ed realms.
 * @return True if no exception occurred.
 */
RLM_API bool realm_set_value(realm_object_t*, realm_property_key_t, realm_value_t new_value, bool is_default);

/**
 * Assign a JSON formatted string to a Mixed property. Underlying structures will be created as needed
 *
 * @param json_string The new value for the property.
 * @return True if no exception occurred.
 */
RLM_API bool realm_set_json(realm_object_t*, realm_property_key_t, const char* json_string);

/**
 * Create an embedded object in a given property.
 *
 * @return A non-NULL pointer if the object was created successfully.
 */
RLM_API realm_object_t* realm_set_embedded(realm_object_t*, realm_property_key_t);

/**
 * Create a collection in a given Mixed property.
 *
 */
RLM_API realm_list_t* realm_set_list(realm_object_t*, realm_property_key_t);
RLM_API realm_dictionary_t* realm_set_dictionary(realm_object_t*, realm_property_key_t);

/** Return the object linked by the given property
 *
 * @return A non-NULL pointer if an object is found.
 */
RLM_API realm_object_t* realm_get_linked_object(realm_object_t*, realm_property_key_t);

/**
 * Serializes an object to json and returns it as string. Serializes a single level of properties only.
 *
 * @return a json-serialized representation of the object.
 */
RLM_API char* realm_object_to_string(realm_object_t*);

/**
 * Set the values for several properties.
 *
 * This is provided as an alternative to calling `realm_get_value()` multiple
 * times in a row, which is particularly useful for language runtimes where
 * crossing the native bridge is comparatively expensive. In addition, it
 * eliminates some parameter validation that would otherwise be repeated for
 * each call.
 *
 * Example use cases:
 *
 *  - Initializing a new object with default values.
 *  - Deserializing some in-memory structure into a realm object.
 *
 * This operation is "atomic"; if an exception occurs due to invalid input (such
 * as type mismatch, nullability mismatch, etc.), the object will remain
 * unmodified.
 *
 * @param num_values The number of elements in @a properties and @a values.
 * @param properties The keys of the properties to set. May not be NULL.
 * @param values The values to assign to the properties. May not be NULL.
 * @param is_default True if the properties are being set as part of setting
 *                   default values for a new object. This has no effect in
 *                   non-sync'ed realms.
 * @return True if no exception occurred.
 */
RLM_API bool realm_set_values(realm_object_t*, size_t num_values, const realm_property_key_t* properties,
                              const realm_value_t* values, bool is_default);

/**
 * Get a list instance for the property of an object.
 *
 * Note: It is up to the caller to call `realm_release()` on the returned list.
 *
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_list_t* realm_get_list(realm_object_t*, realm_property_key_t);

/**
 * Create a `realm_list_t` from a pointer to a `realm::List`, copy-constructing
 * the internal representation.
 *
 * @param plist A pointer to an instance of `realm::List`.
 * @param n Must be equal to `sizeof(realm::List)`.
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_list_t* _realm_list_from_native_copy(const void* plist, size_t n);

/**
 * Create a `realm_list_t` from a pointer to a `realm::List`, move-constructing
 * the internal representation.
 *
 * @param plist A pointer to an instance of `realm::List`.
 * @param n Must be equal to `sizeof(realm::List)`.
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_list_t* _realm_list_from_native_move(void* plist, size_t n);

/**
 * Resolve the list in the context of a given Realm instance.
 *
 * This is equivalent to producing a thread-safe reference and resolving it in the frozen realm.
 *
 * If resolution is possible, a valid resolved object is produced at '*resolved*'.
 * If resolution is not possible, but no error occurs, '*resolved' is set to NULL
 *
 * @return true if no error occurred.
 */
RLM_API bool realm_list_resolve_in(const realm_list_t* list, const realm_t* target_realm, realm_list_t** resolved);

/**
 * Check if a list is valid.
 *
 * @return True if the list is valid.
 */
RLM_API bool realm_list_is_valid(const realm_list_t*);

/**
 * Get the size of a list, in number of elements.
 *
 * This function may fail if the object owning the list has been deleted.
 *
 * @param out_size Where to put the list size. May be NULL.
 * @return True if no exception occurred.
 */
RLM_API bool realm_list_size(const realm_list_t*, size_t* out_size);

/**
 * Get the property that this list came from.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_list_get_property(const realm_list_t*, realm_property_info_t* out_property_info);

/**
 * Get the value at @a index.
 *
 * @param out_value The resulting value, if no error occurred. May be NULL,
 *                  though nonsensical.
 * @return True if no exception occurred.
 */
RLM_API bool realm_list_get(const realm_list_t*, size_t index, realm_value_t* out_value);

/**
 * Find the value in the list passed as parameter.
 * @param value to search in the list
 * @param out_index the index in the list where the value has been found or realm::not_found.
 * @param out_found boolean that indicates whether the value is found or not
 * @return true if no exception occurred.
 */
RLM_API bool realm_list_find(const realm_list_t*, const realm_value_t* value, size_t* out_index, bool* out_found);

/**
 * Set the value at @a index.
 *
 * @param value The value to set.
 * @return True if no exception occurred.
 */
RLM_API bool realm_list_set(realm_list_t*, size_t index, realm_value_t value);

/**
 * Insert @a value at @a index.
 *
 * @param value The value to insert.
 * @return True if no exception occurred.
 */
RLM_API bool realm_list_insert(realm_list_t*, size_t index, realm_value_t value);

/**
 * Insert a collection inside a list (only available for mixed types)
 *
 * @param list valid ptr to a list of mixed
 * @param index position in the list where to add the collection
 * @return pointer to a valid collection that has been just inserted at the index passed as argument
 */
RLM_API realm_list_t* realm_list_insert_list(realm_list_t* list, size_t index);
RLM_API realm_dictionary_t* realm_list_insert_dictionary(realm_list_t* list, size_t index);

/**
 * Set a collection inside a list (only available for mixed types).
 * If the list already contains a collection of the requested type, the
 * operation is idempotent.
 *
 * @param list valid ptr to a list where a nested collection needs to be set
 * @param index position in the list where to set the collection
 * @return a valid ptr representing the collection just set
 */
RLM_API realm_list_t* realm_list_set_list(realm_list_t* list, size_t index);
RLM_API realm_dictionary_t* realm_list_set_dictionary(realm_list_t* list, size_t index);

/**
 * Returns a nested list if such collection exists, NULL otherwise.
 *
 * @param list pointer to the list that containes the nested list
 * @param index index of collection in the list
 * @return a pointer to the the nested list found at the index passed as argument
 */
RLM_API realm_list_t* realm_list_get_list(realm_list_t* list, size_t index);

/**
 * Returns a nested dictionary if such collection exists, NULL otherwise.
 *
 * @param list pointer to the list that containes the nested collection into
 * @param index position of collection in the list
 * @return a pointer to the the nested dictionary found at index passed as argument
 */
RLM_API realm_dictionary_t* realm_list_get_dictionary(realm_list_t* list, size_t index);

/**
 * Move the element at @a from_index to @a to_index.
 *
 * @param from_index The index of the element to move.
 * @param to_index The index to move the element to.
 * @return True if no exception occurred.
 */
RLM_API bool realm_list_move(realm_list_t*, size_t from_index, size_t to_index);

/**
 * Insert an embedded object at a given position.
 *
 * @return A non-NULL pointer if the object was created successfully.
 */
RLM_API realm_object_t* realm_list_insert_embedded(realm_list_t*, size_t index);

/**
 * Create an embedded object at a given position.
 *
 * @return A non-NULL pointer if the object was created successfully.
 */
RLM_API realm_object_t* realm_list_set_embedded(realm_list_t*, size_t index);

/**
 * Get object identified at index
 *
 * @return A non-NULL pointer if value is an object.
 */
RLM_API realm_object_t* realm_list_get_linked_object(realm_list_t*, size_t index);

/**
 * Erase the element at @a index.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_list_erase(realm_list_t*, size_t index);

/**
 * Clear a list, removing all elements in the list. In a list of links, this
 * does *NOT* delete the target objects.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_list_clear(realm_list_t*);

/**
 * In a list of objects, delete all objects in the list and clear the list. In a
 * list of values, clear the list.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_list_remove_all(realm_list_t*);

/**
 * Subscribe to notifications for this object.
 *
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_notification_token_t* realm_list_add_notification_callback(realm_list_t*, realm_userdata_t userdata,
                                                                         realm_free_userdata_func_t userdata_free,
                                                                         realm_key_path_array_t* key_path_array,
                                                                         realm_on_collection_change_func_t on_change);

/**
 * Get an list from a thread-safe reference, potentially originating in a
 * different `realm_t` instance
 */
RLM_API realm_list_t* realm_list_from_thread_safe_reference(const realm_t*, realm_thread_safe_reference_t*);

/**
 * True if an object notification indicates that the object was deleted.
 *
 * This function cannot fail.
 */
RLM_API bool realm_object_changes_is_deleted(const realm_object_changes_t*);

/**
 * Get the number of properties that were modified in an object notification.
 *
 * This function cannot fail.
 */
RLM_API size_t realm_object_changes_get_num_modified_properties(const realm_object_changes_t*);

/**
 * Get the column keys for the properties that were modified in an object
 * notification.
 *
 * This function cannot fail.
 *
 * @param out_modified Where the column keys should be written. May be NULL.
 * @param max The maximum number of column keys to write.
 * @return The number of column keys written to @a out_modified, or the number
 *         of modified properties if @a out_modified is NULL.
 */
RLM_API size_t realm_object_changes_get_modified_properties(const realm_object_changes_t*,
                                                            realm_property_key_t* out_modified, size_t max);

/**
 * Get the number of various types of changes in a collection notification.
 *
 * @param out_num_deletions The number of deletions. May be NULL.
 * @param out_num_insertions The number of insertions. May be NULL.
 * @param out_num_modifications The number of modifications. May be NULL.
 * @param out_num_moves The number of moved elements. May be NULL.
 * @param out_collection_was_cleared a flag to signal if the collection has been cleared. May be NULL
 * @param out_collection_was_deleted a flag to signal if the collection has been deleted. May be NULL
 */
RLM_API void realm_collection_changes_get_num_changes(const realm_collection_changes_t*, size_t* out_num_deletions,
                                                      size_t* out_num_insertions, size_t* out_num_modifications,
                                                      size_t* out_num_moves, bool* out_collection_was_cleared,
                                                      bool* out_collection_was_deleted);

/**
 * Get the number of various types of changes in a collection notification,
 * suitable for acquiring the change indices as ranges, which is much more
 * compact in memory than getting the individual indices when multiple adjacent
 * elements have been modified.
 *
 * @param out_num_deletion_ranges The number of deleted ranges. May be NULL.
 * @param out_num_insertion_ranges The number of inserted ranges. May be NULL.
 * @param out_num_modification_ranges The number of modified ranges. May be
 *                                    NULL.
 * @param out_num_moves The number of moved elements. May be NULL.
 */
RLM_API void realm_collection_changes_get_num_ranges(const realm_collection_changes_t*,
                                                     size_t* out_num_deletion_ranges,
                                                     size_t* out_num_insertion_ranges,
                                                     size_t* out_num_modification_ranges, size_t* out_num_moves);
typedef struct realm_collection_move {
    size_t from;
    size_t to;
} realm_collection_move_t;

typedef struct realm_index_range {
    size_t from;
    size_t to;
} realm_index_range_t;

/**
 * Get the indices of changes in a collection notification.
 *
 * Note: For moves, every `from` index will also be present among deletions, and
 *       every `to` index will also be present among insertions.
 *
 * This function cannot fail.
 *
 * @param out_deletion_indices Where to put the indices of deleted elements
 *                             (*before* the deletion happened). May be NULL.
 * @param max_deletion_indices The max number of indices to write to @a
 *                             out_deletion_indices.
 * @param out_insertion_indices Where the put the indices of inserted elements
 *                              (*after* the insertion happened). May be NULL.
 * @param max_insertion_indices The max number of indices to write to @a
 *                              out_insertion_indices.
 * @param out_modification_indices Where to put the indices of modified elements
 *                                 (*before* any insertions or deletions of
 *                                 other elements). May be NULL.
 * @param max_modification_indices The max number of indices to write to @a
 *                                 out_modification_indices.
 * @param out_modification_indices_after Where to put the indices of modified
 *                                       elements (*after* any insertions or
 *                                       deletions of other elements). May be
 *                                       NULL.
 * @param max_modification_indices_after The max number of indices to write to
 *                                       @a out_modification_indices_after.
 * @param out_moves Where to put the pairs of indices of moved elements. May be
 *                  NULL.
 * @param max_moves The max number of pairs to write to @a out_moves.
 */
RLM_API void realm_collection_changes_get_changes(const realm_collection_changes_t*, size_t* out_deletion_indices,
                                                  size_t max_deletion_indices, size_t* out_insertion_indices,
                                                  size_t max_insertion_indices, size_t* out_modification_indices,
                                                  size_t max_modification_indices,
                                                  size_t* out_modification_indices_after,
                                                  size_t max_modification_indices_after,
                                                  realm_collection_move_t* out_moves, size_t max_moves);

RLM_API void realm_collection_changes_get_ranges(
    const realm_collection_changes_t*, realm_index_range_t* out_deletion_ranges, size_t max_deletion_ranges,
    realm_index_range_t* out_insertion_ranges, size_t max_insertion_ranges,
    realm_index_range_t* out_modification_ranges, size_t max_modification_ranges,
    realm_index_range_t* out_modification_ranges_after, size_t max_modification_ranges_after,
    realm_collection_move_t* out_moves, size_t max_moves);

/**
 * Returns the number of changes occurred to the dictionary passed as argument
 *
 * @param changes valid ptr to the dictionary changes structure
 * @param out_deletions_size number of deletions
 * @param out_insertion_size number of insertions
 * @param out_modification_size number of modifications
 * @param out_was_deleted a flag to signal if the dictionary has been deleted.
 */
RLM_API void realm_dictionary_get_changes(const realm_dictionary_changes_t* changes, size_t* out_deletions_size,
                                          size_t* out_insertion_size, size_t* out_modification_size,
                                          bool* out_was_deleted);

/**
 * Returns the list of keys changed for the dictionary passed as argument.
 * The user must assure that there is enough memory to accomodate all the keys
 * calling `realm_dictionary_get_changes` before.
 *
 * @param changes valid ptr to the dictionary changes structure
 * @param deletions list of deleted keys
 * @param deletions_size size of the list of deleted keys
 * @param insertions list of inserted keys
 * @param insertions_size size of the list of inserted keys
 * @param modifications list of modified keys
 * @param modification_size size of the list of modified keys
 * @param collection_was_cleared whether or not the collection was cleared
 */
RLM_API void realm_dictionary_get_changed_keys(const realm_dictionary_changes_t* changes, realm_value_t* deletions,
                                               size_t* deletions_size, realm_value_t* insertions,
                                               size_t* insertions_size, realm_value_t* modifications,
                                               size_t* modification_size, bool* collection_was_cleared);

/**
 * Get a set instance for the property of an object.
 *
 * Note: It is up to the caller to call `realm_release()` on the returned set.
 *
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_set_t* realm_get_set(realm_object_t*, realm_property_key_t);

/**
 * Create a `realm_set_t` from a pointer to a `realm::object_store::Set`,
 * copy-constructing the internal representation.
 *
 * @param pset A pointer to an instance of `realm::object_store::Set`.
 * @param n Must be equal to `sizeof(realm::object_store::Set)`.
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_set_t* _realm_set_from_native_copy(const void* pset, size_t n);

/**
 * Create a `realm_set_t` from a pointer to a `realm::object_store::Set`,
 * move-constructing the internal representation.
 *
 * @param pset A pointer to an instance of `realm::object_store::Set`.
 * @param n Must be equal to `sizeof(realm::object_store::Set)`.
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_set_t* _realm_set_from_native_move(void* pset, size_t n);

/**
 * Resolve the set in the context of a given Realm instance.
 *
 * This is equivalent to producing a thread-safe reference and resolving it in the frozen realm.
 *
 * If resolution is possible, a valid resolved object is produced at '*resolved*'.
 * If resolution is not possible, but no error occurs, '*resolved' is set to NULL
 *
 * @return true if no error occurred.
 */
RLM_API bool realm_set_resolve_in(const realm_set_t* list, const realm_t* target_realm, realm_set_t** resolved);

/**
 * Check if a set is valid.
 *
 * @return True if the set is valid.
 */
RLM_API bool realm_set_is_valid(const realm_set_t*);

/**
 * Get the size of a set, in number of unique elements.
 *
 * This function may fail if the object owning the set has been deleted.
 *
 * @param out_size Where to put the set size. May be NULL.
 * @return True if no exception occurred.
 */
RLM_API bool realm_set_size(const realm_set_t*, size_t* out_size);

/**
 * Get the property that this set came from.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_set_get_property(const realm_set_t*, realm_property_info_t* out_property_info);

/**
 * Get the value at @a index.
 *
 * Note that elements in a set move around arbitrarily when other elements are
 * inserted/removed.
 *
 * @param out_value The resulting value, if no error occurred. May be NULL,
 *                  though nonsensical.
 * @return True if no exception occurred.
 */
RLM_API bool realm_set_get(const realm_set_t*, size_t index, realm_value_t* out_value);

/**
 * Find an element in a set.
 *
 * If @a value has a type that is incompatible with the set, it will be reported
 * as not existing in the set.
 *
 * @param value The value to look for in the set.
 * @param out_index If non-null, and the element is found, this will be
 *                  populated with the index of the found element in the set.
 * @param out_found If non-null, will be set to true if the element was found,
 *                  otherwise will be set to false.
 * @return True if no exception occurred.
 */
RLM_API bool realm_set_find(const realm_set_t*, realm_value_t value, size_t* out_index, bool* out_found);

/**
 * Insert an element in a set.
 *
 * If the element is already in the set, this function does nothing (and does
 * not report an error).
 *
 * @param value The value to insert.
 * @param out_index If non-null, will be set to the index of the inserted
 *                  element, or the index of the existing element.
 * @param out_inserted If non-null, will be set to true if the element did not
 *                     already exist in the set. Otherwise set to false.
 * @return True if no exception occurred.
 */
RLM_API bool realm_set_insert(realm_set_t*, realm_value_t value, size_t* out_index, bool* out_inserted);

/**
 * Erase an element from a set.
 *
 * If the element does not exist in the set, this function does nothing (and
 * does not report an error).
 *
 * @param value The value to erase.
 * @param out_erased If non-null, will be set to true if the element was found
 *                   and erased, and otherwise set to false.
 * @return True if no exception occurred.
 */
RLM_API bool realm_set_erase(realm_set_t*, realm_value_t value, bool* out_erased);

/**
 * Clear a set of values.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_set_clear(realm_set_t*);

/**
 * In a set of objects, delete all objects in the set and clear the set. In a
 * set of values, clear the set.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_set_remove_all(realm_set_t*);

/**
 * Subscribe to notifications for this object.
 *
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_notification_token_t* realm_set_add_notification_callback(realm_set_t*, realm_userdata_t userdata,
                                                                        realm_free_userdata_func_t userdata_free,
                                                                        realm_key_path_array_t* key_path_array,
                                                                        realm_on_collection_change_func_t on_change);
/**
 * Get an set from a thread-safe reference, potentially originating in a
 * different `realm_t` instance
 */
RLM_API realm_set_t* realm_set_from_thread_safe_reference(const realm_t*, realm_thread_safe_reference_t*);

/**
 * Get a dictionary instance for the property of an object.
 *
 * Note: It is up to the caller to call `realm_release()` on the returned dictionary.
 *
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_dictionary_t* realm_get_dictionary(realm_object_t*, realm_property_key_t);

/**
 * Create a `realm_dictionary_t` from a pointer to a `realm::object_store::Dictionary`,
 * copy-constructing the internal representation.
 *
 * @param pdict A pointer to an instance of `realm::object_store::Dictionary`.
 * @param n Must be equal to `sizeof(realm::object_store::Dictionary)`.
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_dictionary_t* _realm_dictionary_from_native_copy(const void* pdict, size_t n);

/**
 * Create a `realm_dictionary_t` from a pointer to a `realm::object_store::Dictionary`,
 * move-constructing the internal representation.
 *
 * @param pdict A pointer to an instance of `realm::object_store::Dictionary`.
 * @param n Must be equal to `sizeof(realm::object_store::Dictionary)`.
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_dictionary_t* _realm_dictionary_from_native_move(void* pdict, size_t n);

/**
 * Resolve the list in the context of a given Realm instance.
 *
 * This is equivalent to producing a thread-safe reference and resolving it in the frozen realm.
 *
 * If resolution is possible, a valid resolved object is produced at '*resolved*'.
 * If resolution is not possible, but no error occurs, '*resolved' is set to NULL
 *
 * @return true if no error occurred.
 */
RLM_API bool realm_dictionary_resolve_in(const realm_dictionary_t* list, const realm_t* target_realm,
                                         realm_dictionary_t** resolved);

/**
 * Check if a list is valid.
 *
 * @return True if the list is valid.
 */
RLM_API bool realm_dictionary_is_valid(const realm_dictionary_t*);

/**
 * Get the size of a dictionary (the number of unique keys).
 *
 * This function may fail if the object owning the dictionary has been deleted.
 *
 * @param out_size Where to put the dictionary size. May be NULL.
 * @return True if no exception occurred.
 */
RLM_API bool realm_dictionary_size(const realm_dictionary_t*, size_t* out_size);


/**
 * Get the property that this dictionary came from.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_dictionary_get_property(const realm_dictionary_t*, realm_property_info_t* out_info);

/**
 * Find an element in a dictionary.
 *
 * @param key The key to look for.
 * @param out_value If non-null, the value for the corresponding key.
 * @param out_found If non-null, will be set to true if the dictionary contained the key.
 * @return True if no exception occurred.
 */
RLM_API bool realm_dictionary_find(const realm_dictionary_t*, realm_value_t key, realm_value_t* out_value,
                                   bool* out_found);

/**
 * Get the key-value pair at @a index.
 *
 * Note that the indices of elements in the dictionary move around as other
 * elements are inserted/removed.
 *
 * @param index The index in the dictionary.
 * @param out_key If non-null, will be set to the key at the corresponding index.
 * @param out_value If non-null, will be set to the value at the corresponding index.
 * @return True if no exception occurred.
 */
RLM_API bool realm_dictionary_get(const realm_dictionary_t*, size_t index, realm_value_t* out_key,
                                  realm_value_t* out_value);

/**
 * Insert or update an element in a dictionary.
 *
 * If the key already exists, the value will be overwritten.
 *
 * @param key The lookup key.
 * @param value The value to insert.
 * @param out_index If non-null, will be set to the index of the element after
 *                  insertion/update.
 * @param out_inserted If non-null, will be set to true if the key did not
 *                     already exist.
 * @return True if no exception occurred.
 */
RLM_API bool realm_dictionary_insert(realm_dictionary_t*, realm_value_t key, realm_value_t value, size_t* out_index,
                                     bool* out_inserted);

/**
 * Insert an embedded object.
 *
 * @return A non-NULL pointer if the object was created successfully.
 */
RLM_API realm_object_t* realm_dictionary_insert_embedded(realm_dictionary_t*, realm_value_t key);

/**
 * Insert a collection inside a dictionary (only available for mixed types)
 *
 * @param dictionary valid ptr to a dictionary of mixed
 * @param key the mixed representing a key for a dictionary (only string)
 * @return pointer to a valid collection that has been just inserted at the key passed as argument
 */
RLM_API realm_list_t* realm_dictionary_insert_list(realm_dictionary_t* dictionary, realm_value_t key);
RLM_API realm_dictionary_t* realm_dictionary_insert_dictionary(realm_dictionary_t*, realm_value_t);


/**
 * Fetch a list from a dictionary.
 * @return a valid list that needs to be deleted by the caller or nullptr in case of an error.
 */
RLM_API realm_list_t* realm_dictionary_get_list(realm_dictionary_t* dictionary, realm_value_t key);

/**
 * Fetch a dictioanry from a dictionary.
 * @return a valid dictionary that needs to be deleted by the caller or nullptr in case of an error.
 */
RLM_API realm_dictionary_t* realm_dictionary_get_dictionary(realm_dictionary_t* dictionary, realm_value_t key);

/**
 * Get object identified by key
 *
 * @return A non-NULL pointer if the value associated with key is an object.
 */
RLM_API realm_object_t* realm_dictionary_get_linked_object(realm_dictionary_t*, realm_value_t key);

/**
 * Erase a dictionary element.
 *
 * @param key The key of the element to erase.
 * @param out_erased If non-null, will be set to true if the element was found
 *                   and erased.
 * @return True if no exception occurred.
 */
RLM_API bool realm_dictionary_erase(realm_dictionary_t*, realm_value_t key, bool* out_erased);

/**
 * Return the list of keys stored in the dictionary
 *
 * @param out_size number of keys
 * @param out_keys the list of keys in the dictionary, the memory has to be released once it is no longer used.
 * @return True if no exception occurred.
 */
RLM_API bool realm_dictionary_get_keys(realm_dictionary_t*, size_t* out_size, realm_results_t** out_keys);

/**
 * Check if the dictionary contains a certain key
 *
 * @param key to search in the dictionary
 * @param found True if the such key exists
 * @return True if no exception occurred
 */
RLM_API bool realm_dictionary_contains_key(const realm_dictionary_t*, realm_value_t key, bool* found);

/**
 * Check if the dictionary contains a certain value
 *
 * @param value to search in the dictionary
 * @param index the index of the value in the dictionry if such value exists
 * @return True if no exception occurred
 */
RLM_API bool realm_dictionary_contains_value(const realm_dictionary_t*, realm_value_t value, size_t* index);


/**
 * Clear a dictionary.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_dictionary_clear(realm_dictionary_t*);

/**
 * Subscribe to notifications for this object.
 *
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_notification_token_t* realm_dictionary_add_notification_callback(
    realm_dictionary_t*, realm_userdata_t userdata, realm_free_userdata_func_t userdata_free,
    realm_key_path_array_t* key_path_array, realm_on_dictionary_change_func_t on_change);

/**
 * Get an dictionary from a thread-safe reference, potentially originating in a
 * different `realm_t` instance
 */
RLM_API realm_dictionary_t* realm_dictionary_from_thread_safe_reference(const realm_t*,
                                                                        realm_thread_safe_reference_t*);

/**
 * Parse a query string and bind it to a table.
 *
 * If the query failed to parse, the parser error is available from
 * `realm_get_last_error()`.
 *
 * @param target_table The table on which to run this query.
 * @param query_string A zero-terminated string in the Realm Query Language,
 *                     optionally containing argument placeholders (`$0`, `$1`,
 *                     etc.).
 * @param num_args The number of arguments for this query.
 * @param args A pointer to a list of argument values.
 * @return A non-null pointer if the query was successfully parsed and no
 *         exception occurred.
 */
RLM_API realm_query_t* realm_query_parse(const realm_t*, realm_class_key_t target_table, const char* query_string,
                                         size_t num_args, const realm_query_arg_t* args);


/**
 * Get textual representation of query
 *
 * @return a string containing the description. The string memory is managed by the query object.
 */
RLM_API const char* realm_query_get_description(realm_query_t*);


/**
 * Parse a query string and append it to an existing query via logical &&.
 * The query string applies to the same table and Realm as the existing query.
 *
 * If the query failed to parse, the parser error is available from
 * `realm_get_last_error()`.
 *
 * @param query_string A zero-terminated string in the Realm Query Language,
 *                     optionally containing argument placeholders (`$0`, `$1`,
 *                     etc.).
 * @param num_args The number of arguments for this query.
 * @param args A pointer to a list of argument values.
 * @return A non-null pointer if the query was successfully parsed and no
 *         exception occurred.
 */
RLM_API realm_query_t* realm_query_append_query(const realm_query_t*, const char* query_string, size_t num_args,
                                                const realm_query_arg_t* args);

/**
 * Parse a query string and bind it to a list.
 *
 * If the query failed to parse, the parser error is available from
 * `realm_get_last_error()`.
 *
 * @param target_list The list on which to run this query.
 * @param query_string A string in the Realm Query Language, optionally
 *                     containing argument placeholders (`$0`, `$1`, etc.).
 * @param num_args The number of arguments for this query.
 * @param args A pointer to a list of argument values.
 * @return A non-null pointer if the query was successfully parsed and no
 *         exception occurred.
 */
RLM_API realm_query_t* realm_query_parse_for_list(const realm_list_t* target_list, const char* query_string,
                                                  size_t num_args, const realm_query_arg_t* args);

/**
 * Parse a query string and bind it to a set.
 *
 * If the query failed to parse, the parser error is available from
 * `realm_get_last_error()`.
 *
 * @param target_set The set on which to run this query.
 * @param query_string A string in the Realm Query Language, optionally
 *                     containing argument placeholders (`$0`, `$1`, etc.).
 * @param num_args The number of arguments for this query.
 * @param args A pointer to a list of argument values.
 * @return A non-null pointer if the query was successfully parsed and no
 *         exception occurred.
 */
RLM_API realm_query_t* realm_query_parse_for_set(const realm_set_t* target_set, const char* query_string,
                                                 size_t num_args, const realm_query_arg_t* args);
/**
 * Parse a query string and bind it to another query result.
 *
 * If the query failed to parse, the parser error is available from
 * `realm_get_last_error()`.
 *
 * @param target_results The results on which to run this query.
 * @param query_string A zero-terminated string in the Realm Query Language,
 *                     optionally containing argument placeholders (`$0`, `$1`,
 *                     etc.).
 * @param num_args The number of arguments for this query.
 * @param args A pointer to a list of argument values.
 * @return A non-null pointer if the query was successfully parsed and no
 *         exception occurred.
 */
RLM_API realm_query_t* realm_query_parse_for_results(const realm_results_t* target_results, const char* query_string,
                                                     size_t num_args, const realm_query_arg_t* args);

/**
 * Count the number of objects found by this query.
 */
RLM_API bool realm_query_count(const realm_query_t*, size_t* out_count);

/**
 * Return the first object matched by this query.
 *
 * Note: This function can only produce objects, not values. Use the
 *       `realm_results_t` returned by `realm_query_find_all()` to retrieve
 *       values from a list of primitive values.
 *
 * @param out_value Where to write the result, if any object matched the query.
 *                  May be NULL.
 * @param out_found Where to write whether the object was found. May be NULL.
 * @return True if no exception occurred.
 */
RLM_API bool realm_query_find_first(realm_query_t*, realm_value_t* out_value, bool* out_found);

/**
 * Produce a results object for this query.
 *
 * Note: This does not actually run the query until the results are accessed in
 *       some way.
 *
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_results_t* realm_query_find_all(realm_query_t*);

/**
 * Convert a list to results.
 *
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_results_t* realm_list_to_results(realm_list_t*);

/**
 * Convert a set to results.
 *
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_results_t* realm_set_to_results(realm_set_t*);

/**
 * Convert a dictionary to results.
 *
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_results_t* realm_dictionary_to_results(realm_dictionary_t*);

/**
 * Fetch the backlinks for the object passed as argument.
 * @return a valid ptr to realm results that contains all the backlinks for the object, or null in case of errors.
 */
RLM_API realm_results_t* realm_get_backlinks(realm_object_t* object, realm_class_key_t source_table_key,
                                             realm_property_key_t property_key);

/**
 * Delete all objects matched by a query.
 */
RLM_API bool realm_query_delete_all(const realm_query_t*);

/**
 * Set the boolean passed as argument to true or false whether the realm_results passed is valid or not
 * @return true/false if no exception has occurred.
 */
RLM_API bool realm_results_is_valid(const realm_results_t*, bool*);

/**
 * Count the number of results.
 *
 * If the result is "live" (not a snapshot), this may rerun the query if things
 * have changed.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_results_count(realm_results_t*, size_t* out_count);

/**
 * Create a new results object by further filtering existing result.
 *
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_results_t* realm_results_filter(realm_results_t*, realm_query_t*);

/**
 * Create a new results object by further sorting existing result.
 *
 * @param sort_string Specifies a sort condition. It has the format
          <param> ["," <param>]*
          <param> ::= <prop> ["." <prop>]* <direction>,
          <direction> ::= "ASCENDING" | "DESCENDING"
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_results_t* realm_results_sort(realm_results_t* results, const char* sort_string);

/**
 * Create a new results object by removing duplicates
 *
 * @param distinct_string Specifies a distinct condition. It has the format
          <param> ["," <param>]*
          <param> ::= <prop> ["." <prop>]*
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_results_t* realm_results_distinct(realm_results_t* results, const char* distinct_string);

/**
 * Create a new results object by limiting the number of items
 *
 * @param max_count Specifies the number of elements the new result can have at most
 * @return A non-null pointer if no exception occurred.
 */
RLM_API realm_results_t* realm_results_limit(realm_results_t* results, size_t max_count);

/**
 * Get the matching element at @a index in the results.
 *
 * If the result is "live" (not a snapshot), this may rerun the query if things
 * have changed.
 *
 * Note: The bound returned by `realm_results_count()` for a non-snapshot result
 *       is not a reliable way to iterate over elements in the result, because
 *       the result will be live-updated if changes are made in each iteration
 *       that may change the number of query results or even change the
 *       ordering. In other words, this method should probably only be used with
 *       snapshot results.
 *
 * @return True if no exception occurred (including out-of-bounds).
 */
RLM_API bool realm_results_get(realm_results_t*, size_t index, realm_value_t* out_value);

/**
 * Returns an instance of realm_list at the index passed as argument.
 * @return A valid ptr to a list instance or nullptr in case of errors
 */
RLM_API realm_list_t* realm_results_get_list(realm_results_t*, size_t index);

/**
 * Returns an instance of realm_dictionary for the index passed as argument.
 * @return A valid ptr to a dictionary instance or nullptr in case of errors
 */
RLM_API realm_dictionary_t* realm_results_get_dictionary(realm_results_t*, size_t index);

/**
 * Find the index for the value passed as parameter inside realm results pointer passed a input parameter.
 *  @param value the value to find inside the realm results
 *  @param out_index the index where the object has been found, or realm::not_found
 *  @param out_found boolean indicating if the value has been found or not
 *  @return true if no error occurred, false otherwise
 */
RLM_API bool realm_results_find(realm_results_t*, realm_value_t* value, size_t* out_index, bool* out_found);

/**
 * Get the matching object at @a index in the results.
 *
 * If the result is "live" (not a snapshot), this may rerun the query if things
 * have changed.
 *
 * Note: The bound returned by `realm_results_count()` for a non-snapshot result
 *       is not a reliable way to iterate over elements in the result, because
 *       the result will be live-updated if changes are made in each iteration
 *       that may change the number of query results or even change the
 *       ordering. In other words, this method should probably only be used with
 *       snapshot results.
 *
 * @return An instance of `realm_object_t` if no exception occurred.
 */
RLM_API realm_object_t* realm_results_get_object(realm_results_t*, size_t index);

/**
 * Return the query associated to the results passed as argument.
 *
 * @param results the ptr to a valid results object.
 * @return a valid ptr to realm_query_t if no error has occurred
 */
RLM_API realm_query_t* realm_results_get_query(realm_results_t* results);

/**
 * Find the index for the realm object passed as parameter inside realm results pointer passed a input parameter.
 *  @param value the value to find inside the realm results
 *  @param out_index the index where the object has been found, or realm::not_found
 *  @param out_found boolean indicating if the value has been found or not
 *  @return true if no error occurred, false otherwise
 */
RLM_API bool realm_results_find_object(realm_results_t*, realm_object_t* value, size_t* out_index, bool* out_found);

/**
 * Delete all objects in the result.
 *
 * If the result if "live" (not a snapshot), this may rerun the query if things
 * have changed.
 *
 * @return True if no exception occurred.
 */
RLM_API bool realm_results_delete_all(realm_results_t*);

/**
 * Return a snapshot of the results that never automatically updates.
 *
 * The returned result is suitable for use with `realm_results_count()` +
 * `realm_results_get()`.
 */
RLM_API realm_results_t* realm_results_snapshot(const realm_results_t*);

/**
 * Map the Results into a live Realm instance.
 *
 * This is equivalent to producing a thread-safe reference and resolving it in the live realm.
 *
 * @return A live copy of the Results.
 */
RLM_API realm_results_t* realm_results_resolve_in(realm_results_t* from_results, const realm_t* target_realm);

/**
 * Compute the minimum value of a property in the results.
 *
 * @param out_min Where to write the result, if there were matching rows.
 * @param out_found Set to true if there are matching rows.
 * @return True if no exception occurred.
 */
RLM_API bool realm_results_min(realm_results_t*, realm_property_key_t, realm_value_t* out_min, bool* out_found);

/**
 * Compute the maximum value of a property in the results.
 *
 * @param out_max Where to write the result, if there were matching rows.
 * @param out_found Set to true if there are matching rows.
 * @return True if no exception occurred.
 */
RLM_API bool realm_results_max(realm_results_t*, realm_property_key_t, realm_value_t* out_max, bool* out_found);

/**
 * Compute the sum value of a property in the results.
 *
 * @param out_sum Where to write the result. Zero if no rows matched.
 * @param out_found Set to true if there are matching rows.
 * @return True if no exception occurred.
 */
RLM_API bool realm_results_sum(realm_results_t*, realm_property_key_t, realm_value_t* out_sum, bool* out_found);

/**
 * Compute the average value of a property in the results.
 *
 * Note: For numeric columns, the average is always converted to double.
 *
 * @param out_average Where to write the result.
 * @param out_found Set to true if there are matching rows.
 * @return True if no exception occurred.
 */
RLM_API bool realm_results_average(realm_results_t*, realm_property_key_t, realm_value_t* out_average,
                                   bool* out_found);

RLM_API realm_notification_token_t* realm_results_add_notification_callback(realm_results_t*,
                                                                            realm_userdata_t userdata,
                                                                            realm_free_userdata_func_t userdata_free,
                                                                            realm_key_path_array_t* key_path_array,
                                                                            realm_on_collection_change_func_t);

/**
 * Get an results object from a thread-safe reference, potentially originating
 * in a different `realm_t` instance
 */
RLM_API realm_results_t* realm_results_from_thread_safe_reference(const realm_t*, realm_thread_safe_reference_t*);

/**
 * In case of exception thrown in user code callbacks, this api will allow the sdk to store the user code exception
 * and retrieve a it later via realm_get_last_error.
 * Most importantly the SDK is responsible to handle the memory pointed by user_code_error.
 * @param usercode_error pointer representing whatever object the SDK treats as exception/error.
 */
RLM_API void realm_register_user_code_callback_error(realm_userdata_t usercode_error) RLM_API_NOEXCEPT;

#endif // REALM_H
