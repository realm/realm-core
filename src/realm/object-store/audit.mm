////////////////////////////////////////////////////////////////////////////
//
// Copyright 2022 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/object-store/audit.hpp>
#include <realm/object-store/audit_serializer.hpp>

#include <realm/object-store/impl/collection_change_builder.hpp>
#include <realm/object-store/impl/collection_notifier.hpp>
#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/schema.hpp>
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/sync_manager.hpp>
#include <realm/object-store/sync/sync_session.hpp>
#include <realm/object-store/sync/sync_user.hpp>
#include <realm/object-store/util/scheduler.hpp>

#include <realm/dictionary.hpp>
#include <realm/sync/instruction_replication.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/table_view.hpp>
#include <realm/util/file.hpp>
#include <realm/util/logger.hpp>

#include <dispatch/dispatch.h>
#include <external/json/json.hpp>
#include <external/mpark/variant.hpp>
#include <sys/time.h>

using namespace realm;

namespace realm {
static void to_json(nlohmann::json& j, Timestamp const& ts) noexcept
{
    if (ts.is_null()) {
        j = nullptr;
        return;
    }

    auto seconds = time_t(ts.get_seconds());
    char buf[sizeof "1970-01-01T00:00:00.123Z"];
    size_t len = strftime(buf, sizeof buf, "%FT%T", gmtime(&seconds));
    snprintf(buf + len, sizeof ".000Z", ".%03dZ", ts.get_nanoseconds() / 1'000'000);
    j = buf;
}
static void to_json(nlohmann::json& j, StringData s) noexcept
{
    if (s)
        j = std::string(s);
    else
        j = nullptr;
}
} // namespace realm

namespace {
namespace audit_event {
struct Query {
    Timestamp timestamp;
    VersionID version;
    TableKey table;
    std::vector<ObjKey> objects;
};
struct Write {
    Timestamp timestamp;
    VersionID prev_version;
    VersionID version;
};
struct Object {
    // Fields which are always set
    Timestamp timestamp;
    realm::VersionID version;
    TableKey table;
    ObjKey obj;

    // Fields which are set only if this object read was the result of accessing
    // a link, which modifies the serialization of the parent object.
    TableKey parent_table;
    ObjKey parent_obj;
    ColKey parent_col;
};
struct Custom {
    Timestamp timestamp;
    std::string activity;
    util::Optional<std::string> event_type;
    util::Optional<std::string> data;
};
} // namespace audit_event

using Event = mpark::variant<audit_event::Query, audit_event::Write, audit_event::Object, audit_event::Custom>;

util::UniqueFunction<Timestamp()> g_audit_clock;

Timestamp now()
{
    if (g_audit_clock)
        return g_audit_clock();
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return Timestamp(tv.tv_sec, tv.tv_usec * 1000);
}

template <class Container>
void sort_and_unique(Container& c)
{
    std::sort(c.begin(), c.end());
    c.erase(std::unique(c.begin(), c.end()), c.end());
}

template <class Container>
void in_place_set_difference(Container& a, const Container& b)
{
    auto a_begin = a.begin(), a_out = a.begin(), a_end = a.end();
    auto b_begin = b.begin(), b_end = b.end();
    while (a_begin != a_end && b_begin != b_end) {
        if (*a_begin < *b_begin)
            *a_out++ = *a_begin++;
        else if (*a_begin == *b_begin)
            ++a_begin;
        else
            ++b_begin;
    }

    if (a_begin == a_out)
        return;
    while (a_begin != a_end)
        *a_out++ = *a_begin++;
    a.erase(a_out, a_end);
}

class TransactLogHandler {
public:
    TransactLogHandler(Group const& g, AuditObjectSerializer& serializer)
        : m_group(g)
        , m_serializer(serializer)
        , m_data(nlohmann::json::object())
    {
    }

    bool has_any_changes() const
    {
        return !m_data.empty();
    }

    std::string const& data() const
    {
        return m_str;
    }

    void parse_complete()
    {
        for (auto& [table_key, changes] : m_tables) {
            sort_and_unique(changes.insertions);
            sort_and_unique(changes.deletions);
            sort_and_unique(changes.modifications);

            // Filter out inserted and then deleted objects from both insertions
            // and deletions
            auto insertions = changes.insertions;
            in_place_set_difference(changes.insertions, changes.deletions);
            in_place_set_difference(changes.deletions, insertions);

            // Remove insertions and deletions from modifications
            in_place_set_difference(changes.modifications, changes.deletions);
            in_place_set_difference(changes.modifications, changes.insertions);

            auto table = m_group.get_table(table_key);
            if (table->is_embedded()) {
                // Embedded objects don't generate insertion instructions, and
                // instead we just see the modifications on an object that doesn't
                // exist in the previous version. We don't need to report anything
                // for the insertion as the new embedded object will be reported as
                // part of the modification on the parent.
                changes.modifications.erase(std::remove_if(changes.modifications.begin(), changes.modifications.end(),
                                                           [&](ObjKey obj_key) {
                                                               return !table->is_valid(obj_key);
                                                           }),
                                            changes.modifications.end());

                // Deleting an embedded object is reported as a mutation on the
                // parent object.
                changes.deletions.clear();
            }

            if (changes.insertions.empty() && changes.deletions.empty() && changes.modifications.empty()) {
                continue;
            }

            auto object_type = ObjectStore::object_type_for_table_name(table->get_name());
            auto& data = m_data[object_type];
            if (!changes.modifications.empty()) {
                auto& objects = data["modifications"];
                for (auto obj_key : changes.modifications) {
                    auto& obj = objects[objects.size()];
                    m_serializer.to_json(obj["oldValue"], table->get_object(obj_key));
                }
            }
            if (!changes.deletions.empty()) {
                auto& objects = data["deletions"];
                for (auto obj_key : changes.deletions) {
                    m_serializer.to_json(objects[objects.size()], table->get_object(obj_key));
                }
            }
        }
    }

    void after_advance()
    {
        for (auto& [table_key, changes] : m_tables) {
            if (changes.insertions.empty() && changes.deletions.empty() && changes.modifications.empty()) {
                continue;
            }
            auto table = m_group.get_table(table_key);
            auto& data = m_data[ObjectStore::object_type_for_table_name(table->get_name())];
            if (!changes.modifications.empty()) {
                auto& objects = data["modifications"];
                size_t i = 0;
                for (auto obj_key : changes.modifications) {
                    auto& obj = objects[i++];
                    auto& new_value = obj["newValue"];
                    m_serializer.to_json(new_value, table->get_object(obj_key));

                    // Remove all fields from newValue which did not actually change
                    auto& old_value = obj["oldValue"];
                    for (auto it = old_value.begin(); it != old_value.end(); ++it) {
                        if (new_value[it.key()] == it.value())
                            new_value.erase(it.key());
                    }
                }
            }

            if (!changes.insertions.empty()) {
                auto& objects = data["insertions"];
                for (auto obj_key : changes.insertions) {
                    m_serializer.to_json(objects[objects.size()], table->get_object(obj_key));
                }
            }
        }
        m_str = m_data.dump();
    }

    bool select_table(TableKey tk) noexcept
    {
        m_active_table = &m_tables[tk];
        return true;
    }

    bool create_object(ObjKey k) noexcept
    {
        REALM_ASSERT(m_active_table);
        m_active_table->insertions.push_back(k);
        return true;
    }

    bool remove_object(ObjKey k) noexcept
    {
        REALM_ASSERT(m_active_table);
        m_active_table->deletions.push_back(k);
        return true;
    }

    bool modify_object(ColKey, ObjKey obj) noexcept
    {
        REALM_ASSERT(m_active_table);
        m_active_table->modifications.push_back(obj);
        return true;
    }

    bool select_collection(ColKey, ObjKey obj) noexcept
    {
        REALM_ASSERT(m_active_table);
        m_active_table->modifications.push_back(obj);
        return true;
    }

    // clang-format off
    // We don't care about fine-grained changes to collections and just do
    // object-level change tracking, which is covered by select_collection()
    bool list_set(size_t) { return true; }
    bool list_insert(size_t) { return true; }
    bool list_move(size_t, size_t) { return true; }
    bool list_erase(size_t) { return true; }
    bool list_clear(size_t) { return true; }
    bool dictionary_insert(size_t, Mixed const&) { return true; }
    bool dictionary_set(size_t, Mixed const&) { return true; }
    bool dictionary_erase(size_t, Mixed const&) { return true; }
    bool set_insert(size_t) { return true; }
    bool set_erase(size_t) { return true; }
    bool set_clear(size_t) { return true; }

    // We don't run this code on arbitrary transactions that could perform schema changes
    bool insert_group_level_table(TableKey) { unexpected_instruction(); }
    bool erase_class(TableKey) { unexpected_instruction(); }
    bool rename_class(TableKey) { unexpected_instruction(); }
    bool enumerate_string_column(ColKey) { unexpected_instruction(); }
    bool insert_column(ColKey) { unexpected_instruction(); }
    bool erase_column(ColKey) { unexpected_instruction(); }
    bool rename_column(ColKey) { unexpected_instruction(); }
    bool set_link_type(ColKey) { unexpected_instruction(); }
    bool typed_link_change(ColKey, TableKey) { unexpected_instruction(); }
    // clang-format on

private:
    REALM_NORETURN
    REALM_NOINLINE
    void unexpected_instruction()
    {
        REALM_TERMINATE("Unexpected transaction log instruction encountered");
    }

    struct TableChanges {
        std::vector<ObjKey> insertions;
        std::vector<ObjKey> deletions;
        std::vector<ObjKey> modifications;
    };

    Group const& m_group;
    AuditObjectSerializer& m_serializer;
    std::unordered_map<TableKey, TableChanges> m_tables;
    TableChanges* m_active_table = nullptr;
    nlohmann::json m_data;
    std::string m_str;
};

// Deduplication filter which combines redudant reads on objects and folds
// reads on objects after a query into the query's event.
class ReadCombiner {
public:
    ReadCombiner(util::Logger& logger)
        : m_logger(logger)
    {
    }

    bool operator()(audit_event::Query& query)
    {
        m_logger.trace("Audit: Query on %1 at version %2", query.table, query.version);
        if (m_previous_query && m_previous_query->table == query.table &&
            m_previous_query->version == query.version) {
            m_logger.trace("Audit: merging query into previous query");
            m_previous_query->objects.insert(m_previous_query->objects.end(), query.objects.begin(),
                                             query.objects.end());
            return true;
        }
        m_previous_query = &query;
        m_previous_obj = nullptr;
        return false;
    }

    bool operator()(audit_event::Object const& obj)
    {
        m_logger.trace("Audit: Object read on %1 %2 at version %3", obj.table, obj.obj, obj.version);
        if (m_previous_query && m_previous_query->table == obj.table && m_previous_query->version == obj.version) {
            m_logger.trace("Audit: merging read into previous query");
            m_previous_query->objects.push_back(obj.obj);
            return true;
        }
        if (m_previous_obj && m_previous_obj->table == obj.table && m_previous_obj->obj == obj.obj &&
            m_previous_obj->version == obj.version) {
            m_logger.trace("Audit: discarding duplicate read");
            return true;
        }
        m_previous_obj = &obj;
        m_previous_query = nullptr;
        return false;
    }

    bool operator()(audit_event::Write const&)
    {
        return false;
    }
    bool operator()(audit_event::Custom const&)
    {
        return false;
    }

private:
    util::Logger& m_logger;
    const audit_event::Object* m_previous_obj = nullptr;
    audit_event::Query* m_previous_query = nullptr;
};

// Filter which discards events which would be empty (possibly due to
// ReadCombiner merging events, or because they were always empty).
class EmptyQueryFilter {
public:
    EmptyQueryFilter(DB& db, util::Logger& logger)
        : m_db(db)
        , m_logger(logger)
    {
    }

    bool operator()(audit_event::Query& query)
    {
        std::sort(query.objects.begin(), query.objects.end());
        query.objects.erase(std::unique(query.objects.begin(), query.objects.end()), query.objects.end());
        query.objects.erase(std::remove_if(query.objects.begin(), query.objects.end(),
                                           [&](auto& obj) {
                                               return !object_exists(query.version, query.table, obj);
                                           }),
                            query.objects.end());
        if (query.objects.empty())
            m_logger.trace("Audit: discarding empty query on %1", query.table);
        return query.objects.empty();
    }

    bool operator()(audit_event::Object const& obj)
    {
        bool exists = object_exists(obj.version, obj.table, obj.obj);
        if (!exists)
            m_logger.trace("Audit: discarding read on newly created object %1 %2", obj.table, obj.obj);
        return !exists;
    }

    bool operator()(audit_event::Write const&)
    {
        return false;
    }
    bool operator()(audit_event::Custom const&)
    {
        return false;
    }

private:
    DB& m_db;
    util::Logger& m_logger;
    TransactionRef m_transaction;

    bool object_exists(VersionID v, TableKey table, ObjKey obj)
    {
        if (!m_transaction || m_transaction->get_version_of_current_transaction() != v) {
            m_transaction = m_db.start_read(v);
        }
        return m_transaction->get_table(table)->is_valid(obj);
    }
};

class TrackLinkAccesses {
public:
    TrackLinkAccesses(AuditObjectSerializer& serializer)
        : m_serializer(serializer)
    {
    }

    void operator()(audit_event::Object const& obj)
    {
        if (obj.parent_table) {
            m_serializer.link_accessed(obj.version, obj.parent_table, obj.parent_obj, obj.parent_col);
        }
    }

    void operator()(audit_event::Query&) {}
    void operator()(audit_event::Write const&) {}
    void operator()(audit_event::Custom const&) {}

private:
    AuditObjectSerializer& m_serializer;
};

struct MetadataSchema {
    std::vector<std::pair<std::string, std::string>> metadata;
    std::vector<ColKey> metadata_cols;
    ColKey col_timestamp;
    ColKey col_activity;
    ColKey col_event_type;
    ColKey col_data;
};

class AuditEventWriter {
public:
    AuditEventWriter(DB& db, MetadataSchema const& metadata, StringData activity_name, Table& audit_table,
                     AuditObjectSerializer& serializer)
        : m_source_db(db)
        , m_schema(metadata)
        , m_serializer(serializer)
        , m_table(audit_table)
        , m_repl(*m_table.get_parent_group()->get_replication())
        , m_repl_buffer([=]() -> const util::AppendBuffer<char>& {
            REALM_ASSERT(typeid(m_repl) == typeid(sync::ClientReplication));
            return static_cast<sync::SyncReplication&>(m_repl).get_instruction_encoder().buffer();
        }())
        , m_activity(activity_name)
    {
    }

    bool operator()(audit_event::Query const& query)
    {
        auto& g = read(query.version);
        nlohmann::json data;
        auto table = g.get_table(query.table);
        data["type"] = ObjectStore::object_type_for_table_name(table->get_name());
        auto& value = data["value"];
        for (auto& obj : query.objects)
            m_serializer.to_json(value[value.size()], table->get_object(obj));
        auto str = data.dump();
        return write_event(query.timestamp, m_activity, "read", str);
    }

    bool operator()(audit_event::Write const& write)
    {
        auto& g = read(write.prev_version);
        TransactLogHandler changes(g, m_serializer);
        g.advance_read(&changes, write.version);
        changes.after_advance();

        if (changes.has_any_changes())
            return write_event(write.timestamp, m_activity, "write", changes.data());
        return 0;
    }

    bool operator()(audit_event::Object const& obj)
    {
        auto& g = read(obj.version);
        auto table = g.get_table(obj.table);

        nlohmann::json data;
        data["type"] = ObjectStore::object_type_for_table_name(table->get_name());
        m_serializer.to_json(data["value"][0], table->get_object(obj.obj));
        auto str = data.dump();
        return write_event(obj.timestamp, m_activity, "read", str);
    }

    bool operator()(audit_event::Custom const& event)
    {
        return write_event(event.timestamp, event.activity, event.event_type, event.data);
    }

private:
    DB& m_source_db;
    MetadataSchema const& m_schema;
    AuditObjectSerializer& m_serializer;
    Table& m_table;
    Replication& m_repl;
    const util::AppendBuffer<char>& m_repl_buffer;
    const StringData m_activity;

    TransactionRef m_source_transaction;

    std::vector<uint8_t> m_compress_buffer;
    std::vector<uint8_t> m_compress_scratch;

    Transaction& read(VersionID v)
    {
        if (!m_source_transaction || m_source_transaction->get_version_of_current_transaction() != v) {
            m_source_transaction = m_source_db.start_read(v);
            m_serializer.set_version(v);
        }
        return *m_source_transaction;
    }

    bool write_event(Timestamp timestamp, StringData activity, StringData event_type, StringData data)
    {
        // The server has a maximum body size for UPLOAD messages of 16777217
        // bytes. To avoid exceeding this, we need to calculate the size of the
        // changeset we're going to produce and split it up into multiple
        // commits if needed. There's a small amount of overhead for the message
        // header and the instructions, so this number is smaller to ensure that
        // there's space for that.
        constexpr const size_t max_payload_size = 15777217;

        size_t size = activity.size() + event_type.size() + data.size();
        for (size_t i = 0; i < m_schema.metadata.size(); ++i) {
            size += m_schema.metadata[i].first.size() + m_schema.metadata[i].second.size();
        }
        if (size > max_payload_size) {
            // This event by itself is too large. It's unclear what we can even
            // do here.
        }
        if (size + m_repl_buffer.size() >= max_payload_size) {
            // This event doesn't fit in the current transaction, so ask the
            // caller to commit it and call us again
            return true;
        }

        // We never read from the Realm file, and so we can significantly improve
        // performance and reduce the size of the Realm file by bypassing
        // Obj::set() and instead calling Replication::set() so that we only
        // write to the sync replication log instead of writing two copies of
        // each bit of data. Sync replication interally looks up an object's
        // primary key from the Obj, so we do need to create an object for that.
        // We'll delete the object later (using Table::clear() as it's faster
        // than deleting individually). We're called with replication disabled
        // on the current transaction, so the object create and table clear
        // aren't replicated automatically.
        //
        // Creating and deleting this temporary object is ~10% of the runtime
        // of the audit SDK, so it may be worth finding a way to skip doing it.
        Mixed pk = ObjectId::gen();
        auto obj = m_table.create_object_with_primary_key(pk).get_key();
        m_repl.create_object_with_primary_key(&m_table, obj, pk);

        m_repl.set(&m_table, m_schema.col_timestamp, obj, timestamp);
        m_repl.set(&m_table, m_schema.col_activity, obj, activity);
        if (event_type)
            m_repl.set(&m_table, m_schema.col_event_type, obj, event_type);
        if (data)
            m_repl.set(&m_table, m_schema.col_data, obj, data);
        for (size_t i = 0; i < m_schema.metadata.size(); ++i)
            m_repl.set(&m_table, m_schema.metadata_cols[i], obj, m_schema.metadata[i].second);
        return false;
    }
};

// A pool of audit Realms for a sync user. Each audit scope asks the pool for
// a Realm to write the scope to. The pool takes care of rotating between Realms
// when the current one grows too large and ensuring that all of the Realms are
// uploaded to the server.
class AuditRealmPool : public std::enable_shared_from_this<AuditRealmPool> {
public:
    using ErrorHandler = std::function<void(SyncError)>;

    // Get a pool for the given sync user. Pools are cached internally to avoid
    // creating duplicate ones.
    static std::shared_ptr<AuditRealmPool> get_pool(std::shared_ptr<SyncUser> user,
                                                    std::string const& partition_prefix, util::Logger& logger,
                                                    ErrorHandler error_handler);

    // Write to a pooled Realm. The Transaction should not be retained outside
    // of the callback.
    void write(util::FunctionRef<void(Transaction&)> func) REQUIRES(!m_mutex);

    // Do not call directly; use get_pool().
    AuditRealmPool(std::shared_ptr<SyncUser> user, std::string const& partition_prefix, ErrorHandler error_handler,
                   util::Logger& logger, std::string_view app_id);

    // Block the calling thread until all pooled Realms have been fully uploaded,
    // including ones which do not currently have sync sessions. For testing
    // purposes only.
    void wait_for_uploads() REQUIRES(!m_mutex);

private:
    const std::shared_ptr<SyncUser> m_user;
    const std::string m_partition_prefix;
    const ErrorHandler m_error_handler;
    const std::string m_path_root;
    util::Logger& m_logger;

    std::shared_ptr<Realm> m_current_realm;
    std::vector<std::string> m_metadata_columns;

    util::CheckedMutex m_mutex;
    std::condition_variable m_cv;
    std::vector<std::shared_ptr<SyncSession>> m_upload_sessions GUARDED_BY(m_mutex);
    std::unordered_set<std::string> m_open_paths GUARDED_BY(m_mutex);

    void open_new_realm() REQUIRES(!m_mutex);
    void wait_for_upload(std::shared_ptr<SyncSession>) REQUIRES(m_mutex);
    void scan_for_realms_to_upload() REQUIRES(!m_mutex);
    std::string prefixed_partition(std::string const& partition);
};

std::shared_ptr<AuditRealmPool> AuditRealmPool::get_pool(std::shared_ptr<SyncUser> user,
                                                         std::string const& partition_prefix, util::Logger& logger,
                                                         ErrorHandler error_handler) NO_THREAD_SAFETY_ANALYSIS
{
    struct CachedPool {
        std::string user_identity;
        std::string partition_prefix;
        std::string app_id;
        std::weak_ptr<AuditRealmPool> pool;
    };
    static std::mutex s_pool_mutex;
    std::lock_guard lock{s_pool_mutex};
    static std::vector<CachedPool> s_pools;
    s_pools.erase(std::remove_if(s_pools.begin(), s_pools.end(),
                                 [](auto& pool) {
                                     return pool.pool.expired();
                                 }),
                  s_pools.end());

    auto app_id = user->sync_manager()->app().lock()->config().app_id;
    auto it = std::find_if(s_pools.begin(), s_pools.end(), [&](auto& pool) {
        return pool.user_identity == user->identity() && pool.partition_prefix == partition_prefix &&
               pool.app_id == app_id;
    });
    if (it != s_pools.end()) {
        if (auto pool = it->pool.lock()) {
            return pool;
        }
    }

    auto pool = std::make_shared<AuditRealmPool>(user, partition_prefix, error_handler, logger, app_id);
    pool->scan_for_realms_to_upload();
    s_pools.push_back({user->identity(), partition_prefix, app_id, pool});
    return pool;
}

AuditRealmPool::AuditRealmPool(std::shared_ptr<SyncUser> user, std::string const& partition_prefix,
                               ErrorHandler error_handler, util::Logger& logger, std::string_view app_id)
    : m_user(user)
    , m_partition_prefix(partition_prefix)
    , m_error_handler(error_handler)
    , m_path_root([&] {
        auto base_file_path = m_user->sync_manager()->config().base_file_path;
#ifdef _WIN32 // Move to File?
        const char separator[] = "\\";
#else
        const char separator[] = "/";
#endif
        // "$root/realm-audit/$appId/$userId/$partitonPrefix/"
        return util::format("%2%1realm-audit%1%3%1%4%1%5%1", separator, base_file_path, app_id, m_user->identity(),
                            partition_prefix);
    }())
    , m_logger(logger)
{
    util::make_dir_recursive(m_path_root);
}

static std::atomic<int64_t> g_max_partition_size = 256 * 1024 * 1204;

void AuditRealmPool::write(util::FunctionRef<void(Transaction&)> func)
{
    if (m_current_realm) {
        auto size = util::File::get_size_static(m_current_realm->config().path);
        if (size > g_max_partition_size) {
            m_logger.info("Audit: Closing Realm at '%1': size %2 > max size %3", m_current_realm->config().path, size,
                          g_max_partition_size.load());
            auto sync_session = m_current_realm->sync_session();
            {
                // If we're offline and already have a Realm waiting to upload,
                // just close this Realm and we'll come back to it later.
                // Otherwise keep it open and upload it.
                util::CheckedLockGuard lock(m_mutex);
                if (m_upload_sessions.empty() ||
                    sync_session->connection_state() == realm::SyncSession::ConnectionState::Connected) {
                    wait_for_upload(sync_session);
                }
                else {
                    sync_session->log_out();
                    m_open_paths.erase(m_current_realm->config().path);
                }
            }
            m_current_realm->close();
            m_current_realm = nullptr;
        }
        else {
            m_logger.detail("Audit: Reusing existing Realm at '%1'", m_current_realm->config().path);
        }
    }

    if (!m_current_realm) {
        open_new_realm();
    }

    REALM_ASSERT(m_current_realm);
    m_current_realm->begin_transaction();
    try {
        func(static_cast<Transaction&>(m_current_realm->read_group()));
    }
    catch (...) {
        m_current_realm->cancel_transaction();
        throw;
    }
    m_current_realm->commit_transaction();
}

void AuditRealmPool::wait_for_upload(std::shared_ptr<SyncSession> session)
{
    m_logger.info("Audit: Uploading '%1'", session->path());
    m_upload_sessions.push_back(session);
    session->wait_for_upload_completion([this, weak_self = weak_from_this(), session](std::error_code ec) {
        auto self = weak_self.lock();
        if (!self)
            return;

        {
            util::CheckedLockGuard lock(m_mutex);
            auto it = std::find(m_upload_sessions.begin(), m_upload_sessions.end(), session);
            REALM_ASSERT(it != m_upload_sessions.end());
            m_upload_sessions.erase(it);
            auto path = session->path();
            session->close();
            m_open_paths.erase(path);
            if (ec) {
                m_logger.error("Audit: Upload on '%1' failed with error '%2'.", path, ec.message());
                if (m_error_handler) {
                    m_error_handler(SyncError(ec, ec.message(), false));
                }
            }
            else {
                m_logger.info("Audit: Upload on '%1' completed.", path);
                util::File::remove(path);
            }
            if (!m_upload_sessions.empty())
                return;
        }

        // We've fully uploaded all of our currently open files, so check if
        // there's any old ones sitting on disk waiting to be uploaded.
        scan_for_realms_to_upload();
    });
}

std::string AuditRealmPool::prefixed_partition(std::string const& partition)
{
    return util::format("\"%1-%2\"", m_partition_prefix, partition);
}

void AuditRealmPool::scan_for_realms_to_upload()
{
    util::CheckedLockGuard lock(m_mutex);
    m_logger.trace("Audit: Scanning for Realms in '%1' to upload", m_path_root);
    util::DirScanner dir(m_path_root);
    std::string file_name;
    while (dir.next(file_name)) {
        if (!StringData(file_name).ends_with(".realm"))
            continue;

        std::string path = m_path_root + file_name;
        if (m_open_paths.count(path)) {
            m_logger.trace("Audit: Skipping '%1': file is already open", path);
            continue;
        }

        m_logger.trace("Audit: Checking file '%1'", path);
        auto db = DB::create(std::make_unique<sync::ClientReplication>(false), path);
        auto tr = db->start_read();
        if (tr->get_history()->no_pending_local_changes(tr->get_version())) {
            m_logger.info("Audit: Realm at '%1' is fully uploaded", path);
            tr = nullptr;
            db->close();
            util::File::remove(path);
            continue;
        }

        m_open_paths.insert(path);
        auto partition = file_name.substr(0, file_name.size() - 6);
        wait_for_upload(m_user->sync_manager()->get_session(db, SyncConfig{m_user, prefixed_partition(partition)}));
        return;
    }

    // Did not find any files needing to be uploaded, so wake up anyone sitting
    // in wait_for_uploads()
    m_cv.notify_all();
}

void AuditRealmPool::open_new_realm()
{
    ObjectSchema schema = {"AuditEvent",
                           {
                               {"_id", PropertyType::ObjectId, Property::IsPrimary{true}},
                               {"timestamp", PropertyType::Date},
                               {"activity", PropertyType::String},
                               {"event", PropertyType::String | PropertyType::Nullable},
                               {"data", PropertyType::String | PropertyType::Nullable},
                           }};
    for (auto& key : m_metadata_columns) {
        schema.persisted_properties.push_back({key, PropertyType::String | PropertyType::Nullable});
    }

    std::string partition = ObjectId::gen().to_string();
    auto sync_config = std::make_shared<SyncConfig>(m_user, prefixed_partition(partition));
    sync_config->apply_server_changes = false;
    sync_config->client_resync_mode = ClientResyncMode::Manual;
    sync_config->recovery_directory = std::string("io.realm.audit");
    sync_config->stop_policy = SyncSessionStopPolicy::Immediately;
    sync_config->error_handler = [error_handler = m_error_handler, weak_self = weak_from_this()](auto,
                                                                                                 SyncError error) {
        if (auto self = weak_self.lock()) {
            self->m_logger.error("Audit: Received sync error: %1 (ec=%2)", error.message, error.error_code.value());
        }
        if (error_handler) {
            error_handler(error);
        }
        else if (error.is_fatal) {
            abort();
        }
    };

    Realm::Config config;
    config.automatic_change_notifications = false;
    config.cache = false;
    config.path = util::format("%1%2.realm", m_path_root, partition);
    config.scheduler = util::Scheduler::make_dummy();
    config.schema = Schema{schema};
    config.schema_mode = SchemaMode::AdditiveExplicit;
    config.schema_version = 0;
    config.sync_config = sync_config;

    m_logger.info("Audit: Opening new Realm at '%1'", config.path);
    m_current_realm = Realm::get_shared_realm(std::move(config));
    util::CheckedLockGuard lock(m_mutex);
    m_open_paths.insert(m_current_realm->config().path);
}

void AuditRealmPool::wait_for_uploads()
{
    util::CheckedUniqueLock lock(m_mutex);
    m_cv.wait(lock.native_handle(), [this]() NO_THREAD_SAFETY_ANALYSIS {
        return m_upload_sessions.empty();
    });
}

class AuditContext : public AuditInterface, public std::enable_shared_from_this<AuditContext> {
public:
    AuditContext(std::shared_ptr<DB> source_db, RealmConfig const& parent_config, AuditConfig const& audit_config);
    ~AuditContext();

    void update_metadata(std::vector<std::pair<std::string, std::string>> new_metadata) override REQUIRES(!m_mutex);

    void begin_scope(std::string_view name) override REQUIRES(!m_mutex);
    void end_scope(util::UniqueFunction<void(std::exception_ptr)>&& completion) override REQUIRES(!m_mutex);
    void record_event(std::string_view activity, util::Optional<std::string> event_type,
                      util::Optional<std::string> data,
                      util::UniqueFunction<void(std::exception_ptr)>&& completion) override REQUIRES(!m_mutex);

    void record_query(VersionID, TableView const&) override REQUIRES(!m_mutex);
    void record_write(VersionID, VersionID) override REQUIRES(!m_mutex);
    void record_read(VersionID, const Obj& row, const Obj& parent, ColKey col) override REQUIRES(!m_mutex);

    void wait_for_completion() override;
    void wait_for_uploads() override;

    void close();

private:
    struct Scope {
        std::shared_ptr<MetadataSchema> metadata;
        std::string activity_name;
        std::vector<Event> events;
        std::vector<std::shared_ptr<Transaction>> source_transactions;
        util::UniqueFunction<void(std::exception_ptr)> completion;
    };

    std::shared_ptr<MetadataSchema> m_metadata GUARDED_BY(m_mutex);
    std::shared_ptr<DB> m_source_db;
    std::shared_ptr<AuditRealmPool> m_realm_pool;
    std::shared_ptr<AuditObjectSerializer> m_serializer;
    std::shared_ptr<util::Logger> m_logger;

    util::CheckedMutex m_mutex;
    std::shared_ptr<Scope> m_current_scope GUARDED_BY(m_mutex);
    dispatch_queue_t m_queue;

    void pin_version(VersionID) REQUIRES(m_mutex);
    void trigger_write(std::shared_ptr<Scope>) REQUIRES(m_mutex);
    void process_scope(AuditContext::Scope& scope) const;

    friend class AuditEventWriter;
};

void validate_metadata(std::vector<std::pair<std::string, std::string>>& metadata)
{
    for (auto& [key, _] : metadata) {
        if (key.empty() || key.size() > Table::max_column_name_length)
            throw std::logic_error(
                util::format("Invalid audit metadata key '%1': keys must be 1-63 characters long", key));
        static const std::string_view invalid_keys[] = {"_id", "timestamp", "activity", "event", "data"};
        if (std::find(std::begin(invalid_keys), std::end(invalid_keys), key) != std::end(invalid_keys))
            throw std::logic_error(util::format(
                "Invalid audit metadata key '%1': metadata keys cannot overlap with the audit event properties",
                key));
    }
    std::sort(metadata.begin(), metadata.end(), [](auto& a, auto& b) {
        return a.first < b.first;
    });
    auto duplicate = std::adjacent_find(metadata.begin(), metadata.end(), [](auto& a, auto& b) {
        return a.first == b.first;
    });
    if (duplicate != metadata.end())
        throw std::logic_error(util::format("Duplicate audit metadata key '%1'", duplicate->first));
}

AuditContext::AuditContext(std::shared_ptr<DB> source_db, RealmConfig const& parent_config,
                           AuditConfig const& audit_config)
    : m_metadata(std::make_shared<MetadataSchema>(MetadataSchema{audit_config.metadata}))
    , m_source_db(source_db)
    , m_serializer(audit_config.serializer)
    , m_logger(audit_config.logger)
    , m_queue(dispatch_queue_create("Realm audit worker", DISPATCH_QUEUE_SERIAL_WITH_AUTORELEASE_POOL))
{
    validate_metadata(m_metadata->metadata);
    REALM_ASSERT(parent_config.sync_config);

    auto& parent_sync_config = *parent_config.sync_config;
    auto audit_user = audit_config.audit_user;
    if (!audit_user)
        audit_user = parent_sync_config.user;

    if (parent_sync_config.flx_sync_requested && audit_user == parent_sync_config.user) {
        throw std::logic_error("Auditing a flexible sync realm requires setting the audit user to a user associated "
                               "with a partition-based sync app.");
    }

    if (!m_logger)
        m_logger = audit_user->sync_manager()->make_logger();
    if (!m_serializer)
        m_serializer = std::make_shared<AuditObjectSerializer>();

    m_realm_pool = AuditRealmPool::get_pool(audit_user, audit_config.partition_value_prefix, *m_logger,
                                            audit_config.sync_error_handler);
}

void AuditContext::update_metadata(std::vector<std::pair<std::string, std::string>> new_metadata)
{
    util::CheckedLockGuard lock(m_mutex);
    if (m_metadata && m_metadata->metadata != new_metadata) {
        m_metadata = std::make_shared<MetadataSchema>(MetadataSchema{std::move(new_metadata)});
        validate_metadata(m_metadata->metadata);
    }
}

AuditContext::~AuditContext() = default;

void AuditContext::record_query(VersionID version, TableView const& tv)
{
    util::CheckedLockGuard lock(m_mutex);
    if (!m_current_scope)
        return;
    if (tv.size() == 0)
        return; // Query didn't match any objects so there wasn't actually a read

    pin_version(version);
    std::vector<ObjKey> objects;
    for (size_t i = 0, count = tv.size(); i < count; ++i)
        objects.push_back(tv.get_key(i));

    m_current_scope->events.push_back(
        audit_event::Query{now(), version, tv.get_target_table()->get_key(), std::move(objects)});
}

void AuditContext::record_read(VersionID version, const Obj& obj, const Obj& parent, ColKey col)
{
    util::CheckedLockGuard lock(m_mutex);
    if (!m_current_scope)
        return;
    if (obj.get_table()->is_embedded())
        return;
    pin_version(version);
    TableKey parent_table_key;
    ObjKey parent_obj_key;
    if (parent.is_valid()) {
        parent_table_key = parent.get_table()->get_key();
        parent_obj_key = parent.get_key();
    }
    m_current_scope->events.push_back(audit_event::Object{now(), version, obj.get_table()->get_key(), obj.get_key(),
                                                          parent_table_key, parent_obj_key, col});
}

void AuditContext::record_write(realm::VersionID old_version, realm::VersionID new_version)
{
    util::CheckedLockGuard lock(m_mutex);
    if (!m_current_scope)
        return;
    pin_version(old_version);
    m_current_scope->events.push_back(audit_event::Write{now(), old_version, new_version});
}

void AuditContext::record_event(std::string_view activity, util::Optional<std::string> event_type,
                                util::Optional<std::string> data,
                                util::UniqueFunction<void(std::exception_ptr)>&& completion)

{
    util::CheckedLockGuard lock(m_mutex);
    auto scope = std::make_shared<Scope>(Scope{m_metadata, std::string(activity)});
    scope->events.push_back(audit_event::Custom{now(), std::string(activity), event_type, data});
    scope->completion = std::move(completion);
    trigger_write(std::move(scope));
}

void AuditContext::pin_version(VersionID version)
{
    for (auto& transaction : m_current_scope->source_transactions) {
        if (transaction->get_version() == version.version)
            return;
    }
    m_current_scope->source_transactions.push_back(m_source_db->start_read(version));
}

void AuditContext::begin_scope(std::string_view name)
{
    util::CheckedLockGuard lock(m_mutex);
    if (m_current_scope)
        throw std::logic_error("Cannot begin audit scope: audit already in progress");
    m_logger->trace("Audit: Beginning audit scope on '%1' named '%2'", m_source_db->get_path(), name);
    m_current_scope = std::make_shared<Scope>(Scope{m_metadata, std::string(name)});
}

void AuditContext::end_scope(util::UniqueFunction<void(std::exception_ptr)>&& completion)
{
    util::CheckedLockGuard lock(m_mutex);
    if (!m_current_scope)
        throw std::logic_error("Cannot end audit scope: no audit in progress");
    m_logger->trace("Audit: Comitting audit scope on '%1' with %2 events", m_source_db->get_path(),
                    m_current_scope->events.size());
    m_current_scope->completion = std::move(completion);
    trigger_write(std::move(m_current_scope));
    m_current_scope = nullptr;
}

void AuditContext::process_scope(AuditContext::Scope& scope) const
{
    m_logger->info("Audit: Processing scope for '%1'", m_source_db->get_path());
    try {
        // Merge single object reads following a query into that query and discard
        // duplicate reads on objects.
        {
            ReadCombiner combiner{*m_logger};
            auto& events = scope.events;
            events.erase(std::remove_if(events.begin(), events.end(),
                                        [&](auto& event) {
                                            return mpark::visit(combiner, event);
                                        }),
                         events.end());
        }

        // Filter out queries which were made empty by the above pass and filter
        // out reads on newly-created objects
        {
            EmptyQueryFilter filter{*m_source_db, *m_logger};
            auto& events = scope.events;
            events.erase(std::remove_if(events.begin(), events.end(),
                                        [&](auto& event) {
                                            return mpark::visit(filter, event);
                                        }),
                         events.end());
        }

        // Gather information about link accesses so that we can include
        // information about the linked object in the audit event for the parent
        {
            m_serializer->reset_link_accesses();
            TrackLinkAccesses track{*m_serializer};
            auto& events = scope.events;
            for (size_t i = 0; i < events.size(); ++i) {
                m_serializer->set_event_index(i);
                mpark::visit(track, events[i]);
            }
            m_serializer->sort_link_accesses();
        }

        m_realm_pool->write([&](Transaction& tr) {
            auto table = tr.get_table("class_AuditEvent");

            // Read out schema information, creating the metadata columns if needed
            if (!scope.metadata->col_timestamp) {
                scope.metadata->col_timestamp = table->get_column_key("timestamp");
                scope.metadata->col_activity = table->get_column_key("activity");
                scope.metadata->col_event_type = table->get_column_key("event");
                scope.metadata->col_data = table->get_column_key("data");
                for (auto& [key, _] : scope.metadata->metadata) {
                    if (auto col = table->get_column_key(key)) {
                        scope.metadata->metadata_cols.push_back(col);
                    }
                    else {
                        constexpr bool nullable = true;
                        scope.metadata->metadata_cols.push_back(table->add_column(type_String, key, nullable));
                        m_logger->trace("Audit: Adding column for metadata field '%1'", key);
                    }
                }
            }

            AuditEventWriter writer{*m_source_db, *scope.metadata, scope.activity_name, *table, *m_serializer};

            m_logger->trace("Audit: Total event count: %1", scope.events.size());
            for (size_t i = 0; i < scope.events.size();) {
                {
                    // We write directly to the replication log and don't want
                    // the automatic replication to happen
                    DisableReplication dr(tr);

                    // There's awkward nested looping here because we need
                    // replication enabled when we commit intermediate transactions
                    for (; i < scope.events.size(); ++i) {
                        m_serializer->set_event_index(i);
                        if (mpark::visit(writer, scope.events[i])) {
                            // This event didn't fit in the current transaction
                            // so commit and try it again after that.
                            break;
                        }
                    }
                    table->clear();
                }

                // i.e. if we hit the break
                if (i + 1 < scope.events.size()) {
                    m_logger->detail("Audit: Incrementally comitting transaction after %1 events", i);
                    tr.commit_and_continue_writing();
                }
            }
        });

        if (scope.completion)
            scope.completion(nullptr);
        m_logger->detail("Audit: Scope completed");
    }
    catch (std::exception const& e) {
        m_logger->error("Audit: Error when writing scope: %1", e.what());
        if (scope.completion)
            scope.completion(std::current_exception());
    }
    catch (...) {
        m_logger->error("Audit: Unknown error when writing scope");
        if (scope.completion)
            scope.completion(std::current_exception());
    }
    m_serializer->scope_complete();
}

void AuditContext::close()
{
    m_source_db = nullptr;
    m_realm_pool = nullptr;
}

void AuditContext::trigger_write(std::shared_ptr<Scope> scope)
{
    dispatch_async(m_queue, [self = shared_from_this(), scope = std::move(scope)]() {
        self->process_scope(*scope);
    });
}

void AuditContext::wait_for_completion()
{
    dispatch_sync(m_queue, ^{
                      // Don't need to do anything here
                  });
}

void AuditContext::wait_for_uploads()
{
    m_realm_pool->wait_for_uploads();
}

} // anonymous namespace

bool AuditObjectSerializer::get_field(nlohmann::json& field, const Obj& obj, ColKey col, Mixed const& value)
{
    if (value.is_null())
        return true;
    switch (value.get_type()) {
        case type_Int:
            field = value.get<int64_t>();
            return true;
        case type_Bool:
            field = value.get<bool>();
            return true;
        case type_String:
            field = value.get<StringData>();
            return true;
        case type_Timestamp:
            field = value.get<Timestamp>();
            return true;
        case type_Double:
            field = value.get<Double>();
            return true;
        case type_Float:
            field = value.get<Float>();
            return true;
        case type_ObjectId:
            field = value.get<ObjectId>().to_string();
            return true;
        case type_UUID:
            field = value.get<UUID>().to_string();
            return true;
        case type_Link: {
            auto target = obj.get_target_table(col)->get_object(value.get<ObjKey>());
            if (target.get_table()->is_embedded() || accessed_link(m_version.version, obj, col)) {
                to_json(field, target);
                return true;
            }
            return get_field(field, obj, col, target.get_primary_key());
        }
        case type_TypedLink: {
            auto target = obj.get_table()->get_parent_group()->get_object(value.get<ObjLink>());
            if (accessed_link(m_version.version, obj, col)) {
                to_json(field, target);
                return true;
            }
            return get_field(field, obj, col, target.get_primary_key());
        }
        default:
            return false;
    }
}

bool AuditObjectSerializer::get_field(nlohmann::json& field, const Obj& obj, ColKey col)
{
    if (obj.is_null(col)) {
        field = nullptr;
        return true;
    }

    if (col.is_dictionary()) {
        field = nlohmann::json::object();
        auto dictionary = obj.get_dictionary(col);
        for (const auto& [key, value] : dictionary) {
            get_field(field[key.get_string()], obj, col, value);
        }
        return true;
    }

    if (col.is_collection()) {
        field = nlohmann::json::array();
        auto collection = obj.get_collection_ptr(col);
        for (size_t i = 0, size = collection->size(); i < size; ++i) {
            get_field(field[i], obj, col, collection->get_any(i));
        }
        return true;
    }

    return get_field(field, obj, col, obj.get_any(col));
}

void AuditObjectSerializer::to_json(nlohmann::json& out, const Obj& obj)
{
    auto& table = *obj.get_table();
    for (auto col : table.get_column_keys()) {
        auto col_name = table.get_column_name(col);
        if (!get_field(out[col_name], obj, col))
            out.erase(col_name);
    }
}

void AuditObjectSerializer::link_accessed(VersionID version, TableKey table, ObjKey obj, ColKey col)
{
    m_accessed_links.push_back({version.version, table, obj, col, m_index});
}

void AuditObjectSerializer::reset_link_accesses() noexcept
{
    m_accessed_links.clear();
}

void AuditObjectSerializer::sort_link_accesses() noexcept
{
    static constexpr const size_t max = -1;
    std::sort(m_accessed_links.begin(), m_accessed_links.end(), [](auto& a, auto& b) {
        return std::make_tuple(a.version, a.table, a.col, a.obj, max - a.event_ndx) <
               std::make_tuple(b.version, b.table, b.col, b.obj, max - b.event_ndx);
    });
    m_accessed_links.erase(std::unique(m_accessed_links.begin(), m_accessed_links.end(),
                                       [](auto& a, auto& b) {
                                           return std::make_tuple(a.version, a.table, a.col, a.obj) ==
                                                  std::make_tuple(b.version, b.table, b.col, b.obj);
                                       }),
                           m_accessed_links.end());
}

bool AuditObjectSerializer::accessed_link(uint_fast64_t version, const Obj& obj, ColKey col) const noexcept
{
    auto cmp = [](auto& a, auto& b) {
        return std::make_tuple(a.version, a.table, a.col, a.obj) < std::make_tuple(b.version, b.table, b.col, b.obj);
    };
    auto link = LinkAccess{version, obj.get_table()->get_key(), obj.get_key(), col, 0};
    auto it = std::lower_bound(m_accessed_links.begin(), m_accessed_links.end(), link, cmp);
    return it != m_accessed_links.end() && !cmp(link, *it) && it->event_ndx > m_index;
}

namespace realm {
std::shared_ptr<AuditInterface> make_audit_context(std::shared_ptr<DB> db, RealmConfig const& config)
{
    REALM_ASSERT(config.audit_config);
    auto& audit_config = *config.audit_config;
    if (audit_config.partition_value_prefix.empty())
        throw std::logic_error("Audit partition prefix must not be empty");
    if (audit_config.partition_value_prefix.find_first_of("\\/") != std::string::npos)
        throw std::logic_error(util::format("Invalid audit parition prefix '%1': prefix must not contain slashes",
                                            audit_config.partition_value_prefix));
    return std::make_shared<AuditContext>(db, config, audit_config);
}

namespace audit_test_hooks {
void set_maximum_shard_size(int64_t max_size)
{
    g_max_partition_size.store(max_size);
}
void set_clock(util::UniqueFunction<Timestamp()>&& clock)
{
    g_audit_clock = std::move(clock);
}
} // namespace audit_test_hooks
} // namespace realm
