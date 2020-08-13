#include "test.hpp"

#include <realm/sync/history.hpp>
#include <realm/sync/noinst/server_history.hpp>
#include <realm/db.hpp>

#include <realm/sync/object.hpp>
#include <realm/util/base64.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/instruction_applier.hpp>

using namespace realm;
using namespace realm::sync;


namespace {

struct MakeClientHistory {
    static std::unique_ptr<ClientReplication> make_history(const std::string& realm_path)
    {
        return realm::sync::make_client_replication(realm_path);
    }

    static file_ident_type get_client_file_ident(ClientReplication& history)
    {
        version_type current_client_version;
        SaltedFileIdent client_file_ident;
        SyncProgress progress;
        history.get_status(current_client_version, client_file_ident, progress);
        return client_file_ident.ident;
    }
};

struct MakeServerHistory {
    class HistoryContext : public _impl::ServerHistory::Context {
    public:
        bool owner_is_sync_server() const noexcept override final
        {
            return false;
        }
        std::mt19937_64& server_history_get_random() noexcept override final
        {
            return m_random;
        }

    private:
        std::mt19937_64 m_random;
    };
    class WrapServerHistory : public HistoryContext,
                              public _impl::ServerHistory::DummyCompactionControl,
                              public _impl::ServerHistory {
    public:
        explicit WrapServerHistory(const std::string& realm_path)
            : _impl::ServerHistory{realm_path, *this, *this}
        {
        }
    };

    static std::unique_ptr<_impl::ServerHistory> make_history(const std::string& realm_path)
    {
        return std::make_unique<WrapServerHistory>(realm_path);
    }

    static _impl::ServerHistory::file_ident_type get_client_file_ident(_impl::ServerHistory&)
    {
        // For un-migrated Realms, the server's client file ident is always 1.
        return 1;
    }
};

} // unnamed namespace


TEST_TYPES(InstructionReplication_CreateIdColumnInNewTables, MakeClientHistory, MakeServerHistory)
{
    SHARED_GROUP_TEST_PATH(test_dir);
    auto history = TEST_TYPE::make_history(test_dir);
    DBRef sg = DB::create(*history);

    {
        WriteTransaction wt{sg};
        sync::create_table(wt, "class_foo");
        wt.commit();
    }

    // Check that only the AddTable instruction is emitted
    Changeset result;
    auto buffer = history->get_instruction_encoder().release();
    _impl::SimpleNoCopyInputStream stream{buffer.data(), buffer.size()};
    sync::parse_changeset(stream, result);
    CHECK_EQUAL(result.size(), 1);
    CHECK_EQUAL(result.begin()->type(), Instruction::Type::AddTable);
    auto& instr = result.begin()->get_as<Instruction::AddTable>();
    CHECK_EQUAL(result.get_string(instr.table), "foo");

    auto rt = sg->start_read();
    ConstTableRef foo = rt->get_table("class_foo");
    CHECK(foo);

    // Check that a primary-key column of type ObjectID was created.
    CHECK_EQUAL(foo->get_column_count(), 1);
    CHECK(foo->get_primary_key_column());
    CHECK_EQUAL(foo->get_primary_key_column().get_type(), type_ObjectId);
}

TEST_TYPES(InstructionReplication_PopulatesObjectIdColumn, MakeClientHistory, MakeServerHistory)
{
    SHARED_GROUP_TEST_PATH(test_dir);
    auto history = TEST_TYPE::make_history(test_dir);

    DBRef sg = DB::create(*history);

    auto client_file_ident = TEST_TYPE::get_client_file_ident(*history);

    // Tables with integer primary keys:
    {
        WriteTransaction wt{sg};
        TableRef t1 = sync::create_table_with_primary_key(wt, "class_t1", type_Int, "pk");
        auto obj0 = t1->create_object_with_primary_key(123);

        GlobalKey expected_object_id(123);
        CHECK_EQUAL(obj0.get_object_id(), expected_object_id);
    }

    // Tables with string primary keys:
    {
        WriteTransaction wt{sg};
        TableRef t2 = sync::create_table_with_primary_key(wt, "class_t2", type_String, "pk");
        auto obj0 = t2->create_object_with_primary_key("foo");

        GlobalKey expected_object_id("foo");
        CHECK_EQUAL(obj0.get_object_id(), expected_object_id);
    }

    // Attempting to create a table that already exists is a no-op if the same primary key name, type and nullability
    // is used.
    {
        WriteTransaction wt{sg};
        TableRef t1 = sync::create_table_with_primary_key(wt, "class_t1", type_Int, "pk");
        TableRef t11 = sync::create_table_with_primary_key(wt, "class_t1", type_Int, "pk");
        CHECK_EQUAL(t1, t11);

        TableRef t2 = sync::create_table_with_primary_key(wt, "class_t2", type_Int, "pk", /* nullable */ true);
        TableRef t21 = sync::create_table_with_primary_key(wt, "class_t2", type_Int, "pk", /* nullable */ true);
        CHECK_EQUAL(t2, t21);

        TableRef t3 = sync::create_table_with_primary_key(wt, "class_t3", type_String, "pk");
        TableRef t31 = sync::create_table_with_primary_key(wt, "class_t3", type_String, "pk");
        CHECK_EQUAL(t3, t31);

        TableRef t4 = sync::create_table_with_primary_key(wt, "class_t4", type_String, "pk", /* nullable */ true);
        TableRef t41 = sync::create_table_with_primary_key(wt, "class_t4", type_String, "pk", /* nullable */ true);
        CHECK_EQUAL(t4, t41);
    }

    // Attempting to create a table that already exists causes an assertion failure if different primary key name,
    // type, or nullability is specified. This is not currently testable.
}

TEST_TYPES(StableIDs_CollisionMapping, MakeClientHistory, MakeServerHistory)
{
#if REALM_EXERCISE_OBJECT_ID_COLLISION

    // This number corresponds to the mask used to calculate "optimistic"
    // object IDs. See `GlobalKeyProvider::get_optimistic_local_id_hashed`.
    const size_t num_objects_with_guaranteed_collision = 0xff;

    SHARED_GROUP_TEST_PATH(test_dir);

    {
        auto history = TEST_TYPE::make_history(test_dir);
        DBRef sg = DB::create(*history);
        {
            WriteTransaction wt{sg};
            TableRef t0 = sync::create_table_with_primary_key(wt, "class_t0", type_String, "pk");

            char buffer[12];
            for (size_t i = 0; i < num_objects_with_guaranteed_collision; ++i) {
                const char* in = reinterpret_cast<char*>(&i);
                size_t len = base64_encode(in, sizeof(i), buffer, sizeof(buffer));

                sync::create_object_with_primary_key(wt, *t0, StringData{buffer, len});
            }
            wt.commit();
        }

        {
            ReadTransaction rt{sg};
            ConstTableRef t0 = rt.get_table("class_t0");
            // Check that at least one object exists where the 63rd bit is set.
            size_t num_object_keys_with_63rd_bit_set = 0;
            uint64_t bit63 = 0x4000000000000000;
            for (Obj obj : *t0) {
                if (obj.get_key().value & bit63)
                    ++num_object_keys_with_63rd_bit_set;
            }
            CHECK_GREATER(num_object_keys_with_63rd_bit_set, 0);
        }
    }

    // Check that locally allocated IDs are properly persisted
    {
        auto history_2 = TEST_TYPE::make_history(test_dir);
        DBRef sg_2 = DB::create(*history_2);
        WriteTransaction wt{sg_2};
        TableRef t0 = wt.get_table("class_t0");

        // Make objects with primary keys that do not already exist but are guaranteed
        // to cause further collisions.
        char buffer[12];
        for (size_t i = 0; i < num_objects_with_guaranteed_collision; ++i) {
            size_t foo = num_objects_with_guaranteed_collision + i;
            const char* in = reinterpret_cast<char*>(&foo);
            size_t len = base64_encode(in, sizeof(foo), buffer, sizeof(buffer));

            sync::create_object_with_primary_key(wt, *t0, StringData{buffer, len});
        }
    }

#endif // REALM_EXERCISE_ID_COLLISION
}
