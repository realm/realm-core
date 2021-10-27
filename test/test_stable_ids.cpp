#include "test.hpp"

#include <realm/sync/history.hpp>
#include <realm/sync/noinst/server/server_history.hpp>
#include <realm/sync/noinst/client_history_impl.hpp>
#include <realm/db.hpp>

#include <realm/util/base64.hpp>
#include <realm/sync/changeset_parser.hpp>
#include <realm/sync/instruction_applier.hpp>

using namespace realm;
using namespace realm::sync;


namespace {

struct MakeClientHistory {
    static std::unique_ptr<ClientReplication> make_history()
    {
        return realm::sync::make_client_replication();
    }
};

struct MakeServerHistory {
    class HistoryContext : public _impl::ServerHistory::Context {
    public:
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
        WrapServerHistory()
            : _impl::ServerHistory{*this, *this}
        {
        }
    };

    static std::unique_ptr<_impl::ServerHistory> make_history()
    {
        return std::make_unique<WrapServerHistory>();
    }
};

} // unnamed namespace


TEST_TYPES(InstructionReplication_CreateIdColumnInNewTables, MakeClientHistory, MakeServerHistory)
{
    SHARED_GROUP_TEST_PATH(test_dir);
    auto history = TEST_TYPE::make_history();
    DBRef sg = DB::create(*history, test_dir);

    {
        WriteTransaction wt{sg};
        wt.get_or_add_table("class_foo");
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
    CHECK_EQUAL(foo->get_column_count(), 0);
}

TEST_TYPES(InstructionReplication_PopulatesObjectIdColumn, MakeClientHistory, MakeServerHistory)
{
    SHARED_GROUP_TEST_PATH(test_dir);
    auto history = TEST_TYPE::make_history();

    DBRef sg = DB::create(*history, test_dir);

    auto client_file_ident = sg->start_read()->get_sync_file_id();

    // Tables without primary keys:
    {
        WriteTransaction wt{sg};
        TableRef t0 = wt.get_or_add_table("class_t0");

        auto obj0 = t0->create_object();
        auto obj1 = t0->create_object();

        // Object IDs should be peerID plus a sequence number
        CHECK_EQUAL(obj0.get_object_id(), GlobalKey(client_file_ident, 0));
        CHECK_EQUAL(obj1.get_object_id(), GlobalKey(client_file_ident, 1));
    }

    // Tables with integer primary keys:
    {
        WriteTransaction wt{sg};
        TableRef t1 = wt.get_group().add_table_with_primary_key("class_t1", type_Int, "pk");
        auto obj0 = t1->create_object_with_primary_key(123);

        GlobalKey expected_object_id(123);
        CHECK_EQUAL(obj0.get_object_id(), expected_object_id);
    }

    // Tables with string primary keys:
    {
        WriteTransaction wt{sg};
        TableRef t2 = wt.get_group().add_table_with_primary_key("class_t2", type_String, "pk");
        auto obj0 = t2->create_object_with_primary_key("foo");

        GlobalKey expected_object_id("foo");
        CHECK_EQUAL(obj0.get_object_id(), expected_object_id);
    }

    // Attempting to create a table that already exists is a no-op if the same primary key name, type and nullability
    // is used.
    {
        WriteTransaction wt{sg};
        TableRef t1 = wt.get_group().get_or_add_table_with_primary_key("class_t1", type_Int, "pk");
        TableRef t11 = wt.get_group().get_or_add_table_with_primary_key("class_t1", type_Int, "pk");
        CHECK_EQUAL(t1, t11);

        TableRef t2 =
            wt.get_group().get_or_add_table_with_primary_key("class_t2", type_Int, "pk", /* nullable */ true);
        TableRef t21 =
            wt.get_group().get_or_add_table_with_primary_key("class_t2", type_Int, "pk", /* nullable */ true);
        CHECK_EQUAL(t2, t21);

        TableRef t3 = wt.get_group().get_or_add_table_with_primary_key("class_t3", type_String, "pk");
        TableRef t31 = wt.get_group().get_or_add_table_with_primary_key("class_t3", type_String, "pk");
        CHECK_EQUAL(t3, t31);

        TableRef t4 =
            wt.get_group().get_or_add_table_with_primary_key("class_t4", type_String, "pk", /* nullable */ true);
        TableRef t41 =
            wt.get_group().get_or_add_table_with_primary_key("class_t4", type_String, "pk", /* nullable */ true);
        CHECK_EQUAL(t4, t41);
    }

    // Attempting to create a table that already exists causes an assertion failure if different primary key name,
    // type, or nullability is specified. This is not currently testable.
}

TEST(StableIDs_ChangesGlobalObjectIdWhenPeerIdReceived)
{
    SHARED_GROUP_TEST_PATH(test_dir);
    auto repl = make_client_replication();

    DBRef sg = DB::create(*repl, test_dir);

    ColKey link_col;
    {
        WriteTransaction wt{sg};
        TableRef t0 = wt.get_or_add_table("class_t0");
        TableRef t1 = wt.get_or_add_table("class_t1");
        link_col = t0->add_column(*t1, "link");

        Obj t1_k1 = t1->create_object();
        Obj t0_k1 = t0->create_object().set(link_col, t1_k1.get_key());
        Obj t0_k2 = t0->create_object();

        // Object IDs should be peerID plus a sequence number
        CHECK_EQUAL(t0_k1.get_object_id(), GlobalKey(0, 0));
        CHECK_EQUAL(t0_k2.get_object_id(), GlobalKey(0, 1));
        wt.commit();
    }

    bool fix_up_object_ids = true;
    auto& history = repl->get_history();
    history.set_client_file_ident({1, 123}, fix_up_object_ids);

    // Save the changeset to replay later
    UploadCursor upload_cursor{0, 0};
    std::vector<ClientHistory::UploadChangeset> changesets;
    version_type locked_server_version; // Dummy
    history.find_uploadable_changesets(upload_cursor, 2, changesets, locked_server_version);
    CHECK_GREATER_EQUAL(changesets.size(), 1);
    auto& changeset = changesets[0].changeset;
    ChunkedBinaryInputStream stream{changeset};
    Changeset result;
    sync::parse_changeset(stream, result);

    // Check that ObjectIds gets translated correctly
    {
        ReadTransaction rt{sg};
        ConstTableRef t0 = rt.get_table("class_t0");
        ConstTableRef t1 = rt.get_table("class_t1");
        auto it = t0->begin();
        GlobalKey oid0 = it->get_object_id();
        ObjKey link_ndx = it->get<ObjKey>(link_col);
        ++it;
        GlobalKey oid1 = it->get_object_id();
        CHECK_EQUAL(oid0, GlobalKey(1, 0));
        CHECK_EQUAL(oid1, GlobalKey(1, 1));
        GlobalKey oid2 = t1->get_object_id(link_ndx);
        CHECK_EQUAL(oid2.hi(), 1);
        CHECK_EQUAL(oid2, t1->begin()->get_object_id());
    }

    // Replay the transaction to see that the instructions were modified.
    {
        SHARED_GROUP_TEST_PATH(test_dir_2);
        auto history_2 = make_client_replication();
        DBRef sg_2 = DB::create(*history_2, test_dir_2);

        WriteTransaction wt{sg_2};
        InstructionApplier applier{wt};
        applier.apply(result, &test_context.logger);
        wt.commit();

        // Check same invariants as above.
        ReadTransaction rt{sg_2};
        ConstTableRef t0 = rt.get_table("class_t0");
        ConstTableRef t1 = rt.get_table("class_t1");
        auto it = t0->begin();
        GlobalKey oid0 = it->get_object_id();
        ObjKey link_ndx = it->get<ObjKey>(link_col);
        ++it;
        GlobalKey oid1 = it->get_object_id();
        CHECK_EQUAL(oid0, GlobalKey(1, 0));
        CHECK_EQUAL(oid1, GlobalKey(1, 1));
        GlobalKey oid2 = t1->get_object_id(link_ndx);
        CHECK_EQUAL(oid2.hi(), 1);
        CHECK_EQUAL(oid2, t1->begin()->get_object_id());
    }
}

TEST_TYPES(StableIDs_PersistPerTableSequenceNumber, MakeClientHistory, MakeServerHistory)
{
    SHARED_GROUP_TEST_PATH(test_dir);
    {
        auto history = TEST_TYPE::make_history();
        DBRef sg = DB::create(*history, test_dir);
        WriteTransaction wt{sg};
        TableRef t0 = wt.get_or_add_table("class_t0");
        t0->create_object();
        t0->create_object();
        CHECK_EQUAL(t0->size(), 2);
        wt.commit();
    }
    {
        auto history = TEST_TYPE::make_history();
        DBRef sg = DB::create(*history, test_dir);
        WriteTransaction wt{sg};
        TableRef t0 = wt.get_or_add_table("class_t0");
        t0->create_object();
        t0->create_object();
        CHECK_EQUAL(t0->size(), 4);
        wt.commit();
    }
}

TEST_TYPES(StableIDs_CollisionMapping, MakeClientHistory, MakeServerHistory)
{
#if REALM_EXERCISE_OBJECT_ID_COLLISION

    // This number corresponds to the mask used to calculate "optimistic"
    // object IDs. See `GlobalKeyProvider::get_optimistic_local_id_hashed`.
    const size_t num_objects_with_guaranteed_collision = 0xff;

    SHARED_GROUP_TEST_PATH(test_dir);

    {
        auto history = TEST_TYPE::make_history();
        DBRef sg = DB::create(*history, test_dir);
        {
            WriteTransaction wt{sg};
            TableRef t0 = wt.get_group().add_table_with_primary_key("class_t0", type_String, "pk");

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
        auto history_2 = TEST_TYPE::make_history();
        DBRef sg_2 = DB::create(*history_2, test_dir);
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
