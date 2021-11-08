#include "sync_fixtures.hpp"
#include "peer.hpp"
#include "util/compare_groups.hpp"
#include "util/dump_changesets.hpp"

#include "test.hpp"

using namespace realm;
using namespace realm::sync;
using namespace realm::test_util;

TEST(EmbeddedObjects_Basic)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    client_1->create_schema([](WriteTransaction& tr) {
        TableRef top = tr.get_group().add_table_with_primary_key("class_Top", type_Int, "pk");
        TableRef sub = tr.add_embedded_table("class_Sub");
        top->add_column(*sub, "sub");
        sub->add_column(type_Int, "i");
    });

    client_1->transaction([&](auto& c) {
        auto& tr = *c.group;
        auto top = tr.get_table("class_Top");
        auto top_obj = top->create_object_with_primary_key(123);
        auto sub_col = top->get_column_key("sub");
        top_obj.create_and_set_linked_object(sub_col).set("i", 1);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1, test_context.logger));
    CHECK(compare_groups(read_server, read_client_2));
}

TEST(Table_EmbeddedObjectsCircular)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    ColKey col_link1, col_link2, col_link3;

    client_1->create_schema([&](WriteTransaction& tr) {
        Group& g = tr.get_group();
        auto table = g.add_table_with_primary_key("class_table", type_Int, "id");
        auto e1 = g.add_embedded_table("class_e1");
        auto e2 = g.add_embedded_table("class_e2");
        table->add_column(*table, "unused");
        col_link1 = table->add_column(*e1, "link");
        col_link2 = e1->add_column(*e2, "link");
        col_link3 = e2->add_column(*table, "link");
    });

    client_1->transaction([&](auto& c) {
        auto& tr = *c.group;
        auto table = tr.get_table("class_table");
        Obj obj = table->create_object_with_primary_key(1);
        obj.create_and_set_linked_object(col_link1).create_and_set_linked_object(col_link2).set(col_link3,
                                                                                                obj.get_key());
        obj.invalidate();
        obj = table->create_object_with_primary_key(1);
    });
}


TEST(EmbeddedObjects_ArrayOfObjects)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    client_1->create_schema([](WriteTransaction& tr) {
        TableRef top = tr.get_group().add_table_with_primary_key("class_Top", type_Int, "pk");
        TableRef sub = tr.add_embedded_table("class_Sub");
        top->add_column_list(*sub, "sub");
        sub->add_column(type_Int, "i");
    });

    client_1->transaction([&](auto& c) {
        auto& tr = *c.group;
        auto top = tr.get_table("class_Top");
        auto top_obj = top->create_object_with_primary_key(123);
        auto sub_col = top->get_column_key("sub");
        auto obj_list = top_obj.get_linklist(sub_col);
        for (size_t i = 0; i < 10; ++i) {
            obj_list.create_and_insert_linked_object(i).set("i", int64_t(i));
        }
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1, test_context.logger));
    CHECK(compare_groups(read_server, read_client_2));
}

TEST(EmbeddedObjects_DictionaryOfObjects)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    client_1->create_schema([](WriteTransaction& tr) {
        TableRef top = tr.get_group().add_table_with_primary_key("class_Top", type_Int, "pk");
        TableRef sub = tr.add_embedded_table("class_Sub");
        top->add_column_dictionary(*sub, "sub");
        sub->add_column(type_Int, "i");
    });

    client_1->transaction([&](auto& c) {
        auto& tr = *c.group;
        auto top = tr.get_table("class_Top");
        auto top_obj = top->create_object_with_primary_key(123);
        auto sub_col = top->get_column_key("sub");
        auto dict = top_obj.get_dictionary(sub_col);
        for (int64_t i = 0; i < 10; ++i) {
            dict.create_and_insert_linked_object(util::to_string(i)).set("i", i);
        }
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1, test_context.logger));
    CHECK(compare_groups(read_server, read_client_2));
}

TEST(EmbeddedObjects_NestedArray)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    client_1->create_schema([](WriteTransaction& tr) {
        TableRef threads = tr.get_group().add_table_with_primary_key("class_ForumThread", type_Int, "pk");
        TableRef comments = tr.add_embedded_table("class_Comment");
        threads->add_column_list(*comments, "comments");
        comments->add_column_list(*comments, "replies");
        comments->add_column(type_Int, "message");
    });

    client_1->transaction([&](auto& c) {
        // Create 10 threads with 2 comments each, where each comment has two
        // replies, and each reply has two further replies (total = 140).
        auto& tr = *c.group;
        auto threads = tr.get_table("class_ForumThread");

        int64_t message = 0;

        for (int64_t i = 0; i < 10; ++i) {
            auto thread = threads->create_object_with_primary_key(i);
            auto top_comments = thread.get_linklist("comments");
            for (size_t j = 0; j < 2; ++j) {
                auto comment_j = top_comments.create_and_insert_linked_object(j);
                comment_j.set("message", message++);
                auto replies_j = comment_j.get_linklist("replies");
                for (size_t k = 0; k < 2; ++k) {
                    auto comment_k = replies_j.create_and_insert_linked_object(k);
                    comment_k.set("message", message++);
                    auto replies_k = comment_k.get_linklist("replies");
                    for (size_t l = 0; l < 2; ++l) {
                        auto comment_l = replies_k.create_and_insert_linked_object(l);
                        comment_l.set("message", message++);
                    }
                }
            }
        }
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    auto threads = read_server.get_table("class_ForumThread");
    CHECK_EQUAL(threads->size(), 10);
    auto comments = read_server.get_table("class_Comment");
    CHECK_EQUAL(comments->size(), 140);

    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1, test_context.logger));
    CHECK(compare_groups(read_server, read_client_2));
}

TEST(EmbeddedObjects_ImplicitErase)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    Associativity assoc{test_context, 2, changeset_dump_dir_gen.get()};

    assoc.for_each_permutation([&](auto& it) {
        auto server = &*it.server;
        auto client_1 = &*it.clients[0];
        auto client_2 = &*it.clients[1];

        client_1->create_schema([](WriteTransaction& tr) {
            TableRef top = tr.get_group().add_table_with_primary_key("class_Top", type_Int, "pk");
            TableRef sub = tr.add_embedded_table("class_Sub");
            top->add_column(*sub, "sub");
            sub->add_column(type_Int, "i");
        });

        it.sync_all();

        // Client 1 adds an embedded object.
        client_1->transaction([&](auto& c) {
            auto& tr = *c.group;
            TableRef top = tr.get_table("class_Top");
            auto top_obj = top->create_object_with_primary_key(123);
            top_obj.create_and_set_linked_object(top->get_column_key("sub")).set("i", 5);
        });

        // Client 2 sets a non-default NULL value at a higher timestamp - this should delete the object.
        client_2->history.advance_time(1);
        client_2->transaction([&](auto& c) {
            auto& tr = *c.group;
            TableRef top = tr.get_table("class_Top");
            auto top_obj = top->create_object_with_primary_key(123);
            bool is_default = false;
            top_obj.set_null("sub", is_default);
        });

        it.sync_all();

        {
            ReadTransaction read_server(server->shared_group);
            auto top = read_server.get_table("class_Top");
            auto sub = read_server.get_table("class_Sub");
            CHECK_EQUAL(top->size(), 1);
            CHECK_EQUAL(sub->size(), 0);
            auto top_obj = *top->begin();
            CHECK(top_obj.is_null("sub"));
        }
    });
}

TEST(EmbeddedObjects_SetDefaultNullIgnored)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    Associativity assoc{test_context, 2, changeset_dump_dir_gen.get()};
    assoc.for_each_permutation([&](auto& it) {
        auto server = &*it.server;
        auto client_1 = &*it.clients[0];
        auto client_2 = &*it.clients[1];

        client_1->create_schema([](WriteTransaction& tr) {
            TableRef top = tr.get_group().add_table_with_primary_key("class_Top", type_Int, "pk");
            TableRef sub = tr.add_embedded_table("class_Sub");
            top->add_column(*sub, "sub");
            sub->add_column(type_Int, "i");
        });

        it.sync_all();

        // Client 1 adds an embedded object.
        client_1->transaction([&](auto& c) {
            auto& tr = *c.group;
            TableRef top = tr.get_table("class_Top");
            auto top_obj = top->create_object_with_primary_key(123);
            top_obj.create_and_set_linked_object(top->get_column_key("sub")).set("i", 5);
        });

        // Client 2 sets a default NULL value at a higher timestamp - this should not delete the object.
        client_2->history.advance_time(1);
        client_2->transaction([&](auto& c) {
            auto& tr = *c.group;
            TableRef top = tr.get_table("class_Top");
            auto top_obj = top->create_object_with_primary_key(123);
            bool is_default = true;
            top_obj.set_null("sub", is_default);
        });

        it.sync_all();

        {
            ReadTransaction read_server(server->shared_group);
            auto top = read_server.get_table("class_Top");
            auto sub = read_server.get_table("class_Sub");
            CHECK_EQUAL(top->size(), 1);
            CHECK_EQUAL(sub->size(), 1);
            auto top_obj = *top->begin();
            auto sub_obj = top_obj.get_linked_object(top->get_column_key("sub"));
            CHECK_EQUAL(sub_obj.get<int64_t>("i"), 5);
        }
    });
}

TEST(EmbeddedObjects_DiscardThroughImplicitErase)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    Associativity assoc{test_context, 2, changeset_dump_dir_gen.get()};
    assoc.for_each_permutation([&](auto& it) {
        auto server = &*it.server;
        auto client_1 = &*it.clients[0];
        auto client_2 = &*it.clients[1];

        client_1->create_schema([](WriteTransaction& tr) {
            TableRef top = tr.get_group().add_table_with_primary_key("class_Top", type_Int, "pk");
            TableRef sub = tr.add_embedded_table("class_Sub");
            top->add_column(*sub, "sub");
            sub->add_column(type_Int, "i");

            auto top_obj = top->create_object_with_primary_key(123);
            auto sub_obj = top_obj.create_and_set_linked_object(top->get_column_key("sub")).set("i", 5);
        });

        it.sync_all();

        // At T1, client 1 modifies a field in the embedded object.
        client_1->history.advance_time(1);
        client_1->transaction([](auto& c) {
            auto& tr = *c.group;
            TableRef top = tr.get_table("class_Top");
            auto top_obj = *top->begin();
            top_obj.get_linked_object(top->get_column_key("sub")).set("i", 10);
        });

        // At T0, client 2 nullifies (erases) the embedded object.
        client_2->transaction([](auto& c) {
            auto& tr = *c.group;
            TableRef top = tr.get_table("class_Top");
            auto top_obj = *top->begin();
            top_obj.set_null("sub");
        });

        it.sync_all();

        {
            ReadTransaction read_server(server->shared_group);
            auto top = read_server.get_table("class_Top");
            auto sub = read_server.get_table("class_Sub");
            CHECK_EQUAL(top->size(), 1);
            CHECK_EQUAL(sub->size(), 0);
            CHECK(top->begin()->is_null("sub"));
        }
    });
}

TEST(EmbeddedObjects_AdjustPathOnInsert)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    client_1->create_schema([](WriteTransaction& tr) {
        TableRef top = tr.get_group().add_table_with_primary_key("class_Top", type_Int, "pk");
        TableRef sub = tr.add_embedded_table("class_Sub");
        top->add_column_list(*sub, "sub");
        sub->add_column_list(*sub, "sub");
        sub->add_column(type_Int, "i");

        auto top_obj = top->create_object_with_primary_key(123);
        auto top_list = top_obj.get_linklist("sub");
        auto sub_obj = top_list.create_and_insert_linked_object(0);
        sub_obj.set("i", 0);
        auto sub_list = sub_obj.get_linklist("sub");
        auto sub_obj2 = sub_list.create_and_insert_linked_object(0);
        sub_obj2.set("i", 1);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    // Client 1 appends a new entry in the top's list.
    client_1->transaction([&](auto& c) {
        auto& tr = *c.group;
        TableRef top = tr.get_table("class_Top");
        auto top_obj = *top->begin();
        auto top_list = top_obj.get_linklist("sub");
        CHECK_EQUAL(top_list.size(), 1);
        auto sub_obj = top_list.create_and_insert_linked_object(1);
        sub_obj.set("i", 2);
    });

    // Client 2 prepends a new object in the top's list.
    client_2->transaction([&](auto& c) {
        auto& tr = *c.group;
        TableRef top = tr.get_table("class_Top");
        auto top_obj = *top->begin();
        auto top_list = top_obj.get_linklist("sub");
        CHECK_EQUAL(top_list.size(), 1);
        auto sub_obj = top_list.create_and_insert_linked_object(0);
        sub_obj.set("i", 3);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1, test_context.logger));
    CHECK(compare_groups(read_server, read_client_2));

    {
        auto top = read_server.get_table("class_Top");
        auto sub = read_server.get_table("class_Sub");
        CHECK_EQUAL(top->size(), 1);
        CHECK_EQUAL(sub->size(), 4); // the original element had a sub-object
        auto top_obj = *top->begin();
        auto top_list = top_obj.get_linklist("sub");
        CHECK_EQUAL(top_list.size(), 3);
        auto sub_obj0 = top_list.get_object(0);
        auto sub_obj1 = top_list.get_object(1);
        auto sub_obj2 = top_list.get_object(2);
        CHECK_EQUAL(sub_obj0.get<int64_t>("i"), 3);
        CHECK_EQUAL(sub_obj1.get<int64_t>("i"), 0);
        CHECK_EQUAL(sub_obj2.get<int64_t>("i"), 2);
        auto sub_subobj = sub_obj1.get_linklist("sub").get_object(0);
        CHECK_EQUAL(sub_subobj.get<int64_t>("i"), 1);
    }
}

TEST(EmbeddedObjects_AdjustPathOnErase)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    client_1->create_schema([](WriteTransaction& tr) {
        TableRef top = tr.get_group().add_table_with_primary_key("class_Top", type_Int, "pk");
        TableRef sub = tr.add_embedded_table("class_Sub");
        top->add_column_list(*sub, "sub");
        sub->add_column_list(*sub, "sub");
        sub->add_column(type_Int, "i");

        auto top_obj = top->create_object_with_primary_key(123);
        auto top_list = top_obj.get_linklist("sub");
        auto sub_obj = top_list.create_and_insert_linked_object(0);
        sub_obj.set("i", 0);
        auto sub_list = sub_obj.get_linklist("sub");
        auto sub_obj2 = sub_list.create_and_insert_linked_object(0);
        sub_obj2.set("i", 1);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    // Client 1 inserts a new entry in the top's list.
    client_1->transaction([&](auto& c) {
        auto& tr = *c.group;
        TableRef top = tr.get_table("class_Top");
        auto top_obj = *top->begin();
        auto top_list = top_obj.get_linklist("sub");
        CHECK_EQUAL(top_list.size(), 1);
        auto sub_obj = top_list.create_and_insert_linked_object(1);
        sub_obj.set("i", 2);
    });

    // Client 2 erases the first entry in the top's list. Note: This should also
    // erase the sub-objects sub-object (cascading).
    client_2->transaction([&](auto& c) {
        auto& tr = *c.group;
        TableRef top = tr.get_table("class_Top");
        auto top_obj = *top->begin();
        auto top_list = top_obj.get_linklist("sub");
        CHECK_EQUAL(top_list.size(), 1);
        top_list.remove(0);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1, test_context.logger));
    CHECK(compare_groups(read_server, read_client_2));

    {
        auto top = read_server.get_table("class_Top");
        auto sub = read_server.get_table("class_Sub");
        CHECK_EQUAL(top->size(), 1);
        CHECK_EQUAL(sub->size(), 1);
        auto top_obj = *top->begin();
        auto top_list = top_obj.get_linklist("sub");
        CHECK_EQUAL(top_list.size(), 1);
        auto sub_obj = top_list.get_object(0);
        CHECK_EQUAL(sub_obj.get<int64_t>("i"), 2);
    }
}

TEST(EmbeddedObjects_CreateEraseCreateSequencePreservesObject)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    Associativity assoc{test_context, 2, changeset_dump_dir_gen.get()};
    assoc.for_each_permutation([&](auto& it) {
        auto server = &*it.server;
        auto client_1 = &*it.clients[0];
        auto client_2 = &*it.clients[1];

        // Disable history compaction to be certain that create-erase-create
        // cycles are not eliminated.
        server->history.set_disable_compaction(true);
        client_1->history.set_disable_compaction(true);
        client_2->history.set_disable_compaction(true);

        // Create baseline
        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.add_table_with_primary_key("class_table", type_Int, "pk");
            auto embedded = tr.add_embedded_table("class_embedded");
            embedded->add_column(type_Int, "int");
            table->add_column(*embedded, "embedded");
            auto obj = table->create_object_with_primary_key(123);
            // Note: embedded object is NULL at this stage.
        });

        it.sync_all();

        // Create-Erase-Create cycle
        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_table");
            auto obj = table->get_object_with_primary_key(123);
            auto col = table->get_column_key("embedded");

            auto subobj = obj.create_and_set_linked_object(col);
            subobj.set("int", 1);

            subobj.remove();
            REALM_ASSERT(obj.is_null(col));

            subobj = obj.create_and_set_linked_object(col);
            subobj.set("int", 2);
        });

        client_2->history.advance_time(1);
        client_2->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_table");
            auto obj = table->get_object_with_primary_key(123);

            auto subobj = obj.create_and_set_linked_object(table->get_column_key("embedded"));
            subobj.set("int", 3);
        });

        it.sync_all();

        ReadTransaction rt_0{server->shared_group};
        auto table = rt_0.get_table("class_table");
        // FIXME: Core lacks a const Table::get_object_with_primary_key()
        auto objkey = table->find_primary_key(123);
        auto obj = table->get_object(objkey);
        auto subobj = obj.get_linked_object(table->get_column_key("embedded"));
        CHECK_EQUAL(subobj.get<int64_t>("int"), 2);
    });
}

TEST(EmbeddedObjects_CreateEraseCreateSequencePreservesObject_Nested)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    Associativity assoc{test_context, 2, changeset_dump_dir_gen.get()};
    assoc.for_each_permutation([&](auto& it) {
        auto server = &*it.server;
        auto client_1 = &*it.clients[0];
        auto client_2 = &*it.clients[1];

        // Disable history compaction to be certain that create-erase-create
        // cycles are not eliminated.
        server->history.set_disable_compaction(true);
        client_1->history.set_disable_compaction(true);
        client_2->history.set_disable_compaction(true);

        // Create baseline
        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.add_table_with_primary_key("class_table", type_Int, "pk");
            auto embedded = tr.add_embedded_table("class_embedded");
            embedded->add_column(type_Int, "int");
            embedded->add_column(*embedded, "embedded");
            table->add_column(*embedded, "embedded");
            auto obj = table->create_object_with_primary_key(123);
            // Note: embedded object is NULL at this stage.
        });

        it.sync_all();

        // Create-Erase-Create cycle
        client_1->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_table");
            auto embedded = tr.get_table("class_embedded");
            auto obj = table->get_object_with_primary_key(123);
            auto col = table->get_column_key("embedded");
            auto subcol = embedded->get_column_key("embedded");

            auto subobj = obj.create_and_set_linked_object(col);
            auto subsubobj = subobj.create_and_set_linked_object(subcol);
            subsubobj.set("int", 1);

            // subobj.remove(); // FIXME: Core doesn't cascade this to the subsubobj
            obj.set_null("embedded");
            REALM_ASSERT(obj.is_null(col));
            REALM_ASSERT(!subsubobj.is_valid());

            subobj = obj.create_and_set_linked_object(col);
            subsubobj = subobj.create_and_set_linked_object(subcol);
            subsubobj.set("int", 2);
        });

        client_2->history.advance_time(1);
        client_2->transaction([&](Peer& c) {
            auto& tr = *c.group;
            auto table = tr.get_table("class_table");
            auto embedded = tr.get_table("class_embedded");
            auto obj = table->get_object_with_primary_key(123);
            auto subcol = embedded->get_column_key("embedded");

            auto subobj = obj.create_and_set_linked_object(table->get_column_key("embedded"));
            auto subsubobj = subobj.create_and_set_linked_object(subcol);
            subsubobj.set("int", 3);
        });

        it.sync_all();

        ReadTransaction rt_0{server->shared_group};
        auto table = rt_0.get_table("class_table");
        auto embedded = rt_0.get_table("class_embedded");
        // FIXME: Core lacks a const Table::get_object_with_primary_key()
        auto objkey = table->find_primary_key(123);
        auto obj = table->get_object(objkey);
        auto subobj = obj.get_linked_object(table->get_column_key("embedded"));
        auto subsubobj = subobj.get_linked_object(embedded->get_column_key("embedded"));
        CHECK_EQUAL(subsubobj.get<int64_t>("int"), 2);
    });
}
