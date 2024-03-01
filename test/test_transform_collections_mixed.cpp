#include "peer.hpp"
#include "util/dump_changesets.hpp"

using namespace realm;
using namespace realm::test_util;

// Test merging instructions at different level of nesting.

TEST(Transform_CreateArrayVsArrayInsert)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto list = table->get_object_with_primary_key(1).get_list_ptr<Mixed>(col_any);
    CHECK_EQUAL(list->size(), 1);
    CHECK_EQUAL(list->get(0), 42);
}

TEST(Transform_Nested_CreateArrayVsArrayInsert)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::List);
        auto list2 = list->get_list(0);
        list2->insert(0, 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    auto set_nested_list = [](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto list = p.table("class_Table")->get_object_with_primary_key(1).get_list_ptr<Mixed>({col_any, "A", 0});
        list->set_collection(0, CollectionType::List);
    };

    client_2->transaction([&](Peer& p) {
        set_nested_list(p);
    });

    synchronize(server.get(), {client_2.get()});

    client_1->transaction([&](Peer& p) {
        set_nested_list(p);
    });

    client_2->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto list = p.table("class_Table")->get_object_with_primary_key(1).get_list_ptr<Mixed>({col_any, "A", 0, 0});
        list->add(42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    CHECK_EQUAL(table->get_object_with_primary_key(1).get_list_ptr<Mixed>({col_any, "A", 0, 0})->get(0), 42);
}

TEST(Transform_CreateArrayVsDictionaryInsert)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
    });

    client_2->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr(col_any);
        dict->insert("key", 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    CHECK(table->get_object_with_primary_key(1).get_list_ptr<Mixed>(col_any)->is_empty());
}

TEST(Transform_Nested_CreateArrayVsDictionaryInsert)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::Dictionary);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
    });

    client_2->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0, "B"});
        dict->insert("key", 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    CHECK(table->get_object_with_primary_key(1).get_list_ptr<Mixed>({col_any, "A", 0, "B"})->is_empty());
}

TEST(Transform_CreateDictionaryVsDictionaryInsert)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::List);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    auto set_nested_dictionary = [](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::Dictionary);
    };

    client_2->transaction([&](Peer& p) {
        set_nested_dictionary(p);
    });

    synchronize(server.get(), {client_2.get()});

    client_1->transaction([&](Peer& p) {
        set_nested_dictionary(p);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key", 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    CHECK_EQUAL(table->get_object_with_primary_key(1).get_dictionary(col_any).get("key"), 42);
}

TEST(Transform_Nested_CreateDictionaryVsDictionaryInsert)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::List);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    auto set_nested_dictionary = [](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
    };

    client_2->transaction([&](Peer& p) {
        set_nested_dictionary(p);
    });

    synchronize(server.get(), {client_2.get()});

    client_1->transaction([&](Peer& p) {
        set_nested_dictionary(p);
    });

    client_2->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0, "B"});
        dict->insert("key", 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    CHECK_EQUAL(table->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0, "B"})->get("key"), 42);
}

TEST(Transform_CreateDictionaryVsArrayInsert)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::List);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto list = obj.get_list<Mixed>(col_any);
        list.add(42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    CHECK(table->get_object_with_primary_key(1).get_dictionary(col_any).is_empty());
}

TEST(Transform_Nested_CreateDictionaryVsArrayInsert)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::List);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
    });

    client_2->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto list =
            p.table("class_Table")->get_object_with_primary_key(1).get_list_ptr<Mixed>({col_any, "A", 0, "B"});
        list->add(42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    CHECK(table->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0, "B"})->is_empty());
}

TEST(Transform_ArrayInsertVsUpdateString)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
        list.add(2);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto list = obj.get_list<Mixed>(col_any);
        list.add(3);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set(col_any, Mixed{"value"});
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    CHECK_EQUAL(table->get_object_with_primary_key(1).get_any(col_any), "value");
}

TEST(Transform_ClearArrayVsDictionaryInsert)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
        list.add(2);
        list.clear();
        list.add(3);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto list = table->get_object_with_primary_key(1).get_list_ptr<Mixed>(col_any);
    CHECK_EQUAL(list->size(), 1);
    CHECK_EQUAL(list->get(0), 3);
}

// Test merging instructions at same level of nesting (both on Mixed properties and nested collections).

TEST(Transform_CreateArrayBeforeUpdateInt)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        table->add_column(type_Mixed, "any");
        table->create_object_with_primary_key(1);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        obj.set_any("any", 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    CHECK_EQUAL(table->get_object_with_primary_key(1).get_any("any"), 42);
}

TEST(Transform_CreateArrayAfterUpdateInt)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        table->add_column(type_Mixed, "any");
        table->create_object_with_primary_key(1);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_2->history.set_time(1);
    client_1->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        obj.set_any("any", 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    CHECK(table->get_object_with_primary_key(1).get_list<Mixed>("any").is_empty());
}

TEST(Transform_Nested_CreateArrayBeforeUpdateInt)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert("B", "some value");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
    });

    client_2->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0});
        dict->insert("B", 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    CHECK_EQUAL(table->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0})->get("B"), 42);
}

TEST(Transform_CreateDictionaryBeforeUpdateInt)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        table->add_column(type_Mixed, "any");
        table->create_object_with_primary_key(1);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        obj.set_any("any", 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    CHECK_EQUAL(table->get_object_with_primary_key(1).get_any("any"), 42);
}

TEST(Transform_CreateDictionaryAfterUpdateInt)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        table->add_column(type_Mixed, "any");
        table->create_object_with_primary_key(1);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_2->history.set_time(1);
    client_1->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::Dictionary);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        obj.set_any("any", 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    CHECK(table->get_object_with_primary_key(1).get_dictionary("any").is_empty());
}

TEST(Transform_Nested_CreateDictionaryAfterUpdateInt)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert("B", "some value");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_2->history.set_time(1);
    client_1->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
    });

    client_2->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0});
        dict->insert("B", 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    CHECK(table->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0, "B"})->is_empty());
}

TEST(Transform_MergeArrays)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, "a");
        list.insert(1, "b");
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, "c");
        list.insert(1, "d");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto list = table->get_object_with_primary_key(1).get_list_ptr<Mixed>(col_any);
    CHECK_EQUAL(list->size(), 4);
    CHECK_EQUAL(list->get(0), "a");
    CHECK_EQUAL(list->get(1), "b");
    CHECK_EQUAL(list->get(2), "c");
    CHECK_EQUAL(list->get(3), "d");
}

TEST(Transform_Nested_MergeArrays)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::Dictionary);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
        auto list = dict->get_list("B");
        list->insert(0, "a");
        list->insert(1, "b");
    });

    client_2->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
        auto list = dict->get_list("B");
        list->insert(0, "c");
        list->insert(1, "d");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto list = table->get_object_with_primary_key(1).get_list_ptr<Mixed>({col_any, "A", 0, "B"});
    CHECK_EQUAL(list->size(), 4);
    CHECK_EQUAL(list->get(0), "a");
    CHECK_EQUAL(list->get(1), "b");
    CHECK_EQUAL(list->get(2), "c");
    CHECK_EQUAL(list->get(3), "d");
}

TEST(Transform_MergeDictionaries)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto list = obj.get_dictionary(col_any);
        list.insert("key1", "a");
        list.insert("key2", "b");
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto list = obj.get_dictionary(col_any);
        list.insert("key2", "y");
        list.insert("key3", "z");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto dict = table->get_object_with_primary_key(1).get_dictionary(col_any);
    CHECK_EQUAL(dict.size(), 3);
    CHECK_EQUAL(dict.get("key1"), "a");
    CHECK_EQUAL(dict.get("key2"), "y");
    CHECK_EQUAL(dict.get("key3"), "z");
}

TEST(Transform_Nested_MergeDictionaries)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
        auto dict2 = list->get_dictionary(0);
        dict2->insert_collection("B", CollectionType::List);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
        auto dict2 = dict->get_dictionary("B");
        dict2->insert("key1", "a");
        dict2->insert("key2", "b");
    });

    client_2->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
        auto dict2 = dict->get_dictionary("B");
        dict2->insert("key2", "y");
        dict2->insert("key3", "z");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto dict = table->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0, "B"});
    CHECK_EQUAL(dict->size(), 3);
    CHECK_EQUAL(dict->get("key1"), "a");
    CHECK_EQUAL(dict->get("key2"), "y");
    CHECK_EQUAL(dict->get("key3"), "z");
}

TEST(Transform_CreateArrayAfterCreateDictionary)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_2->history.set_time(1);
    client_1->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, "a");
        list.insert(1, "b");
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", "a");
        dict.insert("key2", "b");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto list = table->get_object_with_primary_key(1).get_list_ptr<Mixed>(col_any);
    CHECK_EQUAL(list->size(), 2);
    CHECK_EQUAL(list->get(0), "a");
    CHECK_EQUAL(list->get(1), "b");
}

TEST(Transform_CreateArrayBeforeCreateDictionary)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, "a");
        list.insert(1, "b");
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", "a");
        dict.insert("key2", "b");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto dict = table->get_object_with_primary_key(1).get_dictionary_ptr(col_any);
    CHECK_EQUAL(dict->size(), 2);
    CHECK_EQUAL(dict->get("key1"), "a");
    CHECK_EQUAL(dict->get("key2"), "b");
}

TEST(Transform_Nested_CreateArrayAfterCreateDictionary)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->insert_collection(0, CollectionType::Dictionary);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_2->history.set_time(1);
    client_1->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::List);
        auto list = dict->get_list("B");
        list->insert(0, "a");
        list->insert(1, "b");
    });

    client_2->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A", 0});
        dict->insert_collection("B", CollectionType::Dictionary);
        auto dict2 = dict->get_dictionary("B");
        dict2->insert("key1", "a");
        dict2->insert("key2", "b");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto list = table->get_object_with_primary_key(1).get_list_ptr<Mixed>({col_any, "A", 0, "B"});
    CHECK_EQUAL(list->size(), 2);
    CHECK_EQUAL(list->get(0), "a");
    CHECK_EQUAL(list->get(1), "b");
}

TEST(Transform_Nested_ClearArrayVsUpdateString)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary_ptr(col_any);
        dict->insert_collection("A", CollectionType::List);
        auto list = dict->get_list("A");
        list->add(1);
        list->add(2);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto list = p.table("class_Table")->get_object_with_primary_key(1).get_list_ptr<Mixed>({col_any, "A"});
        list->clear();
        list->add(3);
    });

    client_2->transaction([](Peer& p) {
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = p.table("class_Table")->get_object_with_primary_key(1).get_dictionary_ptr(col_any);
        dict->insert("A", "some value");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto dict = table->get_object_with_primary_key(1).get_dictionary_ptr(col_any);
    CHECK_EQUAL(dict->size(), 1);
    CHECK_EQUAL(dict->get("A"), "some value");
}

TEST(Transform_ClearArrayVsCreateArray)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 1);
        dict.insert("key2", 2);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
        list.clear();
        list.add(2);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(4);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto list = table->get_object_with_primary_key(1).get_list_ptr<Mixed>(col_any);
    CHECK_EQUAL(list->size(), 1);
    CHECK_EQUAL(list->get(0), 2);
}

TEST(Transform_ClearArrayInsideArrayVsCreateArray)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key1", 1);
        dict->insert("key2", 2);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::List);
        auto list2 = list.get_list(0);
        list2->add(1);
        list2->clear();
        list2->add(2);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::List);
        auto list2 = list.get_list(0);
        list2->add(4);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto list = table->get_object_with_primary_key(1).get_list_ptr<Mixed>({col_any, 0});
    CHECK_EQUAL(list->size(), 1);
    CHECK_EQUAL(list->get(0), 2);
}

TEST(Transform_ClearArrayInsideDictionaryVsCreateArray)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key1", 1);
        dict2->insert("key2", 2);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::List);
        auto list = dict.get_list("A");
        list->add(1);
        list->clear();
        list->add(2);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::List);
        auto list = dict.get_list("A");
        list->add(4);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto list = table->get_object_with_primary_key(1).get_list_ptr<Mixed>({col_any, "A"});
    CHECK_EQUAL(list->size(), 1);
    CHECK_EQUAL(list->get(0), 2);
}

TEST(Transform_ClearArrayVsCreateDictionary)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::List);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
        list.add(2);
        list.clear();
        list.add(3);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto dict = table->get_object_with_primary_key(1).get_dictionary(col_any);
    CHECK(dict.is_empty());
}

TEST(Transform_ClearArrayInsideArrayVsCreateDictionary)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::List);
        auto list2 = list.get_list(0);
        list2->add(1);
        list2->clear();
        list2->add(2);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key1", "some value");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto dict = table->get_object_with_primary_key(1).get_dictionary_ptr({col_any, 0});
    CHECK(dict->is_empty());
}

TEST(Transform_ClearArrayInsideDictionaryVsCreateDictionary)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("A", "some value");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::List);
        auto list = dict.get_list("A");
        list->add(1);
        list->clear();
        list->add(2);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key1", "some other value");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto dict = table->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A"});
    CHECK(dict->is_empty());
}

TEST(Transform_ClearDictionaryVsCreateArray)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 1);
        dict.insert("key2", 2);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key3", 3);
        dict.clear();
        dict.insert("key4", 4);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.add(1);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto list = table->get_object_with_primary_key(1).get_list_ptr<Mixed>(col_any);
    CHECK(list->is_empty());
}

TEST(Transform_ClearDictionaryInsideArrayVsCreateArray)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key1", 1);
        dict->insert("key2", 2);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = obj.get_dictionary_ptr({col_any, 0});
        dict->insert("key3", 3);
        dict->clear();
        dict->insert("key4", 4);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::List);
        auto list2 = list.get_list(0);
        list2->add(4);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto list = table->get_object_with_primary_key(1).get_list_ptr<Mixed>({col_any, 0});
    CHECK(list->is_empty());
}

TEST(Transform_ClearDictionaryInsideDictionaryVsCreateArray)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key1", 1);
        dict2->insert("key2", 2);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = obj.get_dictionary_ptr({col_any, "A"});
        dict->insert("key3", 3);
        dict->clear();
        dict->insert("key4", 4);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::List);
        auto list = dict.get_list("A");
        list->add(4);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto list = table->get_object_with_primary_key(1).get_list_ptr<Mixed>({col_any, "A"});
    CHECK(list->is_empty());
}

TEST(Transform_ClearDictionaryVsCreateDictionary)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::List);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key1", 1);
        dict.clear();
        dict.insert("key2", 2);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("key3", 3);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto dict = table->get_object_with_primary_key(1).get_dictionary(col_any);
    CHECK_EQUAL(dict.size(), 1);
    CHECK_EQUAL(dict.get("key2"), 2);
}

TEST(Transform_ClearDictionaryInsideArrayVsCreateDictionary)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::List);
        auto list = obj.get_list<Mixed>(col_any);
        list.insert(0, 42);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto list = obj.get_list<Mixed>(col_any);
        list.insert_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key1", 1);
        dict->clear();
        dict->insert("key2", 2);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto list = obj.get_list<Mixed>(col_any);
        list.set_collection(0, CollectionType::Dictionary);
        auto dict = list.get_dictionary(0);
        dict->insert("key3", 3);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto dict = table->get_object_with_primary_key(1).get_dictionary_ptr({col_any, 0});
    CHECK_EQUAL(dict->size(), 1);
    CHECK_EQUAL(dict->get("key2"), 2);
}

TEST(Transform_ClearDictionaryInsideDictionaryVsCreateDictionary)
{
    auto changeset_dump_dir_gen = get_changeset_dump_dir_generator(test_context);
    auto server = Peer::create_server(test_context, changeset_dump_dir_gen.get());
    auto client_1 = Peer::create_client(test_context, 2, changeset_dump_dir_gen.get());
    auto client_2 = Peer::create_client(test_context, 3, changeset_dump_dir_gen.get());

    // Create baseline
    client_1->transaction([](Peer& c) {
        auto& tr = *c.group;
        TableRef table = tr.add_table_with_primary_key("class_Table", type_Int, "id");
        auto col_any = table->add_column(type_Mixed, "any");
        auto obj = table->create_object_with_primary_key(1);
        obj.set_collection(col_any, CollectionType::Dictionary);
        auto dict = obj.get_dictionary(col_any);
        dict.insert("A", "some value");
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    client_1->history.set_time(1);
    client_2->history.set_time(2);

    client_1->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key1", 1);
        dict2->clear();
        dict2->insert("key2", 2);
    });

    client_2->transaction([](Peer& p) {
        auto obj = p.table("class_Table")->get_object_with_primary_key(1);
        auto col_any = p.table("class_Table")->get_column_key("any");
        auto dict = obj.get_dictionary(col_any);
        dict.insert_collection("A", CollectionType::Dictionary);
        auto dict2 = dict.get_dictionary("A");
        dict2->insert("key3", 3);
    });

    synchronize(server.get(), {client_1.get(), client_2.get()});

    ReadTransaction read_server(server->shared_group);
    ReadTransaction read_client_1(client_1->shared_group);
    ReadTransaction read_client_2(client_2->shared_group);
    CHECK(compare_groups(read_server, read_client_1));
    CHECK(compare_groups(read_server, read_client_2, *test_context.logger));
    auto table = read_server.get_table("class_Table");
    auto col_any = table->get_column_key("any");
    auto dict = table->get_object_with_primary_key(1).get_dictionary_ptr({col_any, "A"});
    CHECK_EQUAL(dict->size(), 1);
    CHECK_EQUAL(dict->get("key2"), 2);
}

