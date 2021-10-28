#include "test.hpp"

#include <realm/sync/noinst/server/encryption_transformer.hpp>
#include <realm/util/file.hpp>
#include <realm/db.hpp>
#include <realm/sync/noinst/server/server_history.hpp>

#include "sync_fixtures.hpp"

using namespace realm;
using namespace realm::sync;
using namespace realm::fixtures;

#if REALM_ENABLE_ENCRYPTION

const size_t num_rows = 100;
void populate(DBRef& sg)
{
    WriteTransaction wt{sg};
    TableRef t = wt.add_table("table");
    t->add_column(type_String, "str_col");
    t->add_column(type_Int, "int_col");
    for (size_t i = 0; i < num_rows; ++i) {
        std::string payload(i, 'a');
        StringData payload_sg(payload);
        t->create_object().set_all(payload_sg, int64_t(i));
    }
    wt.commit();
}

bool verify_populated(DBRef& sg)
{
    auto rt = sg->start_read();
    auto table_key = rt->find_table("table");
    if (!table_key)
        return false;

    ConstTableRef t = rt->get_table(table_key);
    auto str_col_ndx = t->get_column_key("str_col");
    auto int_col_ndx = t->get_column_key("int_col");
    if (!str_col_ndx || !int_col_ndx || t->size() != num_rows)
        return false;

    for (auto& o : *t) {
        StringData sd = o.get<String>(str_col_ndx);
        int64_t length = o.get<Int>(int_col_ndx);
        std::string expected(size_t(length), 'a');
        if (sd != expected)
            return false;
    }
    return true;
}


TEST(EncryptTransform_EmptyConfig)
{
    encryption_transformer::Configuration config;
    CHECK_THROW_ANY(encryption_transformer::encrypt_transform(config));
}


TEST(EncryptTransform_NoHistory)
{
    encryption_transformer::Configuration config;
    SHARED_GROUP_TEST_PATH(sg_path);

    config.type = encryption_transformer::Configuration::TransformType::File;
    config.target_path = sg_path;
    {
        auto sg = DB::create(sg_path);
        populate(sg);
        CHECK(verify_populated(sg));
    }

    // non-encrypted to non-encrypted
    encryption_transformer::encrypt_transform(config);
    {
        auto sg = DB::create(sg_path);
        CHECK(verify_populated(sg));
    }

    const char* encryption_key1 = "GIi4eylwnMdGxsd72BBu3yp3AmP80BbdXLI9IFBUlw6kY9mwB17DfMzHjdP3ym08";
    const char* encryption_key2 = "YMyVNSYKNVHeqRXoIYydQ5n1svKhXoKQ0oyHukCEG32zmKnvavTRr4mTEEGTMdWf";
    std::array<char, 64> key1;
    std::array<char, 64> key2;
    std::memcpy(key1.data(), encryption_key1, 64);
    std::memcpy(key2.data(), encryption_key2, 64);

    // non-encrypted to encrypted
    config.output_key = key1;
    encryption_transformer::encrypt_transform(config);
    {
        bool no_create = true;
        auto sg = DB::create(sg_path, no_create, DBOptions{encryption_key1});
        CHECK(verify_populated(sg));
    }

    // encrypted to encrypted
    config.input_key = key1;
    config.output_key = key2;
    encryption_transformer::encrypt_transform(config);
    {
        bool no_create = true;
        auto sg = DB::create(sg_path, no_create, DBOptions{encryption_key2});
        CHECK(verify_populated(sg));
    }

    // encrypted to non-encrypted
    config.input_key = key2;
    config.output_key = util::none;
    encryption_transformer::encrypt_transform(config);
    {
        bool no_create = true;
        auto sg = DB::create(sg_path, no_create, DBOptions{});
        CHECK(verify_populated(sg));
    }
}


// FIXME: Disabled because it uses partial sync
TEST_IF(EncryptTransform_ServerHistory, false)
{
    TEST_DIR(dir);
    SHARED_GROUP_TEST_PATH(reference_path);
    SHARED_GROUP_TEST_PATH(partial_path);
    TEST_PATH(file_list_path);

    const char* encryption_key1 = "GIi4eylwnMdGxsd72BBu3yp3AmP80BbdXLI9IFBUlw6kY9mwB17DfMzHjdP3ym08";
    const char* encryption_key2 = "YMyVNSYKNVHeqRXoIYydQ5n1svKhXoKQ0oyHukCEG32zmKnvavTRr4mTEEGTMdWf";
    std::array<char, 64> key1;
    std::array<char, 64> key2;
    std::memcpy(key1.data(), encryption_key1, 64);
    std::memcpy(key2.data(), encryption_key2, 64);

    std::string reference_server_path;
    std::string partial_server_path;

    {
        auto reference_sg = DB::create(make_client_replication(), reference_path);
        auto partial_sg = DB::create(make_client_replication(), partial_path);

        ClientServerFixture::Config server_config;
        server_config.server_encryption_key = encryption_key1;
        ClientServerFixture fixture{dir, test_context, server_config};
        fixture.start();

        Session reference_session = fixture.make_session(reference_sg);
        fixture.bind_session(reference_session, "/reference");
        ColKey col_ndx_person_name;
        ColKey col_ndx_person_age;
        ObjKey obj_key;
        {
            WriteTransaction wt{reference_sg};
            TableRef persons = wt.add_table("class_persons");
            col_ndx_person_name = persons->add_column(type_String, "name");
            col_ndx_person_age = persons->add_column(type_Int, "age");
            persons->create_object().set_all("Adam", 28);
            persons->create_object().set_all("Frank", 30);
            persons->create_object().set_all("Ben", 28);
            persons->create_object().set_all("Bobby", 5);
            version_type new_version = wt.commit();
            reference_session.nonsync_transact_notify(new_version);
        }
        reference_session.wait_for_upload_complete_or_client_stopped();

        Session partial_session = fixture.make_session(partial_sg);
        fixture.bind_session(partial_session, "/reference/__partial/test/0");
        partial_session.wait_for_download_complete_or_client_stopped();

        reference_server_path = fixture.map_virtual_to_real_path("/reference");
        partial_server_path = fixture.map_virtual_to_real_path("/reference/__partial/test/0");
        CHECK(util::File::exists(reference_path));
        CHECK(util::File::exists(partial_path));

        StringData table_name_result_sets = g_partial_sync_result_sets_table_name;
        ColKey col_ndx_result_set_query;
        ColKey col_ndx_result_set_matches_property;
        GlobalKey result_set;
        {
            WriteTransaction wt{partial_sg};
            TableRef people = wt.get_table("class_persons");
            CHECK(people);
            TableRef result_sets = wt.get_table(table_name_result_sets);
            col_ndx_result_set_query = result_sets->get_column_key("query");
            col_ndx_result_set_matches_property = result_sets->get_column_key("matches_property");
            // 0 = uninitialized, 1 = initialized, -1 = query parsing failed
            result_sets->add_column_list(*people, "people");
            Obj res = result_sets->create_object();
            res.set(col_ndx_result_set_query, "age < 10");
            res.set(col_ndx_result_set_matches_property, "people");
            result_set = res.get_object_id();
            version_type new_version = wt.commit();
            partial_session.nonsync_transact_notify(new_version);
        }
        partial_session.wait_for_upload_complete_or_client_stopped();
        partial_session.wait_for_download_complete_or_client_stopped();
        {
            WriteTransaction wt{partial_sg};
            TableRef persons = wt.get_table("class_persons");
            CHECK(persons);
            CHECK_EQUAL(persons->size(), 1);
            // This check invalidated by lack of state in partial views.
            // StringData name = persons->get_object(0).get<String>(col_ndx_person_name);
            // CHECK_EQUAL(name, "Bobby");

            TableRef result_sets = wt.get_table(g_partial_sync_result_sets_table_name);
            CHECK(result_sets);
            auto col_ndx_links = result_sets->get_column_key("people");
            CHECK(col_ndx_links);
            result_sets->get_object(obj_key).set(col_ndx_result_set_query, "age == 30");

            version_type new_version = wt.commit();
            partial_session.nonsync_transact_notify(new_version);
        }
        partial_session.wait_for_upload_complete_or_client_stopped();
        partial_session.wait_for_download_complete_or_client_stopped();
        {
            ReadTransaction rt{partial_sg};
            ConstTableRef persons = rt.get_table("class_persons");
            CHECK(persons);
            CHECK_EQUAL(persons->size(), 1);
            // This check invalidated by lack of state in partial views.
            // StringData name = persons->get_object(0).get<String>(col_ndx_person_name);
            // CHECK_EQUAL(name, "Frank");
        }
    }

    // perform a key rotation
    encryption_transformer::Configuration config;
    config.input_key = key1;
    config.output_key = key2;

    util::File target_list(file_list_path, util::File::mode_Write);
    std::string list = partial_server_path + "\n" + reference_server_path + "\n";
    target_list.write(list.data(), list.size());
    target_list.close();
    config.target_path = file_list_path;
    config.type = encryption_transformer::Configuration::TransformType::FileContaingPaths;
    size_t transforms = encryption_transformer::encrypt_transform(config);
    CHECK_EQUAL(transforms, 2);

    class ServerHistoryContext : public _impl::ServerHistory::Context {
    public:
        std::mt19937_64& server_history_get_random() noexcept override
        {
            return m_random;
        }

    private:
        std::mt19937_64 m_random;
    };

    { // check that the partial realm is encrypted with the second key
        DBOptions options;
        options.encryption_key = encryption_key2;
        ServerHistoryContext context;
        _impl::ServerHistory::DummyCompactionControl compaction_control;
        _impl::ServerHistory server_history{context, compaction_control};

        auto server_partial_sg = DB::create(server_history, partial_server_path, options);
        {
            ReadTransaction rt{server_partial_sg};
            ConstTableRef persons = rt.get_table("class_persons");
            CHECK(persons);
            CHECK_EQUAL(persons->size(), 1);
            auto name_col_ndx = persons->get_column_key("name");
            CHECK(name_col_ndx);
            // This check is commented out since there is no state in the
            // partial view.
            // StringData name = persons->get_object(0).get<String>(name_col_ndx);
            // CHECK_EQUAL(name, "Frank");
        }
    }
    { // check that the reference realm is encrypted with the second key
        DBOptions options;
        options.encryption_key = encryption_key2;
        ServerHistoryContext context;
        _impl::ServerHistory::DummyCompactionControl compaction_control;
        _impl::ServerHistory server_history{context, compaction_control};

        auto server_reference_sg = DB::create(server_history, reference_server_path, options);
        {
            ReadTransaction rt{server_reference_sg};
            ConstTableRef persons = rt.get_table("class_persons");
            CHECK(persons);
            CHECK_EQUAL(persons->size(), 4);
            auto name_col_ndx = persons->get_column_key("name");
            CHECK(name_col_ndx);
            auto adam_row = persons->find_first_string(name_col_ndx, "Adam");
            auto frank_row = persons->find_first_string(name_col_ndx, "Frank");
            CHECK_NOT_EQUAL(adam_row, realm::null_key);
            CHECK_NOT_EQUAL(frank_row, realm::null_key);
        }
    }
}


#endif // REALM_ENABLE_ENCRYPTION
