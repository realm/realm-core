////////////////////////////////////////////////////////////////////////////
//
// Copyright 2016 Realm Inc.
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

#include "util/event_loop.hpp"
#include "util/test_file.hpp"
#include "util/test_utils.hpp"

#include <realm/util/file.hpp>
#include <realm/util/scope_exit.hpp>

#include <binding_context.hpp>
#include <impl/object_accessor_impl.hpp>
#include <impl/realm_coordinator.hpp>
#include <object_schema.hpp>
#include <property.hpp>
#include <results.hpp>
#include <schema.hpp>
#include <server/admin_realm.hpp>
#include <server/global_notifier.hpp>

using namespace realm;
using namespace realm::util;

namespace {
    using AnyDict = std::map<std::string, util::Any>;
    using AnyVec = std::vector<util::Any>;
}

struct TestContext : CppContext {
    std::map<std::string, AnyDict> defaults;

    using CppContext::CppContext;
    TestContext(TestContext& parent, realm::Property const& prop)
    : CppContext(parent, prop)
    , defaults(parent.defaults)
    { }

    void will_change(Object const&, Property const&) {}
    void did_change() {}
    std::string print(util::Any) { return "not implemented"; }
    bool allow_missing(util::Any) { return false; }
};

struct TestNotifierCallback : public GlobalNotifier::Callback
{
    TestNotifierCallback(std::function<void()> download_completion_handler,
                         std::function<void(std::exception_ptr)> error_handler,
                         std::function<bool(StringData, StringData)> realm_available_handler,
                         std::function<void(GlobalNotifier*)> realm_changed_handler)
    : m_download_completion_handler(download_completion_handler)
    , m_error_handler(error_handler)
    , m_realm_available_handler(realm_available_handler)
    , m_realm_changed_handler(realm_changed_handler)
    {
    }

    /// Called when the initial download of the admin realm is complete and observation is beginning
    void download_complete()
    {
        m_download_completion_handler();
    }

    /// Called when any error occurs within the global notifier
    void error(std::exception_ptr e)
    {
        m_error_handler(e);
    }

    /// Called to determine whether the application wants to listen for changes
    /// to a particular Realm.
    ///
    /// The Realm name that is passed to the callback is hierarchical and takes
    /// the form of an absolute path (separated by forward slashes). This is a
    /// *virtual path*, i.e, it is not necesarily the file system path of the
    /// Realm on the server.
    ///
    /// If this function returns false, the global notifier will not observe
    /// the Realm.
    ///
    /// \param id A unique identifier for the Realm which will not be reused
    ///           even if multiple Realms are created for a single virtual path.
    /// \param name The name (virtual path) by which the server knows that
    /// Realm.
    bool realm_available(StringData id, StringData virtual_path)
    {
        return m_realm_available_handler(id, virtual_path);
    }

    /// Called when a new version is available in an observed Realm.
    void realm_changed(GlobalNotifier* notifier)
    {
        m_realm_changed_handler(notifier);
    }

private:
    std::function<void()> m_download_completion_handler;
    std::function<void(std::exception_ptr)> m_error_handler;
    std::function<bool(StringData, StringData)> m_realm_available_handler;
    std::function<void(GlobalNotifier*)> m_realm_changed_handler;
};

TEST_CASE("global_notifier: basics", "[sync][global_notifier]") {
    _impl::RealmCoordinator::assert_no_open_realms();

    SECTION("listened to realms trigger notification") {
        SyncServer server(false);
        std::string realm_name = "listened_to";
        std::string table_name = "class_object";
        std::string value_col_name = "value";
        std::string object_name = "object";
        std::string object_table_name = "class_object";

        // Add an object to the Realm so that notifications are needed
        auto make_object = [&server, &object_name, &object_table_name, &value_col_name](std::string realm_name, int64_t value) {
            SyncTestFile config(server, realm_name);
            config.schema = Schema {
                {object_name, {
                    {value_col_name, PropertyType::Int, Property::IsPrimary{true}},
                }},
            };
            auto write_realm = Realm::get_shared_realm(config);
            wait_for_download(*write_realm);
            write_realm->begin_transaction();
            write_realm->read_group().get_table(object_table_name)->create_object_with_primary_key(value);
            write_realm->commit_transaction();
            wait_for_upload(*write_realm);
        };

        auto notify_gn_of_realm_change = [&](std::string path) {
            SyncTestFile admin_config(server, "__admin");
            // See AdminRealmListener, in practice this will be updated by ROS
            admin_config.schema = Schema{
                {"RealmFile", {
                    Property{"path", PropertyType::String, Property::IsPrimary{true}},
                    Property{"counter", PropertyType::Int},
                }},
            };
            auto admin_realm = Realm::get_shared_realm(admin_config);
            wait_for_download(*admin_realm);
            admin_realm->begin_transaction();
            auto table = admin_realm->read_group().get_table("class_RealmFile");
            auto path_col = table->get_column_key("path");
            auto count_col = table->get_column_key("counter");
            auto existing_obj_key = table->find_first_string(path_col, path);
            if (existing_obj_key) {
                auto existing_obj = table->get_object(existing_obj_key);
                existing_obj.set(count_col, existing_obj.get<int64_t>(count_col) + 1);
            }
            else {
                auto obj = table->create_object_with_primary_key(path);
                obj.set(count_col, 0);
            }
            admin_realm->commit_transaction();
            wait_for_upload(*admin_realm);
        };

        SECTION("notifications across two transactions are merged before reported") {
            std::atomic<size_t> triggered_download(0);
            std::atomic<size_t> triggered_realm_notification(0);
            std::atomic<size_t> triggered_realm_change(0);

            std::unique_ptr<TestNotifierCallback> callback = std::make_unique<TestNotifierCallback>(
                                                     [&triggered_download]() {
                                                         triggered_download++;
                                                     },
                                                     [](std::exception_ptr) {
                                                     },
                                                     [&triggered_realm_notification](StringData /*id*/, StringData /*virtual_path*/) {
                                                         triggered_realm_notification++;
                                                         return true;
                                                     },
                                                     [&](GlobalNotifier* gn) {
                                                         REQUIRE(gn != nullptr);
                                                         triggered_realm_change++;
                                                     });

            SyncTestFile gn_config_template(server, "");
            gn_config_template.sync_config->reference_realm_url = server.base_url();
            GlobalNotifier global_notifier(std::move(callback), server.local_root_dir(), *gn_config_template.sync_config);
            REQUIRE(triggered_download.load() == 0);
            global_notifier.start();
            server.start();

            {
                auto next_change = global_notifier.next_changed_realm();
                REQUIRE(!next_change);
                REQUIRE(triggered_realm_notification.load() == 0);
                REQUIRE(triggered_realm_change.load() == 0);
            }

            // add two objects, in different transactions
            constexpr int64_t initial_value = 100;
            make_object(realm_name, initial_value);
            constexpr int64_t second_value = 200;
            make_object(realm_name, second_value);

            EventLoop::main().run_until([&] { return triggered_download.load() > 0; });

            notify_gn_of_realm_change(std::string("/") + realm_name);
            EventLoop::main().run_until([&] { return triggered_realm_notification.load() > 0; });
            EventLoop::main().run_until([&] { return triggered_realm_change.load() > 0; });

            {
                auto next_change = global_notifier.next_changed_realm();
                REQUIRE(bool(next_change));
                REQUIRE(next_change->realm_path == (std::string("/") + realm_name));
                REQUIRE(next_change->type == GlobalNotifier::ChangeNotification::Type::Change);

                auto changes = next_change->get_changes();
                REQUIRE(!changes.empty());
                REQUIRE(changes.size() == 1);
                REQUIRE(changes.find(object_name) != changes.end());
                REQUIRE(changes[object_name].insertions_size() == 2);
                REQUIRE(changes[object_name].modifications_size() == 0);
                REQUIRE(changes[object_name].deletions_size() == 0);

                {
                    auto old_realm = next_change->get_old_realm();
                    REQUIRE(!old_realm->read_group().has_table(table_name));
                }
                {
                    auto new_realm = next_change->get_new_realm();
                    auto object_table = new_realm->read_group().get_table(table_name);
                    REQUIRE(object_table);
                    REQUIRE(object_table->size() == 2);
                    auto value_col_key = object_table->get_column_key(value_col_name);

                    REQUIRE(bool(object_table->find_first_int(value_col_key, initial_value)));
                    REQUIRE(bool(object_table->find_first_int(value_col_key, second_value)));

                    REQUIRE(changes[object_name].get_insertions().size() == 2);
                    for (auto insertion : changes[object_name].get_insertions()) {
                        ObjKey key(insertion);
                        REQUIRE(bool(key));
                        REQUIRE(object_table->get_object(key));
                        int64_t value = object_table->get_object(key).get<int64_t>(value_col_name);
                        REQUIRE((value == initial_value || value == second_value));
                    }
                    // no modifications on inserted objects, but the below loop at least checks for compile errors
                    REQUIRE(changes[object_name].get_modifications().size() == 0);
                    for (auto modification : changes[object_name].get_modifications()) {
                        ObjKey key(modification.first);
                        REQUIRE(bool(key));
                        REQUIRE(object_table->get_object(key));
                        int64_t value = object_table->get_object(key).get<int64_t>(value_col_name);
                        REQUIRE((value == initial_value || value == second_value));
                    }
                    auto deletions = changes[object_name].get_deletions();
                    REQUIRE(deletions.size() == 0);
                    REQUIRE(deletions.begin() == deletions.end());
                }
                next_change = global_notifier.next_changed_realm();
                REQUIRE(!bool(next_change));
            }
        }
    }
}
