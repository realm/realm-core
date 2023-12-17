////////////////////////////////////////////////////////////////////////////
//
// Copyright 2024 Realm Inc.
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

#include "util/sync/baas_admin_api.hpp"
#include "util/sync/sync_test_utils.hpp"
#include "util/unit_test_transport.hpp"
#include <util/sync/flx_sync_harness.hpp>

#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/sync/app_backing_store.hpp>

#include <catch2/catch_all.hpp>

using namespace realm;
using namespace realm::app;
using namespace std::string_literals;

struct TestBackingStore final : public app::BackingStore {

    TestBackingStore(std::weak_ptr<app::App> parent)
        : app::BackingStore(parent)
        , m_file_path_root(util::make_temp_dir())
    {
    }
    virtual ~TestBackingStore()
    {
        if (util::File::exists(m_file_path_root)) {
            util::try_remove_dir_recursive(m_file_path_root);
        }
    }
    std::shared_ptr<SyncUser> get_user(std::string_view user_id, std::string_view refresh_token,
                                       std::string_view access_token, std::string_view device_id) override
        REQUIRES(!m_user_mutex)
    {
        std::shared_ptr<SyncUser> user;
        {
            util::CheckedLockGuard guard(m_user_mutex);
            auto it = std::find_if(m_users.begin(), m_users.end(), [&](const auto& user) {
                return user->user_id() == user_id && user->state() != SyncUser::State::Removed;
            });
            if (it == m_users.end()) {
                // No existing user.
                auto new_user = app::BackingStore::make_user(refresh_token, user_id, access_token, device_id,
                                                             m_parent_app.lock());
                m_users.emplace(m_users.begin(), new_user);
                m_current_user = new_user;
                return new_user;
            }
            user = *it;
        }
        user->log_in(access_token, refresh_token);
        return user;
    }
    std::shared_ptr<SyncUser> get_existing_logged_in_user(std::string_view user_id) const override
        REQUIRES(!m_user_mutex)
    {
        util::CheckedLockGuard lock(m_user_mutex);
        auto matcher = [user_id](auto& el) {
            return el->user_id() == user_id && el->state() == SyncUser::State::LoggedIn;
        };
        auto it = std::find_if(m_users.begin(), m_users.end(), matcher);
        return it == m_users.end() ? nullptr : *it;
    }
    std::vector<std::shared_ptr<SyncUser>> all_users() override REQUIRES(!m_user_mutex)
    {
        util::CheckedLockGuard lock(m_user_mutex);
        m_users.erase(std::remove_if(m_users.begin(), m_users.end(),
                                     [](auto& user) {
                                         bool should_remove = (user->state() == SyncUser::State::Removed);
                                         if (should_remove) {
                                             user->detach_from_backing_store();
                                         }
                                         return should_remove;
                                     }),
                      m_users.end());
        return m_users;
    }
    std::shared_ptr<SyncUser> get_current_user() const override REQUIRES(!m_user_mutex)
    {
        util::CheckedLockGuard lock(m_user_mutex);
        if (m_current_user)
            return m_current_user;
        return nullptr;
    }
    void log_out_user(const SyncUser& user) override REQUIRES(!m_user_mutex)
    {
        util::CheckedLockGuard lock(m_user_mutex);
        // Move this user to the end of the vector
        auto user_pos = std::partition(m_users.begin(), m_users.end(), [&](auto& u) {
            return u.get() != &user;
        });
        auto active_user = std::find_if(m_users.begin(), user_pos, [](auto& u) {
            return u->state() == SyncUser::State::LoggedIn;
        });
        if (active_user != user_pos) {
            m_current_user = *active_user;
        }
        else {
            m_current_user = nullptr;
        }
    }
    void set_current_user(std::string_view user_id) override REQUIRES(!m_user_mutex)
    {
        util::CheckedLockGuard lock(m_user_mutex);
        m_current_user = get_user_for_id(user_id);
    }
    void remove_user(std::string_view user_id) override REQUIRES(!m_user_mutex)
    {
        util::CheckedLockGuard lock(m_user_mutex);
        if (auto user = get_user_for_id(user_id))
            user->invalidate();
    }
    void delete_user(std::string_view user_id) override REQUIRES(!m_user_mutex)
    {
        util::CheckedLockGuard lock(m_user_mutex);
        auto it = std::find_if(m_users.begin(), m_users.end(), [&user_id](auto& user) {
            return user->user_id() == user_id;
        });
        auto user = it == m_users.end() ? nullptr : *it;

        if (!user)
            return;

        // Deletion should happen immediately, not when we do the cleanup
        // task on next launch.
        m_users.erase(it);
        user->detach_from_backing_store();

        if (m_current_user && m_current_user->user_id() == user->user_id())
            m_current_user = nullptr;
    }
    void reset_for_testing() override REQUIRES(!m_user_mutex)
    {
        // Destroy all the users.
        util::CheckedLockGuard lock(m_user_mutex);
        for (auto& user : m_users) {
            user->detach_from_backing_store();
        }
        m_users.clear();
        m_current_user = nullptr;
    }
    bool immediately_run_file_actions(std::string_view) override
    {
        // no-op
        return false;
    }
    bool perform_metadata_update(util::FunctionRef<void(SyncMetadataManager&)>) const override
    {
        return false;
    }
    std::string path_for_realm(std::shared_ptr<SyncUser> user, std::optional<std::string> custom_file_name = none,
                               std::optional<std::string> partition_value = none) const override
    {
        REALM_ASSERT(user);
        auto file_name = [&]() -> std::string {
            if (custom_file_name) {
                return *custom_file_name;
            }
            if (!partition_value) {
                return "flx_sync_default";
            }
            return *partition_value;
        }();

        auto ident = user->user_id();
        auto app_id = user->app().lock()->config().app_id;
        return util::format("%1/%2/%3/%4", m_file_path_root, app_id, ident, file_name);
    }
    std::string audit_path_root(std::shared_ptr<SyncUser> user, std::string_view app_id,
                                std::string_view partition_prefix) const override
    {
        auto ident = user->user_id();
        return util::format("%1/%2/realm-audit/%3/%4", m_file_path_root, app_id, ident, partition_prefix);
    }

    std::string recovery_directory_path(std::optional<std::string> const& = none) const override
    {
        REALM_UNREACHABLE();
    }
    std::optional<SyncAppMetadata> app_metadata() const override
    {
        return util::none;
    }

private:
    std::shared_ptr<SyncUser> get_user_for_id(std::string_view identity) const noexcept REQUIRES(m_user_mutex)
    {
        auto is_active_user = [identity](auto& el) {
            return el->user_id() == identity;
        };
        auto it = std::find_if(m_users.begin(), m_users.end(), is_active_user);
        return it == m_users.end() ? nullptr : *it;
    }

    // Protects m_users
    mutable util::CheckedMutex m_user_mutex;
    // A vector of all SyncUser objects.
    std::vector<std::shared_ptr<SyncUser>> m_users GUARDED_BY(m_user_mutex);
    std::shared_ptr<SyncUser> m_current_user GUARDED_BY(m_user_mutex);
    const std::string m_file_path_root;
};

TEST_CASE("app: custom backing store without sync", "[app][backing store]") {
    App::Config config;
    set_app_config_defaults(config, instance_of<UnitTestTransport>);
    std::shared_ptr<TestBackingStore> test_store;
    size_t stores_created = 0;
    auto factory = [&test_store, &stores_created](SharedApp app) {
        test_store = std::make_shared<TestBackingStore>(app);
        ++stores_created;
        return test_store;
    };
    auto app = App::get_app(app::App::CacheMode::Enabled, config, factory);
    REQUIRE(test_store);
    constexpr bool reuse_anon = false;
    auto creds = app::AppCredentials::anonymous(reuse_anon);
    CHECK(test_store->all_users().size() == 0);
    CHECK(test_store->get_current_user() == nullptr);
    auto user1 = log_in(app, creds);
    auto user2 = log_in(app, creds);
    auto user3 = log_in(app, creds);
    CHECK(user3 == test_store->get_current_user());
    CHECK(user1 == test_store->get_existing_logged_in_user(user1->user_id()));
    REQUIRE(test_store->all_users().size() == 3);
    CHECK(test_store->all_users()[0] == user3);
    CHECK(test_store->all_users()[1] == user2);
    CHECK(test_store->all_users()[2] == user1);
    app->log_out(user3, [](util::Optional<AppError> err) {
        REALM_ASSERT(!err);
    });
    CHECK(test_store->get_current_user() != user3);
    app->remove_user(user2, [](util::Optional<AppError> err) {
        REALM_ASSERT(!err);
    });
    CHECK(test_store->get_current_user() == user1);
    CHECK(test_store->all_users().size() == 1);

    app->delete_user(user1, [](util::Optional<AppError> err) {
        REALM_ASSERT(!err);
    });
    CHECK(test_store->get_current_user() == nullptr);
    CHECK(test_store->all_users().size() == 0);
    App::clear_cached_apps();
    REQUIRE(stores_created == 1);
}

#if REALM_ENABLE_AUTH_TESTS

TEST_CASE("app: custom backing store with sync", "[app][sync][backing store][baas][flx]") {
    FLXSyncTestHarness::Config harness_config("flx_custom_backing_store",
                                              FLXSyncTestHarness::default_server_schema());
    harness_config.factory = [](SharedApp app) {
        return std::make_shared<TestBackingStore>(app);
    };
    FLXSyncTestHarness harness(std::move(harness_config));

    auto foo_obj_id = ObjectId::gen();
    auto bar_obj_id = ObjectId::gen();
    harness.load_initial_data([&](SharedRealm realm) {
        CppContext c(realm);
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", foo_obj_id},
                                        {"queryable_str_field", "foo"s},
                                        {"queryable_int_field", static_cast<int64_t>(5)},
                                        {"non_queryable_field", "non queryable 1"s}}));
        Object::create(c, realm, "TopLevel",
                       std::any(AnyDict{{"_id", bar_obj_id},
                                        {"queryable_str_field", "bar"s},
                                        {"queryable_int_field", static_cast<int64_t>(10)},
                                        {"non_queryable_field", "non queryable 2"s}}));
    });


    harness.do_with_new_realm([&](SharedRealm realm) {
        {
            auto empty_subs = realm->get_latest_subscription_set();
            CHECK(empty_subs.size() == 0);
            CHECK(empty_subs.version() == 0);
            empty_subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        auto table = realm->read_group().get_table("class_TopLevel");
        auto col_key = table->get_column_key("queryable_str_field");
        Query query_foo(table);
        query_foo.equal(col_key, "foo");
        {
            auto new_subs = realm->get_latest_subscription_set().make_mutable_copy();
            new_subs.insert_or_assign(query_foo);
            auto subs = new_subs.commit();
            subs.get_state_change_notification(sync::SubscriptionSet::State::Complete).get();
        }

        {
            wait_for_advance(*realm);
            Results results(realm, table);
            CHECK(results.size() == 1);
            auto obj = results.get<Obj>(0);
            CHECK(obj.is_valid());
            CHECK(obj.get<ObjectId>("_id") == foo_obj_id);
        }
    });
}

#endif // REALM_ENABLE_AUTH_TESTS
