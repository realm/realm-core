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

#include "sync_test_utils.hpp"

#include "util/event_loop.hpp"

#include "sync/sync_manager.hpp"
#include "sync/sync_notifier.hpp"
#include "sync/sync_user.hpp"
#include <realm/util/logger.hpp>
#include <realm/util/scope_exit.hpp>

using namespace realm;
using namespace realm::util;
using File = realm::util::File;

static const std::string base_path = tmp_dir() + "/realm_objectstore_sync_manager/";

namespace realm {

class TestNotifier : public SyncNotifier {
public:
    enum class LastCalled {
        None, UserLoggedIn, UserLoggedOut, SessionBoundToServer, SessionDestroyed, MetadataReset, UserDeleted
    };
    mutable LastCalled last_called = LastCalled::None;

    void user_logged_in(std::shared_ptr<SyncUser>) const override { last_called = LastCalled::UserLoggedIn; }
    void user_logged_out(std::shared_ptr<SyncUser>) const override { last_called = LastCalled::UserLoggedOut; }
    void session_bound_to_server(std::shared_ptr<SyncSession>) const override { last_called = LastCalled::SessionBoundToServer; }
    void session_destroyed(SyncConfig, const std::string&) const override { last_called = LastCalled::SessionDestroyed; }
    void metadata_realm_reset() const override { last_called = LastCalled::MetadataReset; }
    void user_deleted(const std::string&) const override { last_called = LastCalled::UserDeleted; }
};

class TestNotifierFactory : public SyncNotifierFactory {
public:
    TestNotifier* captured_notifier;

    std::unique_ptr<SyncNotifier> make_notifier() override {
        std::unique_ptr<SyncNotifier> notifier = std::make_unique<TestNotifier>();
        captured_notifier = static_cast<TestNotifier*>(&*notifier);
        return notifier;
    }
};

}

namespace {

bool validate_user_in_vector(std::vector<std::shared_ptr<SyncUser>> vector,
                             const std::string& identity,
                             util::Optional<std::string> url,
                             const std::string& token) {
    for (auto& user : vector) {
        if (user->identity() == identity && user->refresh_token() == token && url.value_or("") == user->server_url()) {
           return true;
        }
    }
    return false;
}

}

TEST_CASE("sync_manager: basic property APIs", "[sync]") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);
    SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);

    SECTION("should work for log level") {
        SyncManager::shared().set_log_level(util::Logger::Level::info);
        REQUIRE(SyncManager::shared().log_level() == util::Logger::Level::info);
        SyncManager::shared().set_log_level(util::Logger::Level::error);
        REQUIRE(SyncManager::shared().log_level() == util::Logger::Level::error);
    }

    SECTION("should work for 'should reconnect immediately'") {
        SyncManager::shared().set_client_should_reconnect_immediately(true);
        REQUIRE(SyncManager::shared().client_should_reconnect_immediately());
        SyncManager::shared().set_client_should_reconnect_immediately(false);
        REQUIRE(!SyncManager::shared().client_should_reconnect_immediately());
    }

    SECTION("should work for 'should validate SSL'") {
        SyncManager::shared().set_client_should_validate_ssl(true);
        REQUIRE(SyncManager::shared().client_should_validate_ssl());
        SyncManager::shared().set_client_should_validate_ssl(false);
        REQUIRE(!SyncManager::shared().client_should_validate_ssl());
    }
}

TEST_CASE("sync_manager: `path_for_realm` API", "[sync]") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);
    SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);

    SECTION("should work properly") {
        const std::string identity = "foobarbaz";
        const std::string raw_url = "realms://foo.bar.example.com/realm/something/~/123456/xyz";
        const auto expected = base_path + "realm-object-server/foobarbaz/realms%3A%2F%2Ffoo.bar.example.com%2Frealm%2Fsomething%2F%7E%2F123456%2Fxyz";
        REQUIRE(SyncManager::shared().path_for_realm(identity, raw_url) == expected);
        // This API should also generate the directory if it doesn't already exist.
        REQUIRE_DIR_EXISTS(base_path + "realm-object-server/foobarbaz/");
    }
}

TEST_CASE("sync_manager: event notifier system", "[sync]") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);
    TestNotifierFactory factory;
    SyncManager::shared().set_notifier_factory(factory);

    const std::string identity = "jpsimard";
    const std::string url = "https://realm.example.com/foo";

    SECTION("no metadata") {
        SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);

        SECTION("works for user logging in and out") {
            REQUIRE(factory.captured_notifier->last_called == TestNotifier::LastCalled::None);
            // Log in a new user
            auto u1 = SyncManager::shared().get_user(identity, "not-a-real-token", url);
            REQUIRE(factory.captured_notifier->last_called == TestNotifier::LastCalled::UserLoggedIn);
            // Log the user out
            u1->log_out();
            REQUIRE(factory.captured_notifier->last_called == TestNotifier::LastCalled::UserLoggedOut);
            // Log the user back in (existing user)
            u1 = SyncManager::shared().get_user(identity, "still-a-fake-token", url);
            REQUIRE(factory.captured_notifier->last_called == TestNotifier::LastCalled::UserLoggedIn);
        }

        SECTION("works for session binding to sync server and destruction") {
            SyncServer server;
            REQUIRE(factory.captured_notifier->last_called == TestNotifier::LastCalled::None);
            std::weak_ptr<SyncSession> weak_session;
            {
                // Create a session.
                auto user = SyncManager::shared().get_user("user1a", "not_a_real_token");
                auto session = sync_session(server, user, "/test1a-1",
                                            [&](auto&, auto&) { return s_test_token; },
                                            [&](auto, auto, auto, auto) { }, SyncSessionStopPolicy::Immediately);
                weak_session = session;
                EventLoop::main().run_until([&] { return session_is_active(*session); });
                REQUIRE(factory.captured_notifier->last_called == TestNotifier::LastCalled::SessionBoundToServer);
            }
            // Session lifetime should be over.
            REQUIRE(weak_session.expired());
            REQUIRE(factory.captured_notifier->last_called == TestNotifier::LastCalled::SessionDestroyed);
        }
    }

    SECTION("with metadata") {
        SECTION("works for metadata Realm being reset") {
            SyncManager::shared().configure_file_system(base_path,
                                                    SyncManager::MetadataMode::Encryption,
                                                    make_test_encryption_key());
            SyncManager::shared().reset_for_testing();
            SyncManager::shared().set_notifier_factory(factory);
            SyncManager::shared().configure_file_system(base_path,
                                                        SyncManager::MetadataMode::Encryption,
                                                        make_test_encryption_key(1),
                                                        true);
            REQUIRE(factory.captured_notifier->last_called == TestNotifier::LastCalled::MetadataReset);
        }

        SECTION("works for user being deleted") {
            // Create an entry in the metadata database for a user to be deleted.
            auto file_manager = SyncFileManager(base_path);
            SyncMetadataManager manager(file_manager.metadata_path(), false);
            auto user = SyncUserMetadata(manager, identity);
            user.mark_for_removal();
            // Prepopulate the user directory with a dummy Realm.
            const auto user_dir = file_manager.user_directory(identity);
            create_dummy_realm(user_dir + "123456789");
            // Delete the user and look for a notification.
            SyncManager::shared().set_notifier_factory(factory);
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoEncryption);
            REQUIRE(factory.captured_notifier->last_called == TestNotifier::LastCalled::UserDeleted);
        }
    }
}

TEST_CASE("sync_manager: user state management", "[sync]") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);
    SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);

    const std::string url_1 = "https://example.realm.com/1/";
    const std::string url_2 = "https://example.realm.com/2/";
    const std::string url_3 = "https://example.realm.com/3/";
    const std::string token_1 = "foo_token";
    const std::string token_2 = "bar_token";
    const std::string token_3 = "baz_token";
    const std::string identity_1 = "user-foo";
    const std::string identity_2 = "user-bar";
    const std::string identity_3 = "user-baz";

    SECTION("should get all users that are created during run time") {
        SyncManager::shared().get_user(identity_1, token_1, url_1);
        SyncManager::shared().get_user(identity_2, token_2, url_2);
        auto users = SyncManager::shared().all_logged_in_users();
        REQUIRE(users.size() == 2);
        CHECK(validate_user_in_vector(users, identity_1, url_1, token_1));
        CHECK(validate_user_in_vector(users, identity_2, url_2, token_2));
    }

    SECTION("should properly update state in response to users logging in and out") {
        auto token_3a = "qwerty";
        auto u1 = SyncManager::shared().get_user(identity_1, token_1, url_1);
        auto u2 = SyncManager::shared().get_user(identity_2, token_2, url_2);
        auto u3 = SyncManager::shared().get_user(identity_3, token_3, url_3);
        auto users = SyncManager::shared().all_logged_in_users();
        REQUIRE(users.size() == 3);
        CHECK(validate_user_in_vector(users, identity_1, url_1, token_1));
        CHECK(validate_user_in_vector(users, identity_2, url_2, token_2));
        CHECK(validate_user_in_vector(users, identity_3, url_3, token_3));
        // Log out users 1 and 3
        u1->log_out();
        u3->log_out();
        users = SyncManager::shared().all_logged_in_users();
        REQUIRE(users.size() == 1);
        CHECK(validate_user_in_vector(users, identity_2, url_2, token_2));
        // Log user 3 back in
        u3 = SyncManager::shared().get_user(identity_3, token_3a, url_3);
        users = SyncManager::shared().all_logged_in_users();
        REQUIRE(users.size() == 2);
        CHECK(validate_user_in_vector(users, identity_2, url_2, token_2));
        CHECK(validate_user_in_vector(users, identity_3, url_3, token_3a));
        // Log user 2 out
        u2->log_out();
        users = SyncManager::shared().all_logged_in_users();
        REQUIRE(users.size() == 1);
        CHECK(validate_user_in_vector(users, identity_3, url_3, token_3a));
    }

    SECTION("should contain admin-token users if such users are created.") {
        SyncManager::shared().get_user(identity_2, token_2, url_2);
        SyncManager::shared().get_user(identity_3, token_3, none, true);
        auto users = SyncManager::shared().all_logged_in_users();
        REQUIRE(users.size() == 2);
        CHECK(validate_user_in_vector(users, identity_2, url_2, token_2));
        CHECK(validate_user_in_vector(users, identity_3, none, token_3));
    }
}

TEST_CASE("sync_manager: persistent user state management", "[sync]") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);
    auto file_manager = SyncFileManager(base_path);
    // Open the metadata separately, so we can investigate it ourselves.
    SyncMetadataManager manager(file_manager.metadata_path(), false);

    const std::string url_1 = "https://example.realm.com/1/";
    const std::string url_2 = "https://example.realm.com/2/";
    const std::string url_3 = "https://example.realm.com/3/";
    const std::string token_1 = "foo_token";
    const std::string token_2 = "bar_token";
    const std::string token_3 = "baz_token";

    SECTION("when users are persisted") {
        const std::string identity_1 = "foo-1";
        const std::string identity_2 = "bar-1";
        const std::string identity_3 = "baz-1";
        // First, create a few users and add them to the metadata.
        auto u1 = SyncUserMetadata(manager, identity_1);
        u1.set_state(url_1, token_1);
        auto u2 = SyncUserMetadata(manager, identity_2);
        u2.set_state(url_2, token_2);
        auto u3 = SyncUserMetadata(manager, identity_3);
        u3.set_state(url_3, token_3);
        // The fourth user is an "invalid" user: no token, so shouldn't show up.
        auto u_invalid = SyncUserMetadata(manager, "invalid_user");
        REQUIRE(manager.all_unmarked_users().size() == 4);

        SECTION("they should be added to the active users list when metadata is enabled") {
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoEncryption);
            auto users = SyncManager::shared().all_logged_in_users();
            REQUIRE(users.size() == 3);
            REQUIRE(validate_user_in_vector(users, identity_1, url_1, token_1));
            REQUIRE(validate_user_in_vector(users, identity_2, url_2, token_2));
            REQUIRE(validate_user_in_vector(users, identity_3, url_3, token_3));
        }
        SECTION("they should not be added to the active users list when metadata is disabled") {
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);
            auto users = SyncManager::shared().all_logged_in_users();
            REQUIRE(users.size() == 0);
        }
    }

    SECTION("when users are marked") {
        const std::string identity_1 = "foo-2";
        const std::string identity_2 = "bar-2";
        const std::string identity_3 = "baz-2";
        // Pre-populate the user directories.
        const auto user_dir_1 = file_manager.user_directory(identity_1);
        const auto user_dir_2 = file_manager.user_directory(identity_2);
        const auto user_dir_3 = file_manager.user_directory(identity_3);
        create_dummy_realm(user_dir_1 + "123456789");
        create_dummy_realm(user_dir_1 + "foo");
        create_dummy_realm(user_dir_2 + "123456789");
        create_dummy_realm(user_dir_3 + "foo");
        create_dummy_realm(user_dir_3 + "bar");
        create_dummy_realm(user_dir_3 + "baz");
        // Create the user metadata.
        auto u1 = SyncUserMetadata(manager, identity_1);
        u1.mark_for_removal();
        auto u2 = SyncUserMetadata(manager, identity_2);
        u2.mark_for_removal();
        // Don't mark this user for deletion.
        auto u3 = SyncUserMetadata(manager, identity_3);
        u3.set_state(url_3, token_3);

        SECTION("they should be cleaned up if metadata is enabled") {
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoEncryption);
            auto users = SyncManager::shared().all_logged_in_users();
            REQUIRE(users.size() == 1);
            REQUIRE(validate_user_in_vector(users, identity_3, url_3, token_3));
            REQUIRE_DIR_DOES_NOT_EXIST(user_dir_1);
            REQUIRE_DIR_DOES_NOT_EXIST(user_dir_2);
            REQUIRE_DIR_EXISTS(user_dir_3);
        }
        SECTION("they should be left alone if metadata is disabled") {
            SyncManager::shared().configure_file_system(base_path, SyncManager::MetadataMode::NoMetadata);
            auto users = SyncManager::shared().all_logged_in_users();
            REQUIRE_DIR_EXISTS(user_dir_1);
            REQUIRE_DIR_EXISTS(user_dir_2);
            REQUIRE_DIR_EXISTS(user_dir_3);
        }
    }
}

TEST_CASE("sync_manager: metadata") {
    auto cleanup = util::make_scope_exit([=]() noexcept { SyncManager::shared().reset_for_testing(); });
    reset_test_directory(base_path);

    SECTION("should be reset in case of decryption error") {
        SyncManager::shared().configure_file_system(base_path,
                                                    SyncManager::MetadataMode::Encryption,
                                                    make_test_encryption_key());

        SyncManager::shared().reset_for_testing();

        SyncManager::shared().configure_file_system(base_path,
                                                    SyncManager::MetadataMode::Encryption,
                                                    make_test_encryption_key(1),
                                                    true);
    }
}
