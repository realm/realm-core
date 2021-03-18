////////////////////////////////////////////////////////////////////////////
//
// Copyright 2021 Realm Inc.
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

#include <catch2/catch.hpp>
#include <external/json/json.hpp>

#include <realm/object-store/property.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/sync/app.hpp>
#include <realm/object-store/sync/app_credentials.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>
#include <realm/object-store/sync/sync_session.hpp>

#include <realm/util/base64.hpp>
#include <realm/util/uri.hpp>
#include <realm/util/websocket.hpp>

#include "util/test_utils.hpp"
#include "util/test_file.hpp"
#include "util/event_loop.hpp"

using namespace realm;
using namespace realm::app;
using namespace realm::util;


using PT = PropertyType;

// MARK: AllTypesSyncObject
/// Object representing all available types that can be synced.
/// To add a new type, create a new specialized `Corpus` for it,
/// and add the Corpus to the `corpus_types` tuple.
/// TODO: Test object links.
struct AllTypesSyncObject {
    AllTypesSyncObject(SharedRealm realm);

    AllTypesSyncObject(Obj obj)
    : m_obj(obj)
    {
    }

    static ObjectSchema schema();

    template <typename T>
    T get(PropertyType pt)
    {
        return m_obj.get<T>(std::to_string(static_cast<unsigned int>(pt)));
    }
    template <typename T>
    void set(PropertyType pt, T value) {
        m_obj.set(std::to_string(static_cast<unsigned int>(pt)), value);
    }

    template <typename T, PropertyType PT>
    Lst<T> get_list()
    {
        return m_obj.get_list<T>(std::to_string(static_cast<unsigned int>(PT)));
    }

    Obj m_obj;
};

template <typename T, PropertyType PT>
struct BaseCorpus : std::true_type {
    using value_type = T;
    static constexpr auto property_type = PT;

    static_assert(!is_nullable(PT), "PropertyType cannot be nullable");

    static realm::Property property()
    {
        return realm::Property(std::to_string(static_cast<unsigned int>(PT)), PT);
    }

    T get(AllTypesSyncObject& all_types_sync_object) const
    {
        return all_types_sync_object.get<T>(PT);
    }
    void set(AllTypesSyncObject& all_types_sync_object, const T& value)
    {
        return all_types_sync_object.set(PT, value);
    }
};
template <typename T, PropertyType PT>
struct BaseOptCorpus : std::true_type {
    using value_type = T;
    static constexpr auto property_type = PT;

    static_assert(is_nullable(PT), "PropertyType is not nullable");

    static realm::Property property()
    {
        return realm::Property(std::to_string(static_cast<unsigned int>(PT)), PT);
    }

    Optional<T> get(AllTypesSyncObject& all_types_sync_object) const
    {
        if (all_types_sync_object.m_obj.is_null(property().name)) {
            return none;
        } else {
            return all_types_sync_object.get<T>(PT);
        }
    }
    void set(AllTypesSyncObject& all_types_sync_object, const Optional<T>& value)
    {
        if (value) {
            return all_types_sync_object.set(PT, *value);
        } else {
            all_types_sync_object.m_obj.set_null(property().name);
        }
    }
};
template <typename T, PropertyType PT>
struct BaseLstCorpus: std::true_type {
    using value_type = std::vector<T>;
    static constexpr auto property_type = PT;

    static realm::Property property()
    {
        return realm::Property(std::to_string(static_cast<unsigned int>(PT)), PT);
    }

    std::vector<T> get(AllTypesSyncObject& all_types_sync_object)
    {
        auto key = all_types_sync_object.m_obj.get_table()->get_column_key(property().name);
        return all_types_sync_object.m_obj.get_list_values<T>(key);
    }
    void set(AllTypesSyncObject& all_types_sync_object, const std::vector<T>& new_value)
    {
        for (size_t i = 0; i < new_value.size(); i++) {
            all_types_sync_object.get_list<T, PT>().insert(i, new_value[i]);
        }
    }
};

/// A Corpus is a specialized type use in test.
/// Each contains a `default_value`, which is the value we set the test column to initially.
/// It also contains a `new_value`, which is the value we set it to while testing.
/// The goal is to assert that values can be read, written to, and round tripped
/// to and from the sync server.
template <PropertyType PT>
struct Corpus: std::false_type {
};

// MARK: Int Corpus
template <>
struct Corpus<PT::Int>: BaseCorpus<Int, PT::Int> {
    static constexpr Int default_value = 42;
    static constexpr Int new_value = 84;
};
template <>
struct Corpus<PT::Int | PT::Nullable>: BaseOptCorpus<Int, PT::Int | PT::Nullable> {
    static constexpr Optional<Int> default_value = none;
    static constexpr Optional<Int> new_value = 84;
};
template <>
struct Corpus<PT::Array | PT::Int>: BaseLstCorpus<Int, PT::Array | PT::Int> {
    static inline const auto default_value = std::vector<Int>();
    static inline const auto new_value = std::vector<Int>{1, 2, 3};
};

// MARK: Bool Corpus
template <>
struct Corpus<PT::Bool>: BaseCorpus<Bool, PT::Bool> {
    static constexpr Bool default_value = false;
    static constexpr Bool new_value = true;
};
template <>
struct Corpus<PT::Bool | PT::Nullable>: BaseOptCorpus<Bool, PT::Bool | PT::Nullable> {
    static constexpr Optional<Bool> default_value = none;
    static constexpr Optional<Bool> new_value = true;
};
template <>
struct Corpus<PT::Array | PT::Bool>: BaseLstCorpus<Bool, PT::Array | PT::Bool> {
    static inline const auto default_value = std::vector<Bool>();
    static inline const auto new_value = std::vector<Bool>{true, false, true};
};

// MARK: String Corpus
template <>
struct Corpus<PT::String>: BaseCorpus<StringData, PT::String> {
    static inline const String default_value = String("foo");
    static inline const String new_value = String("bar");
};
template <>
struct Corpus<PT::String | PT::Nullable>: BaseOptCorpus<StringData, PT::String | PT::Nullable> {
    static inline const Optional<String> default_value = none;
    static inline const Optional<String> new_value = String("bar");
};
template <>
struct Corpus<PT::Array | PT::String>: BaseLstCorpus<StringData, PT::Array | PT::String> {
    static inline const auto default_value = std::vector<String>();
    static inline const auto new_value = std::vector<String>{"foo", "bar", "baz"};
};

// MARK: Data Corpus
template <>
struct Corpus<PT::Data>: BaseCorpus<BinaryData, PT::Data> {
    static const inline auto default_value = BinaryData("abc");
    static const inline auto new_value = BinaryData("def");
};
template <>
struct Corpus<PT::Data | PT::Nullable>: BaseOptCorpus<BinaryData, PT::Data | PT::Nullable> {
    static const auto inline default_value = none;
    static const auto inline new_value = Optional<BinaryData>(BinaryData("BBBBB", 5));
};
template <>
struct Corpus<PT::Array | PT::Data>: BaseLstCorpus<BinaryData, PT::Array | PT::Data> {
    static inline const auto default_value = std::vector<BinaryData>();
    static inline const auto new_value = std::vector<BinaryData>{
        BinaryData("AAAAA"), BinaryData("BBBBB"), BinaryData("CCCCC")
    };
};

// MARK: Date Corpus
template <>
struct Corpus<PT::Date>: BaseCorpus<Timestamp, PT::Date> {
    static const inline auto default_value = Timestamp(42, 0);
    static const inline auto new_value = Timestamp(84, 0);
};
template <>
struct Corpus<PT::Date | PT::Nullable>: BaseOptCorpus<Timestamp, PT::Date | PT::Nullable> {
    static const auto inline default_value = none;
    static const auto inline new_value = Timestamp(84, 0);
};
template <>
struct Corpus<PT::Array | PT::Date>: BaseLstCorpus<Timestamp, PT::Array | PT::Date> {
    static inline const auto default_value = std::vector<Timestamp>();
    static inline const auto new_value = std::vector<Timestamp>{
        Timestamp(42, 0), Timestamp(84, 0), Timestamp(168, 0)
    };
};

// MARK: Double Corpus
template <>
struct Corpus<PT::Double>: BaseCorpus<Double, PT::Double> {
    static const inline auto default_value = 42.42;
    static const inline auto new_value = 84.84;
};
template <>
struct Corpus<PT::Double | PT::Nullable>: BaseOptCorpus<Double, PT::Double | PT::Nullable> {
    static const auto inline default_value = none;
    static const auto inline new_value = Optional<Double>(84.84);
};
template <>
struct Corpus<PT::Array | PT::Double>: BaseLstCorpus<Double, PT::Array | PT::Double> {
    static inline const auto default_value = std::vector<Double>();
    static inline const auto new_value = std::vector<Double>{42.42, 84.84, 169.68};
};

// MARK: ObjectId Corpus
template <>
struct Corpus<PT::ObjectId>: BaseCorpus<ObjectId, PT::ObjectId> {
    static const inline auto default_value = ObjectId();
    static const inline auto new_value = ObjectId::gen();
};
template <>
struct Corpus<PT::ObjectId | PT::Nullable>: BaseOptCorpus<ObjectId, PT::ObjectId | PT::Nullable> {
    static const auto inline default_value = none;
    static const auto inline new_value = Optional<ObjectId>(ObjectId::gen());
};
template <>
struct Corpus<PT::Array | PT::ObjectId>: BaseLstCorpus<ObjectId, PT::Array | PT::ObjectId> {
    static inline const auto default_value = std::vector<ObjectId>();
    static inline const auto new_value = std::vector<ObjectId>{
        ObjectId::gen(), ObjectId::gen(), ObjectId::gen()
    };
};

// MARK: Decimal Corpus
template <>
struct Corpus<PT::Decimal>: BaseCorpus<Decimal, PT::Decimal> {
    static const inline auto default_value = Decimal();
    static const inline auto new_value = Decimal("42.42");
};
template <>
struct Corpus<PT::Decimal | PT::Nullable>: BaseOptCorpus<Decimal, PT::Decimal | PT::Nullable> {
    static const auto inline default_value = none;
    static const auto inline new_value = Optional<Decimal>(Decimal("42.42"));
};
template <>
struct Corpus<PT::Array | PT::Decimal>: BaseLstCorpus<Decimal, PT::Array | PT::Decimal> {
    static inline const auto default_value = std::vector<Decimal>();
    static inline const auto new_value = std::vector<Decimal>{
        Decimal("42.42"), Decimal("84.84"), Decimal("169.68"),
    };
};

// MARK: UUID Corpus
template <>
struct Corpus<PT::UUID>: BaseCorpus<UUID, PT::UUID> {
    static const inline auto default_value = UUID();
    static const inline auto new_value = UUID("3b241101-e2bb-4255-8caf-4136c566a962");
};
template <>
struct Corpus<PT::UUID | PT::Nullable>: BaseOptCorpus<UUID, PT::UUID | PT::Nullable> {
    static const auto inline default_value = none;
    static const auto inline new_value = Optional<UUID>(UUID("3b241101-e2bb-4255-8caf-4136c566a962"));
};
template <>
struct Corpus<PT::Array | PT::UUID>: BaseLstCorpus<UUID, PT::Array | PT::UUID> {
    static inline const auto default_value = std::vector<UUID>();
    static inline const auto new_value = std::vector<UUID>{
        UUID("3b241101-e2bb-4255-8caf-4136c566a962"),
        UUID("3b241101-e2bb-4255-8caf-4136c566a963"),
        UUID("3b241101-e2bb-4255-8caf-4136c566a964"),
    };
};

// MARK: Mixed Corpus
template <typename T>
struct BaseMixedCorpus : std::true_type {
    using value_type = T;
    using mixed_type = T;

    static constexpr auto property_type = PT::Mixed | PT::Nullable;

    static realm::Property property()
    {
        return realm::Property(std::to_string(static_cast<unsigned int>(PT::Mixed)), PT::Mixed | PT::Nullable);
    }

    Mixed get(AllTypesSyncObject& all_types_sync_object) const
    {
        return all_types_sync_object.get<Mixed>(PT::Mixed);
    }
    void set(AllTypesSyncObject& all_types_sync_object, const Mixed& value)
    {
        return all_types_sync_object.set(PT::Mixed, value);
    }
};


namespace {
template <typename T>
inline constexpr PropertyType property_type_from_type()
{
    if constexpr(std::is_same_v<T, Int>) { return PT::Int; }
    else if constexpr(std::is_same_v<T, Bool>) { return PT::Bool; }
    else if constexpr(std::is_same_v<T, String>) { return PT::String; }
    else if constexpr(std::is_same_v<T, BinaryData>) { return PT::Data; }
    else if constexpr(std::is_same_v<T, Timestamp>) { return PT::Date; }
    else if constexpr(std::is_same_v<T, Double>) { return PT::Double; }
    else if constexpr(std::is_same_v<T, ObjectId>) { return PT::ObjectId; }
    else if constexpr(std::is_same_v<T, Decimal>) { return PT::Decimal; }
    else if constexpr(std::is_same_v<T, UUID>) { return PT::UUID; }
    else { return PT::Flags; }
}
inline constexpr PropertyType operator<<(PropertyType a, PropertyType b)
{
    // give us a negative unique value simply for the mixed case and having
    // specialized corpus types
    return static_cast<PropertyType>(-1 * (to_underlying(a) << to_underlying(b)));
}
}

template <>
struct Corpus<PT::Mixed << PT::Int>: BaseMixedCorpus<Int> {
    static const auto inline default_value = none;
    static const inline auto new_value = 84;
};
template <>
struct Corpus<PT::Mixed << PT::Bool>: BaseMixedCorpus<Bool> {
    static const auto inline default_value = none;
    static const inline auto new_value = Mixed(true);
};
template <>
struct Corpus<PT::Mixed << PT::String>: BaseMixedCorpus<String> {
    static const auto inline default_value = none;
    static const inline auto new_value = Mixed("bar");
};
template <>
struct Corpus<PT::Mixed << PT::Data>: BaseMixedCorpus<BinaryData> {
    static const auto inline default_value = none;
    static const inline auto new_value = Mixed(BinaryData("def"));
};
template <>
struct Corpus<PT::Mixed << PT::Date>: BaseMixedCorpus<Timestamp> {
    static const auto inline default_value = none;
    static const inline auto new_value = Mixed(Timestamp(84, 0));
};
template <>
struct Corpus<PT::Mixed << PT::Double>: BaseMixedCorpus<Double> {
    static const auto inline default_value = none;
    static const inline auto new_value = Mixed(42.42);
};
template <>
struct Corpus<PT::Mixed << PT::ObjectId>: BaseMixedCorpus<ObjectId> {
    static const auto inline default_value = none;
    static const inline auto new_value = Mixed(ObjectId::gen());
};
template <>
struct Corpus<PT::Mixed << PT::Decimal>: BaseMixedCorpus<Decimal> {
    static const auto inline default_value = none;
    static const inline auto new_value = Mixed(Decimal("42.42"));
};
template <>
struct Corpus<PT::Mixed << PT::UUID>: BaseMixedCorpus<UUID> {
    static const auto inline default_value = none;
    static const inline auto new_value = Mixed(UUID("3b241101-e2bb-4255-8caf-4136c566a962"));
};
template <>
struct Corpus<PT::Mixed | PT::Nullable>: BaseMixedCorpus<Int> {
    static const auto inline default_value = none;
    static const auto inline new_value = Mixed(42);
};
template <>
struct Corpus<PT::Array | PT::Mixed | PT::Nullable>: BaseLstCorpus<Mixed, PT::Array | PT::Mixed | PT::Nullable> {
    static inline const auto default_value = std::vector<Mixed>();
    static inline const auto new_value = std::vector<Mixed>{
        42,
        true,
        "foo",
        BinaryData("abc"),
        Timestamp(42, 0),
        42.42,
        ObjectId::gen(),
        Decimal("42.42"),
        UUID("3b241101-e2bb-4255-8caf-4136c566a962")
    };
};

// MARK: Full Corpus
/// Returns a tuple of all type combinations we are testing.
static constexpr auto corpus_types() {
    return std::tuple<
        Corpus<PT::Int>, Corpus<PT::Int | PT::Nullable>, Corpus<PT::Array | PT::Int>,
        Corpus<PT::Bool>, Corpus<PT::Bool | PT::Nullable>, Corpus<PT::Array | PT::Bool>,
        Corpus<PT::String>, Corpus<PT::String | PT::Nullable>, Corpus<PT::Array | PT::String>,
        Corpus<PT::Data>, Corpus<PT::Data | PT::Nullable>, Corpus<PT::Array | PT::Data>,
        Corpus<PT::Date>, Corpus<PT::Date | PT::Nullable>, Corpus<PT::Array | PT::Date>,
        Corpus<PT::Double>, Corpus<PT::Double | PT::Nullable>, Corpus<PT::Array | PT::Double>,
        Corpus<PT::ObjectId>, Corpus<PT::ObjectId | PT::Nullable>, Corpus<PT::Array | PT::ObjectId>,
        Corpus<PT::Decimal>, Corpus<PT::Decimal | PT::Nullable>, Corpus<PT::Array | PT::Decimal>,
        Corpus<PT::UUID>, Corpus<PT::UUID | PT::Nullable>, Corpus<PT::Array | PT::UUID>,
        Corpus<PT::Mixed | PT::Nullable> /*, TODO: Not supported yet: Corpus<PT::Array | PT::Mixed | PT::Nullable> */
    >{};
}

// MARK: - Schema
ObjectSchema AllTypesSyncObject::schema() {
    auto object_schema = ObjectSchema();
    object_schema.name = "AllTypesSyncObject";
    // push back PK
    object_schema.persisted_properties.push_back(realm::Property("_id", PropertyType::ObjectId, true));
    object_schema.primary_key = "_id";
    std::apply([&object_schema](auto&&... corpus) {
        (object_schema.persisted_properties.push_back(corpus.property()), ...);
    }, corpus_types());
    return object_schema;
}

// MARK: - AllTypesSyncObject ctor
AllTypesSyncObject::AllTypesSyncObject(SharedRealm realm)
{
    auto did_begin_transaction = false;
    if (!realm->is_in_transaction()) {
        realm->begin_transaction();
        did_begin_transaction = true;
    }
    auto& group = realm->read_group();
    auto table = group.get_table("class_AllTypesSyncObject");

    m_obj = table->create_object_with_primary_key(ObjectId::gen());

    std::apply([this](auto&&... corpus) {
        (corpus.set(*this, corpus.default_value), ...);
    }, corpus_types());

    if (did_begin_transaction) {
        realm->commit_transaction();
    }
}

// MARK: Test Harness
/// A contained context for sync tests. Instantiating a new harness
/// will remove all state from the previous harness, giving a clean
/// slate to test sync on. The purpose of this is to emulate and control
/// the flow of syncing, forcing downloads and uploads of data.
struct Harness {
    Harness(std::function<void(Realm::Config&)> set_up = [](auto&){})
    : factory([] { return std::unique_ptr<GenericNetworkTransport>(new IntTestTransport); })
    , app_config(App::Config{get_runtime_app_id(get_config_path()),
        factory,
        get_base_url(),
        util::none,
        Optional<std::string>("A Local App Version"),
        util::none,
        "Object Store Platform Tests",
        "Object Store Platform Version Blah",
        "An sdk version"})
    , base_path(util::make_temp_dir() + app_config.app_id)
    , opt_set_up(set_up)
    , sync_manager(TestSyncManager::Config(app_config), {})
    {
        REQUIRE(!get_base_url().empty());
        REQUIRE(!get_config_path().empty());
        util::try_remove_dir_recursive(base_path);
        util::try_make_dir(base_path);
    }

    const GenericNetworkTransport::NetworkTransportFactory factory;
    App::Config app_config;
    const std::string base_path;
    std::function<void(Realm::Config&)> opt_set_up;
    TestSyncManager sync_manager;

    std::shared_ptr<App> get_app_and_login(SharedApp app)  {
        auto email = util::format("realm_tests_do_autoverify%1@%2.com", random_string(10), random_string(10));
        auto password = random_string(10);
        app->provider_client<App::UsernamePasswordProviderClient>().register_email(
                                                                                   email, password, [&](Optional<app::AppError> error) {
            CHECK(!error);
        });
        app->log_in_with_credentials(realm::app::AppCredentials::username_password(email, password),
                                     [&](std::shared_ptr<realm::SyncUser> user, Optional<app::AppError> error) {
            REQUIRE(user);
            CHECK(!error);
        });
        return app;
    }

    realm::Realm::Config setup_and_get_config(std::shared_ptr<App> app) {
        realm::Realm::Config config;
        config.sync_config = std::make_shared<realm::SyncConfig>(app->current_user(), bson::Bson("foo"));
        config.sync_config->client_resync_mode = ClientResyncMode::Manual;
        config.sync_config->error_handler = [](std::shared_ptr<SyncSession>, SyncError error) {
            std::cout << error.message << std::endl;
        };
        config.schema_version = 1;
        config.path = base_path + "/default.realm";
        const auto dog_schema = realm::ObjectSchema(
                                                    "Dog", {realm::Property("_id", PropertyType::ObjectId | PropertyType::Nullable, true),
            realm::Property("breed", PropertyType::String | PropertyType::Nullable),
            realm::Property("name", PropertyType::String),
            realm::Property("realm_id", PropertyType::String | PropertyType::Nullable)});
        const auto person_schema = realm::ObjectSchema(
                                                       "Person",
                                                       {realm::Property("_id", PropertyType::ObjectId | PropertyType::Nullable, true),
            realm::Property("age", PropertyType::Int),
            realm::Property("dogs", PropertyType::Object | PropertyType::Array, "Dog"),
            realm::Property("firstName", PropertyType::String), realm::Property("lastName", PropertyType::String),
            realm::Property("realm_id", PropertyType::String | PropertyType::Nullable)});
        config.schema = realm::Schema({AllTypesSyncObject::schema(), dog_schema, person_schema});
        return config;
    }

    void run(std::function<void(SharedRealm)> block) {
        auto realm = set_up();
        block(realm);
        tear_down(realm);
    }
private:
    SharedRealm set_up() {
        auto app = get_app_and_login(sync_manager.app());
        auto config = setup_and_get_config(app);
        opt_set_up(config);

        auto realm = realm::Realm::get_shared_realm(config);
        return realm;
    }

    void tear_down(SharedRealm) {
        util::try_remove_dir_recursive(base_path);
        util::try_make_dir(base_path);
    }
};


template <PropertyType PT>
void test_round_trip(Corpus<PT> corpus) {
    static_assert(Corpus<PT>::value, "PropertyType must be added to corpus before testing");
    // Add a new `AllTypesSyncObject` to the Realm
    Harness().run([](auto realm) {
        wait_for_download(*realm);
        auto results = realm::Results(realm, realm->read_group().get_table("class_AllTypesSyncObject"));
        realm->begin_transaction();
        results.clear();
        realm->commit_transaction();
        wait_for_upload(*realm);

        REQUIRE(results.size() == 0);
        realm->begin_transaction();
        const auto obj = AllTypesSyncObject(realm);
        realm->commit_transaction();
        REQUIRE(results.size() == 1);
        wait_for_upload(*realm);
    });
    Harness().run([&](auto realm) {
        wait_for_download(*realm);
        // assert realm has at least one `AllTypesSyncObject`
        auto results = realm::Results(realm, realm->read_group().get_table("class_AllTypesSyncObject"));
        REQUIRE(results.size() == 1);

        realm->begin_transaction();
        auto obj = AllTypesSyncObject(results.get(0));
        // assert the stored value is equal to the expected default value
        REQUIRE(corpus.get(obj) == corpus.default_value);
        corpus.set(obj, corpus.new_value);

        // commit the changes
        realm->commit_transaction();
        wait_for_upload(*realm);
    });
    Harness().run([&corpus](auto realm) {
        wait_for_download(*realm);
        auto results = realm::Results(realm, realm->read_group().get_table("class_AllTypesSyncObject"));
        REQUIRE(results.size() == 1);
        auto obj = AllTypesSyncObject(results.get(0));
        REQUIRE(corpus.get(obj) == corpus.new_value);
    });
}

TEST_CASE("canonical_sync_corpus", "[sync][app]") {
    // MARK: Int Round Trip
    std::apply([](auto... corpus) {
        auto test = [](auto corpus) {
            auto PT = std::decay_t<decltype(corpus)>::property_type;
            DYNAMIC_SECTION(util::format("%1 %2 %3", {
                string_for_property_type(PT & ~PropertyType::Flags),
                is_array(PT) ? "(array)" : is_nullable(PT) ? "(nullable)" : "-",
                "round trip"
            })) {
                test_round_trip(corpus);
            };
        };
        (test(corpus), ...);
    }, corpus_types());

    auto mixed_corpus_types = std::tuple<
        Corpus<PT::Mixed << PT::Int>,
        Corpus<PT::Mixed << PT::Bool>,
        Corpus<PT::Mixed << PT::String>,
        Corpus<PT::Mixed << PT::Data>,
        Corpus<PT::Mixed << PT::Date>,
        Corpus<PT::Mixed << PT::Double>,
        Corpus<PT::Mixed << PT::ObjectId>,
        Corpus<PT::Mixed << PT::Decimal>,
        Corpus<PT::Mixed << PT::UUID>
    >{};
    std::apply([](auto... corpus) {
        auto test = [](auto corpus) {
            DYNAMIC_SECTION(util::format("mixed of type %1 %2", {
                string_for_property_type(property_type_from_type<typename decltype(corpus)::mixed_type>()),
                "round trip"
            })) {
                test_round_trip(corpus);
            };
        };
        (test(corpus), ...);
    }, mixed_corpus_types);
}

TEST_CASE("sync_unhappy_paths", "[sync][app]") {
    // MARK: Expired Session Refresh -
    SECTION("Expired Session Refresh") {
        Harness().run([](auto realm) {
            wait_for_download(*realm);
            auto results = realm::Results(realm, realm->read_group().get_table("class_AllTypesSyncObject"));
            realm->begin_transaction();
            results.clear();
            realm->commit_transaction();
            wait_for_upload(*realm);

            realm->begin_transaction();
            auto _ = AllTypesSyncObject(realm);
            realm->commit_transaction();
            wait_for_upload(*realm);
        });
        auto harness = Harness();
        auto app = harness.get_app_and_login(harness.sync_manager.app());
        // set a bad access token. this will trigger a refresh when the sync session opens
        app->current_user()->update_access_token(ENCODE_FAKE_JWT("fake_access_token"));

        auto config = harness.setup_and_get_config(app);
        auto r = realm::Realm::get_shared_realm(config);
        wait_for_download(*r);
        auto session = app->current_user()->session_for_on_disk_path(r->config().path);
        Results results = Results(r, r->read_group().get_table("class_AllTypesSyncObject"));
        REQUIRE(results.size() == 1);
        REQUIRE(AllTypesSyncObject(results.get(0)).get<Int>(PT::Int) == Corpus<PT::Int>::default_value);
    }

    SECTION("invalid partition error handling") {
        std::atomic<bool> error_did_occur = false;
        auto harness = Harness([&error_did_occur](auto& config) {
            config.sync_config->partition_value = "not a bson serialized string";
            config.sync_config->error_handler = [&error_did_occur](std::shared_ptr<SyncSession>, SyncError error) {
                CHECK(error.message ==
                      "Illegal Realm path (BIND): serialized partition 'not a bson serialized string' is invalid");
                error_did_occur.store(true);
            };
        });
        harness.run([&error_did_occur, &harness](SharedRealm realm) {
            auto session = harness.sync_manager.app()->current_user()->session_for_on_disk_path(realm->config().path); // needed to keep session alive
            util::EventLoop::main().run_until([&] {
                return error_did_occur.load();
            });
            REQUIRE(error_did_occur.load());
        });
    }

    SECTION("invalid pk schema error handling") {
        const std::string invalid_pk_name = "my_primary_key";
        Harness([&invalid_pk_name](auto& config) {
            auto it = config.schema->find("Dog");
            REQUIRE(it != config.schema->end());
            REQUIRE(it->primary_key_property());
            REQUIRE(it->primary_key_property()->name == "_id");
            it->primary_key_property()->name = invalid_pk_name;
            it->primary_key = invalid_pk_name;
            realm::Realm::get_shared_realm(config),
            util::format(
                "The primary key property on a synchronized Realm must be named '%1' but found '%2' for type 'Dog'",
                "_id", invalid_pk_name);
        });
    }

    SECTION("missing pk schema error handling") {
        Harness([](auto& config) {
            auto it = config.schema->find("Dog");
            REQUIRE(it != config.schema->end());
            REQUIRE(it->primary_key_property());
            it->primary_key_property()->is_primary = false;
            it->primary_key = "";
            REQUIRE(!it->primary_key_property());
            REQUIRE_THROWS_CONTAINING(realm::Realm::get_shared_realm(config),
                                      util::format("There must be a primary key property named '%1' on a synchronized "
                                                   "Realm but none was found for type 'Dog'",
                                                   "_id"));
        });
    }

    SECTION("too large sync message error handling") {
        std::vector<SyncError> sync_errors;
        std::mutex sync_error_mutex;
        Harness([&sync_errors, &sync_error_mutex](auto& config) {
            config.sync_config->error_handler = [&](auto, SyncError error) {
                std::lock_guard<std::mutex> lk(sync_error_mutex);
                sync_errors.push_back(std::move(error));
            };
        }).run([&sync_errors, &sync_error_mutex](SharedRealm realm) {
            realm->begin_transaction();
            for (auto i = 'a'; i < 'z'; i++) {
                auto obj = AllTypesSyncObject(realm);
                obj.set(PT::String, random_string(1024 * 1024));
            }
            realm->commit_transaction();

            const auto wait_start = std::chrono::steady_clock::now();
            auto pred = [](const SyncError& error) {
                return error.error_code.category() == util::websocket::websocket_close_status_category();
            };
            util::EventLoop::main().run_until([&]() -> bool {
                std::lock_guard<std::mutex> lk(sync_error_mutex);
                // If we haven't gotten an error in more than 2 minutes, then something has gone wrong
                // and we should fail the test.
                if (std::chrono::steady_clock::now() - wait_start > std::chrono::minutes(2)) {
                    return false;
                }
                return std::any_of(sync_errors.begin(), sync_errors.end(), pred);
            });

            auto captured_error = [&] {
                std::lock_guard<std::mutex> lk(sync_error_mutex);
                const auto it = std::find_if(sync_errors.begin(), sync_errors.end(), pred);
                REQUIRE(it != sync_errors.end());
                return *it;
            }();

            REQUIRE(captured_error.error_code.category() == util::websocket::websocket_close_status_category());
            REQUIRE(captured_error.error_code.value() == 1009);
            REQUIRE(captured_error.message == "read limited at 16777217 bytes");
        });
    }
}
