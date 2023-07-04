////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
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

#if REALM_ENABLE_SYNC
#if REALM_ENABLE_AUTH_TESTS

#include <util/collection_fixtures.hpp>
#include <util/baas_admin_api.hpp>

#include <realm/object-store/sync/mongo_client.hpp>
#include <realm/object-store/sync/mongo_database.hpp>
#include <realm/object-store/sync/mongo_collection.hpp>
#include <realm/object-store/util/bson/bson.hpp>

using namespace realm;
using namespace realm::app;
using util::Optional;


// MARK: - Remote Mongo Client Tests

TEST_CASE("app: remote mongo client", "[sync][app][baas][mongo][new]") {
    TestAppSession session;
    auto app = session.app();

    auto remote_client = app->current_user()->mongo_client("BackingDB");
    auto db = remote_client.db(get_runtime_app_session("").config.mongo_dbname);
    auto dog_collection = db["Dog"];
    auto cat_collection = db["Cat"];
    auto person_collection = db["Person"];

    bson::BsonDocument dog_document{{"name", "fido"}, {"breed", "king charles"}};

    bson::BsonDocument dog_document2{{"name", "bob"}, {"breed", "french bulldog"}};

    auto dog3_object_id = ObjectId::gen();
    bson::BsonDocument dog_document3{
        {"_id", dog3_object_id},
        {"name", "petunia"},
        {"breed", "french bulldog"},
    };

    auto cat_id_string = random_string(10);
    bson::BsonDocument cat_document{
        {"_id", cat_id_string},
        {"name", "luna"},
        {"breed", "scottish fold"},
    };

    bson::BsonDocument person_document{
        {"firstName", "John"},
        {"lastName", "Johnson"},
        {"age", 30},
    };

    bson::BsonDocument person_document2{
        {"firstName", "Bob"},
        {"lastName", "Johnson"},
        {"age", 30},
    };

    bson::BsonDocument bad_document{{"bad", "value"}};

    dog_collection.delete_many(dog_document, [&](uint64_t, Optional<AppError> error) {
        REQUIRE_FALSE(error);
    });

    dog_collection.delete_many(dog_document2, [&](uint64_t, Optional<AppError> error) {
        REQUIRE_FALSE(error);
    });

    dog_collection.delete_many({}, [&](uint64_t, Optional<AppError> error) {
        REQUIRE_FALSE(error);
    });

    dog_collection.delete_many(person_document, [&](uint64_t, Optional<AppError> error) {
        REQUIRE_FALSE(error);
    });

    dog_collection.delete_many(person_document2, [&](uint64_t, Optional<AppError> error) {
        REQUIRE_FALSE(error);
    });

    SECTION("insert") {
        bool processed = false;
        ObjectId dog_object_id;
        ObjectId dog2_object_id;

        dog_collection.insert_one_bson(bad_document, [&](Optional<bson::Bson> bson, Optional<AppError> error) {
            REQUIRE(error);
            REQUIRE(!bson);
        });

        dog_collection.insert_one_bson(dog_document3, [&](Optional<bson::Bson> value, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            auto bson = static_cast<bson::BsonDocument>(*value);
            REQUIRE(static_cast<ObjectId>(bson["insertedId"]) == dog3_object_id);
        });

        cat_collection.insert_one_bson(cat_document, [&](Optional<bson::Bson> value, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            auto bson = static_cast<bson::BsonDocument>(*value);
            REQUIRE(static_cast<std::string>(bson["insertedId"]) == cat_id_string);
        });

        dog_collection.delete_many({}, [&](uint64_t, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        cat_collection.delete_one(cat_document, [&](uint64_t, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        dog_collection.insert_one(bad_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE(error);
            REQUIRE(!object_id);
        });

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*object_id).to_string() != "");
            dog_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.insert_one(dog_document2, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*object_id).to_string() != "");
            dog2_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.insert_one(dog_document3, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(object_id->type() == bson::Bson::Type::ObjectId);
            REQUIRE(static_cast<ObjectId>(*object_id) == dog3_object_id);
        });

        cat_collection.insert_one(cat_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(object_id->type() == bson::Bson::Type::String);
            REQUIRE(static_cast<std::string>(*object_id) == cat_id_string);
        });

        person_document["dogs"] = bson::BsonArray({dog_object_id, dog2_object_id, dog3_object_id});
        person_collection.insert_one(person_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*object_id).to_string() != "");
        });

        dog_collection.delete_many({}, [&](uint64_t, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        cat_collection.delete_one(cat_document, [&](uint64_t, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        bson::BsonArray documents{
            dog_document,
            dog_document2,
            dog_document3,
        };

        dog_collection.insert_many_bson(documents, [&](Optional<bson::Bson> value, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            auto bson = static_cast<bson::BsonDocument>(*value);
            auto insertedIds = static_cast<bson::BsonArray>(bson["insertedIds"]);
        });

        dog_collection.delete_many({}, [&](uint64_t, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        dog_collection.insert_many(documents, [&](std::vector<bson::Bson> inserted_docs, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(inserted_docs.size() == 3);
            REQUIRE(inserted_docs[0].type() == bson::Bson::Type::ObjectId);
            REQUIRE(inserted_docs[1].type() == bson::Bson::Type::ObjectId);
            REQUIRE(inserted_docs[2].type() == bson::Bson::Type::ObjectId);
            REQUIRE(static_cast<ObjectId>(inserted_docs[2]) == dog3_object_id);
            processed = true;
        });

        REQUIRE(processed);
    }

    SECTION("find") {
        bool processed = false;

        dog_collection.find(dog_document, [&](Optional<bson::BsonArray> document_array, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*document_array).size() == 0);
        });

        dog_collection.find_bson(dog_document, {}, [&](Optional<bson::Bson> bson, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(static_cast<bson::BsonArray>(*bson).size() == 0);
        });

        dog_collection.find_one(dog_document, [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(!document);
        });

        dog_collection.find_one_bson(dog_document, {}, [&](Optional<bson::Bson> bson, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((!bson || bson::holds_alternative<util::None>(*bson)));
        });

        ObjectId dog_object_id;
        ObjectId dog2_object_id;

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*object_id).to_string() != "");
            dog_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.insert_one(dog_document2, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*object_id).to_string() != "");
            dog2_object_id = static_cast<ObjectId>(*object_id);
        });

        person_document["dogs"] = bson::BsonArray({dog_object_id, dog2_object_id});
        person_collection.insert_one(person_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*object_id).to_string() != "");
        });

        dog_collection.find(dog_document, [&](Optional<bson::BsonArray> documents, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*documents).size() == 1);
        });

        dog_collection.find_bson(dog_document, {}, [&](Optional<bson::Bson> bson, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(static_cast<bson::BsonArray>(*bson).size() == 1);
        });

        person_collection.find(person_document, [&](Optional<bson::BsonArray> documents, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*documents).size() == 1);
        });

        MongoCollection::FindOptions options{
            2,                                                         // document limit
            Optional<bson::BsonDocument>({{"name", 1}, {"breed", 1}}), // project
            Optional<bson::BsonDocument>({{"breed", 1}})               // sort
        };

        dog_collection.find(dog_document, options,
                            [&](Optional<bson::BsonArray> document_array, Optional<AppError> error) {
                                REQUIRE_FALSE(error);
                                REQUIRE((*document_array).size() == 1);
                            });

        dog_collection.find({{"name", "fido"}}, options,
                            [&](Optional<bson::BsonArray> document_array, Optional<AppError> error) {
                                REQUIRE_FALSE(error);
                                REQUIRE((*document_array).size() == 1);
                                auto king_charles = static_cast<bson::BsonDocument>((*document_array)[0]);
                                REQUIRE(king_charles["breed"] == "king charles");
                            });

        dog_collection.find_one(dog_document, [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            auto name = (*document)["name"];
            REQUIRE(name == "fido");
        });

        dog_collection.find_one(dog_document, options,
                                [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                    REQUIRE_FALSE(error);
                                    auto name = (*document)["name"];
                                    REQUIRE(name == "fido");
                                });

        dog_collection.find_one_bson(dog_document, options, [&](Optional<bson::Bson> bson, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            auto name = (static_cast<bson::BsonDocument>(*bson))["name"];
            REQUIRE(name == "fido");
        });

        dog_collection.find(dog_document, [&](Optional<bson::BsonArray> documents, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*documents).size() == 1);
        });

        dog_collection.find_one_and_delete(dog_document,
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE_FALSE(error);
                                               REQUIRE(document);
                                           });

        dog_collection.find_one_and_delete({{}},
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE_FALSE(error);
                                               REQUIRE(document);
                                           });

        dog_collection.find_one_and_delete({{"invalid", "key"}},
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE_FALSE(error);
                                               REQUIRE(!document);
                                           });

        dog_collection.find_one_and_delete_bson({{"invalid", "key"}}, {},
                                                [&](Optional<bson::Bson> bson, Optional<AppError> error) {
                                                    REQUIRE_FALSE(error);
                                                    REQUIRE((!bson || bson::holds_alternative<util::None>(*bson)));
                                                });

        dog_collection.find(dog_document, [&](Optional<bson::BsonArray> documents, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*documents).size() == 0);
            processed = true;
        });

        REQUIRE(processed);
    }

    SECTION("count and aggregate") {
        bool processed = false;

        ObjectId dog_object_id;
        ObjectId dog2_object_id;

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*object_id).to_string() != "");
        });

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*object_id).to_string() != "");
            dog_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.insert_one(dog_document2, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*object_id).to_string() != "");
            dog2_object_id = static_cast<ObjectId>(*object_id);
        });

        person_document["dogs"] = bson::BsonArray({dog_object_id, dog2_object_id});
        person_collection.insert_one(person_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*object_id).to_string() != "");
        });

        bson::BsonDocument match{{"$match", bson::BsonDocument({{"name", "fido"}})}};

        bson::BsonDocument group{{"$group", bson::BsonDocument({{"_id", "$name"}})}};

        bson::BsonArray pipeline{match, group};

        dog_collection.aggregate(pipeline, [&](Optional<bson::BsonArray> documents, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*documents).size() == 1);
        });

        dog_collection.aggregate_bson(pipeline, [&](Optional<bson::Bson> bson, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(static_cast<bson::BsonArray>(*bson).size() == 1);
        });

        dog_collection.count({{"breed", "king charles"}}, [&](uint64_t count, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(count == 2);
        });

        dog_collection.count_bson({{"breed", "king charles"}}, 0,
                                  [&](Optional<bson::Bson> bson, Optional<AppError> error) {
                                      REQUIRE_FALSE(error);
                                      REQUIRE(static_cast<int64_t>(*bson) == 2);
                                  });

        dog_collection.count({{"breed", "french bulldog"}}, [&](uint64_t count, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(count == 1);
        });

        dog_collection.count({{"breed", "king charles"}}, 1, [&](uint64_t count, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(count == 1);
        });

        person_collection.count(
            {{"firstName", "John"}, {"lastName", "Johnson"}, {"age", bson::BsonDocument({{"$gt", 25}})}}, 1,
            [&](uint64_t count, Optional<AppError> error) {
                REQUIRE_FALSE(error);
                REQUIRE(count == 1);
                processed = true;
            });

        REQUIRE(processed);
    }

    SECTION("find and update") {
        bool processed = false;

        MongoCollection::FindOneAndModifyOptions find_and_modify_options{
            Optional<bson::BsonDocument>({{"name", 1}, {"breed", 1}}), // project
            Optional<bson::BsonDocument>({{"name", 1}}),               // sort,
            true,                                                      // upsert
            true                                                       // return new doc
        };

        dog_collection.find_one_and_update(dog_document, dog_document2,
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE_FALSE(error);
                                               REQUIRE(!document);
                                           });

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*object_id).to_string() != "");
        });

        dog_collection.find_one_and_update(dog_document, dog_document2, find_and_modify_options,
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE_FALSE(error);
                                               auto breed = static_cast<std::string>((*document)["breed"]);
                                               REQUIRE(breed == "french bulldog");
                                           });

        dog_collection.find_one_and_update(dog_document2, dog_document, find_and_modify_options,
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE_FALSE(error);
                                               auto breed = static_cast<std::string>((*document)["breed"]);
                                               REQUIRE(breed == "king charles");
                                           });

        dog_collection.find_one_and_update_bson(dog_document, dog_document2, find_and_modify_options,
                                                [&](Optional<bson::Bson> bson, Optional<AppError> error) {
                                                    REQUIRE_FALSE(error);
                                                    auto breed = static_cast<std::string>(
                                                        static_cast<bson::BsonDocument>(*bson)["breed"]);
                                                    REQUIRE(breed == "french bulldog");
                                                });

        dog_collection.find_one_and_update_bson(dog_document2, dog_document, find_and_modify_options,
                                                [&](Optional<bson::Bson> bson, Optional<AppError> error) {
                                                    REQUIRE_FALSE(error);
                                                    auto breed = static_cast<std::string>(
                                                        static_cast<bson::BsonDocument>(*bson)["breed"]);
                                                    REQUIRE(breed == "king charles");
                                                });

        dog_collection.find_one_and_update({{"name", "invalid name"}}, {{"name", "some name"}},
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE_FALSE(error);
                                               REQUIRE(!document);
                                               processed = true;
                                           });
        REQUIRE(processed);
        processed = false;

        dog_collection.find_one_and_update({{"name", "invalid name"}}, {{}}, find_and_modify_options,
                                           [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                               REQUIRE(error);
                                               REQUIRE(error->reason() == "insert not permitted");
                                               REQUIRE(!document);
                                               processed = true;
                                           });
        REQUIRE(processed);
    }

    SECTION("update") {
        bool processed = false;
        ObjectId dog_object_id;

        dog_collection.update_one(dog_document, dog_document2, true,
                                  [&](MongoCollection::UpdateResult result, Optional<AppError> error) {
                                      REQUIRE_FALSE(error);
                                      REQUIRE((*result.upserted_id).to_string() != "");
                                  });

        dog_collection.update_one(dog_document2, dog_document,
                                  [&](MongoCollection::UpdateResult result, Optional<AppError> error) {
                                      REQUIRE_FALSE(error);
                                      REQUIRE(!result.upserted_id);
                                  });

        cat_collection.update_one({}, cat_document, true,
                                  [&](MongoCollection::UpdateResult result, Optional<AppError> error) {
                                      REQUIRE_FALSE(error);
                                      REQUIRE(result.upserted_id->type() == bson::Bson::Type::String);
                                      REQUIRE(result.upserted_id == cat_id_string);
                                  });

        dog_collection.delete_many({}, [&](uint64_t, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        cat_collection.delete_many({}, [&](uint64_t, Optional<AppError> error) {
            REQUIRE_FALSE(error);
        });

        dog_collection.update_one_bson(dog_document, dog_document2, true,
                                       [&](Optional<bson::Bson> bson, Optional<AppError> error) {
                                           REQUIRE_FALSE(error);
                                           auto upserted_id = static_cast<bson::BsonDocument>(*bson)["upsertedId"];

                                           REQUIRE(upserted_id.type() == bson::Bson::Type::ObjectId);
                                       });

        dog_collection.update_one_bson(dog_document2, dog_document, true,
                                       [&](Optional<bson::Bson> bson, Optional<AppError> error) {
                                           REQUIRE_FALSE(error);
                                           auto document = static_cast<bson::BsonDocument>(*bson);
                                           auto foundUpsertedId = document.find("upsertedId") != document.end();
                                           REQUIRE(!foundUpsertedId);
                                       });

        cat_collection.update_one_bson({}, cat_document, true,
                                       [&](Optional<bson::Bson> bson, Optional<AppError> error) {
                                           REQUIRE_FALSE(error);
                                           auto upserted_id = static_cast<bson::BsonDocument>(*bson)["upsertedId"];
                                           REQUIRE(upserted_id.type() == bson::Bson::Type::String);
                                           REQUIRE(upserted_id == cat_id_string);
                                       });

        person_document["dogs"] = bson::BsonArray();
        bson::BsonDocument person_document_copy = bson::BsonDocument(person_document);
        person_document_copy["dogs"] = bson::BsonArray({dog_object_id});
        person_collection.update_one(person_document, person_document, true,
                                     [&](MongoCollection::UpdateResult, Optional<AppError> error) {
                                         REQUIRE_FALSE(error);
                                         processed = true;
                                     });

        REQUIRE(processed);
    }

    SECTION("update many") {
        bool processed = false;

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*object_id).to_string() != "");
        });

        dog_collection.update_many(dog_document2, dog_document, true,
                                   [&](MongoCollection::UpdateResult result, Optional<AppError> error) {
                                       REQUIRE_FALSE(error);
                                       REQUIRE((*result.upserted_id).to_string() != "");
                                   });

        dog_collection.update_many(dog_document2, dog_document,
                                   [&](MongoCollection::UpdateResult result, Optional<AppError> error) {
                                       REQUIRE_FALSE(error);
                                       REQUIRE(!result.upserted_id);
                                       processed = true;
                                   });

        REQUIRE(processed);
    }

    SECTION("find and replace") {
        bool processed = false;
        ObjectId dog_object_id;
        ObjectId person_object_id;

        MongoCollection::FindOneAndModifyOptions find_and_modify_options{
            Optional<bson::BsonDocument>({{"name", "fido"}}), // project
            Optional<bson::BsonDocument>({{"name", 1}}),      // sort,
            true,                                             // upsert
            true                                              // return new doc
        };

        dog_collection.find_one_and_replace(dog_document, dog_document2,
                                            [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                                REQUIRE_FALSE(error);
                                                REQUIRE(!document);
                                            });

        dog_collection.insert_one(dog_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*object_id).to_string() != "");
            dog_object_id = static_cast<ObjectId>(*object_id);
        });

        dog_collection.find_one_and_replace(dog_document, dog_document2,
                                            [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                                REQUIRE_FALSE(error);
                                                auto name = static_cast<std::string>((*document)["name"]);
                                                REQUIRE(name == "fido");
                                            });

        dog_collection.find_one_and_replace(dog_document2, dog_document, find_and_modify_options,
                                            [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                                REQUIRE_FALSE(error);
                                                auto name = static_cast<std::string>((*document)["name"]);
                                                REQUIRE(static_cast<std::string>(name) == "fido");
                                            });

        person_document["dogs"] = bson::BsonArray({dog_object_id});
        person_document2["dogs"] = bson::BsonArray({dog_object_id});
        person_collection.insert_one(person_document, [&](Optional<bson::Bson> object_id, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE((*object_id).to_string() != "");
            person_object_id = static_cast<ObjectId>(*object_id);
        });

        MongoCollection::FindOneAndModifyOptions person_find_and_modify_options{
            Optional<bson::BsonDocument>({{"firstName", 1}}), // project
            Optional<bson::BsonDocument>({{"firstName", 1}}), // sort,
            false,                                            // upsert
            true                                              // return new doc
        };

        person_collection.find_one_and_replace(person_document, person_document2,
                                               [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                                   REQUIRE_FALSE(error);
                                                   auto name = static_cast<std::string>((*document)["firstName"]);
                                                   // Should return the old document
                                                   REQUIRE(name == "John");
                                                   processed = true;
                                               });

        person_collection.find_one_and_replace(person_document2, person_document, person_find_and_modify_options,
                                               [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                                   REQUIRE_FALSE(error);
                                                   auto name = static_cast<std::string>((*document)["firstName"]);
                                                   // Should return new document, Bob -> John
                                                   REQUIRE(name == "John");
                                               });

        person_collection.find_one_and_replace({{"invalid", "item"}}, {{}},
                                               [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                                   // If a document is not found then null will be returned for the
                                                   // document and no error will be returned
                                                   REQUIRE_FALSE(error);
                                                   REQUIRE(!document);
                                               });

        person_collection.find_one_and_replace({{"invalid", "item"}}, {{}}, person_find_and_modify_options,
                                               [&](Optional<bson::BsonDocument> document, Optional<AppError> error) {
                                                   REQUIRE_FALSE(error);
                                                   REQUIRE(!document);
                                                   processed = true;
                                               });

        REQUIRE(processed);
    }

    SECTION("delete") {
        bool processed = false;

        bson::BsonArray documents;
        documents.assign(3, dog_document);

        dog_collection.insert_many(documents, [&](std::vector<bson::Bson> inserted_docs, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(inserted_docs.size() == 3);
        });

        MongoCollection::FindOneAndModifyOptions find_and_modify_options{
            Optional<bson::BsonDocument>({{"name", "fido"}}), // project
            Optional<bson::BsonDocument>({{"name", 1}}),      // sort,
            true,                                             // upsert
            true                                              // return new doc
        };

        dog_collection.delete_one(dog_document, [&](uint64_t deleted_count, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(deleted_count >= 1);
        });

        dog_collection.delete_many(dog_document, [&](uint64_t deleted_count, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(deleted_count >= 1);
            processed = true;
        });

        person_collection.delete_many_bson(person_document, [&](Optional<bson::Bson> bson, Optional<AppError> error) {
            REQUIRE_FALSE(error);
            REQUIRE(static_cast<int32_t>(static_cast<bson::BsonDocument>(*bson)["deletedCount"]) >= 1);
            processed = true;
        });

        REQUIRE(processed);
    }
}

#endif // REALM_ENABLE_AUTH_TESTS
#endif // REALM_ENABLE_SYNC
