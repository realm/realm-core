////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or utilied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#ifndef REMOTE_MONGO_COLLECTION_HPP
#define REMOTE_MONGO_COLLECTION_HPP

#include "sync/app_service_client.hpp"
#include <realm/util/optional.hpp>
#include <json.hpp>
#include <string>
#include <vector>

namespace realm {
namespace app {

class RemoteMongoCollection {
public:
    
    struct RemoteUpdateResult {
        /// The number of documents that matched the filter.
        int32_t matched_count;
        /// The number of documents modified.
        int32_t modified_count;
        /// The identifier of the inserted document if an upsert took place.
        util::Optional<ObjectId> upserted_id;
    };

    /// Options to use when executing a `find` command on a `RemoteMongoCollection`.
    struct RemoteFindOptions {
        /// The maximum number of documents to return.
        util::Optional<int64_t> limit;

        /// Limits the fields to return for all matching documents.
        util::Optional<bson::BsonDocument> projection_bson;

        /// The order in which to return matching documents.
        util::Optional<bson::BsonDocument> sort_bson;
    };

    /// Options to use when executing a `find_one_and_update`, `find_one_and_replace`,
    /// or `find_one_and_delete` command on a `remote_mongo_collection`.
    struct RemoteFindOneAndModifyOptions {
        /// Limits the fields to return for all matching documents.
        util::Optional<bson::BsonDocument> projection_bson;
        /// The order in which to return matching documents.
        util::Optional<bson::BsonDocument> sort_bson;
        /// Whether or not to perform an upsert, default is false
        /// (only available for find_one_and_replace and find_one_and_update)
        bool upsert = false;
        /// If this is true then the new document is returned,
        /// Otherwise the old document is returned (default)
        /// (only available for find_one_and_replace and find_one_and_update)
        bool return_new_document = false;
        
        void set_bson(bson::BsonDocument &bson)
        {
            if (upsert) {
                bson["upsert"] = true;
            }
            
            if (return_new_document) {
                bson["returnNewDocument"] = true;
            }
            
            if (projection_bson) {
                bson["projection"] = *projection_bson;
            }
            
            if (sort_bson) {
                bson["sort"] = *sort_bson;
            }
        }
    };

    ~RemoteMongoCollection() = default;
    RemoteMongoCollection(RemoteMongoCollection&&) = default;
    RemoteMongoCollection(const RemoteMongoCollection&) = default;
    RemoteMongoCollection& operator=(const RemoteMongoCollection& v) = default;
    RemoteMongoCollection& operator=(RemoteMongoCollection&&) = default;

    const std::string& name() const
    {
        return m_name;
    }

    const std::string& database_name() const
    {
        return m_database_name;
    }

    /// Finds the documents in this collection which match the provided filter.
    /// @param filter_bson A `Document` as bson that should match the query.
    /// @param options `RemoteFindOptions` to use when executing the command.
    /// @param completion_block The resulting bson array of documents or error if one occurs
    void find(const bson::BsonDocument& filter_bson,
              RemoteFindOptions options,
              std::function<void(util::Optional<bson::BsonArray>, util::Optional<AppError>)> completion_block);
    
    /// Finds the documents in this collection which match the provided filter.
    /// @param filter_bson A `Document` as bson that should match the query.
    /// @param completion_block The resulting bson array as a string or error if one occurs
    void find(const bson::BsonDocument& filter_bson,
              std::function<void(util::Optional<bson::BsonArray>, util::Optional<AppError>)> completion_block);

    /// Returns one document from a collection or view which matches the
    /// provided filter. If multiple documents satisfy the query, this method
    /// returns the first document according to the query's sort order or natural
    /// order.
    /// @param filter_bson A `Document` as bson that should match the query.
    /// @param options `RemoteFindOptions` to use when executing the command.
    /// @param completion_block The resulting bson or error if one occurs
    void find_one(const bson::BsonDocument& filter_bson,
                  RemoteFindOptions options,
                  std::function<void(util::Optional<bson::BsonDocument>, util::Optional<AppError>)> completion_block);
    
    /// Returns one document from a collection or view which matches the
    /// provided filter. If multiple documents satisfy the query, this method
    /// returns the first document according to the query's sort order or natural
    /// order.
    /// @param filter_bson A `Document` as bson that should match the query.
    /// @param completion_block The resulting bson or error if one occurs
    void find_one(const bson::BsonDocument& filter_bson,
                  std::function<void(util::Optional<bson::BsonDocument>, util::Optional<AppError>)> completion_block);
    
    /// Runs an aggregation framework pipeline against this collection.
    /// @param pipeline A bson array made up of `Documents` containing the pipeline of aggregation operations to perform.
    /// @param completion_block The resulting bson array of documents or error if one occurs
    void aggregate(const bson::BsonArray& pipeline,
                   std::function<void(util::Optional<bson::BsonArray>, util::Optional<AppError>)> completion_block);

    /// Counts the number of documents in this collection matching the provided filter.
    /// @param filter_bson A `Document` as bson that should match the query.
    /// @param limit The max amount of documents to count
    /// @param completion_block Returns the count of the documents that matched the filter.
    void count(const bson::BsonDocument& filter_bson,
               int64_t limit,
               std::function<void(uint64_t, util::Optional<AppError>)> completion_block);

    /// Counts the number of documents in this collection matching the provided filter.
    /// @param filter_bson A `Document` as bson that should match the query.
    /// @param completion_block Returns the count of the documents that matched the filter.
    void count(const bson::BsonDocument& filter_bson,
               std::function<void(uint64_t, util::Optional<AppError>)> completion_block);
    
    /// Encodes the provided value to BSON and inserts it. If the value is missing an identifier, one will be
    /// generated for it.
    /// @param value_bson  A `Document` value to insert.
    /// @param completion_block The result of attempting to perform the insert. An Id will be returned for the inserted object on sucess
    void insert_one(const bson::BsonDocument& value_bson,
                    std::function<void(util::Optional<ObjectId>, util::Optional<AppError>)> completion_block);
    
    /// Encodes the provided values to BSON and inserts them. If any values are missing identifiers,
    /// they will be generated.
    /// @param documents  The `Document` values in a bson array to insert.
    /// @param completion_block The result of the insert, returns an array inserted document ids in order
    void insert_many(bson::BsonArray documents,
                     std::function<void(std::vector<ObjectId>, util::Optional<AppError>)> completion_block);
    
    /// Deletes a single matching document from the collection.
    /// @param filter_bson A `Document` as bson that should match the query.
    /// @param completion_block The result of performing the deletion. Returns the count of deleted objects
    void delete_one(const bson::BsonDocument& filter_bson,
                    std::function<void(uint64_t, util::Optional<AppError>)> completion_block);

    /// Deletes multiple documents
    /// @param filter_bson Document representing the match criteria
    /// @param completion_block The result of performing the deletion. Returns the count of the deletion
    void delete_many(const bson::BsonDocument& filter_bson,
                     std::function<void(uint64_t, util::Optional<AppError>)> completion_block);
    
    /// Updates a single document matching the provided filter in this collection.
    /// @param filter_bson  A bson `Document` representing the match criteria.
    /// @param update_bson  A bson `Document` representing the update to be applied to a matching document.
    /// @param upsert When true, creates a new document if no document matches the query.
    /// @param completion_block The result of the attempt to update a document.
    void update_one(const bson::BsonDocument& filter_bson,
                    const bson::BsonDocument& update_bson,
                    bool upsert,
                    std::function<void(RemoteUpdateResult, util::Optional<AppError>)> completion_block);
    
    /// Updates a single document matching the provided filter in this collection.
    /// @param filter_bson  A bson `Document` representing the match criteria.
    /// @param update_bson  A bson `Document` representing the update to be applied to a matching document.
    /// @param completion_block The result of the attempt to update a document.
    void update_one(const bson::BsonDocument& filter_bson,
                    const bson::BsonDocument& update_bson,
                    std::function<void(RemoteUpdateResult, util::Optional<AppError>)> completion_block);

    /// Updates multiple documents matching the provided filter in this collection.
    /// @param filter_bson  A bson `Document` representing the match criteria.
    /// @param update_bson  A bson `Document` representing the update to be applied to a matching document.
    /// @param upsert When true, creates a new document if no document matches the query.
    /// @param completion_block The result of the attempt to update a document.
    void update_many(const bson::BsonDocument& filter_bson,
                     const bson::BsonDocument& update_bson,
                     bool upsert,
                     std::function<void(RemoteUpdateResult, util::Optional<AppError>)> completion_block);
    
    /// Updates multiple documents matching the provided filter in this collection.
    /// @param filter_bson  A bson `Document` representing the match criteria.
    /// @param update_bson  A bson `Document` representing the update to be applied to a matching document.
    /// @param completion_block The result of the attempt to update a document.
    void update_many(const bson::BsonDocument& filter_bson,
                     const bson::BsonDocument& update_bson,
                     std::function<void(RemoteUpdateResult, util::Optional<AppError>)> completion_block);

    /// Updates a single document in a collection based on a query filter and
    /// returns the document in either its pre-update or post-update form. Unlike
    /// `update_one`, this action allows you to atomically find, update, and
    /// return a document with the same command. This avoids the risk of other
    /// update operations changing the document between separate find and update
    /// operations.
    /// @param filter_bson  A bson `Document` representing the match criteria.
    /// @param update_bson  A bson `Document` representing the update to be applied to a matching document.
    /// @param options Optional `RemoteFindOneAndModifyOptions` to use when executing the command.
    /// @param completion_block The result of the attempt to update a document.
    void find_one_and_update(const bson::BsonDocument& filter_bson,
                             const bson::BsonDocument& update_bson,
                             RemoteFindOneAndModifyOptions options,
                             std::function<void(util::Optional<bson::BsonDocument>, util::Optional<AppError>)> completion_block);
    
    /// Updates a single document in a collection based on a query filter and
    /// returns the document in either its pre-update or post-update form. Unlike
    /// `update_one`, this action allows you to atomically find, update, and
    /// return a document with the same command. This avoids the risk of other
    /// update operations changing the document between separate find and update
    /// operations.
    /// @param filter_bson  A bson `Document` representing the match criteria.
    /// @param update_bson  A bson `Document` representing the update to be applied to a matching document.
    /// @param completion_block The result of the attempt to update a document.
    void find_one_and_update(const bson::BsonDocument& filter_bson,
                             const bson::BsonDocument& update_bson,
                             std::function<void(util::Optional<bson::BsonDocument>, util::Optional<AppError>)> completion_block);
    
    /// Overwrites a single document in a collection based on a query filter and
    /// returns the document in either its pre-replacement or post-replacement
    /// form. Unlike `update_one`, this action allows you to atomically find,
    /// replace, and return a document with the same command. This avoids the
    /// risk of other update operations changing the document between separate
    /// find and update operations.
    /// @param filter_bson  A `Document` that should match the query.
    /// @param replacement_bson  A `Document` describing the update.
    /// @param options Optional `RemoteFindOneAndModifyOptions` to use when executing the command.
    /// @param completion_block The result of the attempt to replace a document.
    void find_one_and_replace(const bson::BsonDocument& filter_bson,
                              const bson::BsonDocument& replacement_bson,
                              RemoteFindOneAndModifyOptions options,
                              std::function<void(util::Optional<bson::BsonDocument>, util::Optional<AppError>)> completion_block);
    
    /// Overwrites a single document in a collection based on a query filter and
    /// returns the document in either its pre-replacement or post-replacement
    /// form. Unlike `update_one`, this action allows you to atomically find,
    /// replace, and return a document with the same command. This avoids the
    /// risk of other update operations changing the document between separate
    /// find and update operations.
    /// @param filter_bson  A `Document` that should match the query.
    /// @param replacement_bson  A `Document` describing the update.
    /// @param completion_block The result of the attempt to replace a document.
    void find_one_and_replace(const bson::BsonDocument& filter_bson,
                              const bson::BsonDocument& replacement_bson,
                              std::function<void(util::Optional<bson::BsonDocument>, util::Optional<AppError>)> completion_block);

    /// Removes a single document from a collection based on a query filter and
    /// returns a document with the same form as the document immediately before
    /// it was deleted. Unlike `delete_one`, this action allows you to atomically
    /// find and delete a document with the same command. This avoids the risk of
    /// other update operations changing the document between separate find and
    /// delete operations.
    /// @param filter_bson  A `Document` that should match the query.
    /// @param options Optional `RemoteFindOneAndModifyOptions` to use when executing the command.
    /// @param completion_block The result of the attempt to delete a document.
    void find_one_and_delete(const bson::BsonDocument& filter_bson,
                             RemoteFindOneAndModifyOptions options,
                             std::function<void(util::Optional<bson::BsonDocument>, util::Optional<AppError>)> completion_block);
    
    /// Removes a single document from a collection based on a query filter and
    /// returns a document with the same form as the document immediately before
    /// it was deleted. Unlike `delete_one`, this action allows you to atomically
    /// find and delete a document with the same command. This avoids the risk of
    /// other update operations changing the document between separate find and
    /// delete operations.
    /// @param filter_bson  A `Document` that should match the query.
    /// @param completion_block The result of the attempt to delete a document.
    void find_one_and_delete(const bson::BsonDocument& filter_bson,
                             std::function<void(util::Optional<bson::BsonDocument>, util::Optional<AppError>)> completion_block);

private:
    friend class RemoteMongoDatabase;

    RemoteMongoCollection(std::string name,
                          std::string database_name,
                          std::shared_ptr<AppServiceClient> service,
                          std::string service_name)
    : m_name(name)
    , m_database_name(database_name)
    , m_base_operation_args({ { "database" , database_name }, { "collection" , name } })
    , m_service(service)
    , m_service_name(service_name)
    {
    }

    /// The name of this collection.
    std::string m_name;

    /// The name of the database containing this collection.
    std::string m_database_name;

    /// Returns a document of database name and collection name
    bson::BsonDocument m_base_operation_args;
    
    std::shared_ptr<AppServiceClient> m_service;

    std::string m_service_name;
};

} // namespace app
} // namespace realm

#endif /* remote_mongo_collection_h */

