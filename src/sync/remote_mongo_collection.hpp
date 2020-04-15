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

#include "sync/remote_mongo_read_operation.hpp"
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
        uint64_t matched_count;
        /// The number of documents modified.
        uint64_t modified_count;
        /// The identifier of the inserted document if an upsert took place.
        std::string upserted_id;
    };

    /// Options to use when executing a `find` command on a `RemoteMongoCollection`.
    struct RemoteFindOptions {
        /// The maximum number of documents to return.
        util::Optional<uint64_t> limit;

        /// Limits the fields to return for all matching documents.
        util::Optional<std::string> projection_json;

        /// The order in which to return matching documents.
        util::Optional<std::string> sort_json;
    };

    /// Options to use when executing a `find_one_and_update`, `find_one_and_replace`,
    /// or `find_one_and_delete` command on a `remote_mongo_collection`.
    struct RemoteFindOneAndModifyOptions {
        /// Limits the fields to return for all matching documents.
        util::Optional<std::string> projection_json;
        /// The order in which to return matching documents.
        util::Optional<std::string> sort_json;
        /// Whether or not to perform an upsert, default is false
        /// (only available for find_one_and_replace and find_one_and_update)
        bool upsert = false;
        /// If this is true then the new document is returned,
        /// Otherwise the old document is returned (default)
        /// (only available for find_one_and_replace and find_one_and_update)
        bool return_new_document = false;
    };
    
    /// The name of this collection.
    const std::string name;
    
    /// The name of the database containing this collection.
    const std::string database_name;

    RemoteMongoCollection(std::string name,
                          std::string database_name,
                          std::unique_ptr<AppServiceClient> service)
    : name(name), database_name(database_name), m_service(std::move(service)) { }

    
    /// Finds the documents in this collection which match the provided filter.
    /// @param filter_json A `Document` as a json string that should match the query.
    /// @param options `RemoteFindOptions` to use when executing the command.
    /// @param completion_block The resulting json string or error if one occurs
    void find(const std::string& filter_json,
              RemoteFindOptions options,
              std::function<void(std::string, util::Optional<AppError>)> completion_block);
    
    /// Finds the documents in this collection which match the provided filter.
    /// @param filter_json A `Document` as a json string that should match the query.
    /// @param completion_block The resulting json string or error if one occurs
    void find(const std::string& filter_json,
              std::function<void(std::string, util::Optional<AppError>)> completion_block);

    /// Returns one document as a json string from a collection or view which matches the
    /// provided filter. If multiple documents satisfy the query, this method
    /// returns the first document according to the query's sort order or natural
    /// order.
    /// @param filter_json A `Document` as a json string that should match the query.
    /// @param options `RemoteFindOptions` to use when executing the command.
    /// @param completion_block The resulting json string or error if one occurs
    void find_one(const std::string& filter_json,
                  RemoteFindOptions options,
                  std::function<void(std::string, util::Optional<AppError>)> completion_block);
    
    /// Returns one document as a json string from a collection or view which matches the
    /// provided filter. If multiple documents satisfy the query, this method
    /// returns the first document according to the query's sort order or natural
    /// order.
    /// @param filter_json A `Document` as a json string that should match the query.
    /// @param completion_block The resulting json string or error if one occurs
    void find_one(const std::string& filter_json,
                  std::function<void(std::string, util::Optional<AppError>)> completion_block);
    
    /// Runs an aggregation framework pipeline against this collection.
    /// @param pipline A `Document` array made up of jsons strings containing the pipeline of aggregation operations to perform.
    /// @param completion_block The resulting json string or error if one occurs
    void aggregate(std::vector<std::string> pipline,
                   std::function<void(std::string, util::Optional<AppError>)> completion_block);

    /// Counts the number of documents in this collection matching the provided filter.
    /// @param filter_json A `Document` as a json string that should match the query.
    /// @param options Optional `RemoteCountOptions` to use when executing the command.
    /// @param completion_block Returns the count of the documents that matched the filter.
    void count(const std::string& filter_json,
               util::Optional<RemoteFindOptions> options,
               std::function<void(uint64_t, util::Optional<AppError>)> completion_block);

    /// Counts the number of documents in this collection matching the provided filter.
    /// @param filter_json A `Document` as a json string that should match the query.
    /// @param completion_block Returns the count of the documents that matched the filter.
    void count(const std::string& filter_json,
               std::function<void(uint64_t, util::Optional<AppError>)> completion_block);
    
    /// Encodes the provided value to BSON and inserts it. If the value is missing an identifier, one will be
    /// generated for it.
    /// @param value_json  A `json` value to encode and insert.
    /// @param completion_block The result of attempting to perform the insert. An Id will be returned for the inserted object on sucess
    void insert_one(std::string value_json,
                    std::function<void(std::string, util::Optional<AppError>)> completion_block);
    
    /// Encodes the provided values to BSON and inserts them. If any values are missing identifiers,
    /// they will be generated.
    /// @param documents  The `json` values in a vector to insert.
    /// @param completion_block The result of the insert, returns a map of the index of the inserted document to the id of the inserted document.
    void insert_many(std::vector<std::string> documents,
                     std::function<void(std::map<uint64_t, std::string>, util::Optional<AppError>)> completion_block);
    
    /// Deletes a single matching document from the collection.
    /// @param filter_json A `Document` as a json string representing the match criteria.
    /// @param completion_block The result of performing the deletion. Returns the count of deleted objects
    void delete_one(const std::string& filter_json,
                    std::function<void(uint64_t, util::Optional<AppError>)> completion_block);

    /// Deletes multiple documents
    /// @param filter_json Document representing the match criteria
    /// @param completion_block The result of performing the deletion. Returns the count of the deletion
    void delete_many(const std::string& filter_json,
                     std::function<void(uint64_t, util::Optional<AppError>)> completion_block);
    
    /// Updates a single document matching the provided filter in this collection.
    /// @param filter_json  A `Document` as a json string representing the match criteria.
    /// @param update_json  A `Document` as a json string representing the update to be applied to a matching document.
    /// @param options Optional `RemoteUpdateOptions` to use when executing the command.
    /// @param completion_block The result of the attempt to update a document.
    void update_one(const std::string& filter_json,
                    const std::string& update_json,
                    RemoteFindOptions options,
                    std::function<void(RemoteUpdateResult, util::Optional<AppError>)> completion_block);
    
    /// Updates a single document matching the provided filter in this collection.
    /// @param filter_json  A `Document` as a json string representing the match criteria.
    /// @param update_json  A `Document` as a json string representing the update to be applied to a matching document.
    /// @param completion_block The result of the attempt to update a document.
    void update_one(const std::string& filter_json,
                    const std::string& update_json,
                    std::function<void(RemoteUpdateResult, util::Optional<AppError>)> completion_block);

    /// Updates multiple documents matching the provided filter in this collection.
    /// @param filter_json  A `Document` as a json string representing the match criteria.
    /// @param update_json  A `Document` as a json string representing the update to be applied to a matching document.
    /// @param options Optional `RemoteUpdateOptions` to use when executing the command.
    /// @param completion_block The result of the attempt to update a document.
    void update_many(const std::string& filter_json,
                     const std::string& update_json,
                     RemoteFindOptions options,
                     std::function<void(RemoteUpdateResult, util::Optional<AppError>)> completion_block);
    
    /// Updates multiple documents matching the provided filter in this collection.
    /// @param filter_json  A `Document` as a json string representing the match criteria.
    /// @param update_json  A `Document` as a json string representing the update to be applied to a matching document.
    /// @param completion_block The result of the attempt to update a document.
    void update_many(const std::string& filter_json,
                     const std::string& update_json,
                     std::function<void(RemoteUpdateResult, util::Optional<AppError>)> completion_block);

    /// Updates a single document in a collection based on a query filter and
    /// returns the document in either its pre-update or post-update form. Unlike
    /// `update_one`, this action allows you to atomically find, update, and
    /// return a document with the same command. This avoids the risk of other
    /// update operations changing the document between separate find and update
    /// operations.
    /// @param filter_json  A `Document` as a json string that should match the query.
    /// @param update_json  A `Document` as a json string describing the update.
    /// @param options Optional `RemoteFindOneAndModifyOptions` to use when executing the command.
    /// @param completion_block The result of the attempt to update a document.
    void find_one_and_update(const std::string& filter_json,
                             const std::string& update_json,
                             RemoteFindOneAndModifyOptions options,
                             std::function<void(std::string, util::Optional<AppError>)> completion_block);
    
    /// Updates a single document in a collection based on a query filter and
    /// returns the document in either its pre-update or post-update form. Unlike
    /// `update_one`, this action allows you to atomically find, update, and
    /// return a document with the same command. This avoids the risk of other
    /// update operations changing the document between separate find and update
    /// operations.
    /// @param filter_json  A `Document` as a json string that should match the query.
    /// @param update_json  A `Document` as a json string describing the update.
    /// @param completion_block The result of the attempt to update a document.
    void find_one_and_update(const std::string& filter_json,
                             const std::string& update_json,
                             std::function<void(std::string, util::Optional<AppError>)> completion_block);
    
    /// Overwrites a single document in a collection based on a query filter and
    /// returns the document in either its pre-replacement or post-replacement
    /// form. Unlike `update_one`, this action allows you to atomically find,
    /// replace, and return a document with the same command. This avoids the
    /// risk of other update operations changing the document between separate
    /// find and update operations.
    /// @param filter_json  A `Document` that should match the query.
    /// @param replacement_json  A `Document` describing the update.
    /// @param options Optional `RemoteFindOneAndModifyOptions` to use when executing the command.
    /// @param completion_block The result of the attempt to replace a document.
    void find_one_and_replace(const std::string& filter_json,
                              const std::string& replacement_json,
                              RemoteFindOneAndModifyOptions options,
                              std::function<void(std::string, util::Optional<AppError>)> completion_block);
    
    /// Overwrites a single document in a collection based on a query filter and
    /// returns the document in either its pre-replacement or post-replacement
    /// form. Unlike `update_one`, this action allows you to atomically find,
    /// replace, and return a document with the same command. This avoids the
    /// risk of other update operations changing the document between separate
    /// find and update operations.
    /// @param filter_json  A `Document` that should match the query.
    /// @param replacement_json  A `Document` describing the update.
    /// @param completion_block The result of the attempt to replace a document.
    void find_one_and_replace(const std::string& filter_json,
                              const std::string& replacement_json,
                              std::function<void(std::string, util::Optional<AppError>)> completion_block);

    /// Removes a single document from a collection based on a query filter and
    /// returns a document with the same form as the document immediately before
    /// it was deleted. Unlike `delete_one`, this action allows you to atomically
    /// find and delete a document with the same command. This avoids the risk of
    /// other update operations changing the document between separate find and
    /// delete operations.
    /// @param filter  A `Document` as a json string that should match the query.
    /// @param options Optional `RemoteFindOneAndModifyOptions` to use when executing the command.
    /// @param completion_block The result of the attempt to delete a document.
    void find_one_and_delete(const std::string& filter_json,
                             RemoteFindOneAndModifyOptions options,
                             std::function<void(std::string, util::Optional<AppError>)> completion_block);
    
    /// Removes a single document from a collection based on a query filter and
    /// returns a document with the same form as the document immediately before
    /// it was deleted. Unlike `delete_one`, this action allows you to atomically
    /// find and delete a document with the same command. This avoids the risk of
    /// other update operations changing the document between separate find and
    /// delete operations.
    /// @param filter  A `Document` as a json string that should match the query.
    /// @param completion_block The result of the attempt to delete a document.
    void find_one_and_delete(const std::string& filter_json,
                             std::function<void(std::string, util::Optional<AppError>)> completion_block);

private:
    
    /// Returns a document of database name and collection name
    nlohmann::json m_base_operation_args {
        { "database" , database_name },
        { "collection" , name }
    };
    
    std::unique_ptr<AppServiceClient> m_service;

};

} // namespace app
} // namespace realm

#endif /* remote_mongo_collection_h */

