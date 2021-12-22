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

#include <realm/object-store/sync/mongo_collection.hpp>

#include <realm/object-store/sync/app_service_client.hpp>
#include <realm/object-store/sync/generic_network_transport.hpp>
#include <realm/util/uri.hpp>

namespace realm {
namespace app {

using namespace bson;
template <typename T>
using ResponseHandler = MongoCollection::ResponseHandler<T>;

namespace {

template <typename T>
util::Optional<T> get(const std::unordered_map<std::string, Bson>& map, const char* key)
{
    if (auto it = map.find(key); it != map.end()) {
        return static_cast<T>(it->second);
    }
    return util::none;
}

ResponseHandler<util::Optional<Bson>> get_delete_count_handler(ResponseHandler<uint64_t>&& completion)
{
    return [completion = std::move(completion)](util::Optional<Bson>&& value, util::Optional<AppError>&& error) {
        if (value && !error) {
            try {
                auto& document = static_cast<const BsonDocument&>(*value).entries();
                return completion(get<int32_t>(document, "deletedCount").value_or(0), std::move(error));
            }
            catch (const std::exception& e) {
                return completion(0, AppError(make_error_code(JSONErrorCode::bad_bson_parse), e.what()));
            }
        }

        return completion(0, std::move(error));
    };
}

ResponseHandler<util::Optional<Bson>> get_update_handler(ResponseHandler<MongoCollection::UpdateResult>&& completion)
{
    return [completion = std::move(completion)](util::Optional<Bson>&& value, util::Optional<AppError>&& error) {
        if (error) {
            return completion({}, std::move(error));
        }

        try {
            auto& document = static_cast<const BsonDocument&>(*value).entries();
            return completion(MongoCollection::UpdateResult{get<int32_t>(document, "matchedCount").value_or(0),
                                                            get<int32_t>(document, "modifiedCount").value_or(0),
                                                            get<ObjectId>(document, "upsertedId")},
                              std::move(error));
        }
        catch (const std::exception& e) {
            return completion({}, AppError(make_error_code(JSONErrorCode::bad_bson_parse), e.what()));
        }
    };
}

ResponseHandler<util::Optional<Bson>> get_document_handler(ResponseHandler<util::Optional<BsonDocument>>&& completion)
{
    return [completion = std::move(completion)](util::Optional<Bson>&& value, util::Optional<AppError>&& error) {
        if (error) {
            return completion(util::none, std::move(error));
        }

        if (!value) {
            // no docs were found
            return completion(util::none, util::none);
        }

        if (holds_alternative<util::None>(*value)) {
            // no docs were found
            return completion(util::none, util::none);
        }

        return completion(static_cast<BsonDocument>(*value), util::none);
    };
}

} // anonymous namespace

MongoCollection::MongoCollection(const std::string& name, const std::string& database_name,
                                 const std::shared_ptr<SyncUser>& user,
                                 const std::shared_ptr<AppServiceClient>& service, const std::string& service_name)
    : m_name(name)
    , m_database_name(database_name)
    , m_base_operation_args({{"database", m_database_name}, {"collection", m_name}})
    , m_user(user)
    , m_service(service)
    , m_service_name(service_name)
{
}

void MongoCollection::find(const BsonDocument& filter_bson, const FindOptions& options,
                           ResponseHandler<util::Optional<BsonArray>>&& completion)
{
    find_bson(filter_bson, options,
              [completion = std::move(completion)](util::Optional<Bson>&& value, util::Optional<AppError>&& error) {
                  if (error) {
                      return completion(util::none, std::move(error));
                  }

                  return completion(static_cast<BsonArray>(*value), util::none);
              });
}

void MongoCollection::find(const BsonDocument& filter_bson, ResponseHandler<util::Optional<BsonArray>>&& completion)
{
    find(filter_bson, {}, std::move(completion));
}

void MongoCollection::find_one(const BsonDocument& filter_bson, const FindOptions& options,
                               ResponseHandler<util::Optional<BsonDocument>>&& completion)
{
    find_one_bson(filter_bson, options, get_document_handler(std::move(completion)));
}

void MongoCollection::find_one(const BsonDocument& filter_bson,
                               ResponseHandler<util::Optional<BsonDocument>>&& completion)
{
    find_one(filter_bson, {}, std::move(completion));
}

void MongoCollection::insert_one(const BsonDocument& value_bson, ResponseHandler<util::Optional<Bson>>&& completion)
{
    insert_one_bson(value_bson, [completion = std::move(completion)](util::Optional<Bson>&& value,
                                                                     util::Optional<AppError>&& error) {
        if (error) {
            return completion(util::none, std::move(error));
        }

        auto& document = static_cast<const BsonDocument&>(*value);
        return completion(document.at("insertedId"), util::none);
    });
}

void MongoCollection::aggregate(const BsonArray& pipeline, ResponseHandler<util::Optional<BsonArray>>&& completion)
{
    aggregate_bson(pipeline, [completion = std::move(completion)](util::Optional<Bson>&& value,
                                                                  util::Optional<AppError>&& error) {
        if (error) {
            return completion(util::none, std::move(error));
        }

        return completion(static_cast<BsonArray>(*value), util::none);
    });
}

void MongoCollection::count(const BsonDocument& filter_bson, int64_t limit, ResponseHandler<uint64_t>&& completion)
{
    count_bson(filter_bson, limit,
               [completion = std::move(completion)](util::Optional<Bson>&& value, util::Optional<AppError>&& error) {
                   if (error) {
                       return completion(0, std::move(error));
                   }

                   return completion(static_cast<int64_t>(*value), util::none);
               });
}

void MongoCollection::count(const BsonDocument& filter_bson, ResponseHandler<uint64_t>&& completion)
{
    count(filter_bson, 0, std::move(completion));
}

void MongoCollection::insert_many(const BsonArray& documents, ResponseHandler<std::vector<Bson>>&& completion)
{
    insert_many_bson(documents, [completion = std::move(completion)](util::Optional<Bson>&& value,
                                                                     util::Optional<AppError>&& error) {
        if (error) {
            return completion({}, std::move(error));
        }

        auto& bson = static_cast<const BsonDocument&>(*value).entries();
        return completion(get<BsonArray>(bson, "insertedIds").value_or(BsonArray()), std::move(error));
    });
}

void MongoCollection::delete_one(const BsonDocument& filter_bson, ResponseHandler<uint64_t>&& completion)
{
    delete_one_bson(filter_bson, get_delete_count_handler(std::move(completion)));
}

void MongoCollection::delete_many(const BsonDocument& filter_bson, ResponseHandler<uint64_t>&& completion)
{
    delete_many_bson(filter_bson, get_delete_count_handler(std::move(completion)));
}

void MongoCollection::update_one(const BsonDocument& filter_bson, const BsonDocument& update_bson, bool upsert,
                                 ResponseHandler<MongoCollection::UpdateResult>&& completion)
{
    update_one_bson(filter_bson, update_bson, upsert, get_update_handler(std::move(completion)));
}

void MongoCollection::update_one(const BsonDocument& filter_bson, const BsonDocument& update_bson,
                                 ResponseHandler<MongoCollection::UpdateResult>&& completion)
{
    update_one(filter_bson, update_bson, {}, std::move(completion));
}

void MongoCollection::update_many(const BsonDocument& filter_bson, const BsonDocument& update_bson, bool upsert,
                                  ResponseHandler<MongoCollection::UpdateResult>&& completion)
{
    update_many_bson(filter_bson, update_bson, upsert, get_update_handler(std::move(completion)));
}

void MongoCollection::update_many(const BsonDocument& filter_bson, const BsonDocument& update_bson,
                                  ResponseHandler<MongoCollection::UpdateResult>&& completion)
{
    update_many(filter_bson, update_bson, {}, std::move(completion));
}

void MongoCollection::find_one_and_update(const BsonDocument& filter_bson, const BsonDocument& update_bson,
                                          const MongoCollection::FindOneAndModifyOptions& options,
                                          ResponseHandler<util::Optional<BsonDocument>>&& completion)
{
    find_one_and_update_bson(filter_bson, update_bson, options, get_document_handler(std::move(completion)));
}

void MongoCollection::find_one_and_update(const BsonDocument& filter_bson, const BsonDocument& update_bson,
                                          ResponseHandler<util::Optional<BsonDocument>>&& completion)
{
    find_one_and_update(filter_bson, update_bson, {}, std::move(completion));
}

void MongoCollection::find_one_and_replace(const BsonDocument& filter_bson, const BsonDocument& replacement_bson,
                                           const MongoCollection::FindOneAndModifyOptions& options,
                                           ResponseHandler<util::Optional<BsonDocument>>&& completion)
{
    find_one_and_replace_bson(filter_bson, replacement_bson, options, get_document_handler(std::move(completion)));
}

void MongoCollection::find_one_and_replace(const BsonDocument& filter_bson, const BsonDocument& replacement_bson,
                                           ResponseHandler<util::Optional<BsonDocument>>&& completion)
{
    find_one_and_replace(filter_bson, replacement_bson, {}, std::move(completion));
}

void MongoCollection::find_one_and_delete(const BsonDocument& filter_bson,
                                          const MongoCollection::FindOneAndModifyOptions& options,
                                          ResponseHandler<util::Optional<BsonDocument>>&& completion)
{
    find_one_and_delete_bson(filter_bson, options, get_document_handler(std::move(completion)));
}

void MongoCollection::find_one_and_delete(const BsonDocument& filter_bson,
                                          ResponseHandler<util::Optional<BsonDocument>>&& completion)
{
    find_one_and_delete(filter_bson, {}, std::move(completion));
}

void MongoCollection::call_function(const char* name, const bson::BsonDocument& arg,
                                    ResponseHandler<util::Optional<bson::Bson>>&& completion)
{
    m_service->call_function(m_user, name, BsonArray({arg}), m_service_name, std::move(completion));
}

static void set_options(BsonDocument& base_args, const MongoCollection::FindOptions& options)
{
    if (options.limit) {
        base_args["limit"] = *options.limit;
    }

    if (options.projection_bson) {
        base_args["project"] = *options.projection_bson;
    }

    if (options.sort_bson) {
        base_args["sort"] = *options.sort_bson;
    }
}

void MongoCollection::find_bson(const BsonDocument& filter_bson, const FindOptions& options,
                                ResponseHandler<util::Optional<Bson>>&& completion)
try {
    auto base_args = m_base_operation_args;
    base_args["query"] = filter_bson;
    set_options(base_args, options);

    call_function("find", base_args, std::move(completion));
}
catch (const std::exception& e) {
    return completion(util::none, AppError(make_error_code(JSONErrorCode::malformed_json), e.what()));
}

void MongoCollection::find_one_bson(const BsonDocument& filter_bson, const FindOptions& options,
                                    ResponseHandler<util::Optional<Bson>>&& completion)
try {
    auto base_args = m_base_operation_args;
    base_args["query"] = filter_bson;
    set_options(base_args, options);
    call_function("findOne", base_args, std::move(completion));
}
catch (const std::exception& e) {
    return completion(util::none, AppError(make_error_code(JSONErrorCode::malformed_json), e.what()));
}

void MongoCollection::insert_one_bson(const BsonDocument& value_bson,
                                      ResponseHandler<util::Optional<Bson>>&& completion)
{
    auto base_args = m_base_operation_args;
    base_args["document"] = value_bson;
    call_function("insertOne", base_args, std::move(completion));
}

void MongoCollection::aggregate_bson(const BsonArray& pipline, ResponseHandler<util::Optional<Bson>>&& completion)
{
    auto base_args = m_base_operation_args;
    base_args["pipeline"] = pipline;
    call_function("aggregate", base_args, std::move(completion));
}

void MongoCollection::count_bson(const BsonDocument& filter_bson, int64_t limit,
                                 ResponseHandler<util::Optional<Bson>>&& completion)
{
    auto base_args = m_base_operation_args;
    base_args["query"] = filter_bson;
    if (limit != 0) {
        base_args["limit"] = limit;
    }
    call_function("count", base_args, std::move(completion));
}

void MongoCollection::insert_many_bson(const BsonArray& documents, ResponseHandler<util::Optional<Bson>>&& completion)
{
    auto base_args = m_base_operation_args;
    base_args["documents"] = documents;
    call_function("insertMany", base_args, std::move(completion));
}

void MongoCollection::delete_one_bson(const BsonDocument& filter_bson,
                                      ResponseHandler<util::Optional<Bson>>&& completion)
{
    auto base_args = m_base_operation_args;
    base_args["query"] = filter_bson;
    call_function("deleteOne", base_args, std::move(completion));
}

void MongoCollection::delete_many_bson(const BsonDocument& filter_bson,
                                       ResponseHandler<util::Optional<Bson>>&& completion)
{
    auto base_args = m_base_operation_args;
    base_args["query"] = filter_bson;
    call_function("deleteMany", base_args, std::move(completion));
}

void MongoCollection::update_one_bson(const BsonDocument& filter_bson, const BsonDocument& update_bson, bool upsert,
                                      ResponseHandler<util::Optional<Bson>>&& completion)
{
    auto base_args = m_base_operation_args;
    base_args["query"] = filter_bson;
    base_args["update"] = update_bson;
    base_args["upsert"] = upsert;
    call_function("updateOne", base_args, std::move(completion));
}

void MongoCollection::update_many_bson(const BsonDocument& filter_bson, const BsonDocument& update_bson, bool upsert,
                                       ResponseHandler<util::Optional<Bson>>&& completion)
{
    auto base_args = m_base_operation_args;
    base_args["query"] = filter_bson;
    base_args["update"] = update_bson;
    base_args["upsert"] = upsert;
    call_function("updateMany", base_args, std::move(completion));
}

void MongoCollection::find_one_and_update_bson(const BsonDocument& filter_bson, const BsonDocument& update_bson,
                                               const MongoCollection::FindOneAndModifyOptions& options,
                                               ResponseHandler<util::Optional<Bson>>&& completion)
{
    auto base_args = m_base_operation_args;
    base_args["filter"] = filter_bson;
    base_args["update"] = update_bson;
    options.set_bson(base_args);
    call_function("findOneAndUpdate", base_args, std::move(completion));
}

void MongoCollection::find_one_and_replace_bson(const BsonDocument& filter_bson, const BsonDocument& replacement_bson,
                                                const MongoCollection::FindOneAndModifyOptions& options,
                                                ResponseHandler<util::Optional<Bson>>&& completion)
{
    auto base_args = m_base_operation_args;
    base_args["filter"] = filter_bson;
    base_args["update"] = replacement_bson;
    options.set_bson(base_args);
    call_function("findOneAndReplace", base_args, std::move(completion));
}

void MongoCollection::find_one_and_delete_bson(const BsonDocument& filter_bson,
                                               const MongoCollection::FindOneAndModifyOptions& options,
                                               ResponseHandler<util::Optional<Bson>>&& completion)
{
    auto base_args = m_base_operation_args;
    base_args["filter"] = filter_bson;
    options.set_bson(base_args);
    call_function("findOneAndDelete", base_args, std::move(completion));
}

void WatchStream::feed_buffer(std::string_view input)
{
    REALM_ASSERT(m_state == NEED_DATA);
    m_buffer += input;
    advance_buffer_state();
}

void WatchStream::advance_buffer_state()
{
    REALM_ASSERT(m_state == NEED_DATA);
    while (m_state == NEED_DATA) {
        if (m_buffer_offset == m_buffer.size()) {
            m_buffer.clear();
            m_buffer_offset = 0;
            return;
        }

        // NOTE not supporting CR-only newlines, just LF and CRLF.
        auto next_newline = m_buffer.find('\n', m_buffer_offset);
        if (next_newline == std::string::npos) {
            // We have a partial line.
            if (m_buffer_offset != 0) {
                // Slide the partial line down to the front of the buffer.
                m_buffer.assign(m_buffer.data() + m_buffer_offset, m_buffer.size() - m_buffer_offset);
                m_buffer_offset = 0;
            }
            return;
        }

        feed_line(std::string_view(m_buffer.data() + m_buffer_offset, next_newline - m_buffer_offset));
        m_buffer_offset = next_newline + 1; // Advance past this line, including its newline.
    }
}

void WatchStream::feed_line(std::string_view line)
{
    REALM_ASSERT(m_state == NEED_DATA);
    // This is an implementation of the algorithm described at
    // https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation.
    // Currently the server does not use id or retry lines, so that processing isn't implemented.

    // ignore trailing LF if not removed by SDK.
    if (!line.empty() && line.back() == '\n')
        line = line.substr(0, line.size() - 1);

    // ignore trailing CR from CRLF
    if (!line.empty() && line.back() == '\r')
        line = line.substr(0, line.size() - 1);

    if (line.empty()) {
        // This is the "dispatch the event" portion of the algorithm.
        if (m_data_buffer.empty()) {
            m_event_type.clear();
            return;
        }

        if (m_data_buffer.back() == '\n')
            m_data_buffer.pop_back();

        feed_sse({m_data_buffer, m_event_type});
        m_data_buffer.clear();
        m_event_type.clear();
        return;
    }

    if (line[0] == ':')
        return;

    const auto colon = line.find(':');
    const auto field = line.substr(0, colon);
    auto value = colon == std::string::npos ? std::string_view() : line.substr(colon + 1);
    if (!value.empty() && value[0] == ' ')
        value = value.substr(1);

    if (field == "event") {
        m_event_type = value;
    }
    else if (field == "data") {
        m_data_buffer += value;
        m_data_buffer += '\n';
    }
    else {
        // line is ignored (even if field is id or retry).
    }
}

void WatchStream::feed_sse(ServerSentEvent sse)
{
    REALM_ASSERT(m_state == NEED_DATA);
    std::string buffer; // must outlast if-block since we bind sse.data to it.
    size_t first_percent = sse.data.find('%');
    if (first_percent != std::string::npos) {
        // For some reason, the stich server decided to add percent-encoding for '%', '\n', and '\r' to its
        // event-stream replies. But it isn't real urlencoding, since most characters pass through, so we can't use
        // uri_percent_decode() here.
        buffer.reserve(sse.data.size());
        size_t start = 0;
        while (true) {
            auto percent = start == 0 ? first_percent : sse.data.find('%', start);
            if (percent == std::string::npos) {
                buffer += sse.data.substr(start);
                break;
            }

            buffer += sse.data.substr(start, percent - start);

            auto encoded = sse.data.substr(percent, 3); // may be smaller than 3 if string ends with %
            if (encoded == "%25") {
                buffer += '%';
            }
            else if (encoded == "%0A") {
                buffer += '\x0A'; // '\n'
            }
            else if (encoded == "%0D") {
                buffer += '\x0D'; // '\r'
            }
            else {
                buffer += encoded; // propagate as-is
            }
            start = percent + encoded.size();
        }

        sse.data = buffer;
    }

    if (sse.eventType.empty() || sse.eventType == "message") {
        try {
            auto parsed = parse(sse.data);
            if (parsed.type() == Bson::Type::Document) {
                m_next_event = parsed.operator const BsonDocument&();
                m_state = HAVE_EVENT;
                return;
            }
        }
        catch (...) {
            // fallthrough to same handling as for non-document value.
        }
        m_state = HAVE_ERROR;
        m_error = std::make_unique<AppError>(app::make_error_code(JSONErrorCode::bad_bson_parse),
                                             "server returned malformed event: " + std::string(sse.data));
    }
    else if (sse.eventType == "error") {
        m_state = HAVE_ERROR;

        // default error message if we have issues parsing the reply.
        m_error = std::make_unique<AppError>(app::make_error_code(ServiceErrorCode::unknown), std::string(sse.data));
        try {
            auto parsed = parse(sse.data);
            if (parsed.type() != Bson::Type::Document)
                return;
            auto& obj = static_cast<BsonDocument&>(parsed);
            auto& code = obj.at("error_code");
            auto& msg = obj.at("error");
            if (code.type() != Bson::Type::String)
                return;
            if (msg.type() != Bson::Type::String)
                return;
            m_error = std::make_unique<AppError>(
                app::make_error_code(app::service_error_code_from_string(static_cast<const std::string&>(code))),
                std::move(static_cast<std::string&>(msg)));
        }
        catch (...) {
            return; // Use the default state.
        }
    }
    else {
        // Ignore other event types
    }
}
} // namespace app
} // namespace realm
