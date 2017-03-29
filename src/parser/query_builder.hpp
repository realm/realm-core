////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
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

#ifndef REALM_QUERY_BUILDER_HPP
#define REALM_QUERY_BUILDER_HPP

#include <string>
#include <memory>
#include <vector>

#include <realm/null.hpp>
#include <realm/timestamp.hpp>

namespace realm {
class Query;
class Realm;
class Schema;

namespace parser {
    struct Predicate;
}

namespace query_builder {
class Arguments;

void apply_predicate(Query& query, const parser::Predicate& predicate,
                     Arguments& arguments, const Schema& schema,
                     const std::string& objectType);

class Arguments {
public:
    virtual bool bool_for_argument(size_t argument_index) = 0;
    virtual long long long_for_argument(size_t argument_index) = 0;
    virtual float float_for_argument(size_t argument_index) = 0;
    virtual double double_for_argument(size_t argument_index) = 0;
    virtual std::string string_for_argument(size_t argument_index) = 0;
    virtual std::string binary_for_argument(size_t argument_index) = 0;
    virtual Timestamp timestamp_for_argument(size_t argument_index) = 0;
    virtual size_t object_index_for_argument(size_t argument_index) = 0;
    virtual bool is_argument_null(size_t argument_index) = 0;
};

template<typename ValueType, typename ContextType>
class ArgumentConverter : public Arguments {
public:
    ArgumentConverter(ContextType& context, std::shared_ptr<Realm> realm, std::vector<ValueType> arguments)
    : m_arguments(std::move(arguments))
    , m_ctx(context)
    , m_realm(std::move(realm))
    {}

    virtual bool bool_for_argument(size_t argument_index) { return m_ctx.to_bool(argument_at(argument_index)); }
    virtual long long long_for_argument(size_t argument_index) { return m_ctx.to_long(argument_at(argument_index)); }
    virtual float float_for_argument(size_t argument_index) { return m_ctx.to_float(argument_at(argument_index)); }
    virtual double double_for_argument(size_t argument_index) { return m_ctx.to_double(argument_at(argument_index)); }
    virtual std::string string_for_argument(size_t argument_index) { return m_ctx.to_string(argument_at(argument_index)); }
    virtual std::string binary_for_argument(size_t argument_index) { return m_ctx.to_binary(argument_at(argument_index)); }
    virtual Timestamp timestamp_for_argument(size_t argument_index) { return m_ctx.to_timestamp(argument_at(argument_index)); }
    virtual size_t object_index_for_argument(size_t argument_index) { return m_ctx.to_existing_object_index(m_realm, argument_at(argument_index)); }
    virtual bool is_argument_null(size_t argument_index) { return m_ctx.is_null(argument_at(argument_index)); }

private:
    std::vector<ValueType> m_arguments;
    ContextType& m_ctx;
    std::shared_ptr<Realm> m_realm;

    ValueType& argument_at(size_t index) const
    {
        return m_arguments.at(index);
    }
};
} // namespace query_builder
} // namespace realm

#endif // REALM_QUERY_BUILDER_HPP
