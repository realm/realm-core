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

#include "keypath_mapping.hpp"
#include <realm/util/serializer.hpp>

#include <functional>

namespace realm {
namespace query_parser {

using namespace realm::util;

std::size_t TableAndColHash::operator()(const std::pair<TableKey, std::string>& p) const
{
    auto h1 = std::hash<std::string>{}(p.second);
    return h1 ^ p.first.value;
}


bool KeyPathMapping::add_mapping(ConstTableRef table, std::string name, std::string alias)
{
    auto table_key = table->get_key();
    auto it_and_success = m_mapping.insert({{table_key, name}, alias});
    return it_and_success.second;
}

bool KeyPathMapping::remove_mapping(ConstTableRef table, std::string name)
{
    auto table_key = table->get_key();
    return m_mapping.erase({table_key, name}) > 0;
}

bool KeyPathMapping::has_mapping(ConstTableRef table, const std::string& name) const
{
    return (m_mapping.size() > 0) && m_mapping.find({table->get_key(), name}) != m_mapping.end();
}

util::Optional<std::string> KeyPathMapping::get_mapping(TableKey table_key, const std::string& name) const
{
    util::Optional<std::string> ret;
    if (m_mapping.size() > 0) {
        auto it = m_mapping.find({table_key, name});
        if (it != m_mapping.end()) {
            ret = it->second;
        }
    }
    return ret;
}

bool KeyPathMapping::add_table_mapping(ConstTableRef table, std::string alias)
{
    std::string real_table_name = table->get_name();
    if (alias == real_table_name) {
        return false; // preventing an infinite mapping loop
    }
    auto it_and_success = m_table_mappings.insert({alias, real_table_name});
    return it_and_success.second;
}

bool KeyPathMapping::remove_table_mapping(std::string alias_to_remove)
{
    return m_table_mappings.erase(alias_to_remove) > 0;
}

bool KeyPathMapping::has_table_mapping(const std::string& alias) const
{
    return m_table_mappings.count(alias) > 0;
}

util::Optional<std::string> KeyPathMapping::get_table_mapping(const std::string alias) const
{
    if (auto it = m_table_mappings.find(alias); it != m_table_mappings.end()) {
        return it->second;
    }
    return {};
}

constexpr static size_t max_substitutions_allowed = 50;

std::string KeyPathMapping::translate_table_name(const std::string& identifier)
{
    size_t substitutions = 0;
    std::string alias = identifier;
    while (auto mapped = get_table_mapping(alias)) {
        if (substitutions > max_substitutions_allowed) {
            throw MappingError(
                util::format("Substitution loop detected while processing class name mapping from '%1' to '%2'.",
                             identifier, *mapped));
        }
        alias = *mapped;
        substitutions++;
    }
    if (substitutions == 0 && m_backlink_class_prefix.size()) {
        alias = get_backlink_class_prefix() + alias;
    }
    return alias;
}

std::string KeyPathMapping::translate(ConstTableRef table, const std::string& identifier)
{
    size_t substitutions = 0;
    auto tk = table->get_key();
    std::string alias = identifier;
    while (auto mapped = get_mapping(tk, alias)) {
        if (substitutions > max_substitutions_allowed) {
            throw MappingError(util::format(
                "Substitution loop detected while processing '%1' -> '%2' found in type '%3'", alias, *mapped,
                util::serializer::get_printable_table_name(table->get_name(), m_backlink_class_prefix)));
        }
        alias = *mapped;
        substitutions++;
    }
    return alias;
}

std::string KeyPathMapping::translate(LinkChain& link_chain, const std::string& identifier)
{
    auto table = link_chain.get_current_table();
    return translate(table, identifier);
}

void KeyPathMapping::set_backlink_class_prefix(std::string prefix)
{
    m_backlink_class_prefix = prefix;
}

} // namespace query_parser
} // namespace realm
