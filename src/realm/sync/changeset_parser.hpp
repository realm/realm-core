
#ifndef REALM_SYNC_CHANGESET_PARSER_HPP
#define REALM_SYNC_CHANGESET_PARSER_HPP

#include "realm/mixed.hpp"
#include <realm/sync/changeset.hpp>
#include <realm/util/input_stream.hpp>

namespace realm {
namespace sync {

void parse_changeset(util::NoCopyInputStream&, Changeset& out_log);
void parse_changeset(util::InputStream&, Changeset& out_log);

class OwnedMixed : public Mixed {
public:
    explicit OwnedMixed(std::string&& str)
        : m_owned_string(std::move(str))
    {
        m_type = int(type_String);
        string_val = m_owned_string;
    }

    using Mixed::Mixed;

private:
    std::string m_owned_string;
};

// The server may send us primary keys of objects in json-encoded error messages as base64-encoded changeset payloads.
// This function takes such a base64-encoded payload and returns it parsed as an owned Mixed value. If it cannot
// be decoded, this throws a BadChangeset exception.
OwnedMixed parse_base64_encoded_primary_key(std::string_view str);

} // namespace sync
} // namespace realm

#endif // REALM_SYNC_CHANGESET_PARSER_HPP
