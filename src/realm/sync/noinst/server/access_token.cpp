#include <cstddef>
#include <algorithm>
#include <vector>
#include <stack>

#include <realm/util/base64.hpp>
#include <realm/util/json_parser.hpp>
#include <realm/sync/noinst/server/permissions.hpp>
#include <realm/sync/noinst/server/access_token.hpp>

using namespace realm;
using namespace realm::util;
using namespace realm::sync;

namespace {

struct AccessTokenParser {
    using JSONEvent = JSONParser::EventType;
    using JSONError = JSONParser::Error;

    AccessToken m_token;

    std::error_condition operator()(const JSONParser::Event& event) noexcept
    {
        if (state_stack.empty()) {
            if (event.type != JSONEvent::object_begin)
                return JSONError::unexpected_token;
            state_stack.push(parser_state::toplevel);
            return std::error_condition{}; // ok
        }
        switch (state_stack.top()) {
            case toplevel: {
                if (event.type == JSONEvent::object_end) {
                    state_stack.pop();
                    break; // End of object
                }
                if (event.type != JSONEvent::string)
                    return JSONError::unexpected_token;
                StringData escaped_key = event.escaped_string_value();
                if (escaped_key == "access") {
                    state_stack.push(await_access);
                }
                else if (escaped_key == "identity" || escaped_key == "sub") {
                    state_stack.push(await_identity);
                }
                else if (escaped_key == "admin" || escaped_key == "isAdmin") {
                    state_stack.push(await_admin);
                }
                else if (escaped_key == "timestamp" || escaped_key == "iat") {
                    state_stack.push(await_timestamp);
                }
                else if (escaped_key == "expires" || escaped_key == "exp") {
                    state_stack.push(await_expires);
                }
                else if (escaped_key == "path") {
                    state_stack.push(await_path);
                }
                else if (escaped_key == "stitch_data") {
                    state_stack.push(await_stitchdata);
                }
                else if (escaped_key == "sync_label" || escaped_key == "syncLabel") {
                    state_stack.push(await_sync_label);
                }
                else if (escaped_key == "app_id" || escaped_key == "appId") {
                    state_stack.push(await_app_id);
                }
                else {
                    state_stack.push(skip_value);
                }
                break;
            }
            case skip_value: {
                if (event.type == JSONEvent::object_begin || event.type == JSONEvent::array_begin) {
                    ++m_skip_depth;
                }
                else if (event.type == JSONEvent::object_end || event.type == JSONEvent::array_end) {
                    --m_skip_depth;
                }
                if (m_skip_depth == 0) {
                    state_stack.pop();
                }
                break;
            }
            case await_stitchdata: {
                if (event.type != JSONEvent::object_begin) {
                    return JSONError::unexpected_token;
                }
                state_stack.pop();
                state_stack.push(await_stitchdata_object);
                break;
            }
            case await_stitchdata_object: {
                if (event.type == JSONEvent::object_end) {
                    state_stack.pop();
                    break; // End of object
                }
                StringData escaped_key = event.escaped_string_value();
                if (escaped_key == "realm_sync_label") {
                    state_stack.push(await_sync_label);
                }
                else if (escaped_key == "realm_path") {
                    state_stack.push(await_path);
                }
                else if (escaped_key == "realm_access") {
                    state_stack.push(await_access);
                }
                else {
                    state_stack.push(skip_value);
                }
                break;
            }
            case await_identity: {
                if (event.type != JSONEvent::string) {
                    return JSONError::unexpected_token;
                }
                m_token.identity = UserIdent(event.escaped_string_value()); // FIXME: Unescape
                state_stack.pop();
                break;
            }
            case await_admin: {
                if (event.type != JSONEvent::boolean) {
                    return JSONError::unexpected_token;
                }
                m_token.admin_field = true;
                m_token.admin = event.boolean;
                state_stack.pop();
                break;
            }
            case await_timestamp: {
                if (event.type == JSONEvent::null) {
                    m_token.timestamp = 0;
                }
                else if (event.type == JSONEvent::number) {
                    m_token.timestamp = std::int_fast64_t(event.number);
                }
                else {
                    return JSONError::unexpected_token;
                }
                state_stack.pop();
                break;
            }
            case await_expires: {
                if (event.type == JSONEvent::null) {
                    m_token.expires = 0;
                }
                else if (event.type == JSONEvent::number) {
                    m_token.expires = std::int_fast64_t(event.number);
                }
                else {
                    return JSONError::unexpected_token;
                }
                state_stack.pop();
                break;
            }
            case await_sync_label: {
                if (event.type != JSONEvent::string)
                    return JSONError::unexpected_token;
                m_token.sync_label = SyncLabel(event.escaped_string_value()); // FIXME: Unescape
                state_stack.pop();
                break;
            }
            case await_app_id: {
                if (event.type != JSONEvent::string)
                    return JSONError::unexpected_token;
                m_token.app_id = event.escaped_string_value(); // FIXME: Unescape
                state_stack.pop();
                break;
            }
            case await_path: {
                if (event.type != JSONEvent::string)
                    return JSONError::unexpected_token;
                m_token.path = RealmFileIdent{event.escaped_string_value()}; // FIXME: Unescape
                state_stack.pop();
                break;
            }
            case await_access: {
                if (event.type != JSONEvent::array_begin)
                    return JSONError::unexpected_token;
                state_stack.pop();
                state_stack.push(await_access_strings);
                break;
            }
            case await_access_strings: {
                if (event.type == JSONEvent::string) {
                    StringData access_string = event.escaped_string_value();
                    if (access_string == "download")
                        m_token.access |= 0 | Privilege::Download;
                    else if (access_string == "upload")
                        m_token.access |= 0 | Privilege::Upload;
                    else if (access_string == "manage")
                        m_token.access |= Privilege::ModifySchema | Privilege::SetPermissions;
                }
                else if (event.type == JSONEvent::array_end) {
                    state_stack.pop();
                }
                break;
            }
        }
        return std::error_condition{}; // ok
    }

private:
    enum parser_state {
        toplevel,
        await_stitchdata,
        await_stitchdata_object,
        await_identity,
        await_admin,
        await_timestamp,
        await_expires,
        await_app_id,
        await_path,
        await_sync_label,
        await_access,
        await_access_strings,
        skip_value,
    };

    std::stack<parser_state> state_stack;
    std::size_t m_skip_depth = 0;
};

} // unnamed namespace

bool AccessToken::parseJWT(StringData signed_token, AccessToken& token, ParseError& error, Verifier* verifier)
{
    const char* signed_token_begin = signed_token.data();
    const char* signed_token_end = signed_token_begin + signed_token.size();

    auto sep1 = std::find(signed_token_begin, signed_token_end, '.');
    if (sep1 == signed_token_end) {
        error = ParseError::invalid_jwt;
        return false;
    }
    std::size_t sep_pos = sep1 - signed_token_begin;
    auto sep2 = std::find(sep1 + 1, signed_token_end, '.');
    if (sep2 == signed_token_end) {
        error = ParseError::invalid_jwt;
        return false;
    }
    std::size_t sep2_pos = sep2 - signed_token_begin;

    // Decode signature
    if (verifier) {
        StringData signature_base64 = StringData{sep2 + 1, signed_token.size() - sep2_pos - 1};
        std::vector<char> signature_buffer;
        signature_buffer.resize(base64_decoded_size(signature_base64.size()));
        Optional<std::size_t> num_bytes_signature =
            base64_decode(signature_base64, signature_buffer.data(), signature_base64.size());
        if (!num_bytes_signature) {
            error = ParseError::invalid_base64;
            return false;
        }

        // Verify signature
        BinaryData signature{signature_buffer.data(), *num_bytes_signature};
        bool verified = verifier->verify(BinaryData{signed_token.data(), sep2_pos},
                                         signature); // Throws
        if (!verified) {
            error = ParseError::invalid_signature;
            return false;
        }
    }

    Optional<std::vector<char>> payload_vec = base64_decode_to_vector(StringData{sep1 + 1, sep2_pos - sep_pos - 1});
    if (!payload_vec) {
        error = ParseError::invalid_base64;
        return false;
    }

    StringData payload = StringData{payload_vec->data(), payload_vec->size()};

    // Parse decoded user token
    JSONParser parser{payload};
    AccessTokenParser token_parser;
    auto json_error = parser.parse(token_parser);

    if (json_error) {
        error = ParseError::invalid_json;
        return false;
    }

    token = std::move(token_parser.m_token);
    return true;
}

bool AccessToken::parse(StringData signed_token, AccessToken& token, ParseError& error, Verifier* verifier)
{
    const char* signed_token_begin = signed_token.data();
    const char* signed_token_end = signed_token_begin + signed_token.size();

    auto sep = std::find(signed_token_begin, signed_token_end, ':');
    StringData token_base64;
    StringData signature_base64;
    if (sep != signed_token_end) {
        std::size_t sep_pos = sep - signed_token_begin;
        std::size_t sig_len = signed_token.size() - sep_pos - 1;
        token_base64 = StringData{signed_token.data(), sep_pos};
        signature_base64 = StringData{signed_token.data() + sep_pos + 1, sig_len};
    }
    else {
        // Could be that we have a JWT instead of the old format
        auto jwtSep = std::find(signed_token_begin, signed_token_end, '.');
        if (jwtSep != signed_token_end) {
            return parseJWT(signed_token, token, error, verifier);
        }

        token_base64 = signed_token;
        signature_base64 = ""; // This will only ever pass verification if we're
                               // running without a public key.
    }

    // Decode user token
    std::vector<char> token_buffer;
    token_buffer.resize(base64_decoded_size(token_base64.size())); // Throws
    StringData token_2;
    {
        Optional<std::size_t> num_bytes = base64_decode(token_base64, token_buffer.data(), token_buffer.size());
        if (!num_bytes) {
            error = ParseError::invalid_base64;
            return false;
        }
        token_2 = StringData{token_buffer.data(), *num_bytes};
    }

    // Decode signature
    if (verifier) {
        std::vector<char> buffer;
        buffer.resize(base64_decoded_size(signature_base64.size()));
        Optional<std::size_t> num_bytes = base64_decode(signature_base64, buffer.data(), buffer.size());
        if (!num_bytes) {
            error = ParseError::invalid_base64;
            return false;
        }

        // Verify signature
        BinaryData signature{buffer.data(), *num_bytes};
        bool verified = verifier->verify(BinaryData{token_2.data(), token_2.size()},
                                         signature); // Throws
        if (!verified) {
            error = ParseError::invalid_signature;
            return false;
        }
    }

    // Parse decoded user token
    JSONParser parser{token_2};
    AccessTokenParser token_parser;
    auto json_error = parser.parse(token_parser);

    if (json_error) {
        error = ParseError::invalid_json;
        return false;
    }

    token = std::move(token_parser.m_token);
    return true;
}
