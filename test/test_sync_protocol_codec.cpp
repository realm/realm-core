/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include "testsettings.hpp"
#include "test.hpp"

#include <realm/sync/noinst/protocol_codec.hpp>

#include <string>
#include <memory>

using namespace realm;
using namespace realm::util;

using TestContext = test_util::unit_test::TestContext;

template <class A, class B>
void compare_out_string(const A& expected, const B& out, TestContext& test_context)
{
    CHECK_EQUAL(std::string_view(expected.data(), expected.size()), std::string_view(out.data(), out.size()));
}

TEST(Protocol_Codec_Bind_PBS)
{
    auto protocol = _impl::ClientProtocol();
    std::string expected_out_string;
    auto out = _impl::ClientProtocol::OutputBuffer();

    expected_out_string = "bind 888234 0 5 1 0\ntoken";
    protocol.make_pbs_bind_message(7, out, 888234, std::string{}, "token", true, false);
    compare_out_string(expected_out_string, out, test_context);

    out.reset();
    expected_out_string = "bind 999123 11 12 0 1\nserver/pathtoken_string";
    protocol.make_pbs_bind_message(8, out, 999123, "server/path", "token_string", false, true);
    compare_out_string(expected_out_string, out, test_context);
}

TEST(Protocol_Codec_Bind_FLX)
{
    auto protocol = _impl::ClientProtocol();
    std::string expected_out_string;
    auto out = _impl::ClientProtocol::OutputBuffer();

    auto json_data = nlohmann::json();
    json_data["valA"] = 123;
    json_data["valB"] = "something";

    out.reset();
    expected_out_string = "bind 345888 0 6 1 0\ntoken2";
    protocol.make_flx_bind_message(8, out, 345888, {}, "token2", true, false);
    compare_out_string(expected_out_string, out, test_context);

    out.reset();
    expected_out_string = "bind 456888 31 7 0 1\n{\"valA\":123,\"valB\":\"something\"}token21";
    protocol.make_flx_bind_message(8, out, 456888, json_data, "token21", false, true);
    compare_out_string(expected_out_string, out, test_context);
}

TEST(Protocol_Codec_Ident_PBS)
{
    auto protocol = _impl::ClientProtocol();
    auto out = _impl::ClientProtocol::OutputBuffer();

    auto file_ident = _impl::ClientProtocol::SaltedFileIdent{999123, 123999};
    auto progress = _impl::ClientProtocol::SyncProgress{{1, 2}, {3, 4}, {5, 6}};

    std::string expected_out_string = "ident 234888 999123 123999 3 4 1 2\n";
    protocol.make_pbs_ident_message(out, 234888, file_ident, progress);
    compare_out_string(expected_out_string, out, test_context);
}

TEST(Protocol_Codec_Ident_FLX)
{
    auto protocol = _impl::ClientProtocol();
    auto out = _impl::ClientProtocol::OutputBuffer();

    auto file_ident = _impl::ClientProtocol::SaltedFileIdent{999234, 234999};
    auto progress = _impl::ClientProtocol::SyncProgress{{3, 4}, {5, 6}, {7, 8}};
    std::string query_string = "{\"table\": \"(key == \"value\")\"}";

    std::string expected_out_string = "ident 888234 999234 234999 5 6 3 4 3 29\n{\"table\": \"(key == \"value\")\"}";
    protocol.make_flx_ident_message(out, 888234, file_ident, progress, 3, query_string);
    compare_out_string(expected_out_string, out, test_context);
}

TEST(Protocol_Codec_Query_Change)
{
    auto protocol = _impl::ClientProtocol();
    auto out = _impl::ClientProtocol::OutputBuffer();

    std::string expected_out_string = "query 238881 5 26\n{\"table\": \"(key < value)\"}";
    protocol.make_query_change_message(out, 238881, 5, "{\"table\": \"(key < value)\"}");
    compare_out_string(expected_out_string, out, test_context);
}

TEST(Protocol_Codec_JSON_Error)
{
    auto protocol = _impl::ClientProtocol();
    auto out = _impl::ClientProtocol::OutputBuffer();

    auto json_data = nlohmann::json();
    json_data["valA"] = 123;
    json_data["valB"] = "something";
    std::string json_string = json_data.dump();

    std::string expected_out_string = "json_error 9099 31 234888\n{\"valA\":123,\"valB\":\"something\"}";
    protocol.make_json_error_message(out, 234888, 9099, json_string);
    compare_out_string(expected_out_string, out, test_context);
}

TEST(Protocol_Codec_Test_Command)
{
    auto protocol = _impl::ClientProtocol();
    auto out = _impl::ClientProtocol::OutputBuffer();

    std::string expected_out_string = "test_command 234888 1000 17\nsome test command";
    protocol.make_test_command_message(out, 234888, 1000, "some test command");
    compare_out_string(expected_out_string, out, test_context);
}

TEST(Protocol_Codec_Upload)
{
    auto protocol = _impl::ClientProtocol();
    auto out = _impl::ClientProtocol::OutputBuffer();
    {
        auto upload_message_builder = protocol.make_upload_message_builder(); // Throws
        std::string data1 = "AABBCCDDEEFFGGHHIIJJKKLLMMNNOOPP";
        std::string data2 = "EEFFGGHHIIJJKKLLMMNNOOPPQQRRSSTT";

        std::string expected_out_string =
            "upload 999123 0 122 0 30 17 10\n29 18 259604001718 888123 32 AABBCCDDEEFFGGHHIIJJKKLLMMNNOOPP30 19 "
            "259604001850 888234 32 EEFFGGHHIIJJKKLLMMNNOOPPQQRRSSTT";
        upload_message_builder.add_changeset(29, 18, 259604001718, 888123, BinaryData(data1.c_str(), data1.length()));
        upload_message_builder.add_changeset(30, 19, 259604001850, 888234, BinaryData(data2.c_str(), data2.length()));
        upload_message_builder.make_upload_message(7, out, 999123, 30, 17, 10);
        compare_out_string(expected_out_string, out, test_context);
    }

    {
        out.reset();
        auto upload_message_builder = protocol.make_upload_message_builder(); // Throws
        // Create a changeset that exceeds the compression threshold (1024 bytes)
        std::string data1 = std::string(512, 'A') + std::string(512, 'B') + std::string(512, 'C');
        std::string data2 = std::string(util::format("4 2 259609999999 123999 %1 ", data1.length())) + data1;

        std::vector<char> expected_data;
        std::vector<char> compressed;
        util::compression::CompressMemoryArena cmp_memory_arena;
        CHECK_NOT(
            util::compression::allocate_and_compress(cmp_memory_arena, {data2.c_str(), data2.length()}, compressed));
        std::string expected_out_string =
            util::format("upload 888123 1 %1 %2 4 2 0\n", data2.length(), compressed.size());
        expected_data.insert(expected_data.begin(), expected_out_string.begin(), expected_out_string.end());
        expected_data.insert(expected_data.end(), compressed.begin(), compressed.end());

        upload_message_builder.add_changeset(4, 2, 259609999999, 123999, BinaryData(data1.c_str(), data1.size()));
        upload_message_builder.make_upload_message(7, out, 888123, 4, 2, 0);
        compare_out_string(expected_data, out, test_context);

        // Find the compressed changeset and uncompress - first look for the newline
        auto out_span = out.as_span();
        int nl_break = [&out_span] {
            int i = 0;
            for (auto pos = out_span.begin(); pos != out_span.end(); ++pos, i++) {
                if (*pos == '\n')
                    return i;
            }
            return -1;
        }();
        CHECK_GREATER_EQUAL(nl_break, 0);

        // create a new span with the changeset contents
        auto changeset = out_span.last(out_span.size() - (nl_break + 1));
        CHECK_EQUAL(changeset.size(), compressed.size());
        Buffer<char> decompressed_buf(data2.length());
        CHECK_NOT(util::compression::decompress(changeset, decompressed_buf));
        compare_out_string(data2, decompressed_buf, test_context);
    }
}

TEST(Protocol_Codec_Unbind)
{
    auto protocol = _impl::ClientProtocol();
    auto out = _impl::ClientProtocol::OutputBuffer();

    std::string expected_out_string = "unbind 234888\n";
    protocol.make_unbind_message(out, 234888);
    compare_out_string(expected_out_string, out, test_context);
}

TEST(Protocol_Codec_Mark)
{
    auto protocol = _impl::ClientProtocol();
    auto out = _impl::ClientProtocol::OutputBuffer();

    std::string expected_out_string = "mark 234888 888234\n";
    protocol.make_mark_message(out, 234888, 888234);
    compare_out_string(expected_out_string, out, test_context);
}

TEST(Protocol_Codec_Ping)
{
    auto protocol = _impl::ClientProtocol();
    auto out = _impl::ClientProtocol::OutputBuffer();

    std::string expected_out_string = "ping 1234567890 23\n";
    protocol.make_ping(out, 1234567890, 23);
    compare_out_string(expected_out_string, out, test_context);
}
