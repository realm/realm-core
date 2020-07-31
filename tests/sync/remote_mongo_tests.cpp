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

#include "sync/remote_mongo_collection.hpp"

#include <sstream>
#include "catch2/catch.hpp"

#ifndef REALM_ENABLE_MONGO_CLIENT_TESTS
#define REALM_ENABLE_MONGO_CLIENT_TESTS 1
#endif

#if REALM_ENABLE_MONGO_CLIENT_TESTS

namespace realm::app {
using bson::Bson;

namespace {
/**
 * Strips leading spaces off of each line, and removes the first line if empty
 * Makes multi-line R"(raw string literals)" cleaner by allowing indentation.
 */
std::string operator ""_nows(const char* str, size_t len) {
    auto stream = std::stringstream(std::string(str, len));
    auto out = std::string();
    auto first = true;
    for (auto line = std::string(); std::getline(stream, line); ) {
        if (first) {
            first = false;
            if (line.empty())
                continue;
        }

        auto leading_spaces = line.find_first_not_of(" ");
        if (leading_spaces != std::string::npos) {
            out += std::string_view(line).substr(leading_spaces);
        }
        out += '\n';
    }
    if (!out.empty())
        out.pop_back(); // remove the extra '\n' we added above

    return out;
}
}

TEST_CASE("Validate _nows helper", "[mongo]") {
    // WARNING: if you are debugging this test, be aware that catch can be inconsistent with leading whitespace when
    // printing mulit-line strings. You may want to do your own printing.

    CHECK(R"(
        hello
        mr bob
    )"_nows == "hello\nmr bob\n");

    CHECK(R"(

        hello


        mr bob

    )"_nows == "\nhello\n\n\nmr bob\n\n");

    CHECK(R"(
        hello
        mr bob)"_nows == "hello\nmr bob");

    CHECK(R"(hello
             mr bob)"_nows == "hello\nmr bob");
}

TEST_CASE("WatchStream SSE processing", "[mongo]") {
    WatchStream ws;

    SECTION("successes") {
        SECTION("empty kind") {
            ws.feed_sse({R"({"a": 1})", ""});
            REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
            CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
            CHECK(ws.state() == WatchStream::NEED_DATA);
        }
        SECTION("message kind") {
            ws.feed_sse({R"({"a": 1})", "message"});
            REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
            CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
            CHECK(ws.state() == WatchStream::NEED_DATA);
        }
        SECTION("message kind by default") {
            ws.feed_sse({R"({"a": 1})"});
            REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
            CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
            CHECK(ws.state() == WatchStream::NEED_DATA);
        }
        SECTION("two messages") {
            ws.feed_sse({R"({"a": 1})"});
            REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
            CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
            CHECK(ws.state() == WatchStream::NEED_DATA);
            ws.feed_sse({R"({"a": 2})"});
            REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
            CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 2})"));
            CHECK(ws.state() == WatchStream::NEED_DATA);
        }
        SECTION("unknown kinds are ignored") {
            ws.feed_sse({R"({"a": 1})", "ignoreme"});
            REQUIRE(ws.state() == WatchStream::NEED_DATA);
            ws.feed_sse({R"({"a": 2})"});
            REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
            CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 2})"));
            CHECK(ws.state() == WatchStream::NEED_DATA);
        }
        SECTION("percent encoding (all valid)") {
            // Note that %0A and %0D are both whitespace control characters,
            // so they are not allowed to appear in json strings, and are
            // ignored like whitespace during parsing. The error section
            // provides more coverage for them.
            ws.feed_sse({R"({"a": "%25" %0A %0D })"});
            REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
            CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": "%"})"));
            CHECK(ws.state() == WatchStream::NEED_DATA);
        }
        SECTION("percent encoding (some invalid)") {
            // Unknown % sequences are ignored.
            ws.feed_sse({R"({"a": "%25 %26%" %0A %0D })"});
            REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
            CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": "% %26%"})"));
            CHECK(ws.state() == WatchStream::NEED_DATA);
        }
    }

    SECTION("errors") {
        SECTION("well-formed server error") {
            SECTION("simple") {
                ws.feed_sse({R"({"error_code": "BadRequest", "error": ":("})", "error"});
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(ServiceErrorCode::bad_request));
                CHECK(ws.error().message == ":(");
            }
            SECTION("reading error doesn't consume it") {
                ws.feed_sse({R"({"error_code": "BadRequest", "error": ":("})", "error"});
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(ServiceErrorCode::bad_request));
                CHECK(ws.error().message == ":(");
                // Above is same as "simple" SECTION.
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(ServiceErrorCode::bad_request));
                CHECK(ws.error().message == ":(");
            }
            SECTION("with unknown code") {
                ws.feed_sse({R"({"error_code": "WhoKnows", "error": ":("})", "error"});
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(ServiceErrorCode::unknown));
                CHECK(ws.error().message == ":(");
            }
            SECTION("percent encoding") {
                ws.feed_sse({R"({"error_code": "BadRequest", "error": "100%25 failure"})", "error"});
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(ServiceErrorCode::bad_request));
                CHECK(ws.error().message == "100% failure");
            }
            SECTION("extra field") {
                ws.feed_sse({R"({"bonus": "field", "error_code": "BadRequest", "error": ":("})", "error"});
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(ServiceErrorCode::bad_request));
                CHECK(ws.error().message == ":(");
            }
        }
        SECTION("malformed server error") {
            SECTION("invalid json") {
                ws.feed_sse({R"({"no closing: "}")", "error"});
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(ServiceErrorCode::unknown));
                CHECK(ws.error().message == R"({"no closing: "}")");
            }
            SECTION("missing error") {
                ws.feed_sse({R"({"error_code": "BadRequest"})", "error"});
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(ServiceErrorCode::unknown));
                CHECK(ws.error().message == R"({"error_code": "BadRequest"})");
            }
            SECTION("missing error_code") {
                ws.feed_sse({R"({"error": ":("})", "error"});
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(ServiceErrorCode::unknown));
                CHECK(ws.error().message == R"({"error": ":("})");
            }
            SECTION("error wrong type") {
                ws.feed_sse({R"({"error_code": "BadRequest", "error": 1})", "error"});
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(ServiceErrorCode::unknown));
                CHECK(ws.error().message == R"({"error_code": "BadRequest", "error": 1})");
            }
            SECTION("error_code wrong type") {
                ws.feed_sse({R"({"error_code": 1, "error": ":("})", "error"});
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(ServiceErrorCode::unknown));
                CHECK(ws.error().message == R"({"error_code": 1, "error": ":("})");
            }
            SECTION("not an object") {
                ws.feed_sse({R"("I'm just a string in the world")", "error"});
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(ServiceErrorCode::unknown));
                CHECK(ws.error().message == R"("I'm just a string in the world")");
            }
            SECTION("a lot of percent encoding") {
                // Note, trailing % is a special case that should be preserved if more is added.
                ws.feed_sse({R"(%25%26%0A%0D%)", "error"});
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(ServiceErrorCode::unknown));
                CHECK(ws.error().message == "%%26\n\r%"); // NOTE: not a raw string so has real CR and LF bytes.
            }
        }
        SECTION("malformed ordinary event") {
            SECTION("invalid json") {
                ws.feed_sse({R"({"no closing: "}")"});
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(JSONErrorCode::bad_bson_parse));
                CHECK(ws.error().message == R"(server returned malformed event: {"no closing: "}")");
            }
            SECTION("not an object") {
                ws.feed_sse({R"("I'm just a string in the world")"});
                REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
                CHECK(ws.error().error_code == make_error_code(JSONErrorCode::bad_bson_parse));
                CHECK(ws.error().message == R"(server returned malformed event: "I'm just a string in the world")");
            }
        }
    }
}

// Defining a shorthand so that it is less disruptive to put this after every line.
#define REQ_ND REQUIRE(ws.state() == WatchStream::NEED_DATA)

TEST_CASE("WatchStream line processing", "[mongo]") {
    WatchStream ws;

    SECTION("simple") {
        ws.feed_line(R"(event: message)");
        REQ_ND;
        ws.feed_line(R"(data: {"a": 1})");
        REQ_ND;
        ws.feed_line(R"()");
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("with LF") {
        ws.feed_line(R"(event: message)" "\n");
        REQ_ND;
        ws.feed_line(R"(data: {"a": 1})" "\n");
        REQ_ND;
        ws.feed_line(R"()" "\n");
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("with CR") {
        ws.feed_line(R"(event: message)" "\r");
        REQ_ND;
        ws.feed_line(R"(data: {"a": 1})" "\r");
        REQ_ND;
        ws.feed_line(R"()" "\r");
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("with CRLF") {
        ws.feed_line(R"(event: message)" "\r\n");
        REQ_ND;
        ws.feed_line(R"(data: {"a": 1})" "\r\n");
        REQ_ND;
        ws.feed_line(R"()" "\r\n");
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("no space") {
        ws.feed_line(R"(event:message)");
        REQ_ND;
        ws.feed_line(R"(data:{"a": 1})");
        REQ_ND;
        ws.feed_line(R"()");
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("only last event kind used") {
        ws.feed_line(R"(event: error)");
        REQ_ND;
        ws.feed_line(R"(data: {"a": 1})");
        REQ_ND;
        ws.feed_line(R"(event: gibberish)");
        REQ_ND;
        ws.feed_line(R"(event: message)");
        REQ_ND;
        ws.feed_line(R"()");
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("multiple") {
        ws.feed_line(R"(event: message)");
        REQ_ND;
        ws.feed_line(R"(data: {"a": 1})");
        REQ_ND;
        ws.feed_line(R"()");
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
        ws.feed_line(R"(event:message)");
        REQ_ND;
        ws.feed_line(R"(data:{"a": 2})");
        REQ_ND;
        ws.feed_line(R"()");
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 2})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("multiple with implicit event kind") {
        ws.feed_line(R"(data: {"a": 1})");
        REQ_ND;
        ws.feed_line(R"()");
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
        ws.feed_line(R"(data:{"a": 2})");
        REQ_ND;
        ws.feed_line(R"()");
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 2})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("data spread over multiple lines") {
        ws.feed_line(R"(data: {"a")");
        REQ_ND;
        ws.feed_line(R"(data::)");
        REQ_ND;
        ws.feed_line(R"(data: 1})");
        ws.feed_line(R"()");
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("comments ignored") {
        ws.feed_line(R"(:)");
        REQ_ND;
        ws.feed_line(R"(data: {"a")");
        REQ_ND;
        ws.feed_line(R"(:)");
        REQ_ND;
        ws.feed_line(R"(data::)");
        REQ_ND;
        ws.feed_line(R"(:)");
        REQ_ND;
        ws.feed_line(R"(data: 1})");
        REQ_ND;
        ws.feed_line(R"(:)");
        REQ_ND;
        ws.feed_line(R"()");
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("unknown fields ignored") {
        ws.feed_line(R"(hmm: thinking)");
        REQ_ND;
        ws.feed_line(R"(data: {"a")");
        REQ_ND;
        ws.feed_line(R"(id: 12345)"); // id is a part of the spec we don't use
        REQ_ND;
        ws.feed_line(R"(data::)");
        REQ_ND;
        ws.feed_line(R"(retry: 12345)"); // retry is a part of the spec we don't use
        REQ_ND;
        ws.feed_line(R"(data: 1})");
        REQ_ND;
        ws.feed_line(R"(lines with no colon are treated as all field and ignored)");
        REQ_ND;
        ws.feed_line(R"()");
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("events without data are ignored") {
        ws.feed_line(R"(event: message)");
        REQ_ND;
        ws.feed_line(R"()"); // noop dispatch
        REQ_ND;
        ws.feed_line(R"(event: error)");
        REQ_ND;
        ws.feed_line(R"()"); // noop dispatch
        REQ_ND;
        // Note, because prior events are ignored, this is treated as if there was no event kind, so it uses the
        // default "message" kind
        ws.feed_line(R"(data: {"a": 1})");
        REQ_ND;
        ws.feed_line(R"()");
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("new line handling (prestripped)") {
        // since newlines are ignored in json, this tests using the mal-formed error case
        ws.feed_line(R"(event: error)");
        REQ_ND;
        ws.feed_line(R"(data: this error)");
        REQ_ND;
        ws.feed_line(R"(data:  has three lines)");
        REQ_ND;
        ws.feed_line(R"(data:  but only two LFs)");
        REQ_ND;
        ws.feed_line(R"()");
        REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
        CHECK(ws.error().message == "this error\n has three lines\n but only two LFs");
    }
    SECTION("new line handling (LF)") {
        // since newlines are ignored in json, this tests using the mal-formed error case
        ws.feed_line(R"(event: error)" "\n");
        REQ_ND;
        ws.feed_line(R"(data: this error)" "\n");
        REQ_ND;
        ws.feed_line(R"(data:  has three lines)" "\n");
        REQ_ND;
        ws.feed_line(R"(data:  but only two LFs)" "\n");
        REQ_ND;
        ws.feed_line(R"()" "\n");
        REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
        CHECK(ws.error().message == "this error\n has three lines\n but only two LFs");
    }
    SECTION("new line handling (CR)") {
        // since newlines are ignored in json, this tests using the mal-formed error case
        ws.feed_line(R"(event: error)" "\r");
        REQ_ND;
        ws.feed_line(R"(data: this error)" "\r");
        REQ_ND;
        ws.feed_line(R"(data:  has three lines)" "\r");
        REQ_ND;
        ws.feed_line(R"(data:  but only two LFs)" "\r");
        REQ_ND;
        ws.feed_line(R"()" "\r");
        REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
        CHECK(ws.error().message == "this error\n has three lines\n but only two LFs");
    }
    SECTION("new line handling (CRLF)") {
        // since newlines are ignored in json, this tests using the mal-formed error case
        ws.feed_line(R"(event: error)" "\r\n");
        REQ_ND;
        ws.feed_line(R"(data: this error)" "\r\n");
        REQ_ND;
        ws.feed_line(R"(data:  has three lines)" "\r\n");
        REQ_ND;
        ws.feed_line(R"(data:  but only two LFs)" "\r\n");
        REQ_ND;
        ws.feed_line(R"()" "\r\n");
        REQUIRE(ws.state() == WatchStream::HAVE_ERROR);
        CHECK(ws.error().message == "this error\n has three lines\n but only two LFs");
    }
}

TEST_CASE("WatchStream buffer processing", "[mongo]") {
    WatchStream ws;

    SECTION("simple") {
        ws.feed_buffer(R"(
            event: message
            data: {"a": 1}

            )"_nows);
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("multi") {
        ws.feed_buffer(R"(
            event: message
            data: {"a": 1}

            )"_nows);
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        REQUIRE(ws.state() == WatchStream::NEED_DATA);
        ws.feed_buffer(R"(
            event: message
            data: {"a": 2}

            )"_nows);
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 2})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("multi in one buffer") {
        ws.feed_buffer(R"(
            event: message
            data: {"a": 1}

            event: message
            data: {"a": 2}

            )"_nows);
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 2})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }
    SECTION("partial lines") {
        ws.feed_buffer(R"(
            event: message
            data: {"a":)"_nows);
        REQ_ND;
        ws.feed_buffer(R"(
            1)"_nows);
        REQ_ND;
        ws.feed_buffer(R"(
            }

            )"_nows);
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }

    SECTION("multi and partial lines") {
        ws.feed_buffer(R"(
            event: message
            data: {"a": 1}

            event: message
            data: {"a":)"_nows);
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
        REQUIRE(ws.state() == WatchStream::NEED_DATA);
        ws.feed_buffer(R"(
            2)"_nows);
        REQ_ND;
        ws.feed_buffer(R"(
            }

            event: message
            data: {"a": 3}

            )"_nows);
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 2})"));
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 3})"));
        CHECK(ws.state() == WatchStream::NEED_DATA);
    }

    SECTION("CR alone isn't treated as a newline") {
        // This is a deviation from the spec. We do not support the legacy macOS < 10 CR-only newlines.
        // The server does not generate them, and there would be some overhead to supporting them.
        ws.feed_buffer("event: message\rdata: {\"a\": 1}\r\r");
#if true
        // This is what we do.
        CHECK(ws.state() == WatchStream::NEED_DATA);
#else
        // This is what we would do if following the spec.
        REQUIRE(ws.state() == WatchStream::HAVE_EVENT);
        CHECK(Bson(ws.next_event()) == bson::parse(R"({"a": 1})"));
#endif
    }
}
}

#endif
