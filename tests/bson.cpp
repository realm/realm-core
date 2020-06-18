/*************************************************************************
*
* Copyright 2020 Realm Inc.
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

#include <stdio.h>
#include <json.hpp>
#include <string_view>

#include "catch2/catch.hpp"
#include "util/test_utils.hpp"
#include "util/test_file.hpp"
#include "util/bson/bson.hpp"

using namespace nlohmann;
using namespace realm;
using namespace bson;

using namespace std::string_view_literals;

static inline std::string remove_whitespace(const char* c) {
    std::string str(c);
    str.erase(std::remove_if(str.begin(), str.end(), isspace), str.end());
    return str;
}

/**
 ======== BSON CORPUS ========
 */
template <typename T>
using CorpusCheck = void(*)(T);

template <typename T>
struct CorpusEntry {
    const char* canonical_extjson;
    CorpusCheck<T> check;
    bool lossy;
};

template <typename T>
static inline void run_corpus(const char* test_key, const CorpusEntry<T>& entry) {
    std::string canonical_extjson = remove_whitespace(entry.canonical_extjson);
    auto val = static_cast<BsonDocument>(bson::parse(canonical_extjson));
    auto& test_value = val[test_key];
    REQUIRE(bson::holds_alternative<T>(test_value));
    entry.check((T)test_value);
    if (!entry.lossy) {
        std::stringstream s;
        s << val;
        CHECK(s.str() == canonical_extjson);
    }
}

TEST_CASE("canonical_extjson_fragments", "[bson]") {
    SECTION("Array") {
        auto const b = bson::parse("[]");
        auto const array = static_cast<BsonArray>(b);
        CHECK(array.empty());
    }

    SECTION("Array with Object") {
        auto const b = bson::parse("[{\"a\": \"foo\"}]");
        auto const array = static_cast<BsonArray>(b);
        CHECK(array.size() == 1);
        auto doc = static_cast<BsonDocument>(array[0]);
        CHECK(static_cast<std::string>(doc["a"]) == "foo");
    }

    SECTION("Null") {
        auto const b = bson::parse("null");
        CHECK(bson::holds_alternative<util::None>(b));
    }

    SECTION("String") {
        auto const b = bson::parse("\"foo\"");
        auto const str = static_cast<std::string>(b);
        CHECK(str == "foo");
    }

    SECTION("Boolean") {
        auto b = bson::parse("true");
        auto boolean = static_cast<bool>(b);
        CHECK(boolean);

        b = bson::parse("false");
        boolean = static_cast<bool>(b);
        CHECK(!boolean);
    }
}

TEST_CASE("canonical_extjson_corpus", "[bson]") {
    SECTION("Array") {
        SECTION("Empty") {
            run_corpus<BsonArray>("a", {
                "{\"a\" : []}",
                [](auto val) { CHECK(val.empty()); }
            });
        }
        SECTION("Single Element Array") {
            run_corpus<BsonArray>("a", {
                "{\"a\" : [{\"$numberInt\": \"10\"}]}",
                [](auto val) { CHECK((int32_t)val[0] == 10); }
            });
        }
        SECTION("Single Element Boolean Array") {
            run_corpus<BsonArray>("a", {
                "{\"a\" : [true]}",
                [](auto val) { CHECK((bool)val[0]); }
            });
        }
        SECTION("Multi Element Array") {
            run_corpus<BsonArray>("a", {
                "{\"a\" : [{\"$numberInt\": \"10\"}, {\"$numberInt\": \"20\"}]}",
                [](auto val) {
                    CHECK((int32_t)val[0] == 10);
                    CHECK((int32_t)val[1] == 20);
                }
            });
        }
    }

    SECTION("Binary") {
        SECTION("subtype 0x00 (Zero-length)") {
            run_corpus<std::vector<char>>("x", {
                "{\"x\" : { \"$binary\" : {\"base64\" : \"\", \"subType\" : \"00\"}}}",
                [](auto val) { CHECK(val == std::vector<char>()); }
            });
        }
        SECTION("subtype 0x00 (Zero-length, keys reversed)") {
            run_corpus<std::vector<char>>("x", {

                "{\"x\" : { \"$binary\" : {\"base64\" : \"\", \"subType\" : \"00\"}}}",
                [](auto val) { CHECK(val == std::vector<char>()); }
            });
        }

        SECTION("subtype 0x00") {
            run_corpus<std::vector<char>>("x", {
                "{\"x\" : { \"$binary\" : {\"base64\" : \"//8=\", \"subType\" : \"00\"}}}",
                [](auto val) {
                    std::string bin = "//8=";
                    CHECK(val == std::vector<char>(bin.begin(), bin.end()));
                }
            });
        }
    }

    SECTION("Boolean") {
        SECTION("True") {
            run_corpus<bool>("b", {
                "{\"b\" : true}",
                [](auto val) { CHECK(val); }
            });
        }

        SECTION("False") {
            run_corpus<bool>("b", {
                "{\"b\" : false}",
                [](auto val) { CHECK(!val); }
            });
        }
    }

    SECTION("DateTime") {
        SECTION("epoch") {
            run_corpus<realm::Timestamp>("a", {
                "{\"a\" : {\"$date\" : {\"$numberLong\" : \"0\"}}}",
                [](auto val) {
                    CHECK(val.get_seconds() == 0);
                    CHECK(val.get_nanoseconds() == 0);
                }
            });
        }
        SECTION("positive ms") {
            run_corpus<realm::Timestamp>("a", {
                "{\"a\" : {\"$date\" : {\"$numberLong\" : \"1356351330501\"}}}",
                [](auto val) {
                    CHECK(val.get_seconds() == 1356351330501 / 1000);
                    CHECK(val.get_nanoseconds() == 501000000);
                }
            });
        }
        SECTION("negative") {
            run_corpus<realm::Timestamp>("a", {
                "{\"a\" : {\"$date\" : {\"$numberLong\" : \"-284643869501\"}}}",
                [](auto val) {
                    CHECK(val.get_seconds() == -284643869501 / 1000);
                    CHECK(val.get_nanoseconds() == -501000000);
                }
            });
        }
        SECTION("Y10K") {
            run_corpus<realm::Timestamp>("a", {
                "{\"a\":{\"$date\":{\"$numberLong\":\"253402300800000\"}}}",
                [](auto val) {
                    CHECK(val.get_seconds() == 253402300800000 / 1000);
                    CHECK(val.get_nanoseconds() == 0);
                }
            });
        };
    }

    SECTION("Decimal") {
        SECTION("Special - Canonical NaN") {
            run_corpus<Decimal128>("d", {
                "{\"d\" : {\"$numberDecimal\" : \"NaN\"}}",
                [](auto val) { CHECK(val.is_nan());  }
            });
        }

        SECTION("Special - Canonical Positive Infinity") {
            run_corpus<Decimal128>("d", {
                "{\"d\" : {\"$numberDecimal\" : \"Infinity\"}}",
                [](auto val) { CHECK(val == Decimal128("Infinity")); }
            });
        }
        SECTION("Special - Canonical Negative Infinity") {
            run_corpus<Decimal128>("d", {
                "{\"d\" : {\"$numberDecimal\" : \"-Infinity\"}}",
                [](auto val) { CHECK(val == Decimal128("-Infinity")); }
            });
        }
        SECTION("Regular - Smallest") {
            run_corpus<Decimal128>("d", {
                "{\"d\" : {\"$numberDecimal\" : \"1.234E-3\"}}",
                [](auto val) { CHECK(val == Decimal128("0.001234")); }
            });
        }

        SECTION("Regular - 0.1") {
            run_corpus<Decimal128>("d", {
                "{\"d\" : {\"$numberDecimal\" : \"1E-1\"}}",
                [](auto val) { CHECK(val == Decimal128("0.1")); }
            });
        };
    }

    SECTION("Document") {
        SECTION("Empty subdoc") {
            run_corpus<BsonDocument>("x", {
                "{\"x\" : {}}",
                [](auto val) { CHECK(val.empty()); }
            });
        }
        SECTION("Empty-string key subdoc") {
            run_corpus<BsonDocument>("x", {
                "{\"x\" : {\"\" : \"b\"}}",
                [](auto val) { CHECK((std::string)val[""] == "b"); }
            });
        }
        SECTION("Single-character key subdoc") {
            run_corpus<BsonDocument>("x", {
                "{\"x\" : {\"a\" : \"b\"}}",
                [](auto val) { CHECK((std::string)val["a"] == "b"); }
            });
        }
        SECTION("Special characters in field name") {
            run_corpus<BsonDocument>("x", {
                R"({"x" : {">\n\t\u0002\\\"<" : "b"}})",
                [](auto val) { CHECK((std::string)val[">\n\t\x02\\\"<"] == "b"); }
            });
        }
        SECTION("Nested Objects") {
            run_corpus<BsonDocument>("x", {
                R"({"x": {"value": {"hello": "world", "_id": {"$oid": "5ec38e1e693f9e61e968f701"}}}})",
                [](auto val) {
                    CHECK(val.size() == 1);
                    auto inner = val["value"].operator const BsonDocument&();
                    CHECK(inner.size() == 2);
                    CHECK(inner["hello"].operator const std::string&() == "world");
                    CHECK(inner["_id"].operator ObjectId() == ObjectId("5ec38e1e693f9e61e968f701"));
                }
            });
        }
        SECTION("Nested Objects 2") {
            run_corpus<BsonDocument>("x", {
                R"({"x": {"value": {"hello": {"$numberInt": "42"}}}})",
                [](auto val) {
                    CHECK(val.size() == 1);
                    auto inner = val["value"].operator const BsonDocument&();
                    CHECK(inner.size() == 1);
                    CHECK(inner["hello"].operator int32_t() == 42);
                }
            });
        }
        SECTION("Nested Objects 3") {
            run_corpus<BsonDocument>("x", {
                R"({"x": {"value": {"hello": "world"}}})",
                [](auto val) {
                    CHECK(val.size() == 1);
                    auto inner = val["value"].operator const BsonDocument&();
                    CHECK(inner.size() == 1);
                    CHECK(inner["hello"].operator const std::string&() == "world");
                }
            });
        }
        SECTION("Nested Objects 3") {
            run_corpus<BsonDocument>("x", {
                R"({"x": {"value": {"hello": "world", "hello_2": "world_2"}}})",
                [](auto val) {
                    CHECK(val.size() == 1);
                    auto inner = val["value"].operator const BsonDocument&();
                    CHECK(inner.size() == 2);
                    CHECK(inner["hello"].operator const std::string&() == "world");
                    CHECK(inner["hello_2"].operator const std::string&() == "world_2");
                }
            });
        }
        SECTION("Nested Array Empty Objects") {
            run_corpus<BsonArray>("value", {
                "{\"value\": [ {}, {} ] }",
                [](auto val) {;
                    CHECK(static_cast<BsonDocument>(val[0]).size() == 0);
                    CHECK(static_cast<BsonDocument>(val[1]).size() == 0);
                }
            });
        }
        SECTION("Doubly Nested Array") {
            run_corpus<BsonArray>("value", {
                "{\"value\": [ [ {\"$numberInt\": \"1\"}, true, {\"$numberInt\": \"3\"} ] ] }",
                [](auto val) {;
                    const BsonArray sub_array = static_cast<BsonArray>(val[0]);
                    CHECK(sub_array.size() == 3);
                    CHECK(sub_array[0] == 1);
                    CHECK(sub_array[1] == true);
                    CHECK(sub_array[2] == 3);
                }
            });
        }
        SECTION("Doubly Nested Array 2") {
            run_corpus<BsonArray>("value", {
                "{\"value\": [ [ {\"$numberInt\": \"1\"}, \"Realm\", {\"$numberInt\": \"3\"} ] ] }",
                [](auto val) {;
                    const BsonArray sub_array = static_cast<BsonArray>(val[0]);
                    CHECK(sub_array.size() == 3);
                    CHECK(sub_array[0] == 1);
                    CHECK(sub_array[1] == "Realm");
                    CHECK(sub_array[2] == 3);
                }
            });
        }
        SECTION("Doubly Nested Array 3") {
            run_corpus<BsonArray>("value", {
                "{\"value\": [ {\"KEY\": \"666\"}, {\"KEY\": \"666\"}, {}] }",
                [](auto val) {;
                    CHECK(val.size() == 3);
                    CHECK(val[0] == BsonDocument({{"KEY", "666"}}));
                    CHECK(val[1] == BsonDocument({{"KEY", "666"}}));
                    CHECK(val[2] == BsonDocument());
                }
            });
        }
    }

    SECTION("Double type") {
        static float epsilon = 0.000000001;

        SECTION("+1.0") {
            run_corpus<double>("d", {
                "{\"d\" : {\"$numberDouble\": \"1\"}}",
                [](auto val) { CHECK(abs(val - 1.0) < epsilon); }
            });
        }
        SECTION("-1.0") {
            run_corpus<double>("d", {
                "{\"d\" : {\"$numberDouble\": \"-1\"}}",
                [](auto val) { CHECK(abs(val - -1.0) < epsilon); }
            });
        }
        SECTION("+1.0001220703125") {
            run_corpus<double>("d", {
                "{\"d\" : {\"$numberDouble\": \"1.0001220703125\"}}",
                [](auto val) { CHECK(abs(val - 1.0001220703125) < epsilon); },
                true
            });
        }
        SECTION("-1.0001220703125") {
            run_corpus<double>("d", {
                "{\"d\" : {\"$numberDouble\": \"-1.0001220703125\"}}",
                [](auto val) { CHECK(abs(val - -1.0001220703125) < epsilon); },
                true
            });
        }
        SECTION("1.2345678921232E+18") {
            run_corpus<double>("d", {
                "{\"d\" : {\"$numberDouble\": \"1.2345678921232E+18\"}}",
                [](auto val) { CHECK(abs(val - 1.2345678921232E+18) < epsilon); },
                true
            });
        }
        SECTION("-1.2345678921232E+18") {
            run_corpus<double>("d", {
                "{\"d\" : {\"$numberDouble\": \"-1.2345678921232E+18\"}}",
                [](auto val) { CHECK(abs(val - -1.2345678921232E+18) < epsilon); },
                true
            });
        }
        SECTION("0.0") {
            run_corpus<double>("d", {
                "{\"d\" : {\"$numberDouble\": \"0\"}}",
                [](auto val) { CHECK(abs(val - 0.0) < epsilon); }

            });
        }
        SECTION("-0.0") {
            run_corpus<double>("d", {
                "{\"d\" : {\"$numberDouble\": \"-0\"}}",
                [](auto val) { CHECK(abs(val - -0.0) < epsilon); }
            });
        }
        SECTION("NaN") {
            run_corpus<double>("d", {
                "{\"d\": {\"$numberDouble\": \"NaN\"}}",
                [](auto val) { CHECK(std::isnan(val)); }
            });
        }
        SECTION("Inf") {
            run_corpus<double>("d", {
                "{\"d\": {\"$numberDouble\": \"Infinity\"}}",
                [](auto val) { CHECK(val == std::numeric_limits<double>::infinity()); }
            });
        }
        SECTION("-Inf") {
            run_corpus<double>("d", {
                "{\"d\": {\"$numberDouble\": \"-Infinity\"}}",
                [](auto val) { CHECK(val == (-1 * std::numeric_limits<double>::infinity())); }
            });
        }
    }

    SECTION("Int32 type") {
        SECTION("MinValue") {
            run_corpus<int32_t>("i", {
                "{\"i\" : {\"$numberInt\": \"-2147483648\"}}",
                [](auto val) { CHECK(val == -2147483648); }
            });
        }
        SECTION("MaxValue") {
            run_corpus<int32_t>("i", {
                "{\"i\" : {\"$numberInt\": \"2147483647\"}}",
                [](auto val) { CHECK(val == 2147483647); }
            });
        }
        SECTION("-1") {
            run_corpus<int32_t>("i", {
                "{\"i\" : {\"$numberInt\": \"-1\"}}",
                [](auto val) { CHECK(val == -1); }
            });
        }
        SECTION("0") {
            run_corpus<int32_t>("i", {
                "{\"i\" : {\"$numberInt\": \"0\"}}",
                [](auto val) { CHECK(val == 0); }
            });
        }
        SECTION("1") {
            run_corpus<int32_t>("i", {
                "{\"i\" : {\"$numberInt\": \"1\"}}",
                [](auto val) { CHECK(val == 1); }
            });
        }
    }

    SECTION("Int64 type") {
        SECTION("MinValue") {
            run_corpus<int64_t>("a", {
                "{\"a\" : {\"$numberLong\" : \"-9223372036854775808\"}}",
                [](auto val) { CHECK(val == LLONG_MIN); }
            });
        }
        SECTION("MaxValue") {
            run_corpus<int64_t>("a", {
                "{\"a\" : {\"$numberLong\" : \"9223372036854775807\"}}",
                [](auto val) { CHECK(val == LLONG_MAX); }
            });
        }
        SECTION("-1") {
            run_corpus<int64_t>("a", {
                "{\"a\" : {\"$numberLong\" : \"-1\"}}",
                [](auto val) { CHECK(val == -1); }
            });
        }
        SECTION("0") {
            run_corpus<int64_t>("a", {
                "{\"a\" : {\"$numberLong\" : \"0\"}}",
                [](auto val) { CHECK(val == 0); }
            });
        }
        SECTION("1") {
            run_corpus<int64_t>("a", {
                "{\"a\" : {\"$numberLong\" : \"1\"}}",
                [](auto val) { CHECK(val == 1); }
            });
        }
    }

    SECTION("Maxkey type") {
        run_corpus<MaxKey>("a", {
                "{\"a\" : {\"$maxKey\" : 1}}",
                [](auto val) { CHECK(val == max_key); }
        });
    }

    SECTION("Minkey type") {
        run_corpus<MinKey>("a", {
                "{\"a\" : {\"$minKey\" : 1}}",
                [](auto val) { CHECK(val == min_key); }
        });
    }

    SECTION("Multiple types within the same documment") {
        auto canonical_extjson = remove_whitespace("{\"_id\": {\"$oid\": \"57e193d7a9cc81b4027498b5\"}, \"String\": \"string\", \"Int32\": {\"$numberInt\": \"42\"}, \"Int64\": {\"$numberLong\": \"42\"}, \"Double\": {\"$numberDouble\": \"-1\"}, \"Binary\": { \"$binary\" : {\"base64\": \"o0w498Or7cijeBSpkquNtg==\", \"subType\": \"00\"}}, \"BinaryUserDefined\": { \"$binary\" : {\"base64\": \"AQIDBAU=\", \"subType\": \"00\"}}, \"Subdocument\": {\"foo\": \"bar\"}, \"Array\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}, {\"$numberInt\": \"4\"}, {\"$numberInt\": \"5\"}], \"Timestamp\": {\"$timestamp\": {\"t\": 42, \"i\": 1}}, \"Regex\": {\"$regularExpression\": {\"pattern\": \"pattern\", \"options\": \"\"}}, \"DatetimeEpoch\": {\"$date\": {\"$numberLong\": \"0\"}}, \"DatetimePositive\": {\"$date\": {\"$numberLong\": \"2147483647\"}}, \"DatetimeNegative\": {\"$date\": {\"$numberLong\": \"-2147483648\"}}, \"True\": true, \"False\": false, \"Minkey\": {\"$minKey\": 1}, \"Maxkey\": {\"$maxKey\": 1}, \"Null\": null}");

        std::string binary = "o0w498Or7cijeBSpkquNtg==";
        std::string binary_user_defined = "AQIDBAU=";

        const BsonDocument document = {
            { "_id", ObjectId("57e193d7a9cc81b4027498b5") },
            { "String", std::string("string") },
            { "Int32", 42 },
            { "Int64", int64_t(42) },
            { "Double", -1.0 },
            { "Binary", std::vector<char>(binary.begin(), binary.end()) },
            { "BinaryUserDefined", std::vector<char>(binary_user_defined.begin(), binary_user_defined.end()) },
            { "Subdocument", BsonDocument {
                {"foo", std::string("bar") }
            }},
            { "Array", BsonArray {1, 2, 3, 4, 5} },
            { "Timestamp", MongoTimestamp(42, 1) },
            { "Regex", RegularExpression("pattern", "") },
            { "DatetimeEpoch", realm::Timestamp(0, 0) },
            { "DatetimePositive", realm::Timestamp(INT_MAX/1000, 647000000) },
            { "DatetimeNegative", realm::Timestamp(INT_MIN/1000, -648000000) },
            { "True", true },
            { "False", false },
            { "Minkey", min_key },
            { "Maxkey", max_key },
            { "Null", util::none }
        };

        CHECK((static_cast<BsonDocument>(bson::parse(canonical_extjson)) == document));
        std::stringstream s;
        s << Bson(document);
        CHECK(canonical_extjson == s.str());
    }

    SECTION("Null type") {
        run_corpus<realm::util::None>("a", {
            "{\"a\" : null}",
            [](auto) { CHECK(true); }
        });
    }

    SECTION("ObjectId") {
        SECTION("All zeroes") {
            run_corpus<ObjectId>("a", {
                "{\"a\" : {\"$oid\" : \"000000000000000000000000\"}}",
                [](auto val) { CHECK(val == ObjectId("000000000000000000000000")); }
            });
        }
        SECTION("All ones") {
            run_corpus<ObjectId>("a", {
                "{\"a\" : {\"$oid\" : \"ffffffffffffffffffffffff\"}}",
                [](auto val) { CHECK(val == ObjectId("ffffffffffffffffffffffff")); }
            });
        }
        SECTION("Random") {
            run_corpus<ObjectId>("a", {
                "{\"a\" : {\"$oid\" : \"56e1fc72e0c917e9c4714161\"}}",
                [](auto val) { CHECK(val == ObjectId("56e1fc72e0c917e9c4714161")); }
            });
        }
    }

    SECTION("Regular Expression type") {
        SECTION("empty regex with no options") {
            run_corpus<RegularExpression>("a", {
                "{\"a\" : {\"$regularExpression\" : { \"pattern\": \"\", \"options\" : \"\"}}}",
                [](auto val) { CHECK(val == RegularExpression()); }
            });
        }
        SECTION("regex without options") {
            run_corpus<RegularExpression>("a", {
                "{\"a\" : {\"$regularExpression\" : { \"pattern\": \"abc\", \"options\" : \"\"}}}",
                [](auto val) { CHECK(val == RegularExpression("abc", "")); }
            });
        }
        SECTION("regex with options") {
            run_corpus<RegularExpression>("a", {
                "{\"a\" : {\"$regularExpression\" : { \"pattern\": \"abc\", \"options\" : \"im\"}}}",
                [](auto val) {
                    CHECK(val.pattern() == "abc");
                    CHECK(((val.options() & RegularExpression::Option::IgnoreCase) != RegularExpression::Option::None));
                    CHECK(((val.options() & RegularExpression::Option::Multiline) != RegularExpression::Option::None));
                }
            });
        }
        SECTION("regex with options (keys reversed)") {
            run_corpus<RegularExpression>("a", {
                "{\"a\" : {\"$regularExpression\" : {\"options\" : \"im\", \"pattern\": \"abc\"}}}",
                [](auto val) {
                    CHECK(val.pattern() == "abc");
                    CHECK(((val.options() & RegularExpression::Option::IgnoreCase) != RegularExpression::Option::None));
                    CHECK(((val.options() & RegularExpression::Option::Multiline) != RegularExpression::Option::None));
                },
                true
            });
        }
        SECTION("regex with slash") {
            run_corpus<RegularExpression>("a", {
                "{\"a\" : {\"$regularExpression\" : { \"pattern\": \"ab/cd\", \"options\" : \"im\"}}}",
                [](auto val) {
                    CHECK(val.pattern() == "ab/cd");
                    CHECK(((val.options() & RegularExpression::Option::IgnoreCase) != RegularExpression::Option::None));
                    CHECK(((val.options() & RegularExpression::Option::Multiline) != RegularExpression::Option::None));
                }
            });
        }
        SECTION("flags not alphabetized") {
            run_corpus<RegularExpression>("a", {
                "{\"a\" : {\"$regularExpression\" : { \"pattern\": \"abc\", \"options\" : \"mix\"}}}",
                [](auto val) {
                    CHECK(val.pattern() == "abc");
                    CHECK(((val.options() & RegularExpression::Option::IgnoreCase) != RegularExpression::Option::None));
                    CHECK(((val.options() & RegularExpression::Option::Multiline) != RegularExpression::Option::None));
                    CHECK(((val.options() & RegularExpression::Option::Extended) != RegularExpression::Option::None));
                },
                true
            });
        }
        SECTION("Regular expression as value of $regex query operator") {
            run_corpus<RegularExpression>("$regex", {
                "{\"$regex\" : {\"$regularExpression\" : { \"pattern\": \"pattern\", \"options\" : \"ix\"}}}",
                [](auto val) {
                    CHECK(val.pattern() == "pattern");
                    CHECK(((val.options() & RegularExpression::Option::IgnoreCase) != RegularExpression::Option::None));
                    CHECK(((val.options() & RegularExpression::Option::Extended) != RegularExpression::Option::None));
                }
            });
        }
    }

    SECTION("String") {
        SECTION("Empty string") {
            run_corpus<std::string>("a", {
                "{\"a\" : \"\"}",
                [](auto val) { CHECK(val.empty()); }
            });
        }
        SECTION("Single character") {
            run_corpus<std::string>("a", {
                "{\"a\" : \"b\"}",
                [](auto val) { CHECK(val == "b"); }
            });
        }
        SECTION("Multi-character") {
            run_corpus<std::string>("a", {
                "{\"a\" : \"abababababab\"}",
                [](auto val) { CHECK(val == "abababababab"); }
            });
        }
        SECTION("Special characters in string") {
            run_corpus<std::string>("x", {
                R"({"x" : ">\n\t\u0000\\\"<"})",
                [](auto val) { CHECK(val == ">\n\t\x00\\\"<"sv); }
            });
        }
    }
    
    SECTION("Timestamp") {
        SECTION("Timestamp: (123456789, 42)") {
            run_corpus<MongoTimestamp>("a", {
                "{\"a\" : {\"$timestamp\" : {\"t\" : 123456789, \"i\" : 42} } }",
                [](auto val) {
                    CHECK(val.seconds == 123456789);
                    CHECK(val.increment == 42);
                },
                true
            });
        }
        SECTION("Timestamp: (123456789, 42) (keys reversed)") {
            run_corpus<MongoTimestamp>("a", {
                "{\"a\" : {\"$timestamp\" : {\"i\" : 42, \"t\" : 123456789} } }",
                [](auto val) {
                    CHECK(val.seconds == 123456789);
                    CHECK(val.increment == 42);
                },
                true
            });
        }
        SECTION("Timestamp with high-order bit set on both seconds and increment") {
            run_corpus<MongoTimestamp>("a", {
                "{\"a\" : {\"$timestamp\" : {\"t\" : 4294967295, \"i\" :  4294967295} } }",
                [](auto val) {
                    CHECK(val.seconds == 4294967295);
                    CHECK(val.increment == 4294967295);
                },
                true
            });
        }
    }
    
}

TEST_CASE("nested types parsing", "[bson]") {
    
    SECTION("Nested types") {
        auto d1 = bson::BsonDocument({{"aNest", bson::BsonArray({1, 2, 3})}, {"anotherKey", "hey"}});
        auto d2 = bson::BsonDocument({{"numberArray", bson::BsonArray({1, 2, 3})}});
        auto d3 = bson::BsonDocument({{"nestOfNested", bson::BsonDocument({{"firstNest", bson::BsonDocument({{"secondNest", "hey"}})}})}});
        auto d4 = bson::BsonDocument({{"stringArray", bson::BsonArray({"one", "two", "three"})}});
        bson::BsonArray nested_document_array {
            bson::BsonDocument({{"layerOne", d1}}),
            bson::BsonDocument({{"layerTwo", d2}}),
            bson::BsonDocument({{"layerThree", d3}}),
            bson::BsonDocument({{"layerFour", d4}})
        };
        
        bson::BsonDocument nested_document1 {
            {"name", d1},
            {"breed", d2}
        };
        
        bson::BsonDocument nested_document2 {
            {"name", d3},
            {"breed", d4}
        };
        
        std::stringstream nested_array_str;
        nested_array_str << bson::Bson(nested_document_array);
        
        std::stringstream nested_document_str;
        nested_document_str << bson::Bson(nested_document1);
        
        std::stringstream nested_document2_str;
        nested_document2_str << bson::Bson(nested_document2);
        
        auto nested_document_expectation = "[{\"layerOne\":{\"aNest\":[{\"$numberInt\":\"1\"},{\"$numberInt\":\"2\"},{\"$numberInt\":\"3\"}],\"anotherKey\":\"hey\"}},{\"layerTwo\":{\"numberArray\":[{\"$numberInt\":\"1\"},{\"$numberInt\":\"2\"},{\"$numberInt\":\"3\"}]}},{\"layerThree\":{\"nestOfNested\":{\"firstNest\":{\"secondNest\":\"hey\"}}}},{\"layerFour\":{\"stringArray\":[\"one\",\"two\",\"three\"]}}]";
        
        auto nested_document1_expectation = "{\"name\":{\"aNest\":[{\"$numberInt\":\"1\"},{\"$numberInt\":\"2\"},{\"$numberInt\":\"3\"}],\"anotherKey\":\"hey\"},\"breed\":{\"numberArray\":[{\"$numberInt\":\"1\"},{\"$numberInt\":\"2\"},{\"$numberInt\":\"3\"}]}}";
        
        auto nested_document2_expectation = "{\"name\":{\"nestOfNested\":{\"firstNest\":{\"secondNest\":\"hey\"}}},\"breed\":{\"stringArray\":[\"one\",\"two\",\"three\"]}}";
        
        CHECK(nested_array_str.str() == nested_document_expectation);
        CHECK(nested_document_str.str() == nested_document1_expectation);
        CHECK(nested_document2_str.str() == nested_document2_expectation);
    }
    
}
