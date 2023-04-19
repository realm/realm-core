#include <locale>

#include "test.hpp"

#include <realm/util/buffer_stream.hpp>

using namespace realm;
using namespace realm::util;

TEST(Util_BufferStream_Basics)
{
    ResettableExpandableBufferOutputStream out;
    out.imbue(std::locale::classic());
    CHECK_EQUAL("", std::string(out.data(), out.size()));
    out << 23456;
    CHECK_EQUAL("23456", std::string(out.data(), out.size()));
    out << "Grassmann";
    CHECK_EQUAL("23456Grassmann", std::string(out.data(), out.size()));
    out.reset();
    CHECK_EQUAL("", std::string(out.data(), out.size()));
    out << "Minkowski";
    CHECK_EQUAL("Minkowski", std::string(out.data(), out.size()));
    out << 24680;
    CHECK_EQUAL("Minkowski24680", std::string(out.data(), out.size()));
    out.reset();
    CHECK_EQUAL("", std::string(out.data(), out.size()));
}
